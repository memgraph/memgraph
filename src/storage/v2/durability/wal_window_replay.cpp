// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/durability/wal_window_replay.hpp"

#include <memory>
#include <optional>
#include <variant>

#include "storage/v2/commit_args.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/exceptions.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/durability/wal_schema_delta_apply.hpp"
#include "storage/v2/inmemory/storage.hpp"

namespace memgraph::storage::durability {

namespace {

// The eight pure DATA-plane WalDeltaData alternatives -- exactly the set storage::ApplyWalDataDelta
// (wal_delta_apply.hpp, S3b) knows how to apply. Every other non-framing alternative is a
// SCHEMA-plane op (index/constraint/enum/TTL/description-store) and is dispatched to
// storage::ApplyWalSchemaDelta (wal_schema_delta_apply.hpp) instead -- see
// ResolveWalTransactionAccessType below for how each kind maps to a StorageAccessType.
bool IsDataPlaneWalDelta(WalDeltaData const &delta) {
  return std::holds_alternative<WalVertexCreate>(delta.data_) || std::holds_alternative<WalVertexDelete>(delta.data_) ||
         std::holds_alternative<WalVertexAddLabel>(delta.data_) ||
         std::holds_alternative<WalVertexRemoveLabel>(delta.data_) ||
         std::holds_alternative<WalVertexSetProperty>(delta.data_) ||
         std::holds_alternative<WalEdgeCreate>(delta.data_) || std::holds_alternative<WalEdgeDelete>(delta.data_) ||
         std::holds_alternative<WalEdgeSetProperty>(delta.data_);
}

// Translates the durability-layer TransactionAccessType (as captured off a WalTransactionStart
// delta) into the storage-layer StorageAccessType the recovery-replay accessor needs. Mirrors
// InMemoryReplicationHandlers::ReadAndApplyDeltasSingleTxn's own `translate_access_type` lambda
// (dbms/inmemory/replication_handlers.cpp) byte-for-byte -- same enum, same 1:1 mapping.
StorageAccessType TranslateAccessType(TransactionAccessType access_type) {
  switch (access_type) {
    case TransactionAccessType::UNIQUE:
      return StorageAccessType::UNIQUE;
    case TransactionAccessType::WRITE:
      return StorageAccessType::WRITE;
    case TransactionAccessType::READ:
      return StorageAccessType::READ;
    case TransactionAccessType::READ_ONLY:
      return StorageAccessType::READ_ONLY;
    default:
      throw RecoveryFailure("Graph Versioning v1: unrecognized WAL transaction access type during window replay");
  }
}

// Legacy-WAL (pre-kTtlSupport) fallback hint, used only when a transaction's WalTransactionStart
// didn't carry an explicit `access_type` (nullopt). Mirrors ReadAndApplyDeltasSingleTxn's per-arm
// `acc_hint` argument to `get_replication_accessor`: every schema-plane kind hints UNIQUE except
// the four IndexStats Set/Clear pairs, which -- like every data-plane kind -- hint the default
// WRITE (`kSharedAccess` there). A single WAL transaction is data XOR schema (DDL can't share a
// transaction with DML, interpreter.cpp's IndexInMulticommandTxException), so resolving off
// whichever in-scope delta is seen first is always correct for the whole transaction.
StorageAccessType HintForWalDelta(WalDeltaData const &delta) {
  if (IsDataPlaneWalDelta(delta)) {
    return StorageAccessType::WRITE;
  }
  bool const is_index_stats_kind = std::holds_alternative<WalLabelIndexStatsSet>(delta.data_) ||
                                   std::holds_alternative<WalLabelIndexStatsClear>(delta.data_) ||
                                   std::holds_alternative<WalLabelPropertyIndexStatsSet>(delta.data_) ||
                                   std::holds_alternative<WalLabelPropertyIndexStatsClear>(delta.data_);
  return is_index_stats_kind ? StorageAccessType::WRITE : StorageAccessType::UNIQUE;
}

// Resolves the StorageAccessType a recovery-replay accessor must open with for the transaction
// `first_in_scope_delta` belongs to: the real recorded access_type (captured off
// WalTransactionStart) wins when present, exactly like `access_type.value_or(acc_hint)` in
// ReadAndApplyDeltasSingleTxn; otherwise fall back to the per-kind hint above.
StorageAccessType ResolveWalTransactionAccessType(std::optional<TransactionAccessType> const &access_type_captured,
                                                  WalDeltaData const &first_in_scope_delta) {
  return access_type_captured ? TranslateAccessType(*access_type_captured) : HintForWalDelta(first_in_scope_delta);
}

struct WindowTxnOutcome {
  uint64_t deltas_consumed{0};
  // The original commit timestamp, if this transaction was above `floor_ts` and got replayed.
  std::optional<uint64_t> committed_ts;
};

// Replays exactly one WAL transaction -- from its WalTransactionStart through its
// WalTransactionEnd, inclusive -- starting at `decoder`'s current position. Mirrors the per-txn
// framing shape of InMemoryReplicationHandlers::ReadAndApplyDeltasSingleTxn
// (dbms/inmemory/replication_handlers.cpp): transaction start/end detection via
// IsWalDeltaDataTransactionEnd, lazy accessor creation on the first in-scope delta (data-plane OR
// schema-plane -- BUG-1 fix), commit exactly on WalTransactionEnd -- but supplies recovery-replay's
// OWN framing (a CreateRecoveryReplayAccessor + CommitArgs::make_recovery_replay, S3a) instead of
// replica-apply's (storage->Access + CommitArgs::make_replica_write).
WindowTxnOutcome ReplayOneWindowTransaction(InMemoryStorage *storage, BaseDecoder *decoder, uint64_t version,
                                            uint64_t floor_ts, EdgeSetPropertyCache &edge_set_property_cache,
                                            FindEdgeFallback const &find_edge_fallback) {
  std::unique_ptr<Storage::Accessor> accessor;
  uint64_t commit_ts = 0;
  bool should_commit = true;
  std::optional<uint64_t> replayed_ts;
  uint64_t delta_idx = 0;
  // Captured off WalTransactionStart, below -- the ORIGINAL access type this transaction used on
  // main (present for WAL files at/after kTtlSupport; nullopt for older ones). Resolved into a
  // concrete StorageAccessType, on the first in-scope delta, by ResolveWalTransactionAccessType.
  std::optional<TransactionAccessType> access_type_captured;

  for (bool transaction_complete = false; !transaction_complete; ++delta_idx) {
    auto const delta_timestamp = ReadWalDeltaHeader(decoder);
    auto delta = ReadWalDeltaData(decoder, version);
    transaction_complete = IsWalDeltaDataTransactionEnd(delta, version);

    if (auto const *txn_start = std::get_if<WalTransactionStart>(&delta.data_)) {
      // loading_wal-equivalent semantics (this is our OWN local, already-durable WAL, not a
      // replica RPC stream) -- WalTransactionStart is the sole authority on whether this
      // transaction's deltas were ever actually committed on main (mirrors LoadWal/
      // ReadAndApplyDeltasSingleTxn's `loading_wal` handling of `data.commit`).
      should_commit = txn_start->commit.value_or(true);
      // BUG-1 fix: capture the transaction's real access type too -- needed to open the
      // recovery-replay accessor with the correct StorageAccessType (UNIQUE for DDL/schema
      // replay) once the first in-scope body delta is seen, below.
      access_type_captured = txn_start->access_type;
      continue;
    }

    if (std::holds_alternative<WalTransactionEnd>(delta.data_)) {
      if (should_commit && accessor) {
        // FIX B/C (S3a): drive timestamp_ to the exact original commit ts immediately before
        // calling PrepareForCommitPhase, so the unmodified GetCommitTimestamp() (timestamp_++)
        // naturally lands on it; CommitArgs::make_recovery_replay then forces the WAL-suppress
        // early-return while still running FinalizeCommitPhase's delta-visibility + commit_log_/
        // LDT bookkeeping.
        storage->timestamp_ = commit_ts;
        auto const commit_result = accessor->PrepareForCommitPhase(CommitArgs::make_recovery_replay(commit_ts));
        if (!commit_result) {
          throw RecoveryFailure(
              "Graph Versioning v1: failed to replay a branch-durability WAL window transaction at original "
              "commit timestamp {}",
              commit_ts);
        }
        replayed_ts = commit_ts;
      } else if (accessor) {
        // Uncommitted / 2PC-prepared-only transaction (should_commit == false) that happened to
        // touch data above the floor: it was never durably committed on main, so it must not be
        // replayed. Abort discards its deltas cleanly -- mirrors LoadWal/
        // ReadAndApplyDeltasSingleTxn's `if (loading_wal && !should_commit) continue;` gate.
        accessor->Abort();
      }
      continue;
    }

    if (!should_commit || delta_timestamp < floor_ts) {
      // Either part of an uncommitted/2PC-prepared txn, or already materialized by the base-to-F
      // pass (RecoverData's ceiling'd LoadWal, S3c-i). BUG-2 fix: the floor is now STRICT (`<`, not
      // `<=`) -- base-to-F is EXCLUSIVE (materializes only ts < floor_ts), so the transaction
      // committing at EXACTLY floor_ts is no longer baked flat into base and must be applied HERE as
      // a real MVCC delta (its own commit_ts == floor_ts == fork_ts, which CreateHistoricalTransaction's
      // strict `ts < start_timestamp` visibility, mvcc.hpp, then correctly hides from the fork).
      continue;
    }

    if (!accessor) {
      // FIX B (S3a): start_ts = Ci - 1 (the immediately preceding original commit ts) -- sees
      // base@floor_ts plus every prior window commit, never anything newer. timestamp_ is not
      // touched by opening this accessor.
      //
      // BUG-2 fix (underflow guard): the window can now receive delta_timestamp == floor_ts == 0
      // (RegisterForkPin captures fork_ts = timestamp_ WITHOUT incrementing, and timestamp_ starts
      // at kTimestampInitialId == 0, so a branch forked before the very first commit has
      // fork_ts == 0, and that first commit can itself land at ts == 0) -- `delta_timestamp - 1`
      // would underflow to UINT64_MAX. Clamp to 0: start_timestamp == 0 with strict `<` visibility
      // correctly sees nothing prior, which is right for the first-ever commit.
      //
      // BUG-1 fix: open with the transaction's RESOLVED access type (UNIQUE for DDL/schema replay,
      // WRITE/READ/READ_ONLY for data-plane replay) instead of a hardcoded WRITE -- see
      // ResolveWalTransactionAccessType above. A single WAL transaction is data XOR schema, so this
      // one resolution (made off whichever delta is first in scope) is correct for every remaining
      // delta in the transaction.
      accessor = storage->CreateRecoveryReplayAccessor(delta_timestamp == 0 ? 0 : delta_timestamp - 1,
                                                       ResolveWalTransactionAccessType(access_type_captured, delta));
      commit_ts = delta_timestamp;
    }
    auto *replication_view = static_cast<ReplicationAccessor *>(accessor.get());
    if (IsDataPlaneWalDelta(delta)) {
      ApplyWalDataDelta(
          replication_view, storage, delta, delta_timestamp, delta_idx, edge_set_property_cache, find_edge_fallback);
    } else {
      // BUG-1 fix: schema/index/constraint/enum/TTL/description-store WAL ops committed strictly
      // inside a branch's replay window are now replayed too, via the shared applier -- previously
      // these were warn-and-skipped, silently regressing main's own schema on restart whenever a
      // branch existed (see wal_window_replay.hpp's doc-comment, updated to reflect this fix).
      ApplyWalSchemaDelta(replication_view, storage, delta, delta_idx);
    }
  }

  return {.deltas_consumed = delta_idx, .committed_ts = replayed_ts};
}

}  // namespace

WindowReplayResult ReplayWalWindow(InMemoryStorage *storage, std::filesystem::path const &wal_directory,
                                   std::string const &uuid, uint64_t floor_ts,
                                   FindEdgeFallback const &find_edge_fallback) {
  WindowReplayResult result{.last_replayed_commit_ts = floor_ts, .transactions_replayed = 0};

  auto const maybe_wal_files = GetWalFiles(wal_directory, uuid);
  if (!maybe_wal_files || maybe_wal_files->empty()) {
    return result;
  }

  // Shared across the whole window (not just one file/transaction) -- mirrors
  // ReadAndApplyDeltasSingleTxn's cache, which is (re)created per single-transaction call there;
  // here a single EdgeSetPropertyCache spanning ALL replayed transactions is still correct because
  // its key includes delta_timestamp (== each transaction's own distinct commit ts), so entries
  // from different transactions never collide.
  EdgeSetPropertyCache edge_set_property_cache;

  for (auto const &wal_file : *maybe_wal_files) {
    if (wal_file.to_timestamp < floor_ts) {
      // Entirely covered by the base-to-F pass (RecoverData's ceiling'd LoadWal) -- nothing in
      // this file is in scope for the window. Mirrors LoadWal's own (now-exclusive) ceiling skip
      // (wal.cpp). BUG-2 fix: strict `<`, not `<=` -- a file whose LAST delta is exactly floor_ts
      // still holds the ts == floor_ts transaction that base-to-F now excludes and this window
      // must apply; `<=` here would silently drop it (a gap, never touched by either pass).
      continue;
    }

    Decoder decoder;
    auto const version = decoder.Initialize(wal_file.path, kWalMagic);
    if (!version) {
      throw RecoveryFailure("Graph Versioning v1: couldn't read WAL magic/version replaying {}", wal_file.path);
    }
    if (!IsVersionSupported(*version)) {
      throw RecoveryFailure("Graph Versioning v1: unsupported WAL version replaying {}", wal_file.path);
    }

    // CRC verification already happened once, at ReadWalInfo/GetWalFiles time when this WAL was
    // first durably written and validated -- same reasoning LoadWal's own header comment gives for
    // not re-checking it here ("CRC verification is done in ReadWalInfo and is not needed later
    // on"). This is our own already-durable local WAL, not an untrusted replica RPC stream.
    auto const info = ReadWalInfo(wal_file.path);
    decoder.SetPosition(info.offset_deltas);

    for (uint64_t processed = 0; processed < info.num_deltas;) {
      auto const outcome = ReplayOneWindowTransaction(
          storage, &decoder, *version, floor_ts, edge_set_property_cache, find_edge_fallback);
      processed += outcome.deltas_consumed;
      if (outcome.committed_ts) {
        result.last_replayed_commit_ts = *outcome.committed_ts;
        ++result.transactions_replayed;
      }
    }
  }

  return result;
}

}  // namespace memgraph::storage::durability
