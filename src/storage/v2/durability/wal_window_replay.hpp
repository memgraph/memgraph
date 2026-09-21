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

#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

#include "storage/v2/durability/wal_delta_apply.hpp"

namespace memgraph::storage {
class InMemoryStorage;
}  // namespace memgraph::storage

// Graph Versioning v1 branch durability, slice S3d (see opencode-work/versioning-v1/
// 2026-07-13--durability-S2S3-design-v4.html §4). This is the NEW per-transaction window-replay
// reader: unlike storage::durability::LoadWal (monolithic, direct-SkipList-mutate, no per-txn
// seam -- used for the "base-to-F" pass only, S3c-i), this reader rebuilds the (F, now] portion of
// main's retained WAL as REAL MVCC transactions, via InMemoryStorage::CreateRecoveryReplayAccessor
// (S3a, FIX B) + storage::ApplyWalDataDelta (S3b) / storage::ApplyWalSchemaDelta (BUG-1 fix) +
// CommitArgs::make_recovery_replay (S3a, FIX C). Structurally mirrors
// InMemoryReplicationHandlers::ReadAndApplyDeltasSingleTxn's per-transaction framing shape
// (dbms/inmemory/replication_handlers.cpp) -- transaction-start/end detection, lazy accessor
// creation on the first in-scope delta, the accessor opened with the transaction's own resolved
// StorageAccessType (UNIQUE for DDL/schema replay, WRITE/READ/READ_ONLY for data-plane replay) --
// but supplies its own framing (forced original commit timestamp + WAL-suppressed commit) in place
// of replica-apply's.
namespace memgraph::storage::durability {

struct WindowReplayResult {
  // The highest original commit timestamp actually replayed. Equals `floor_ts` unchanged if the
  // window was empty (nothing in the retained WAL had a committed data-plane delta above the
  // floor) -- the caller (InMemoryStorage's ctor) can compare against its own `floor_ts` to detect
  // that case without a separate optional.
  uint64_t last_replayed_commit_ts{0};
  uint64_t transactions_replayed{0};
};

// Replays every committed transaction in the retained WAL directory `wal_directory` (filtered to
// `uuid`) whose original commit timestamp is STRICTLY greater than `floor_ts`, rebuilding the MVCC
// delta chain in that ORIGINAL timestamp frame. Drives `storage`'s `timestamp_` forward as it goes
// (one tick past each replayed commit, mirroring `CommitArgs::make_recovery_replay`'s contract) --
// on return, `storage->timestamp_ == result.last_replayed_commit_ts + 1` if anything was replayed,
// otherwise untouched.
//
// Does NOT seed `commit_log_` or any fork pin -- purely a delta-chain-rebuilding primitive; ctor
// sequencing (pin seeding before this call, the blanket `MarkFinishedInRange` seed after) is the
// caller's responsibility (InMemoryStorage's ctor, S3d).
//
// `find_edge_fallback` resolves the two WalEdgeSetProperty legacy-WAL-format cases that need
// InMemoryStorage's PRIVATE FindEdge(Gid)/FindEdge(Gid, Gid) overloads -- supplied by the caller
// (InMemoryStorage's own ctor, which has the necessary access as a member function) exactly as
// InMemoryReplicationHandlers does for the replica-apply path. See FindEdgeFallback's doc-comment
// (wal_delta_apply.hpp) for the exact contract.
//
// SCHEMA-COMPLETE (BUG-1 fix): both the eight DATA-plane WAL delta kinds (vertex/edge
// create/delete/set-property/add-remove-label, via storage::ApplyWalDataDelta) AND every
// SCHEMA-plane kind (index/constraint/enum/TTL/description-store, via storage::ApplyWalSchemaDelta)
// committed by main strictly inside (floor_ts, now] are replayed. A schema op is replayed through a
// recovery-replay accessor opened with StorageAccessType::UNIQUE (resolved from the transaction's
// own recorded access type, or a per-kind hint for pre-kTtlSupport WAL files that never recorded
// one) so it satisfies the same `MG_ASSERT(type() == UNIQUE || ...)` gates a live DDL transaction
// would. This closes the previous v1 limitation where main's own index/constraint/enum/TTL/
// description-store changes made after a branch's fork point silently regressed on restart
// whenever a branch existed.
//
// @throw storage::durability::RecoveryFailure on any WAL corruption or a replay commit that fails
// (mirrors LoadWal's own @throw contract) -- the caller (InMemoryStorage's ctor) is expected to let
// this propagate exactly like an ordinary RecoveryFailure from the base-to-F pass.
WindowReplayResult ReplayWalWindow(InMemoryStorage *storage, std::filesystem::path const &wal_directory,
                                   std::string const &uuid, uint64_t floor_ts,
                                   FindEdgeFallback const &find_edge_fallback);

}  // namespace memgraph::storage::durability
