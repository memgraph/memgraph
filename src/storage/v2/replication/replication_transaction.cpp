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

#include "storage/v2/replication/replication_transaction.hpp"

#include "memory/db_arena_fwd.hpp"
#include "storage/v2/commit_args.hpp"
#include "storage/v2/storage.hpp"
#include "utils/atomic_utils.hpp"
#include "utils/variant_helpers.hpp"

#include <algorithm>
#include <exception>
#include <string>
#include <utility>

namespace memgraph::storage {

namespace {
auto ReplicationModeToString(replication_coordination_glue::ReplicationMode mode) -> std::string {
  switch (mode) {
    case replication_coordination_glue::ReplicationMode::SYNC:
      return "SYNC";
    case replication_coordination_glue::ReplicationMode::ASYNC:
      return "ASYNC";
    case replication_coordination_glue::ReplicationMode::STRICT_SYNC:
      return "STRICT_SYNC";
  }
  return "UNKNOWN";
}

auto StartTxnErrorToReason(StartTxnReplicationError const &error) -> ReplicaFailureReason {
  return std::visit(utils::Overloaded{
                        [](FailedToConnectErr const &) { return ReplicaFailureReason::NOT_IN_SYNC; },
                        [](ReplicaNotInSyncErr const &) { return ReplicaFailureReason::NOT_IN_SYNC; },
                        [](FailedToGetAsyncRpcLock const &) { return ReplicaFailureReason::FAILED_TO_GET_LOCK; },
                        [](GenericRpcError const &) { return ReplicaFailureReason::RPC_ERROR; },
                        [](ReplicaDivergedErr const &) { return ReplicaFailureReason::DIVERGED; },
                    },
                    error);
}
}  // namespace

// For all replicas, we append transaction end
// When handling STRICT_SYNC replica, we send deltas as part of the 1st phase of the 2PC protocol and wait for the
// response.
// When handling some other type of replica, it is checked whether there is another STRICT_SYNC replica. There are 2
// possible cluster combinations: STRICT_SYNC and ASYNC or SYNC and ASYNC. If there are no STRICT_SYNC replicas in the
// cluster, we send all deltas and commit immediately on replicas.
auto TransactionReplication::ShipDeltas(uint64_t durability_commit_timestamp, CommitArgs const &commit_args) -> bool {
  if (locked_clients->empty()) return true;

  MG_ASSERT(commit_args.replication_allowed(),
            "Any clients assumes we are MAIN, we should have gatekeeper_access_wrapper so we can correctly "
            "handle ASYNC tasks");

  auto const &db_acc = commit_args.database_protector();

  auto const record_failure = [&](ReplicationStorageClient const *client, ShipResult const &finalized) {
    if (finalized.has_value()) {
      return;
    }
    auto const client_name = std::string{client->Name()};
    // StartTransactionReplication may have already recorded a failure for this replica;
    // avoid reporting the same instance twice with a follow-up reason derived from the empty stream.
    auto const already_failed =
        std::ranges::any_of(replication_failures_, [&](ReplicaFailure const &f) { return f.name == client_name; });
    if (!already_failed) {
      auto const reason = [&] {
        switch (finalized.error()) {
          case io::network::ClientCommunicationError::TIMEOUT_ERROR:
            return ReplicaFailureReason::TIMEOUT;
          case io::network::ClientCommunicationError::SOCKET_FAILED_TO_CONNECT:
            return ReplicaFailureReason::NOT_IN_SYNC;
          default:
            return ReplicaFailureReason::RPC_ERROR;
        }
      }();
      replication_failures_.push_back(
          {.name = client_name, .mode = ReplicationModeToString(client->Mode()), .reason = reason});
    }
  };

  // A streamless replica got no fused task; its bookkeeping is quick and RPC-free, so run it inline.
  // Queueing it would make the commit thread await this replica's worker, whose queue may hold a task
  // that blocks on the engine_lock_ this thread holds — a deadlock.
  for (auto &&[client, replica_stream] : ranges::views::zip(*locked_clients, streams)) {
    if (replica_stream) {
      continue;
    }
    auto *raw_client = client.get();
    if (raw_client->Mode() == replication_coordination_glue::ReplicationMode::ASYNC) {
      // Even if it fails, we don't care, it's ASYNC
      // NOLINTNEXTLINE(bugprone-unused-return-value)
      ShipOne(raw_client, replica_stream, durability_commit_timestamp, db_acc);
      continue;
    }
    record_failure(raw_client, ShipOne(raw_client, replica_stream, durability_commit_timestamp, db_acc));
  }

  // Collect every fused task before rethrowing anything: an abandoned task keeps running against this
  // frame's db_acc, streams and this. record_failure allocates, so it stays inside the try: a
  // bad_alloc from it must not exit the loop before the drain finished.
  std::exception_ptr first_error;
  for (auto &[client, ship_result] : ship_futures_) {
    try {
      auto const finalized = ship_result.get();
      // Even if it failed, we don't care, it's ASYNC
      if (client->Mode() != replication_coordination_glue::ReplicationMode::ASYNC) {
        record_failure(client, finalized);
      }
    } catch (...) {
      if (!first_error) {
        first_error = std::current_exception();
      }
    }
  }
  ship_futures_.clear();
  if (first_error) {
    std::rethrow_exception(first_error);
  }
  return replication_failures_.empty();
}

auto TransactionReplication::ShipOne(ReplicationStorageClient *raw_client, std::optional<ReplicaStream> &replica_stream,
                                     uint64_t const durability_commit_timestamp, DatabaseProtector const &db_acc) const
    -> ShipResult {
  raw_client->IfStreamingTransaction([&](auto &stream) { stream.AppendTransactionEnd(durability_commit_timestamp); },
                                     replica_stream);
  // If I am STRICT SYNC replica, ship deltas as part of the 1st phase and preserve replica stream.
  if (raw_client->Mode() == replication_coordination_glue::ReplicationMode::STRICT_SYNC) {
    return raw_client->FinalizePrepareCommitPhase(replica_stream, durability_commit_timestamp);
  }
  // If there are no STRICT_SYNC replicas, shipping deltas means finalizing the transaction
  // RPC stream gets destroyed => RPC lock released.
  if (!ShouldRunTwoPC()) {
    return raw_client->FinalizeTransactionReplication(
        db_acc, std::move(replica_stream), durability_commit_timestamp, commit_num_committed_txns_);
  }
  // SYNC replica cannot be part of 2PC; an ASYNC replica in 2PC finalizes with the 2nd-phase decision.
  return {};
}

// RPC locks will get released at the end of this function for all STRICT_SYNC and ASYNC replicas
// We shouldn't execute this code for SYNC replicas, this is only executed if these replicas are part of STRICT_SYNC
// cluster
auto TransactionReplication::FinalizeTransaction(bool const decision, utils::UUID const &storage_uuid,
                                                 DatabaseProtector const &protector,
                                                 uint64_t const durability_commit_timestamp) -> bool {
  std::vector<std::pair<ReplicationStorageClient *, std::future<bool>>> decisions;
  // Reserve first: an emplace_back that throws after its task was enqueued would orphan a running
  // task with no future to collect.
  decisions.reserve(locked_clients->size());

  for (auto &&[client, replica_stream] : ranges::views::zip(*locked_clients, streams)) {
    if (client->Mode() == replication_coordination_glue::ReplicationMode::STRICT_SYNC) {
      // A streamless replica was down before voting, so there is no prepared transaction to decide on
      // (SendFinalizeCommitRpc succeeds trivially without a stream). Queueing a task the commit thread
      // must await could deadlock behind a recovery or state-check task blocking on engine_lock_.
      if (!replica_stream) {
        continue;
      }
      auto *raw_client = client.get();
      decisions.emplace_back(raw_client,
                             raw_client->ScheduleTask([this,
                                                       raw_client,
                                                       decision,
                                                       storage_uuid,
                                                       durability_commit_timestamp,
                                                       stream = std::move(replica_stream)]() mutable {
                               try {
                                 const memory::DbArenaScope db_arena_scope{arena_pool_};
                                 return raw_client->SendFinalizeCommitRpc(
                                     decision, storage_uuid, durability_commit_timestamp, std::move(stream));
                               } catch (...) {
                                 // Containment mirrors ScheduleEncode: left in REPLICATING, the
                                 // replica would heal only on the next commit because the recovery
                                 // checker reacts to MAYBE_BEHIND alone.
                                 spdlog::error("Failed to send the 2PC decision to replica {}.", raw_client->Name());
                                 stream.reset();
                                 raw_client->SetMaybeBehind();
                                 return false;
                               }
                             }));
    } else if (client->Mode() == replication_coordination_glue::ReplicationMode::ASYNC) {
      if (decision) {
        // NOLINTNEXTLINE(bugprone-unused-return-value)
        client->FinalizeTransactionReplication(
            protector, std::move(replica_stream), durability_commit_timestamp, commit_num_committed_txns_);
      } else if (replica_stream) {
        // Reconnect needed because we optimistically prepared PrepareCommitReq message already.
        // We should only do this if we own the RPC lock.
        client->AbortRpcClient();
      }
    }
  }

  bool strict_sync_replicas_succ{true};
  // Collect every future before rethrowing anything: an abandoned task keeps running against this,
  // which the caller's unwind destroys — same rule as ShipDeltas. The push_back allocates, so it
  // stays inside the try: a bad_alloc from it must not exit the loop before the drain finished.
  std::exception_ptr first_error;
  for (auto &[client, commit_result] : decisions) {
    bool commit_res = false;
    try {
      commit_res = commit_result.get();
      if (!commit_res) {
        finalize_failures_.push_back(
            {.name = std::string{client->Name()}, .mode = "STRICT_SYNC", .reason = ReplicaFailureReason::RPC_ERROR});
      }
    } catch (...) {
      if (!first_error) {
        first_error = std::current_exception();
      }
    }
    strict_sync_replicas_succ &= commit_res;
  }
  if (first_error) {
    std::rethrow_exception(first_error);
  }
  return strict_sync_replicas_succ;
}

void TransactionReplication::DrainShipFutures() noexcept {
  for (auto &entry : ship_futures_) {
    try {
      entry.second.wait();
      // NOLINTNEXTLINE(bugprone-empty-catch)
    } catch (...) {
    }
  }
  ship_futures_.clear();
}

auto TransactionReplication::CollectAllFailures() -> std::vector<ReplicaFailure> {
  // Build failed_replicas_ from both replication failures and finalize failures
  // so UpdateCommitTsInfo skips all of them.
  failed_replicas_.clear();
  for (auto const &f : replication_failures_) {
    failed_replicas_.insert(f.name);
  }
  for (auto const &f : finalize_failures_) {
    failed_replicas_.insert(f.name);
  }

  // Only replication_failures_ are returned (triggers ReplicationException).
  // finalize_failures_ only affect UpdateCommitTsInfo skipping — no exception thrown for them.
  return replication_failures_;
}

void TransactionReplication::UpdateCommitTsInfo() {
  CommitTsInfo const observed{.ldt_ = durability_commit_timestamp_, .num_committed_txns_ = commit_num_committed_txns_};
  for (auto const &client : *locked_clients) {
    if (failed_replicas_.contains(client->Name())) continue;
    // ASYNC replicas update their own commit_ts_info_ inside the async task
    // upon confirmed success — updating here would be optimistic and could
    // overcount if the async replication later fails.
    if (client->Mode() == replication_coordination_glue::ReplicationMode::ASYNC) continue;
    // Advance-only merge to this txn's absolute value rather than a blind +1, so a heartbeat that already folded in
    // the replica's self-reported count for this txn can't be double-counted.
    atomic_struct_update<CommitTsInfo>(client->commit_ts_info_,
                                       [observed](CommitTsInfo const &cur) { return Max(cur, observed); });
  }
}

TransactionReplication::TransactionReplication(uint64_t const durability_commit_timestamp, Storage *storage,
                                               CommitArgs const &commit_args, ReplicationStorageClientList &clients)
    : locked_clients{clients.ReadLock()},
      arena_pool_{storage->DbArenaPool()},
      durability_commit_timestamp_{durability_commit_timestamp},
      // This transaction is the next one main commits, so its absolute committed-txn count is main's current count
      // + 1. Captured here (under engine_lock_, before FinalizeCommitPhase bumps main) so every up-to-date replica
      // converges to the same value; see UpdateCommitTsInfo and FinalizeTransactionReplication.
      commit_num_committed_txns_{
          storage->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).num_committed_txns_ + 1} {
  if (!locked_clients->empty()) {
    streams.reserve(locked_clients->size());
    auto const &db_acc = commit_args.database_protector();
    for (const auto &client : *locked_clients) {
      // If any client requires two phase commit, then we are running that phase
      run_two_phase_commit |= client->TwoPhaseCommit();
      auto res = client->StartTransactionReplication(storage, db_acc, durability_commit_timestamp);
      if (res.has_value()) {
        streams.emplace_back(std::move(res.value()));
      } else {
        streams.emplace_back(std::nullopt);
        // ASYNC replica errors are not reported — fire-and-forget
        if (client->Mode() != replication_coordination_glue::ReplicationMode::ASYNC) {
          replication_failures_.push_back({.name = client->Name(),
                                           .mode = ReplicationModeToString(client->Mode()),
                                           .reason = StartTxnErrorToReason(res.error())});
        }
      }
    }
  }
}

}  // namespace memgraph::storage
