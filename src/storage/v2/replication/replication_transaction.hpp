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

#include <expected>
#include <future>
#include <optional>
#include <stdexcept>
#include <unordered_set>
#include <vector>

#include "memory/db_arena_fwd.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/commit_probe.hpp"
#include "storage/v2/database_protector.hpp"
#include "storage/v2/replication/replication_client.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/synchronized.hpp"

#include <range/v3/view.hpp>

namespace memgraph::storage {

struct CommitArgs;
class CommitTicket;

using ReplicationStorageClientList =
    utils::Synchronized<std::vector<std::unique_ptr<ReplicationStorageClient>>, utils::RWSpinLock>;

class TransactionReplication {
 public:
  // This will block until we retrieve RPC streams for all STRICT_SYNC and SYNC replicas. It is OK to not be able to
  // obtain the RPC lock for the ASYNC replica.
  // `ticket` is non-null on a ticketed (pipelined-commit) execution and selects the stream cleanup that execution
  // needs; OFF and replica callers omit it. A stream opened before the constructor throws is shut down while it
  // still owns the RPC lock, reset and its client set MAYBE_BEHIND before the exception propagates.
  TransactionReplication(uint64_t durability_commit_timestamp, Storage *storage, CommitArgs const &commit_args,
                         ReplicationStorageClientList &clients, CommitTicket *ticket = nullptr);

  ~TransactionReplication() = default;

  using ShipResult = std::expected<void, io::network::ClientCommunicationError>;

  // Schedules one fused task per streaming replica: encode the transaction into the stream, wait on
  // the WAL result — durability gates the transaction end, so a replica can never commit what main
  // did not make durable — and ship the transaction end. Any failure is contained per replica (the
  // stream is dropped and the replica falls back to recovery via MAYBE_BEHIND) and surfaces as that
  // replica's ShipResult; it never aborts main by itself. `db_acc` may be null only when no client
  // has a stream (a replica applying a transaction). The caller must keep everything `encode` and
  // `db_acc` reference alive until ShipDeltas returns.
  template <InvocableWithStream F>
  void ScheduleEncodeAndShip(F encode, std::shared_future<void> wal_result, uint64_t durability_commit_timestamp,
                             DatabaseProtector const *db_acc) {
    // Reserve first: an emplace_back that throws after its task was enqueued would orphan a running
    // task with no future to collect.
    ship_futures_.reserve(locked_clients->size());
    auto *hooks = TestHooks();
    for (auto &&[client, replica_stream] : ranges::views::zip(*locked_clients, streams)) {
      // A streamless replica already failed to start this transaction; ShipDeltas runs its quick,
      // RPC-free bookkeeping inline. Queueing a no-op would make the commit thread await this
      // replica's worker, whose queue may hold a task that blocks on the engine_lock_ this thread
      // holds.
      if (!replica_stream) {
        continue;
      }
      auto *raw_client = client.get();
      auto *stream_ptr = &replica_stream;
      // Test-only: a scheduling failure part-way through, after earlier tasks were enqueued.
      if (hooks != nullptr && hooks->throw_before_enqueue_for &&
          hooks->throw_before_enqueue_for(raw_client->Name(), durability_commit_timestamp)) {
        throw std::runtime_error("injected scheduling failure");
      }
      ship_futures_.emplace_back(
          raw_client,
          raw_client->ScheduleTask(
              [this, raw_client, stream_ptr, encode, wal_result, durability_commit_timestamp, db_acc, hooks]()
                  -> ShipResult {
                try {
                  const memory::DbArenaScope db_arena_scope{arena_pool_};
                  raw_client->IfStreamingTransaction(encode, *stream_ptr);
                  if (hooks != nullptr && hooks->before_wal_result_wait) {
                    hooks->before_wal_result_wait(raw_client->Name(), durability_commit_timestamp);
                  }
                  // The durability gate: rethrows on a WAL failure, so the transaction end never ships.
                  wal_result.get();
                  auto result = ShipOne(raw_client, *stream_ptr, durability_commit_timestamp, *db_acc);
                  if (hooks != nullptr && hooks->on_task_done)
                    hooks->on_task_done(raw_client->Name(), durability_commit_timestamp);
                  return result;
                } catch (...) {
                  spdlog::error("Failed to replicate transaction to replica {}.", raw_client->Name());
                  // A partially transmitted request must not survive on the connection: retire the socket while this
                  // task still owns the stream's RPC lock, then release it. Ticketed executions only; the flag-off path
                  // keeps its behaviour.
                  if (ticketed_ && stream_ptr->has_value()) raw_client->AbortRpcClient();
                  stream_ptr->reset();
                  raw_client->SetMaybeBehind();
                  if (hooks != nullptr && hooks->on_task_done)
                    hooks->on_task_done(raw_client->Name(), durability_commit_timestamp);
                  return std::unexpected{io::network::ClientCommunicationError::GENERIC_ERROR};
                }
              }));
    }
  }

  // Shuts down, resets and marks MAYBE_BEHIND every stream this transaction still holds; for a ticketed execution
  // that exits before any WAL frame was written. Skips empty optionals entirely, so a second invocation is a no-op.
  void DiscardUnpreparedStreams() noexcept;

  auto ticketed() const noexcept -> bool { return ticketed_; }

  // Waits for every scheduled fused task without consuming results; for the unwind paths, so no
  // worker is left referencing the caller's frame.
  void DrainShipFutures() noexcept;

  // Collects every fused task scheduled by ScheduleEncodeAndShip and runs the inline bookkeeping for
  // streamless replicas. RPC streams won't be destroyed at the end of this function.
  // Returns true if all SYNC/STRICT_SYNC replicas succeeded, false otherwise.
  // Failures are cached internally in replication_failures_ for CollectAllFailures.
  auto ShipDeltas(uint64_t durability_commit_timestamp, CommitArgs const &commit_args) -> bool;

  auto FinalizeTransaction(bool decision, utils::UUID const &storage_uuid, DatabaseProtector const &protector,
                           uint64_t durability_commit_timestamp) -> bool;

  auto ShouldRunTwoPC() const -> bool { return run_two_phase_commit; }

  // Returns all replication failures (start-txn + ship/finalize) and additionally
  // marks 2nd-phase finalize failures in failed_replicas_ so UpdateCommitTsInfo skips them.
  // Finalize failures are NOT included in the returned vector (no ReplicationException for them).
  // Must be called after ShipDeltas and FinalizeTransaction (if applicable), and before UpdateCommitTsInfo.
  auto CollectAllFailures() -> std::vector<ReplicaFailure>;

  // Advance-only merges each successfully-committed (non-ASYNC) replica's cached progress to this transaction's
  // absolute (last_durable_ts, num_committed_txns) via CommitTsInfo::Max, skipping replicas in failed_replicas_.
  // ASYNC replicas instead update their own cache in the async finalize task. Must be called after CollectAllFailures.
  void UpdateCommitTsInfo();

 private:
  // Ships the transaction end for one replica: the 1st 2PC phase for STRICT_SYNC, the full finalize
  // when no 2PC runs. Quick and RPC-free for a null stream.
  auto ShipOne(ReplicationStorageClient *raw_client, std::optional<ReplicaStream> &replica_stream,
               uint64_t durability_commit_timestamp, DatabaseProtector const &db_acc) const -> ShipResult;

  // The storage-owned test hooks (null in production).
  auto TestHooks() const noexcept -> ReplicationTestHooks *;

  // The storage this transaction commits on; retained so the scheduled tasks and the decision phase can reach its
  // test hooks and counters.
  Storage *storage_{nullptr};
  // True when constructed by a ticketed (pipelined-commit) execution; gates every cleanup the flag-off path lacks.
  bool ticketed_{false};
  std::vector<std::optional<ReplicaStream>> streams;
  std::vector<std::pair<ReplicationStorageClient *, std::future<ShipResult>>> ship_futures_;
  utils::Synchronized<std::vector<std::unique_ptr<ReplicationStorageClient>>, utils::RWSpinLock>::ReadLockedPtr
      locked_clients;
  // The database's arena pool, installed on replica workers for the tasks scheduled by this transaction.
  memory::ArenaPool *arena_pool_{nullptr};
  bool run_two_phase_commit{false};
  // Replicas that failed start-txn or ship/finalize (excludes ASYNC — fire-and-forget).
  // Populated by the constructor and ShipDeltas. Returned by CollectAllFailures.
  std::vector<ReplicaFailure> replication_failures_;
  // Replicas that failed the 2nd phase of 2PC (SendFinalizeCommitRpc failed).
  // Populated by FinalizeTransaction. NOT returned by CollectAllFailures (no ReplicationException),
  // but added to failed_replicas_ so UpdateCommitTsInfo skips them.
  std::vector<ReplicaFailure> finalize_failures_;
  // Union of all failed replica names, built by CollectAllFailures.
  std::unordered_set<std::string> failed_replicas_;
  // last_durable_ts this transaction commits at; paired with commit_num_committed_txns_ when advancing replica caches.
  uint64_t durability_commit_timestamp_;
  // Absolute num_committed_txns_ this transaction advances every up-to-date replica to. Captured once at construction,
  // before FinalizeCommitPhase bumps main's counter and ordered against other committers by the commit serializer
  // (or, on a ticketed execution, by the commit-order gate), so replica caches converge to a single authoritative
  // value via Max instead of being blindly incremented, which would double-count against the heartbeat merge.
  uint64_t commit_num_committed_txns_;
};

}  // namespace memgraph::storage
