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

#include <future>
#include <optional>
#include <unordered_set>
#include <vector>

#include "storage/v2/database_protector.hpp"
#include "storage/v2/replication/replication_client.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/synchronized.hpp"

#include <range/v3/view.hpp>

namespace memgraph::storage {

struct CommitArgs;

using ReplicationStorageClientList =
    utils::Synchronized<std::vector<std::unique_ptr<ReplicationStorageClient>>, utils::RWSpinLock>;

class TransactionReplication {
 public:
  // This will block until we retrieve RPC streams for all STRICT_SYNC and SYNC replicas. It is OK to not be able to
  // obtain the RPC lock for the ASYNC replica.
  TransactionReplication(uint64_t durability_commit_timestamp, Storage *storage, CommitArgs const &commit_args,
                         ReplicationStorageClientList &clients);

  ~TransactionReplication() = default;

  // Schedules `encode` on every streaming replica's worker. The single worker per replica keeps this
  // task ordered before the transaction-end task ShipDeltas schedules later. The caller must keep
  // everything `encode` references alive until WaitEncodeDone returns.
  template <InvocableWithStream F>
  void ScheduleEncode(F encode) {
    for (auto &&[client, replica_stream] : ranges::views::zip(*locked_clients, streams)) {
      auto *raw_client = client.get();
      auto *stream_ptr = &replica_stream;
      encode_futures_.push_back(raw_client->ScheduleTask(
          [raw_client, stream_ptr, encode]() { raw_client->IfStreamingTransaction(encode, *stream_ptr); }));
    }
  }

  // Waits for every scheduled encode task; rethrows the first failure only after all of them finished,
  // so no worker is left referencing the caller's frame.
  void WaitEncodeDone();

  // RPC stream won't be destroyed at the end of this function.
  // Returns true if all SYNC/STRICT_SYNC replicas succeeded, false otherwise.
  // Failures are cached internally in ship_failures_ for CollectAllFailures.
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
  std::vector<std::optional<ReplicaStream>> streams;
  std::vector<std::future<void>> encode_futures_;
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
  // Absolute num_committed_txns_ this transaction advances every up-to-date replica to. Captured once at construction
  // (under engine_lock_, before main bumps its own counter) so replica caches converge to a single authoritative
  // value via Max instead of being blindly incremented, which would double-count against the heartbeat merge.
  uint64_t commit_num_committed_txns_;
};

}  // namespace memgraph::storage
