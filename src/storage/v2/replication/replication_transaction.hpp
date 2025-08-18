// Copyright 2025 Memgraph Ltd.
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

#include <optional>

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
  TransactionReplication(uint64_t const durability_commit_timestamp, Storage *storage, CommitArgs const &commit_args,
                         ReplicationStorageClientList &clients);

  ~TransactionReplication() = default;

  template <typename... Args>
  void AppendDelta(Args &&...args) {
    for (auto &&[client, replica_stream] : ranges::views::zip(*locked_clients, streams)) {
      client->IfStreamingTransaction(
          [&...args = std::forward<Args>(args)](auto &stream) { stream.AppendDelta(std::forward<Args>(args)...); },
          replica_stream);
    }
  }

  template <typename Func>
  void EncodeToReplicas(Func &&func) {
    for (auto &&[client, replica_stream] : ranges::views::zip(*locked_clients, streams)) {
      client->IfStreamingTransaction(
          [&](auto &stream) {
            auto encoder = stream.encoder();
            func(encoder);
          },
          replica_stream);
    }
  }

  // RPC stream won't be destroyed at the end of this function
  auto ShipDeltas(uint64_t durability_commit_timestamp, CommitArgs const &commit_args) -> bool;

  auto FinalizeTransaction(bool const decision, utils::UUID const &storage_uuid, DatabaseProtector const &protector,
                           uint64_t const durability_commit_timestamp) -> bool;

  auto ShouldRunTwoPC() const -> bool { return run_two_phase_commit; }

 private:
  std::vector<std::optional<ReplicaStream>> streams;
  utils::Synchronized<std::vector<std::unique_ptr<ReplicationStorageClient>>, utils::RWSpinLock>::ReadLockedPtr
      locked_clients;
  bool run_two_phase_commit = false;
};

}  // namespace memgraph::storage
