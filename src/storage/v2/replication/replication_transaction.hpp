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

#include "storage/v2/database_access.hpp"
#include "storage/v2/replication/replication_client.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/synchronized.hpp"

#include <range/v3/view.hpp>

namespace memgraph::storage {

class TransactionReplication {
 public:
  // The contract of the constructor is the following: If streams are empty, it means starting txn failed and we cannot
  // proceed with the Commit.
  TransactionReplication(uint64_t const seq_num, Storage *storage, DatabaseAccessProtector db_acc, auto &clients)
      : locked_clients{clients.ReadLock()} {
    spdlog::trace("Successfully locked clients");
    streams.reserve(locked_clients->size());
    for (const auto &client : *locked_clients) {
      // SYNC and ASYNC replicas should commit immediately when receiving deltas
      bool const should_commit_immediately =
          client->Mode() != replication_coordination_glue::ReplicationMode::STRICT_SYNC;
      streams.emplace_back(client->StartTransactionReplication(seq_num, storage, db_acc, should_commit_immediately));
    }
  }

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
  auto ShipDeltas(uint64_t durability_commit_timestamp, DatabaseAccessProtector db_acc) -> bool;

  auto FinalizeTransaction(bool decision, utils::UUID const &storage_uuid, DatabaseAccessProtector db_acc,
                           uint64_t durability_commit_timestamp) -> bool;

  auto AllStrictSyncReplicasUp() const -> bool;

  auto ShouldRunTwoPC() const -> bool;

 private:
  std::vector<std::optional<ReplicaStream>> streams;
  utils::Synchronized<std::vector<std::unique_ptr<ReplicationStorageClient>>, utils::RWSpinLock>::ReadLockedPtr
      locked_clients;
};

}  // namespace memgraph::storage
