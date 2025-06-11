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
  TransactionReplication(uint64_t const seq_num, Storage *storage, DatabaseAccessProtector const &db_acc, auto &clients)
      : locked_clients{clients.ReadLock()} {
    streams.reserve(locked_clients->size());
    for (const auto &client : *locked_clients) {
      // TODO: (andi) Think about this when handling ASYNC replication, is it really necessary to add std::nullopt for
      // ASYNC replicas
      // TODO: (andi) This is super-important when thinking about mixed cluster with multiple sync/strict_sync/async
      // replicas For SYNC and STRICT_SYNC replicas we should be able to always acquire the stream because we will block
      // until that happens For ASYNC replicas we only try to acquire stream and this could fail. However, we still add
      // nullopt to streams for back-compatibility

      // If replica should commit immediately upon finalizing txn replication
      bool const should_commit_immediately = !ShouldRunTwoPC();

      streams.emplace_back(client->StartTransactionReplication(seq_num, storage, db_acc, should_commit_immediately));
    }
  }
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
  auto FinalizePrepareCommitPhase(uint64_t durability_commit_timestamp, DatabaseAccessProtector db_acc) -> bool;

  // TODO: (andi) Do you need db_acc protector here?
  auto SendFinalizeCommitRpc(bool decision, utils::UUID const &storage_uuid, DatabaseAccessProtector db_acc,
                             uint64_t durability_commit_timestamp) const noexcept -> bool;

  // If we don't check locked_clients, this check would fail for replicas since replicas have empty locked_clients
  auto ReplicationStartSuccessful() const -> bool;

  auto ShouldRunTwoPC() const -> bool;

 private:
  std::vector<std::optional<ReplicaStream>> streams;
  utils::Synchronized<std::vector<std::unique_ptr<ReplicationStorageClient>>, utils::RWSpinLock>::ReadLockedPtr
      locked_clients;
};

}  // namespace memgraph::storage
