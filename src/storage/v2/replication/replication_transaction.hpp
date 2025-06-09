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
      // TODO: (andi) Think about this when handling ASYNC replication
      // For SYNC replicas we should be able to always acquire the stream because we will block until that happens
      // For ASYNC replicas we only try to acquire stream and hence may fail. However, we still add nullopt to streams
      if (auto stream = client->StartTransactionReplication(seq_num, storage, db_acc);
          stream.has_value() || client->Mode() == replication_coordination_glue::ReplicationMode::ASYNC) {
        streams.push_back(std::move(stream));
      } else {
        // For 2PC, we need valid replication streams for all instances. Clear streams, release RPC locks and return.
        streams.clear();
        return;
      }
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

  // RPC stream still won't be destroyed
  bool FinalizePrepareCommitPhase(uint64_t durability_commit_timestamp, DatabaseAccessProtector db_acc) {
    bool sync_replicas_succ{true};
    MG_ASSERT(locked_clients->empty() || db_acc.has_value(),
              "Any clients assumes we are MAIN, we should have gatekeeper_access_wrapper so we can correctly "
              "handle ASYNC tasks");
    for (auto &&[client, replica_stream] : ranges::views::zip(*locked_clients, streams)) {
      client->IfStreamingTransaction([&](auto &stream) { stream.AppendTransactionEnd(durability_commit_timestamp); },
                                     replica_stream);

      const auto finalized =
          client->FinalizeTransactionReplication(db_acc, replica_stream, durability_commit_timestamp);

      if (client->Mode() == replication_coordination_glue::ReplicationMode::SYNC) {
        sync_replicas_succ = finalized && sync_replicas_succ;
      }
    }
    return sync_replicas_succ;
  }

  // TODO: (andi) Do you need db_acc protector here?
  bool SendFinalizeCommitRpc(bool const decision, utils::UUID const &storage_uuid, DatabaseAccessProtector db_acc,
                             uint64_t const durability_commit_timestamp) noexcept {
    bool sync_replicas_succ{true};
    for (auto &client : *locked_clients) {
      if (client->Mode() == replication_coordination_glue::ReplicationMode::SYNC) {
        sync_replicas_succ &=
            client->SendFinalizeCommitRpc(decision, storage_uuid, db_acc, durability_commit_timestamp);
      }
    }
    return sync_replicas_succ;
  }

  // If we don't check locked_clients, this check would fail for replicas since replicas have empty locked_clients
  auto ReplicationStartSuccessful() const -> bool { return locked_clients->empty() || !streams.empty(); }

 private:
  std::vector<std::optional<ReplicaStream>> streams;
  utils::Synchronized<std::vector<std::unique_ptr<ReplicationStorageClient>>, utils::RWSpinLock>::ReadLockedPtr
      locked_clients;
};

}  // namespace memgraph::storage
