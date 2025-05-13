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
  TransactionReplication(uint64_t const seq_num, Storage *storage, DatabaseAccessProtector db_acc, auto &clients)
      : locked_clients{clients.ReadLock()} {
    streams.reserve(locked_clients->size());
    for (const auto &client : *locked_clients) {
      streams.emplace_back(client->StartTransactionReplication(seq_num, storage, db_acc));
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

  bool FinalizeTransaction(uint64_t durability_commit_timestamp, DatabaseAccessProtector db_acc) {
    bool finalized_on_all_replicas{true};
    MG_ASSERT(locked_clients->empty() || db_acc.has_value(),
              "Any clients assumes we are MAIN, we should have gatekeeper_access_wrapper so we can correctly "
              "handle ASYNC tasks");
    for (auto &&[client, replica_stream] : ranges::views::zip(*locked_clients, streams)) {
      client->IfStreamingTransaction([&](auto &stream) { stream.AppendTransactionEnd(durability_commit_timestamp); },
                                     replica_stream);
      const auto finalized =
          client->FinalizeTransactionReplication(db_acc, std::move(replica_stream), durability_commit_timestamp);
      if (client->Mode() == replication_coordination_glue::ReplicationMode::SYNC) {
        finalized_on_all_replicas = finalized && finalized_on_all_replicas;
      }
    }
    return finalized_on_all_replicas;
  }

 private:
  std::vector<std::optional<ReplicaStream>> streams;
  utils::Synchronized<std::vector<std::unique_ptr<ReplicationStorageClient>>, utils::RWSpinLock>::ReadLockedPtr
      locked_clients;
};

}  // namespace memgraph::storage
