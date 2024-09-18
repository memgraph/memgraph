// Copyright 2024 Memgraph Ltd.
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

#include <atomic>
#include <utility>

#include "kvstore/kvstore.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/durability/storage_global_operation.hpp"
#include "storage/v2/transaction.hpp"
#include "utils/exceptions.hpp"
#include "utils/result.hpp"

/// REPLICATION ///
#include "replication/config.hpp"
#include "replication/epoch.hpp"
#include "replication/state.hpp"
#include "storage/v2/database_access.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/replication/global.hpp"
#include "storage/v2/replication/rpc.hpp"
#include "storage/v2/replication/serialization.hpp"
#include "utils/synchronized.hpp"

#include <range/v3/view.hpp>
#include <span>

namespace memgraph::storage {

class Storage;

class ReplicationStorageClient;
class ReplicaStream;

struct ReplicationStorageState {
  // Only MAIN can send
  auto InitializeTransaction(uint64_t seq_num, Storage *storage, DatabaseAccessProtector db_acc)
      -> std::vector<std::optional<ReplicaStream>>;

  template <typename... Args>
  void AppendDelta(std::span<std::optional<ReplicaStream>> replica_streams, Args &&...args) {
    replication_clients_.WithLock([&](auto &clients) {
      for (auto &&[client, replica_stream] : ranges::views::zip(clients, replica_streams)) {
        client->IfStreamingTransaction([&](auto &stream) { stream.AppendDelta(args...); }, replica_stream);
      }
    });
  }

  template <typename Func>
  void EncodeToReplicas(std::span<std::optional<ReplicaStream>> replica_streams, Func &&func) {
    replication_clients_.WithLock([&](auto &clients) {
      for (auto &&[client, replica_stream] : ranges::views::zip(clients, replica_streams)) {
        client->IfStreamingTransaction(
            [&](auto &stream) {
              auto encoder = stream.encoder();
              func(encoder);
            },
            replica_stream);
      }
    });
  }

  bool FinalizeTransaction(uint64_t timestamp, Storage *storage, DatabaseAccessProtector db_acc,
                           std::vector<std::optional<ReplicaStream>> replica_stream);

  // Getters
  auto GetReplicaState(std::string_view name) const -> std::optional<replication::ReplicaState>;
  auto ReplicasInfo(const Storage *storage) const -> std::vector<ReplicaInfo>;

  // History
  void TrackLatestHistory();
  void AddEpochToHistoryForce(std::string prev_epoch);

  void Reset();

  template <typename F>
  bool WithClient(std::string_view replica_name, F &&callback) {
    return replication_clients_.WithLock([replica_name, cb = std::forward<F>(callback)](auto &clients) {
      for (const auto &client : clients) {
        if (client->Name() == replica_name) {
          cb(*client);
          return true;
        }
      }
      return false;
    });
  }

  // Questions:
  //    - storage durability <- databases/*name*/wal and snapshots (where this for epoch_id)
  //    - multi-tenant durability <- databases/.durability (there is a list of all active tenants)
  // History of the previous epoch ids.
  // Each value consists of the epoch id along the last commit belonging to that
  // epoch.
  std::deque<std::pair<std::string, uint64_t>> history;
  std::atomic<uint64_t> last_durable_timestamp_{kTimestampInitialId};

  // We create ReplicationClient using unique_ptr so we can move
  // newly created client into the vector.
  // We cannot move the client directly because it contains ThreadPool
  // which cannot be moved. Also, the move is necessary because
  // we don't want to create the client directly inside the vector
  // because that would require the lock on the list putting all
  // commits (they iterate list of clients) to halt.
  // This way we can initialize client in main thread which means
  // that we can immediately notify the user if the initialization
  // failed.
  using ReplicationClientPtr = std::unique_ptr<ReplicationStorageClient>;
  using ReplicationClientList = utils::Synchronized<std::vector<ReplicationClientPtr>, utils::RWSpinLock>;

  ReplicationClientList replication_clients_;

  memgraph::replication::ReplicationEpoch epoch_;
};

}  // namespace memgraph::storage
