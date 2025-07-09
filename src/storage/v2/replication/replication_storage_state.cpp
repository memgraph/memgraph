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

#include "storage/v2/replication/replication_storage_state.hpp"

#include "replication/replication_server.hpp"

namespace memgraph::storage {

auto ReplicationStorageState::StartPrepareCommitPhase(uint64_t seq_num, Storage *storage,
                                                      DatabaseAccessProtector db_acc) -> TransactionReplication {
  return {seq_num, storage, db_acc, replication_storage_clients_};
}

std::optional<replication::ReplicaState> ReplicationStorageState::GetReplicaState(std::string_view const name) const {
  return replication_storage_clients_.WithReadLock(
      [&](auto const &clients) -> std::optional<replication::ReplicaState> {
        auto const name_matches = [=](ReplicationStorageClientPtr const &client) { return client->Name() == name; };
        auto const client_it = std::find_if(clients.cbegin(), clients.cend(), name_matches);
        if (client_it == clients.cend()) {
          return std::nullopt;
        }
        return (*client_it)->State();
      });
}

void ReplicationStorageState::Reset() {
  replication_storage_clients_.WithLock([](auto &clients) { clients.clear(); });
}

// Don't save epochs for which ldt wasn't changed
void ReplicationStorageState::TrackLatestHistory() {
  auto const new_ldt = last_durable_timestamp_.load(std::memory_order_acquire);
  if (!history.empty() && history.back().second == new_ldt) {
    return;
  }

  // Generate new epoch id and save the last one to the history.
  if (constexpr uint16_t kEpochHistoryRetention = 1000; history.size() == kEpochHistoryRetention) {
    history.pop_front();
  }
  history.emplace_back(epoch_.id(), new_ldt);
}

void ReplicationStorageState::AddEpochToHistoryForce(std::string prev_epoch) {
  history.emplace_back(std::move(prev_epoch), last_durable_timestamp_.load(std::memory_order_acquire));
}

}  // namespace memgraph::storage
