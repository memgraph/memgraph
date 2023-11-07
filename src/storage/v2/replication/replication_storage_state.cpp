// Copyright 2023 Memgraph Ltd.
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
#include "storage/v2/replication/replication_client.hpp"
#include "storage/v2/storage.hpp"

namespace memgraph::storage {

void ReplicationStorageState::InitializeTransaction(uint64_t seq_num, Storage *storage) {
  replication_clients_.WithLock([=](auto &clients) {
    for (auto &client : clients.clients) {
      auto id = client->ID();
      auto &replica_state = clients.states_[id];
      client->StartTransactionReplication(seq_num, storage, replica_state);
    }
  });
}

void ReplicationStorageState::AppendDelta(const Delta &delta, const Vertex &vertex, uint64_t timestamp,
                                          Storage *storage) {
  replication_clients_.WithLock([&](auto &clients) {
    for (auto &client : clients.clients) {
      auto id = client->ID();
      auto &replica_state = clients.states_[id];
      client->IfStreamingTransaction([&](auto &stream) { stream.AppendDelta(delta, vertex, timestamp); }, storage,
                                     replica_state);
    }
  });
}

void ReplicationStorageState::AppendDelta(const Delta &delta, const Edge &edge, uint64_t timestamp, Storage *storage) {
  replication_clients_.WithLock([&](auto &clients) {
    for (auto &client : clients.clients) {
      auto id = client->ID();
      auto &replica_state = clients.states_[id];
      client->IfStreamingTransaction([&](auto &stream) { stream.AppendDelta(delta, edge, timestamp); }, storage,
                                     replica_state);
    }
  });
}
void ReplicationStorageState::AppendOperation(durability::StorageMetadataOperation operation, LabelId label,
                                              const std::set<PropertyId> &properties, const LabelIndexStats &stats,
                                              const LabelPropertyIndexStats &property_stats,
                                              uint64_t final_commit_timestamp, Storage *storage) {
  replication_clients_.WithLock([&](auto &clients) {
    for (auto &client : clients.clients) {
      auto id = client->ID();
      auto &replica_state = clients.states_[id];
      client->IfStreamingTransaction(
          [&](auto &stream) {
            stream.AppendOperation(operation, label, properties, stats, property_stats, final_commit_timestamp);
          },
          storage, replica_state);
    }
  });
}

bool ReplicationStorageState::FinalizeTransaction(uint64_t timestamp, Storage *storage) {
  return replication_clients_.WithLock([=](auto &clients) {
    bool finalized_on_all_replicas = true;
    for (ReplicationClientPtr &client : clients.clients) {
      auto id = client->ID();
      auto &replica_state = clients.states_[id];
      client->IfStreamingTransaction([&](auto &stream) { stream.AppendTransactionEnd(timestamp); }, storage,
                                     replica_state);
      const auto finalized = client->FinalizeTransactionReplication(storage, replica_state);

      if (client->Mode() == memgraph::replication::ReplicationMode::SYNC) {
        finalized_on_all_replicas = finalized && finalized_on_all_replicas;
      }
    }
    return finalized_on_all_replicas;
  });
}

std::optional<replication::ReplicaState> ReplicationStorageState::GetReplicaState(std::string_view name) const {
  return replication_clients_.WithReadLock([&](auto const &clients) -> std::optional<replication::ReplicaState> {
    auto const name_matches = [=](ReplicationClientPtr const &client) { return client->Name() == name; };
    auto const client_it = std::find_if(clients.clients.cbegin(), clients.clients.cend(), name_matches);
    if (client_it == clients.clients.cend()) {
      return std::nullopt;
    }
    auto const id = (*client_it)->ID();
    return {clients.states_.at(id).load()};
  });
}

std::vector<ReplicaInfo> ReplicationStorageState::ReplicasInfo(Storage *storage) {
  return replication_clients_.WithLock([=](auto &clients) {
    std::vector<ReplicaInfo> replica_infos;
    replica_infos.reserve(clients.clients.size());
    auto const asReplicaInfo = [&clients, storage](ReplicationClientPtr &client) -> ReplicaInfo {
      auto &replica_state = clients.states_.at(client->ID());

      return {client->Name(), client->Mode(), client->Endpoint(), replica_state.load(),
              client->GetTimestampInfo(storage, replica_state)};
    };
    std::transform(clients.clients.begin(), clients.clients.end(), std::back_inserter(replica_infos), asReplicaInfo);
    return replica_infos;
  });
}

void ReplicationStorageState::Reset() {
  replication_clients_.WithLock([](auto &clients) {
    clients.clients.clear();
    clients.states_.clear();
  });
}

void ReplicationStorageState::TrackLatestHistory() {
  constexpr uint16_t kEpochHistoryRetention = 1000;
  // Generate new epoch id and save the last one to the history.
  if (history.size() == kEpochHistoryRetention) {
    history.pop_front();
  }
  history.emplace_back(epoch_.id(), last_commit_timestamp_);
}

void ReplicationStorageState::AddEpochToHistoryForce(std::string prev_epoch) {
  history.emplace_back(std::move(prev_epoch), last_commit_timestamp_);
}

}  // namespace memgraph::storage
