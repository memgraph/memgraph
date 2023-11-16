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

namespace memgraph::storage {

void ReplicationStorageState::InitializeTransaction(uint64_t seq_num, Storage *storage) {
  replication_clients_.WithLock([=](auto &clients) {
    for (auto &client : clients) {
      client->StartTransactionReplication(seq_num, storage);
    }
  });
}

void ReplicationStorageState::AppendDelta(const Delta &delta, const Vertex &vertex, uint64_t timestamp) {
  replication_clients_.WithLock([&](auto &clients) {
    for (auto &client : clients) {
      client->IfStreamingTransaction([&](auto &stream) { stream.AppendDelta(delta, vertex, timestamp); });
    }
  });
}

void ReplicationStorageState::AppendDelta(const Delta &delta, const Edge &edge, uint64_t timestamp) {
  replication_clients_.WithLock([&](auto &clients) {
    for (auto &client : clients) {
      client->IfStreamingTransaction([&](auto &stream) { stream.AppendDelta(delta, edge, timestamp); });
    }
  });
}
void ReplicationStorageState::AppendOperation(durability::StorageMetadataOperation operation, LabelId label,
                                              const std::set<PropertyId> &properties, const LabelIndexStats &stats,
                                              const LabelPropertyIndexStats &property_stats,
                                              uint64_t final_commit_timestamp) {
  replication_clients_.WithLock([&](auto &clients) {
    for (auto &client : clients) {
      client->IfStreamingTransaction([&](auto &stream) {
        stream.AppendOperation(operation, label, properties, stats, property_stats, final_commit_timestamp);
      });
    }
  });
}

bool ReplicationStorageState::FinalizeTransaction(uint64_t timestamp, Storage *storage) {
  return replication_clients_.WithLock([=](auto &clients) {
    bool finalized_on_all_replicas = true;
    for (ReplicationClientPtr &client : clients) {
      client->IfStreamingTransaction([&](auto &stream) { stream.AppendTransactionEnd(timestamp); });
      const auto finalized = client->FinalizeTransactionReplication(storage);

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
    auto const client_it = std::find_if(clients.cbegin(), clients.cend(), name_matches);
    if (client_it == clients.cend()) {
      return std::nullopt;
    }
    return (*client_it)->State();
  });
}

std::vector<ReplicaInfo> ReplicationStorageState::ReplicasInfo(Storage *storage) const {
  return replication_clients_.WithReadLock([storage](auto const &clients) {
    std::vector<ReplicaInfo> replica_infos;
    replica_infos.reserve(clients.size());
    auto const asReplicaInfo = [storage](ReplicationClientPtr const &client) -> ReplicaInfo {
      return {client->Name(), client->Mode(), client->Endpoint(), client->State(), client->GetTimestampInfo(storage)};
    };
    std::transform(clients.begin(), clients.end(), std::back_inserter(replica_infos), asReplicaInfo);
    return replica_infos;
  });
}

void ReplicationStorageState::Reset() {
  replication_clients_.WithLock([](auto &clients) { clients.clear(); });
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

ReplicationStorageClient *ReplicationStorageState::GetClient(std::string_view replica_name) {
  return replication_clients_.WithLock([replica_name](auto &clients) -> ReplicationStorageClient * {
    for (const auto &client : clients) {
      if (client->Name() == replica_name) {
        return client.get();
      }
    }
    return nullptr;
  });
}

}  // namespace memgraph::storage
