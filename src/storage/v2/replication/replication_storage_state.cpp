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

#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/replication/replication_client.hpp"
#include "storage/v2/replication/replication_server.hpp"
#include "storage/v2/storage.hpp"

namespace memgraph::storage {

using OOMExceptionEnabler = utils::MemoryTracker::OutOfMemoryExceptionEnabler;

namespace {

std::string RegisterReplicaErrorToString(RegisterReplicaError error) {
  using enum RegisterReplicaError;
  switch (error) {
    case NAME_EXISTS:
      return "NAME_EXISTS";
    case END_POINT_EXISTS:
      return "END_POINT_EXISTS";
    case CONNECTION_FAILED:
      return "CONNECTION_FAILED";
    case COULD_NOT_BE_PERSISTED:
      return "COULD_NOT_BE_PERSISTED";
  }
}
}  // namespace

void ReplicationStorageState::Reset() {
  replication_server_.reset();
  replication_clients_.WithLock([&](auto &clients) { clients.clear(); });
}

bool ReplicationStorageState::SetReplicationRoleMain(storage::Storage *storage,
                                                     memgraph::replication::ReplicationEpoch &epoch) {
  // We don't want to generate new epoch_id and do the
  // cleanup if we're already a MAIN
  if (IsMain()) {
    return false;
  }

  // Main instance does not need replication server
  // This should be always called first so we finalize everything
  replication_server_.reset(nullptr);

  auto prev_epoch = epoch.NewEpoch();
  storage->PrepareForNewEpoch(std::move(prev_epoch));

  if (!TryPersistRoleMain()) {
    return false;
  }

  SetRole(memgraph::replication::ReplicationRole::MAIN);
  return true;
}

void ReplicationStorageState::AppendOperation(durability::StorageMetadataOperation operation, LabelId label,
                                              const std::set<PropertyId> &properties, const LabelIndexStats &stats,
                                              const LabelPropertyIndexStats &property_stats,
                                              uint64_t final_commit_timestamp) {
  if (IsMain()) {
    replication_clients_.WithLock([&](auto &clients) {
      for (auto &client : clients) {
        client->IfStreamingTransaction([&](auto &stream) {
          stream.AppendOperation(operation, label, properties, stats, property_stats, final_commit_timestamp);
        });
      }
    });
  }
}

void ReplicationStorageState::InitializeTransaction(uint64_t seq_num) {
  if (IsMain()) {
    replication_clients_.WithLock([&](auto &clients) {
      for (auto &client : clients) {
        client->StartTransactionReplication(seq_num);
      }
    });
  }
}

void ReplicationStorageState::AppendDelta(const Delta &delta, const Edge &parent, uint64_t timestamp) {
  replication_clients_.WithLock([&](auto &clients) {
    for (auto &client : clients) {
      client->IfStreamingTransaction([&](auto &stream) { stream.AppendDelta(delta, parent, timestamp); });
    }
  });
}

void ReplicationStorageState::AppendDelta(const Delta &delta, const Vertex &parent, uint64_t timestamp) {
  replication_clients_.WithLock([&](auto &clients) {
    for (auto &client : clients) {
      client->IfStreamingTransaction([&](auto &stream) { stream.AppendDelta(delta, parent, timestamp); });
    }
  });
}

bool ReplicationStorageState::FinalizeTransaction(uint64_t timestamp) {
  bool finalized_on_all_replicas = true;
  replication_clients_.WithLock([&](auto &clients) {
    for (auto &client : clients) {
      client->IfStreamingTransaction([&](auto &stream) { stream.AppendTransactionEnd(timestamp); });
      const auto finalized = client->FinalizeTransactionReplication();

      if (client->Mode() == memgraph::replication::ReplicationMode::SYNC) {
        finalized_on_all_replicas = finalized && finalized_on_all_replicas;
      }
    }
  });
  return finalized_on_all_replicas;
}

utils::BasicResult<RegisterReplicaError> ReplicationStorageState::RegisterReplica(
    const replication::RegistrationMode registration_mode, const memgraph::replication::ReplicationClientConfig &config,
    Storage *storage) {
  MG_ASSERT(IsMain(), "Only main instance can register a replica!");

  auto name_check = [&config](auto &clients) {
    auto name_matches = [&name = config.name](const auto &client) { return client->Name() == name; };
    return std::any_of(clients.begin(), clients.end(), name_matches);
  };

  auto desired_endpoint = io::network::Endpoint{config.ip_address, config.port};
  auto endpoint_check = [&](auto &clients) {
    auto endpoint_matches = [&](const auto &client) { return client->Endpoint() == desired_endpoint; };
    return std::any_of(clients.begin(), clients.end(), endpoint_matches);
  };

  auto task = [&](auto &clients) -> utils::BasicResult<RegisterReplicaError> {
    if (name_check(clients)) {
      return RegisterReplicaError::NAME_EXISTS;
    }

    if (endpoint_check(clients)) {
      return RegisterReplicaError::END_POINT_EXISTS;
    }

    using enum replication::RegistrationMode;
    if (registration_mode != RESTORE && !TryPersistRegisteredReplica(config)) {
      return RegisterReplicaError::COULD_NOT_BE_PERSISTED;
    }

    auto client = storage->CreateReplicationClient(config);
    client->Start();

    if (client->State() == replication::ReplicaState::INVALID) {
      if (replication::RegistrationMode::RESTORE != registration_mode) {
        return RegisterReplicaError::CONNECTION_FAILED;
      }

      spdlog::warn("Connection failed when registering replica {}. Replica will still be registered.", client->Name());
    }

    clients.push_back(std::move(client));
    return {};
  };

  return replication_clients_.WithLock(task);
}

bool ReplicationStorageState::SetReplicationRoleReplica(const memgraph::replication::ReplicationServerConfig &config,
                                                        Storage *storage) {
  // We don't want to restart the server if we're already a REPLICA
  if (IsReplica()) {
    return false;
  }

  replication_server_ = storage->CreateReplicationServer(config);
  bool res = replication_server_->Start();
  if (!res) {
    spdlog::error("Unable to start the replication server.");
    return false;
  }

  if (!TryPersistRoleReplica(config)) {
    return false;
  }

  SetRole(memgraph::replication::ReplicationRole::REPLICA);
  return true;
}

bool ReplicationStorageState::UnregisterReplica(std::string_view name) {
  MG_ASSERT(IsMain(), "Only main instance can unregister a replica!");
  if (!TryPersistUnregisterReplica(name)) {
    return false;
  }

  return replication_clients_.WithLock([&](auto &clients) {
    return std::erase_if(clients, [&](const auto &client) { return client->Name() == name; });
  });
}

std::optional<replication::ReplicaState> ReplicationStorageState::GetReplicaState(const std::string_view name) {
  return replication_clients_.WithLock([&](auto &clients) -> std::optional<replication::ReplicaState> {
    const auto client_it =
        std::find_if(clients.cbegin(), clients.cend(), [name](auto &client) { return client->Name() == name; });
    if (client_it == clients.cend()) {
      return std::nullopt;
    }
    return (*client_it)->State();
  });
}

std::vector<ReplicaInfo> ReplicationStorageState::ReplicasInfo() {
  return replication_clients_.WithLock([](auto &clients) {
    std::vector<ReplicaInfo> replica_info;
    replica_info.reserve(clients.size());
    std::transform(
        clients.begin(), clients.end(), std::back_inserter(replica_info), [](const auto &client) -> ReplicaInfo {
          return {client->Name(), client->Mode(), client->Endpoint(), client->State(), client->GetTimestampInfo()};
        });
    return replica_info;
  });
}

void ReplicationStorageState::RestoreReplication(Storage *storage) {
  if (!ShouldPersist()) {
    return;
  }

  spdlog::info("Restoring replication role.");

  auto replicationData = FetchReplicationData();

  if (replicationData.HasError()) {
    switch (replicationData.GetError()) {
      using enum ReplicationStorageState::FetchReplicationError;
      case NOTHING_FETCHED: {
        spdlog::debug("Cannot find data needed for restore replication role in persisted metadata.");
        return;
      }
      case PARSE_ERROR: {
        LOG_FATAL("Cannot parse previously saved configuration of replication role.");
        return;
      }
    }
  }

  ReplicationState &repl_state = *this;

  std::visit(utils::Overloaded{
                 [this, storage, &repl_state](ReplicationStorageState::ReplicationDataMain const &configs) {
                   replication_server_.reset();
                   repl_state.SetRole(memgraph::replication::ReplicationRole::MAIN);
                   for (const auto &config : configs) {
                     spdlog::info("Replica {} restored for {}.", config.name, storage->id());
                     auto ret = RegisterReplica(replication::RegistrationMode::RESTORE, config, storage);
                     if (ret.HasError()) {
                       MG_ASSERT(RegisterReplicaError::CONNECTION_FAILED != ret.GetError());
                       LOG_FATAL("Failure when restoring replica {}: {}.", config.name,
                                 RegisterReplicaErrorToString(ret.GetError()));
                     }
                     spdlog::info("Replica {} restored for {}.", config.name, storage->id());
                   }
                   spdlog::info("Replication role restored to MAIN.");
                 },
                 [this, storage, &repl_state](ReplicationStorageState::ReplicationDataReplica const &config) {
                   auto replication_server = storage->CreateReplicationServer(config);
                   if (!replication_server->Start()) {
                     LOG_FATAL("Unable to start the replication server.");
                   }
                   replication_server_ = std::move(replication_server);
                   repl_state.SetRole(memgraph::replication::ReplicationRole::REPLICA);
                   spdlog::info("Replication role restored to REPLICA.");
                 },
             },
             *replicationData);
}

constexpr uint16_t kEpochHistoryRetention = 1000;

void ReplicationStorageState::AddEpochToHistory(std::string prev_epoch) {
  // Generate new epoch id and save the last one to the history.
  if (history.size() == kEpochHistoryRetention) {
    history.pop_front();
  }
  history.emplace_back(std::move(prev_epoch), last_commit_timestamp_);
}

void ReplicationStorageState::AddEpochToHistoryForce(std::string prev_epoch) {
  history.emplace_back(std::move(prev_epoch), last_commit_timestamp_);
}

}  // namespace memgraph::storage
