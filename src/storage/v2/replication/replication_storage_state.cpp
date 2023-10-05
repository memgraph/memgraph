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

#include "replication/replication_state.hpp"
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

storage::ReplicationStorageState::ReplicationStorageState(bool restore, std::filesystem::path durability_dir) {
  if (restore) {
    utils::EnsureDirOrDie(durability_dir / durability::kReplicationDirectory);
    durability_ = std::make_unique<kvstore::KVStore>(durability_dir / durability::kReplicationDirectory);
  }
}

void storage::ReplicationStorageState::Reset() {
  replication_server_.reset();
  replication_clients_.WithLock([&](auto &clients) { clients.clear(); });
}

bool storage::ReplicationStorageState::SetMainReplicationRole(storage::Storage *storage) {
  // We don't want to generate new epoch_id and do the
  // cleanup if we're already a MAIN
  if (IsMain()) {
    return false;
  }

  // Main instance does not need replication server
  // This should be always called first so we finalize everything
  replication_server_.reset(nullptr);

  storage->EstablishNewEpoch();

  if (ShouldStoreAndRestoreReplicationState()) {
    // Only thing that matters here is the role saved as MAIN
    auto data = replication::ReplicationStatusToJSON(
        replication::ReplicationStatus{.name = replication::kReservedReplicationRoleName,
                                       .ip_address = "",
                                       .port = 0,
                                       .sync_mode = replication::ReplicationMode::SYNC,
                                       .replica_check_frequency = std::chrono::seconds(0),
                                       .ssl = std::nullopt,
                                       .role = memgraph::replication::ReplicationRole::MAIN});

    if (!durability_->Put(replication::kReservedReplicationRoleName, data.dump())) {
      spdlog::error("Error when saving MAIN replication role in settings.");
      return false;
    }
  }

  SetRole(memgraph::replication::ReplicationRole::MAIN);
  return true;
}

void storage::ReplicationStorageState::AppendOperation(durability::StorageMetadataOperation operation, LabelId label,
                                                       const std::set<PropertyId> &properties,
                                                       const LabelIndexStats &stats,
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

void storage::ReplicationStorageState::InitializeTransaction(uint64_t seq_num) {
  if (IsMain()) {
    replication_clients_.WithLock([&](auto &clients) {
      for (auto &client : clients) {
        client->StartTransactionReplication(seq_num);
      }
    });
  }
}

void storage::ReplicationStorageState::AppendDelta(const Delta &delta, const Edge &parent, uint64_t timestamp) {
  replication_clients_.WithLock([&](auto &clients) {
    for (auto &client : clients) {
      client->IfStreamingTransaction([&](auto &stream) { stream.AppendDelta(delta, parent, timestamp); });
    }
  });
}

void storage::ReplicationStorageState::AppendDelta(const Delta &delta, const Vertex &parent, uint64_t timestamp) {
  replication_clients_.WithLock([&](auto &clients) {
    for (auto &client : clients) {
      client->IfStreamingTransaction([&](auto &stream) { stream.AppendDelta(delta, parent, timestamp); });
    }
  });
}

bool storage::ReplicationStorageState::FinalizeTransaction(uint64_t timestamp) {
  bool finalized_on_all_replicas = true;
  replication_clients_.WithLock([&](auto &clients) {
    for (auto &client : clients) {
      client->IfStreamingTransaction([&](auto &stream) { stream.AppendTransactionEnd(timestamp); });
      const auto finalized = client->FinalizeTransactionReplication();

      if (client->Mode() == replication::ReplicationMode::SYNC) {
        finalized_on_all_replicas = finalized && finalized_on_all_replicas;
      }
    }
  });
  return finalized_on_all_replicas;
}

utils::BasicResult<RegisterReplicaError> ReplicationStorageState::RegisterReplica(
    const replication::RegistrationMode registration_mode, const replication::ReplicationClientConfig &config,
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

    if (!TryPersistReplicaClient(config)) {
      return RegisterReplicaError::COULD_NOT_BE_PERSISTED;
    }

    auto client = storage->CreateReplicationClient(config);
    client->Start();

    if (client->State() == replication::ReplicaState::INVALID) {
      if (replication::RegistrationMode::CAN_BE_INVALID != registration_mode) {
        return RegisterReplicaError::CONNECTION_FAILED;
      }

      spdlog::warn("Connection failed when registering replica {}. Replica will still be registered.", client->Name());
    }

    clients.push_back(std::move(client));
    return {};
  };

  return replication_clients_.WithLock(task);
}

bool ReplicationStorageState::TryPersistReplicaClient(const replication::ReplicationClientConfig &config) {
  if (!ShouldStoreAndRestoreReplicationState()) return true;
  auto data = replication::ReplicationStatusToJSON(
      replication::ReplicationStatus{.name = config.name,
                                     .ip_address = config.ip_address,
                                     .port = config.port,
                                     .sync_mode = config.mode,
                                     .replica_check_frequency = config.replica_check_frequency,
                                     .ssl = config.ssl,
                                     .role = memgraph::replication::ReplicationRole::REPLICA});
  if (durability_->Put(config.name, data.dump())) return true;
  spdlog::error("Error when saving replica {} in settings.", config.name);
  return false;
}

bool ReplicationStorageState::SetReplicaRole(const replication::ReplicationServerConfig &config, Storage *storage) {
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

  if (ShouldStoreAndRestoreReplicationState()) {
    // Only thing that matters here is the role saved as REPLICA and the listening port
    auto data = replication::ReplicationStatusToJSON(
        replication::ReplicationStatus{.name = replication::kReservedReplicationRoleName,
                                       .ip_address = config.ip_address,
                                       .port = config.port,
                                       .sync_mode = replication::ReplicationMode::SYNC,
                                       .replica_check_frequency = std::chrono::seconds(0),
                                       .ssl = std::nullopt,
                                       .role = memgraph::replication::ReplicationRole::REPLICA});

    if (!durability_->Put(replication::kReservedReplicationRoleName, data.dump())) {
      spdlog::error("Error when saving REPLICA replication role in settings.");
      return false;
    }
  }

  SetRole(memgraph::replication::ReplicationRole::REPLICA);
  return true;
}

bool ReplicationStorageState::UnregisterReplica(std::string_view name) {
  MG_ASSERT(IsMain(), "Only main instance can unregister a replica!");
  if (ShouldStoreAndRestoreReplicationState()) {
    if (!durability_->Delete(name)) {
      spdlog::error("Error when removing replica {} from settings.", name);
      return false;
    }
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

void ReplicationStorageState::RestoreReplicationRole(Storage *storage) {
  if (!ShouldStoreAndRestoreReplicationState()) {
    return;
  }

  spdlog::info("Restoring replication role.");
  uint16_t port = replication::kDefaultReplicationPort;

  const auto replication_data = durability_->Get(replication::kReservedReplicationRoleName);
  if (!replication_data.has_value()) {
    spdlog::debug("Cannot find data needed for restore replication role in persisted metadata.");
    return;
  }

  const auto maybe_replication_status = replication::JSONToReplicationStatus(nlohmann::json::parse(*replication_data));
  if (!maybe_replication_status.has_value()) {
    LOG_FATAL("Cannot parse previously saved configuration of replication role {}.",
              replication::kReservedReplicationRoleName);
  }

  const auto replication_status = *maybe_replication_status;
  if (!replication_status.role.has_value()) {
    SetRole(memgraph::replication::ReplicationRole::MAIN);
  } else {
    SetRole(*replication_status.role);
    port = replication_status.port;
  }

  if (IsReplica()) {
    replication_server_ = storage->CreateReplicationServer(replication::ReplicationServerConfig{
        .ip_address = replication::kDefaultReplicationServerIp,
        .port = port,
    });
    bool res = replication_server_->Start();
    if (!res) {
      LOG_FATAL("Unable to start the replication server.");
    }
  }

  spdlog::info("Replication role restored to {}.", IsMain() ? "MAIN" : "REPLICA");
}

void ReplicationStorageState::RestoreReplicas(Storage *storage) {
  if (!ShouldStoreAndRestoreReplicationState()) {
    return;
  }
  spdlog::info("Restoring replicas.");

  for (const auto &[replica_name, replica_data] : *durability_) {
    spdlog::info("Restoring replica {}.", replica_name);

    const auto maybe_replica_status = replication::JSONToReplicationStatus(nlohmann::json::parse(replica_data));
    if (!maybe_replica_status.has_value()) {
      LOG_FATAL("Cannot parse previously saved configuration of replica {}.", replica_name);
    }

    auto replica_status = *maybe_replica_status;
    MG_ASSERT(replica_status.name == replica_name, "Expected replica name is '{}', but got '{}'", replica_status.name,
              replica_name);

    if (replica_name == replication::kReservedReplicationRoleName) {
      continue;
    }

    auto ret = RegisterReplica(replication::RegistrationMode::CAN_BE_INVALID,
                               replication::ReplicationClientConfig{
                                   .name = replica_status.name,
                                   .mode = replica_status.sync_mode,
                                   .ip_address = replica_status.ip_address,
                                   .port = replica_status.port,
                                   .replica_check_frequency = replica_status.replica_check_frequency,
                                   .ssl = replica_status.ssl,
                               },
                               storage);

    if (ret.HasError()) {
      MG_ASSERT(RegisterReplicaError::CONNECTION_FAILED != ret.GetError());
      LOG_FATAL("Failure when restoring replica {}: {}.", replica_name, RegisterReplicaErrorToString(ret.GetError()));
    }
    spdlog::info("Replica {} restored.", replica_name);
  }
}

constexpr uint16_t kEpochHistoryRetention = 1000;

void ReplicationStorageState::NewEpoch() {
  // Generate new epoch id and save the last one to the history.
  if (history.size() == kEpochHistoryRetention) {
    history.pop_front();
  }
  auto prevEpoch = epoch_.NewEpoch();
  history.emplace_back(std::move(prevEpoch), last_commit_timestamp_);
}

void ReplicationStorageState::AppendEpoch(std::string new_epoch) {
  auto prevEpoch = epoch_.SetEpoch(std::move(new_epoch));
  history.emplace_back(std::move(prevEpoch), last_commit_timestamp_);
}

}  // namespace memgraph::storage
