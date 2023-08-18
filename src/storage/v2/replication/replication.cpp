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

#include "storage/v2/replication/replication.hpp"
#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/edge_direction.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/stat.hpp"

/// REPLICATION ///
#include "storage/v2/replication/replication_client.hpp"
#include "storage/v2/replication/replication_server.hpp"
#include "storage/v2/replication/rpc.hpp"
#include "storage/v2/storage_error.hpp"

#include "storage/v2/inmemory/replication/replication_client.hpp"
#include "storage/v2/inmemory/replication/replication_server.hpp"

namespace memgraph::storage {

using OOMExceptionEnabler = utils::MemoryTracker::OutOfMemoryExceptionEnabler;

namespace {

static std::string RegisterReplicaErrorToString(ReplicationState::RegisterReplicaError error) {
  using enum ReplicationState::RegisterReplicaError;
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

storage::ReplicationState::ReplicationState(bool restore, std::filesystem::path durability_dir) {
  if (restore) {
    utils::EnsureDirOrDie(durability_dir / durability::kReplicationDirectory);
    durability_ = std::make_unique<kvstore::KVStore>(durability_dir / durability::kReplicationDirectory);
  }
}

void storage::ReplicationState::Reset() {
  replication_server_.reset();
  replication_clients_.WithLock([&](auto &clients) { clients.clear(); });
}

bool storage::ReplicationState::SetMainReplicationRole(storage::Storage *storage) {
  // We don't want to generate new epoch_id and do the
  // cleanup if we're already a MAIN
  if (GetRole() == replication::ReplicationRole::MAIN) {
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
                                       .role = replication::ReplicationRole::MAIN});

    if (!durability_->Put(replication::kReservedReplicationRoleName, data.dump())) {
      spdlog::error("Error when saving MAIN replication role in settings.");
      return false;
    }
  }

  SetRole(replication::ReplicationRole::MAIN);
  return true;
}

bool storage::ReplicationState::AppendToWalDataDefinition(const uint64_t seq_num,
                                                          durability::StorageGlobalOperation operation, LabelId label,
                                                          const std::set<PropertyId> &properties,
                                                          uint64_t final_commit_timestamp) {
  bool finalized_on_all_replicas = true;
  // TODO Should we return true if not MAIN?
  if (GetRole() == replication::ReplicationRole::MAIN) {
    replication_clients_.WithLock([&](auto &clients) {
      for (auto &client : clients) {
        client->StartTransactionReplication(seq_num);
        client->IfStreamingTransaction(
            [&](auto &stream) { stream.AppendOperation(operation, label, properties, final_commit_timestamp); });

        const auto finalized = client->FinalizeTransactionReplication();
        if (client->Mode() == replication::ReplicationMode::SYNC) {
          finalized_on_all_replicas = finalized && finalized_on_all_replicas;
        }
      }
    });
  }
  return finalized_on_all_replicas;
}

void storage::ReplicationState::InitializeTransaction(uint64_t seq_num) {
  if (GetRole() == replication::ReplicationRole::MAIN) {
    replication_clients_.WithLock([&](auto &clients) {
      for (auto &client : clients) {
        client->StartTransactionReplication(seq_num);
      }
    });
  }
}

void storage::ReplicationState::AppendDelta(const Delta &delta, const Edge &parent, uint64_t timestamp) {
  replication_clients_.WithLock([&](auto &clients) {
    for (auto &client : clients) {
      client->IfStreamingTransaction([&](auto &stream) { stream.AppendDelta(delta, parent, timestamp); });
    }
  });
}

void storage::ReplicationState::AppendDelta(const Delta &delta, const Vertex &parent, uint64_t timestamp) {
  replication_clients_.WithLock([&](auto &clients) {
    for (auto &client : clients) {
      client->IfStreamingTransaction([&](auto &stream) { stream.AppendDelta(delta, parent, timestamp); });
    }
  });
}

bool storage::ReplicationState::FinalizeTransaction(uint64_t timestamp) {
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

utils::BasicResult<ReplicationState::RegisterReplicaError> ReplicationState::RegisterReplica(
    std::string name, io::network::Endpoint endpoint, const replication::ReplicationMode replication_mode,
    const replication::RegistrationMode registration_mode, const replication::ReplicationClientConfig &config,
    InMemoryStorage *storage) {
  MG_ASSERT(GetRole() == replication::ReplicationRole::MAIN, "Only main instance can register a replica!");

  const bool name_exists = replication_clients_.WithLock([&](auto &clients) {
    return std::any_of(clients.begin(), clients.end(), [&name](const auto &client) { return client->Name() == name; });
  });

  if (name_exists) {
    return RegisterReplicaError::NAME_EXISTS;
  }

  const auto end_point_exists = replication_clients_.WithLock([&endpoint](auto &clients) {
    return std::any_of(clients.begin(), clients.end(),
                       [&endpoint](const auto &client) { return client->Endpoint() == endpoint; });
  });

  if (end_point_exists) {
    return RegisterReplicaError::END_POINT_EXISTS;
  }

  if (ShouldStoreAndRestoreReplicationState()) {
    auto data = replication::ReplicationStatusToJSON(
        replication::ReplicationStatus{.name = name,
                                       .ip_address = endpoint.address,
                                       .port = endpoint.port,
                                       .sync_mode = replication_mode,
                                       .replica_check_frequency = config.replica_check_frequency,
                                       .ssl = config.ssl,
                                       .role = replication::ReplicationRole::REPLICA});
    if (!durability_->Put(name, data.dump())) {
      spdlog::error("Error when saving replica {} in settings.", name);
      return RegisterReplicaError::COULD_NOT_BE_PERSISTED;
    }
  }

  auto client =
      std::make_unique<InMemoryReplicationClient>(std::move(name), storage, endpoint, replication_mode, config);
  client->Start();

  if (client->State() == replication::ReplicaState::INVALID) {
    if (replication::RegistrationMode::CAN_BE_INVALID != registration_mode) {
      return RegisterReplicaError::CONNECTION_FAILED;
    }

    spdlog::warn("Connection failed when registering replica {}. Replica will still be registered.", client->Name());
  }

  return replication_clients_.WithLock(
      [&](auto &clients) -> utils::BasicResult<ReplicationState::RegisterReplicaError> {
        // Another thread could have added a client with same name while
        // we were connecting to this client.
        if (std::any_of(clients.begin(), clients.end(),
                        [&](const auto &other_client) { return client->Name() == other_client->Name(); })) {
          return RegisterReplicaError::NAME_EXISTS;
        }

        if (std::any_of(clients.begin(), clients.end(), [&client](const auto &other_client) {
              return client->Endpoint() == other_client->Endpoint();
            })) {
          return RegisterReplicaError::END_POINT_EXISTS;
        }

        clients.push_back(std::move(client));
        return {};
      });
}

bool ReplicationState::SetReplicaRole(io::network::Endpoint endpoint,
                                      const replication::ReplicationServerConfig &config, InMemoryStorage *storage) {
  // We don't want to restart the server if we're already a REPLICA
  if (GetRole() == replication::ReplicationRole::REPLICA) {
    return false;
  }

  auto port = endpoint.port;  // assigning because we will move the endpoint
  replication_server_ = std::make_unique<InMemoryReplicationServer>(storage, std::move(endpoint), config);
  bool res = replication_server_->Start();
  if (!res) {
    spdlog::error("Unable to start the replication server.");
    return false;
  }

  if (ShouldStoreAndRestoreReplicationState()) {
    // Only thing that matters here is the role saved as REPLICA and the listening port
    auto data = replication::ReplicationStatusToJSON(
        replication::ReplicationStatus{.name = replication::kReservedReplicationRoleName,
                                       .ip_address = "",
                                       .port = port,
                                       .sync_mode = replication::ReplicationMode::SYNC,
                                       .replica_check_frequency = std::chrono::seconds(0),
                                       .ssl = std::nullopt,
                                       .role = replication::ReplicationRole::REPLICA});

    if (!durability_->Put(replication::kReservedReplicationRoleName, data.dump())) {
      spdlog::error("Error when saving REPLICA replication role in settings.");
      return false;
    }
  }

  SetRole(replication::ReplicationRole::REPLICA);
  return true;
}

bool ReplicationState::UnregisterReplica(std::string_view name) {
  MG_ASSERT(GetRole() == replication::ReplicationRole::MAIN, "Only main instance can unregister a replica!");
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

std::optional<replication::ReplicaState> ReplicationState::GetReplicaState(const std::string_view name) {
  return replication_clients_.WithLock([&](auto &clients) -> std::optional<replication::ReplicaState> {
    const auto client_it =
        std::find_if(clients.cbegin(), clients.cend(), [name](auto &client) { return client->Name() == name; });
    if (client_it == clients.cend()) {
      return std::nullopt;
    }
    return (*client_it)->State();
  });
}

std::vector<ReplicaInfo> ReplicationState::ReplicasInfo() {
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

void ReplicationState::RestoreReplicationRole(InMemoryStorage *storage) {
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
    SetRole(replication::ReplicationRole::MAIN);
  } else {
    SetRole(*replication_status.role);
    port = replication_status.port;
  }

  if (GetRole() == replication::ReplicationRole::REPLICA) {
    io::network::Endpoint endpoint(replication::kDefaultReplicationServerIp, port);
    replication_server_ = std::make_unique<InMemoryReplicationServer>(storage, std::move(endpoint),
                                                                      replication::ReplicationServerConfig{});
    bool res = replication_server_->Start();
    if (!res) {
      LOG_FATAL("Unable to start the replication server.");
    }
  }

  spdlog::info("Replication role restored to {}.",
               GetRole() == replication::ReplicationRole::MAIN ? "MAIN" : "REPLICA");
}

void ReplicationState::RestoreReplicas(InMemoryStorage *storage) {
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

    auto ret =
        RegisterReplica(std::move(replica_status.name), {std::move(replica_status.ip_address), replica_status.port},
                        replica_status.sync_mode, replication::RegistrationMode::CAN_BE_INVALID,
                        {
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

}  // namespace memgraph::storage
