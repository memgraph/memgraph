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

#include "storage/v2/replication/replication_handler.hpp"

#include "replication/state.hpp"
#include "storage/v2/storage.hpp"

namespace memgraph::storage {

namespace {

std::string RegisterReplicaErrorToString(RegisterReplicaError error) {
  switch (error) {
    using enum RegisterReplicaError;
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

bool ReplicationHandler::SetReplicationRoleMain() {
  // We don't want to generate new epoch_id and do the
  // cleanup if we're already a MAIN
  // TODO: under lock
  if (repl_state_.IsMain()) {
    return false;
  }

  // STEP 1) bring down all REPLICA servers
  auto current_epoch = std::string(repl_state_.GetEpoch().id());
  {  // TODO: foreach storage
    // ensure replica server brought down
    storage_.repl_storage_state_.replication_server_.reset(nullptr);
    // Remember old epoch + storage timestamp association
    storage_.PrepareForNewEpoch(current_epoch);
  }

  // STEP 2) Change to MAIN
  repl_state_.GetEpoch().NewEpoch();
  if (!repl_state_.TryPersistRoleMain()) {
    // TODO: On failure restore old epoch? restore replication servers?
    return false;
  }
  repl_state_.SetRole(memgraph::replication::ReplicationRole::MAIN);
  return true;
}
memgraph::utils::BasicResult<RegisterReplicaError> ReplicationHandler::RegisterReplica(
    const RegistrationMode registration_mode, const memgraph::replication::ReplicationClientConfig &config) {
  MG_ASSERT(repl_state_.IsMain(), "Only main instance can register a replica!");

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

    using enum RegistrationMode;
    if (registration_mode != RESTORE && !repl_state_.TryPersistRegisteredReplica(config)) {
      return RegisterReplicaError::COULD_NOT_BE_PERSISTED;
    }

    auto client = storage_.CreateReplicationClient(config);
    client->Start();

    if (client->State() == replication::ReplicaState::INVALID) {
      if (registration_mode != RESTORE) {
        return RegisterReplicaError::CONNECTION_FAILED;
      }

      spdlog::warn("Connection failed when registering replica {}. Replica will still be registered.", client->Name());
    }

    clients.push_back(std::move(client));
    return {};
  };

  return storage_.repl_storage_state_.replication_clients_.WithLock(task);
}
bool ReplicationHandler::SetReplicationRoleReplica(const memgraph::replication::ReplicationServerConfig &config) {
  // We don't want to restart the server if we're already a REPLICA
  if (repl_state_.IsReplica()) {
    return false;
  }

  std::unique_ptr<ReplicationServer> replication_server = storage_.CreateReplicationServer(config);
  bool res = replication_server->Start();
  if (!res) {
    spdlog::error("Unable to start the replication server.");
    return false;
  }
  storage_.repl_storage_state_.replication_server_ = std::move(replication_server);

  if (!repl_state_.TryPersistRoleReplica(config)) {
    return false;
  }

  repl_state_.SetRole(memgraph::replication::ReplicationRole::REPLICA);
  return true;
}
auto ReplicationHandler::UnregisterReplica(std::string_view name) -> UnregisterReplicaResult {
  if (repl_state_.IsReplica()) {
    return UnregisterReplicaResult::NOT_MAIN;
  }

  if (!repl_state_.TryPersistUnregisterReplica(name)) {
    return UnregisterReplicaResult::COULD_NOT_BE_PERSISTED;
  }

  auto const n_unregistered = storage_.repl_storage_state_.replication_clients_.WithLock([&](auto &clients) {
    return std::erase_if(clients, [&](const auto &client) { return client->Name() == name; });
  });
  return (n_unregistered != 0) ? UnregisterReplicaResult::SUCCESS : UnregisterReplicaResult::CAN_NOT_UNREGISTER;
}
void ReplicationHandler::RestoreReplication() {
  if (!repl_state_.ShouldPersist()) {
    return;
  }

  spdlog::info("Restoring replication role.");

  using memgraph::replication::ReplicationState;

  auto replicationData = repl_state_.FetchReplicationData();
  if (replicationData.HasError()) {
    switch (replicationData.GetError()) {
      using enum ReplicationState::FetchReplicationError;
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

  /// MAIN
  auto const recover_main = [this](ReplicationState::ReplicationDataMain const &configs) {
    storage_.repl_storage_state_.replication_server_.reset();
    repl_state_.SetRole(memgraph::replication::ReplicationRole::MAIN);
    for (const auto &config : configs) {
      spdlog::info("Replica {} restored for {}.", config.name, storage_.id());
      auto ret = RegisterReplica(RegistrationMode::RESTORE, config);
      if (ret.HasError()) {
        MG_ASSERT(RegisterReplicaError::CONNECTION_FAILED != ret.GetError());
        LOG_FATAL("Failure when restoring replica {}: {}.", config.name, RegisterReplicaErrorToString(ret.GetError()));
      }
      spdlog::info("Replica {} restored for {}.", config.name, storage_.id());
    }
    spdlog::info("Replication role restored to MAIN.");
  };

  /// REPLICA
  auto const recover_replica = [this](ReplicationState::ReplicationDataReplica const &config) {
    auto replication_server = storage_.CreateReplicationServer(config);
    if (!replication_server->Start()) {
      LOG_FATAL("Unable to start the replication server.");
    }
    storage_.repl_storage_state_.replication_server_ = std::move(replication_server);
    repl_state_.SetRole(memgraph::replication::ReplicationRole::REPLICA);
    spdlog::info("Replication role restored to REPLICA.");
  };

  std::visit(
      utils::Overloaded{
          recover_main,
          recover_replica,
      },
      *replicationData);
}
auto ReplicationHandler::GetRole() const -> memgraph::replication::ReplicationRole { return repl_state_.GetRole(); }
bool ReplicationHandler::IsMain() const { return repl_state_.IsMain(); }
bool ReplicationHandler::IsReplica() const { return repl_state_.IsReplica(); }
}  // namespace memgraph::storage
