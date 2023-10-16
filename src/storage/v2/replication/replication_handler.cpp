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

using memgraph::replication::ReplicationClientConfig;
using memgraph::replication::ReplicationState;

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
  // TODO: restore replication servers if false?
  return repl_state_.SetReplicationRoleMain();
}

bool ReplicationHandler::SetReplicationRoleReplica(const memgraph::replication::ReplicationServerConfig &config) {
  // We don't want to restart the server if we're already a REPLICA
  if (repl_state_.IsReplica()) {
    return false;
  }

  // Remove registered replicas
  storage_.repl_storage_state_.replication_clients_.WithLock([](auto &clients) { clients.clear(); });

  // Start
  std::unique_ptr<ReplicationServer> replication_server =
      storage_.CreateReplicationServer(config, &repl_state_.GetEpoch());
  if (!replication_server->Start()) {
    spdlog::error("Unable to start the replication server.");
    return false;
  }
  storage_.repl_storage_state_.replication_server_ = std::move(replication_server);

  // TODO: restore main on false?
  return repl_state_.SetReplicationRoleReplica(config);
}

auto ReplicationHandler::RegisterReplica(const RegistrationMode registration_mode,
                                         const memgraph::replication::ReplicationClientConfig &config)
    -> memgraph::utils::BasicResult<RegisterReplicaError> {
  MG_ASSERT(repl_state_.IsMain(), "Only main instance can register a replica!");

  if (registration_mode != RegistrationMode::RESTORE) {
    auto res = repl_state_.RegisterReplica(config);
    switch (res) {
      case memgraph::replication::RegisterReplicaError::NOT_MAIN:
        MG_ASSERT(false, "Only main instance can register a replica!");
        return {};
      case memgraph::replication::RegisterReplicaError::NAME_EXISTS:
        return memgraph::storage::RegisterReplicaError::NAME_EXISTS;
      case memgraph::replication::RegisterReplicaError::END_POINT_EXISTS:
        return memgraph::storage::RegisterReplicaError::END_POINT_EXISTS;
      case memgraph::replication::RegisterReplicaError::COULD_NOT_BE_PERSISTED:
        return memgraph::storage::RegisterReplicaError::COULD_NOT_BE_PERSISTED;
      case memgraph::replication::RegisterReplicaError::SUCCESS:
        break;
    }
  }
  return storage_.repl_storage_state_.replication_clients_.WithLock(
      [this, registration_mode, &config](auto &clients) -> utils::BasicResult<RegisterReplicaError> {
        auto client = storage_.CreateReplicationClient(config, &repl_state_.GetEpoch());
        client->Start();

        if (client->State() == replication::ReplicaState::INVALID) {
          if (registration_mode == RegistrationMode::MUST_BE_INSTANTLY_VALID) {
            return RegisterReplicaError::CONNECTION_FAILED;
          }

          spdlog::warn("Connection failed when registering replica {}. Replica will still be registered.",
                       client->Name());
        }
        clients.push_back(std::move(client));
        return {};
      });
}

auto ReplicationHandler::UnregisterReplica(std::string_view name) -> UnregisterReplicaResult {
  auto const replica_handler = [](ReplicationState::ReplicationDataReplica_t const &) -> UnregisterReplicaResult {
    return UnregisterReplicaResult::NOT_MAIN;
  };
  auto const main_handler = [this, name](ReplicationState::ReplicationDataMain_t &mainData) -> UnregisterReplicaResult {
    if (!repl_state_.TryPersistUnregisterReplica(name)) {
      return UnregisterReplicaResult::COULD_NOT_BE_PERSISTED;
    }
    auto const n_unregistered = std::erase_if(
        mainData, [&](ReplicationClientConfig const &registered_config) { return registered_config.name == name; });

    // don't care how many got erased
    storage_.repl_storage_state_.replication_clients_.WithLock(
        [&](auto &clients) { std::erase_if(clients, [&](const auto &client) { return client->Name() == name; }); });

    return n_unregistered != 0 ? UnregisterReplicaResult::SUCCESS : UnregisterReplicaResult::CAN_NOT_UNREGISTER;
  };

  return std::visit(utils::Overloaded{main_handler, replica_handler}, repl_state_.ReplicationData());
}

void ReplicationHandler::RestoreReplication() {
  if (!repl_state_.ShouldPersist()) {
    return;
  }

  spdlog::info("Restoring replication role.");

  /// MAIN
  auto const recover_main = [this](ReplicationState::ReplicationDataMain_t const &configs) {
    storage_.repl_storage_state_.replication_server_.reset();
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
  auto const recover_replica = [this](ReplicationState::ReplicationDataReplica_t const &config) {
    auto replication_server = storage_.CreateReplicationServer(config, &repl_state_.GetEpoch());
    if (!replication_server->Start()) {
      LOG_FATAL("Unable to start the replication server.");
    }
    storage_.repl_storage_state_.replication_server_ = std::move(replication_server);
    spdlog::info("Replication role restored to REPLICA.");
  };

  std::visit(
      utils::Overloaded{
          recover_main,
          recover_replica,
      },
      std::as_const(repl_state_.ReplicationData()));
}

auto ReplicationHandler::GetRole() const -> memgraph::replication::ReplicationRole { return repl_state_.GetRole(); }

bool ReplicationHandler::IsMain() const { return repl_state_.IsMain(); }

bool ReplicationHandler::IsReplica() const { return repl_state_.IsReplica(); }
}  // namespace memgraph::storage
