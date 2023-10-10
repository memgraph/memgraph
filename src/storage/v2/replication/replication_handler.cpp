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
  repl_state_.GetEpoch().NewEpoch();  // TODO: need Epoch to be part of the durability state
  if (!repl_state_.TryPersistRoleMain()) {
    // TODO: On failure restore old epoch? restore replication servers?
    return false;
  }
  repl_state_.ReplicationData() = memgraph::replication::ReplicationState::ReplicationDataMain_t{};
  return true;
}

bool ReplicationHandler::SetReplicationRoleReplica(const memgraph::replication::ReplicationServerConfig &config) {
  // We don't want to restart the server if we're already a REPLICA
  if (repl_state_.IsReplica()) {
    return false;
  }

  std::unique_ptr<ReplicationServer> replication_server =
      storage_.CreateReplicationServer(config, &repl_state_.GetEpoch());
  bool res = replication_server->Start();
  if (!res) {
    spdlog::error("Unable to start the replication server.");
    return false;
  }
  storage_.repl_storage_state_.replication_server_ = std::move(replication_server);

  if (!repl_state_.TryPersistRoleReplica(config)) {
    return false;
  }

  repl_state_.ReplicationData() = memgraph::replication::ReplicationState::ReplicationDataReplica_t{config};
  return true;
}

auto ReplicationHandler::RegisterReplica(const RegistrationMode registration_mode,
                                         const memgraph::replication::ReplicationClientConfig &config)
    -> memgraph::utils::BasicResult<RegisterReplicaError> {
  auto &repl_data = repl_state_.ReplicationData();

  auto const replica_handler =
      [](ReplicationState::ReplicationDataReplica_t &) -> utils::BasicResult<RegisterReplicaError> {
    MG_ASSERT(false, "Only main instance can register a replica!");
    __builtin_unreachable();
  };
  auto const main_handler =
      [this, registration_mode,
       &config](ReplicationState::ReplicationDataMain_t &mainData) -> utils::BasicResult<RegisterReplicaError> {
    // TODO: decouple the RegisterReplica to be replica state agnostic during restore
    if (registration_mode != RegistrationMode::RESTORE) {
      // name check
      auto name_check = [&config](ReplicationState::ReplicationDataMain_t &replicas) {
        auto name_matches = [&name = config.name](ReplicationClientConfig const &registered_config) {
          return registered_config.name == name;
        };
        return std::any_of(replicas.begin(), replicas.end(), name_matches);
      };
      if (name_check(mainData)) {
        return RegisterReplicaError::NAME_EXISTS;
      }

      // endpoint check
      auto endpoint_check = [&](ReplicationState::ReplicationDataMain_t &replicas) {
        auto endpoint_matches = [&config](ReplicationClientConfig const &registered_config) {
          return registered_config.ip_address == config.ip_address && registered_config.port == config.port;
        };
        return std::any_of(replicas.begin(), replicas.end(), endpoint_matches);
      };
      if (endpoint_check(mainData)) {
        return RegisterReplicaError::END_POINT_EXISTS;
      }

      // Durability
      if (!repl_state_.TryPersistRegisteredReplica(config)) {
        return RegisterReplicaError::COULD_NOT_BE_PERSISTED;
      }

      // set
      mainData.emplace_back(config);
    }

    // TODO: Per storage
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
  };

  return std::visit(utils::Overloaded{main_handler, replica_handler}, repl_data);
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
