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

#include "dbms/replication_handler.hpp"

#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "dbms/inmemory/replication_server.hpp"
#include "replication/state.hpp"

using memgraph::replication::ReplicationClientConfig;
using memgraph::replication::ReplicationState;
using memgraph::replication::RoleMainData;
using memgraph::replication::RoleReplicaData;

namespace memgraph::dbms {

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
  auto const main_handler = [](RoleMainData const &) {
    // If we are already MAIN, we don't want to change anything
    return false;
  };
  auto const replica_handler = [this](RoleReplicaData const &) {
    dbms_handler_.ForEach([](storage::Storage &storage) {
      // STEP 1) bring down all REPLICA servers
      // TODO: foreach storage
      {
        // ensure replica server brought down
        storage.repl_storage_state_.replication_server_.reset(nullptr);
        // Remember old epoch + storage timestamp association
        storage.PrepareForNewEpoch();
      }
    });

    // STEP 2) Change to MAIN
    // TODO: restore replication servers if false?
    if (!repl_state_.SetReplicationRoleMain()) {
      // TODO: Handle recovery on failure???
      return false;
    }

    // STEP 3) We are now MAIN, update storage local epoch
    dbms_handler_.ForEach([&](storage::Storage &storage) {
      storage.repl_storage_state_.epoch_ = std::get<RoleMainData>(std::as_const(repl_state_).ReplicationData()).epoch_;
    });

    return true;
  };

  // TODO: under lock
  return std::visit(utils::Overloaded{main_handler, replica_handler}, repl_state_.ReplicationData());
}

bool ReplicationHandler::SetReplicationRoleReplica(const memgraph::replication::ReplicationServerConfig &config) {
  // We don't want to restart the server if we're already a REPLICA
  if (repl_state_.IsReplica()) {
    return false;
  }

  // Remove registered replicas
  dbms_handler_.ForEach([&](storage::Storage &storage) {
    storage.repl_storage_state_.replication_clients_.WithLock([](auto &clients) { clients.clear(); });
  });

  // Creates the server
  repl_state_.SetReplicationRoleReplica(config);

  // Start
  const auto success =
      std::visit(utils::Overloaded{[](auto) {
                                     // ASSERT
                                     return false;
                                   },
                                   [this](RoleReplicaData const &data) {
                                     // Register handlers
                                     storage::InMemoryReplicationServer::Register(&dbms_handler_, *data.server);
                                     if (!data.server->Start()) {
                                       spdlog::error("Unable to start the replication server.");
                                       return false;
                                     }
                                     return true;
                                   }},
                 repl_state_.ReplicationData());
  // TODO Handle error (restore to main?)
  return success;
}

auto ReplicationHandler::RegisterReplica(const memgraph::replication::ReplicationClientConfig &config)
    -> memgraph::utils::BasicResult<RegisterReplicaError> {
  MG_ASSERT(repl_state_.IsMain(), "Only main instance can register a replica!");

  auto res = repl_state_.RegisterReplica(config);
  switch (res) {
    case memgraph::replication::RegisterReplicaError::NOT_MAIN:
      MG_ASSERT(false, "Only main instance can register a replica!");
      return {};
    case memgraph::replication::RegisterReplicaError::NAME_EXISTS:
      return memgraph::dbms::RegisterReplicaError::NAME_EXISTS;
    case memgraph::replication::RegisterReplicaError::END_POINT_EXISTS:
      return memgraph::dbms::RegisterReplicaError::END_POINT_EXISTS;
    case memgraph::replication::RegisterReplicaError::COULD_NOT_BE_PERSISTED:
      return memgraph::dbms::RegisterReplicaError::COULD_NOT_BE_PERSISTED;
    case memgraph::replication::RegisterReplicaError::SUCCESS:
      break;
  }

  bool ok = true;

  dbms_handler_.ForEach([&](Database *db) {
    auto *storage = db->storage();
    // ATM only IN_MEMORY_TRANSACTIONAL
    if (storage->storage_mode_ != storage::StorageMode::IN_MEMORY_TRANSACTIONAL)
      return;  // SKIP non-IN_MEMORY_TRANSACTIONAL

    ok &= storage->repl_storage_state_.replication_clients_.WithLock(
        [storage, &config](auto &clients) -> /*utils::BasicResult<RegisterReplicaError>*/ bool {
          auto client = storage->CreateReplicationClient(config, &storage->repl_storage_state_.epoch_);
          client->Start();

          if (client->State() == storage::replication::ReplicaState::INVALID) {
            return false;  // RegisterReplicaError::CONNECTION_FAILED;

            spdlog::warn("Connection failed when registering replica {}. Replica will still be registered.",
                         client->Name());
          }
          clients.push_back(std::move(client));
          return true;  //{};
        });
  });
  if (!ok) return RegisterReplicaError::CONNECTION_FAILED;  // TODO: this happen to 1 or many...what to do
  return {};
}

auto ReplicationHandler::UnregisterReplica(std::string_view name) -> UnregisterReplicaResult {
  auto const replica_handler = [](RoleReplicaData const &) -> UnregisterReplicaResult {
    return UnregisterReplicaResult::NOT_MAIN;
  };
  auto const main_handler = [this, name](RoleMainData &mainData) -> UnregisterReplicaResult {
    if (!repl_state_.TryPersistUnregisterReplica(name)) {
      return UnregisterReplicaResult::COULD_NOT_BE_PERSISTED;
    }
    auto const n_unregistered =
        std::erase_if(mainData.registered_replicas_,
                      [&](ReplicationClientConfig const &registered_config) { return registered_config.name == name; });

    dbms_handler_.ForEach([&](Database *db) {
      auto *storage = db->storage();
      // don't care how many got erased
      storage->repl_storage_state_.replication_clients_.WithLock(
          [&](auto &clients) { std::erase_if(clients, [&](const auto &client) { return client->Name() == name; }); });
    });

    return n_unregistered != 0 ? UnregisterReplicaResult::SUCCESS : UnregisterReplicaResult::CAN_NOT_UNREGISTER;
  };

  return std::visit(utils::Overloaded{main_handler, replica_handler}, repl_state_.ReplicationData());
}

auto ReplicationHandler::GetRole() const -> memgraph::replication::ReplicationRole { return repl_state_.GetRole(); }

bool ReplicationHandler::IsMain() const { return repl_state_.IsMain(); }

bool ReplicationHandler::IsReplica() const { return repl_state_.IsReplica(); }

void RestoreReplication(const replication::ReplicationState &repl_state, storage::Storage &storage) {
  if (!repl_state.ShouldPersist()) {
    return;
  }
  spdlog::info("Restoring replication role.");

  /// MAIN
  auto const recover_main = [&storage](RoleMainData const &mainData) {
    storage.repl_storage_state_.replication_server_.reset();
    for (const auto &config : mainData.registered_replicas_) {
      spdlog::info("Replica {} restored for {}.", config.name, storage.id());

      auto register_replica = [&storage](const memgraph::replication::ReplicationClientConfig &config)
          -> memgraph::utils::BasicResult<RegisterReplicaError> {
        return storage.repl_storage_state_.replication_clients_.WithLock(
            [&storage, &config](auto &clients) -> utils::BasicResult<RegisterReplicaError> {
              auto client = storage.CreateReplicationClient(config, &storage.repl_storage_state_.epoch_);
              client->Start();

              if (client->State() == storage::replication::ReplicaState::INVALID) {
                spdlog::warn("Connection failed when registering replica {}. Replica will still be registered.",
                             client->Name());
              }
              clients.push_back(std::move(client));
              return {};
            });
      };

      auto ret = register_replica(config);
      if (ret.HasError()) {
        MG_ASSERT(RegisterReplicaError::CONNECTION_FAILED != ret.GetError());
        LOG_FATAL("Failure when restoring replica {}: {}.", config.name, RegisterReplicaErrorToString(ret.GetError()));
      }
      spdlog::info("Replica {} restored for {}.", config.name, storage.id());
    }
    spdlog::info("Replication role restored to MAIN.");
  };

  /// REPLICA
  auto const recover_replica = [](RoleReplicaData const &data) { /*nothing to do*/ };

  std::visit(
      utils::Overloaded{
          recover_main,
          recover_replica,
      },
      std::as_const(repl_state).ReplicationData());
}
}  // namespace memgraph::dbms
