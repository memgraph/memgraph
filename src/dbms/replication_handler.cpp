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
#include "dbms/inmemory/replication_handlers.hpp"
#include "dbms/inmemory/storage_helper.hpp"
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

ReplicationHandler::ReplicationHandler(DbmsHandler &dbms_handler)
    : dbms_handler_(dbms_handler), repl_state_(dbms_handler_.ReplicationState()) {}

bool ReplicationHandler::SetReplicationRoleMain() {
  auto const main_handler = [](RoleMainData const &) {
    // If we are already MAIN, we don't want to change anything
    return false;
  };
  auto const replica_handler = [this](RoleReplicaData const &) {
    // STEP 1) bring down all REPLICA servers
    dbms_handler_.ForEach([](Database *db) {
      auto *storage = db->storage();
      // Remember old epoch + storage timestamp association
      storage->PrepareForNewEpoch();
    });

    // STEP 2) Change to MAIN
    // TODO: restore replication servers if false?
    if (!repl_state_.SetReplicationRoleMain()) {
      // TODO: Handle recovery on failure???
      return false;
    }

    // STEP 3) We are now MAIN, update storage local epoch
    const auto &epoch = std::get<RoleMainData>(std::as_const(repl_state_).ReplicationData()).epoch_;
    dbms_handler_.ForEach([&](Database *db) {
      auto *storage = db->storage();
      storage->repl_storage_state_.epoch_ = epoch;
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

  // Remove database specific clients
  dbms_handler_.ForEach([&](Database *db) {
    auto *storage = db->storage();
    storage->repl_storage_state_.replication_clients_.WithLock([](auto &clients) { clients.clear(); });
  });
  // Remove instance level clients
  std::get<RoleMainData>(repl_state_.ReplicationData()).registered_replicas_.clear();

  // Creates the server
  repl_state_.SetReplicationRoleReplica(config);

  // Start
  const auto success =
      std::visit(utils::Overloaded{[](RoleMainData const &) {
                                     // ASSERT
                                     return false;
                                   },
                                   [this](RoleReplicaData const &data) {
                                     // Register handlers
                                     InMemoryReplicationHandlers::Register(&dbms_handler_, *data.server);
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

  auto instance_client = repl_state_.RegisterReplica(config);
  if (instance_client.HasError()) switch (instance_client.GetError()) {
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
  // No client error, start instance level client
  instance_client.GetValue()->Start(&dbms_handler_);

  if (!allow_mt_repl && dbms_handler_.All().size() > 1) {
    spdlog::warn("Multi-tenant replication is currently not supported!");
  }

  bool all_clients_good = true;

  // Add database specific clients (NOTE Currently all databases are connected to each replica)
  dbms_handler_.ForEach([&](Database *db) {
    auto *storage = db->storage();
    if (!allow_mt_repl && storage->id() != kDefaultDB) {
      return;
    }
    // TODO: ATM only IN_MEMORY_TRANSACTIONAL, fix other modes
    if (storage->storage_mode_ != storage::StorageMode::IN_MEMORY_TRANSACTIONAL) return;

    all_clients_good &= storage->repl_storage_state_.replication_clients_.WithLock(
        [storage, &instance_client](auto &storage_clients) -> bool {
          auto client = storage->CreateReplicationClient(*instance_client.GetValue());
          client->Start(storage);

          if (client->State() == storage::replication::ReplicaState::MAYBE_BEHIND) {
            return false;
          }
          storage_clients.push_back(std::move(client));
          return true;
        });
  });

  // NOTE Currently if any databases fails, we revert back
  if (!all_clients_good) {
    spdlog::error("Failed to register all databases to the REPLICA \"{}\"", config.name);
    UnregisterReplica(config.name);
    return RegisterReplicaError::CONNECTION_FAILED;
  }
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
    // Remove database specific clients
    dbms_handler_.ForEach([name](Database *db) {
      db->storage()->repl_storage_state_.replication_clients_.WithLock([&name](auto &clients) {
        std::erase_if(clients, [name](const auto &client) { return client->Name() == name; });
      });
    });
    // Remove instance level clients
    auto const n_unregistered =
        std::erase_if(mainData.registered_replicas_, [name](auto const &client) { return client->name_ == name; });
    return n_unregistered != 0 ? UnregisterReplicaResult::SUCCESS : UnregisterReplicaResult::CAN_NOT_UNREGISTER;
  };

  return std::visit(utils::Overloaded{main_handler, replica_handler}, repl_state_.ReplicationData());
}

auto ReplicationHandler::GetRole() const -> memgraph::replication::ReplicationRole { return repl_state_.GetRole(); }

bool ReplicationHandler::IsMain() const { return repl_state_.IsMain(); }

bool ReplicationHandler::IsReplica() const { return repl_state_.IsReplica(); }

// Per storage
// NOTE Storage will connect to all replicas. Future work might change this
void RestoreReplication(replication::ReplicationState &repl_state, storage::Storage &storage) {
  spdlog::info("Restoring replication role.");

  /// MAIN
  auto const recover_main = [&storage](RoleMainData &mainData) {
    // Each individual client has already been restored and started. Here we just go through each database and start its
    // client
    for (auto &instance_client : mainData.registered_replicas_) {
      spdlog::info("Replica {} restoration started for {}.", instance_client->name_, storage.id());

      const auto &ret = storage.repl_storage_state_.replication_clients_.WithLock(
          [&](auto &storage_clients) -> utils::BasicResult<RegisterReplicaError> {
            auto client = storage.CreateReplicationClient(*instance_client);
            client->Start(&storage);

            if (client->State() == storage::replication::ReplicaState::MAYBE_BEHIND) {
              spdlog::warn("Connection failed when registering replica {}. Replica will still be registered.",
                           client->Name());
            }
            storage_clients.push_back(std::move(client));
            return {};
          });

      if (ret.HasError()) {
        MG_ASSERT(RegisterReplicaError::CONNECTION_FAILED != ret.GetError());
        LOG_FATAL("Failure when restoring replica {}: {}.", instance_client->name_,
                  RegisterReplicaErrorToString(ret.GetError()));
      }
      spdlog::info("Replica {} restored for {}.", instance_client->name_, storage.id());
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
      repl_state.ReplicationData());
}
}  // namespace memgraph::dbms
