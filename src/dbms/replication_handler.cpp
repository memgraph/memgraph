// Copyright 2024 Memgraph Ltd.
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

#include <algorithm>

#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "dbms/inmemory/replication_handlers.hpp"
#include "dbms/replication_client.hpp"
#include "replication/state.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/replication/rpc.hpp"

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

ReplicationHandler::ReplicationHandler(DbmsHandler &dbms_handler) : dbms_handler_(dbms_handler) {}

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
    if (!dbms_handler_.ReplicationState().SetReplicationRoleMain()) {
      // TODO: Handle recovery on failure???
      return false;
    }

    // STEP 3) We are now MAIN, update storage local epoch
    const auto &epoch =
        std::get<RoleMainData>(std::as_const(dbms_handler_.ReplicationState()).ReplicationData()).epoch_;
    dbms_handler_.ForEach([&](Database *db) {
      auto *storage = db->storage();
      storage->repl_storage_state_.epoch_ = epoch;
    });

    return true;
  };

  // TODO: under lock
  return std::visit(utils::Overloaded{main_handler, replica_handler},
                    dbms_handler_.ReplicationState().ReplicationData());
}

bool ReplicationHandler::SetReplicationRoleReplica(const memgraph::replication::ReplicationServerConfig &config) {
  // We don't want to restart the server if we're already a REPLICA
  if (dbms_handler_.ReplicationState().IsReplica()) {
    return false;
  }

  // TODO StorageState needs to be synched. Could have a dangling reference if someone adds a database as we are
  //      deleting the replica.
  // Remove database specific clients
  dbms_handler_.ForEach([&](Database *db) {
    auto *storage = db->storage();
    storage->repl_storage_state_.replication_clients_.WithLock([](auto &clients) { clients.clear(); });
  });
  // Remove instance level clients
  std::get<RoleMainData>(dbms_handler_.ReplicationState().ReplicationData()).registered_replicas_.clear();

  // Creates the server
  dbms_handler_.ReplicationState().SetReplicationRoleReplica(config);

  // Start
  const auto success =
      std::visit(utils::Overloaded{[](RoleMainData const &) {
                                     // ASSERT
                                     return false;
                                   },
                                   [this](RoleReplicaData const &data) {
                                     // Register handlers
                                     InMemoryReplicationHandlers::Register(&dbms_handler_, *data.server);
                                     RegisterSystemRPC(data, dbms_handler_);
                                     if (!data.server->Start()) {
                                       spdlog::error("Unable to start the replication server.");
                                       return false;
                                     }
                                     return true;
                                   }},
                 dbms_handler_.ReplicationState().ReplicationData());
  // TODO Handle error (restore to main?)
  return success;
}

auto ReplicationHandler::RegisterReplica(const memgraph::replication::ReplicationClientConfig &config)
    -> memgraph::utils::BasicResult<RegisterReplicaError> {
  MG_ASSERT(dbms_handler_.ReplicationState().IsMain(), "Only main instance can register a replica!");

  auto instance_client = dbms_handler_.ReplicationState().RegisterReplica(config);
  if (instance_client.HasError()) {
    switch (instance_client.GetError()) {
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
  }

  if (!allow_mt_repl && dbms_handler_.All().size() > 1) {
    spdlog::warn("Multi-tenant replication is currently not supported!");
  } else {
#ifdef MG_ENTERPRISE
    // Update system before enabling individual storage <-> replica clients
    dbms_handler_.SystemRestore(*instance_client.GetValue());
#endif
  }

  bool all_clients_good = true;

  // Add database specific clients (NOTE Currently all databases are connected to each replica)
  dbms_handler_.ForEach([&](DatabaseAccess db_acc) {
    auto *storage = db_acc->storage();
    if (!allow_mt_repl && storage->name() != kDefaultDB) {
      return;
    }
    // TODO: ATM only IN_MEMORY_TRANSACTIONAL, fix other modes
    if (storage->storage_mode_ != storage::StorageMode::IN_MEMORY_TRANSACTIONAL) return;

    all_clients_good &= storage->repl_storage_state_.replication_clients_.WithLock(
        [storage, &instance_client, db_acc = std::move(db_acc)](auto &storage_clients) mutable {  // NOLINT
          auto client = std::make_unique<storage::ReplicationStorageClient>(*instance_client.GetValue());
          // All good, start replica client
          client->Start(storage, std::move(db_acc));
          // After start the storage <-> replica state should be READY or RECOVERING (if correctly started)
          // MAYBE_BEHIND isn't a statement of the current state, this is the default value
          // Failed to start due an error like branching of MAIN and REPLICA
          if (client->State() == storage::replication::ReplicaState::MAYBE_BEHIND) {
            return false;
          }
          storage_clients.push_back(std::move(client));
          return true;
        });
  });

  // NOTE Currently if any databases fails, we revert back
  if (!all_clients_good) {
    spdlog::error("Failed to register all databases on the REPLICA \"{}\"", config.name);
    UnregisterReplica(config.name);
    return RegisterReplicaError::CONNECTION_FAILED;
  }

  // No client error, start instance level client
  StartReplicaClient(dbms_handler_, *instance_client.GetValue());
  return {};
}

auto ReplicationHandler::UnregisterReplica(std::string_view name) -> UnregisterReplicaResult {
  auto const replica_handler = [](RoleReplicaData const &) -> UnregisterReplicaResult {
    return UnregisterReplicaResult::NOT_MAIN;
  };
  auto const main_handler = [this, name](RoleMainData &mainData) -> UnregisterReplicaResult {
    if (!dbms_handler_.ReplicationState().TryPersistUnregisterReplica(name)) {
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
        std::erase_if(mainData.registered_replicas_, [name](auto const &client) { return client.name_ == name; });
    return n_unregistered != 0 ? UnregisterReplicaResult::SUCCESS : UnregisterReplicaResult::CAN_NOT_UNREGISTER;
  };

  return std::visit(utils::Overloaded{main_handler, replica_handler},
                    dbms_handler_.ReplicationState().ReplicationData());
}

auto ReplicationHandler::GetRole() const -> memgraph::replication::ReplicationRole {
  return dbms_handler_.ReplicationState().GetRole();
}

bool ReplicationHandler::IsMain() const { return dbms_handler_.ReplicationState().IsMain(); }

bool ReplicationHandler::IsReplica() const { return dbms_handler_.ReplicationState().IsReplica(); }

// Per storage
// NOTE Storage will connect to all replicas. Future work might change this
void RestoreReplication(replication::ReplicationState &repl_state, DatabaseAccess db_acc) {
  spdlog::info("Restoring replication role.");

  /// MAIN
  auto const recover_main = [db_acc = std::move(db_acc)](RoleMainData &mainData) mutable {  // NOLINT
    // Each individual client has already been restored and started. Here we just go through each database and start its
    // client
    for (auto &instance_client : mainData.registered_replicas_) {
      spdlog::info("Replica {} restoration started for {}.", instance_client.name_, db_acc->name());

      // Phase 1: ensure DB exists
      // auto s = instance_client.rpc_client_.Stream<memgraph::replication::????>();

      // Phase 2: ensure storage is in sync
      const auto &ret = db_acc->storage()->repl_storage_state_.replication_clients_.WithLock(
          [&, db_acc](auto &storage_clients) mutable -> utils::BasicResult<RegisterReplicaError> {
            // ONLY VALID if storage.id() exists on REPLICA
            auto client = std::make_unique<storage::ReplicationStorageClient>(instance_client);
            auto *storage = db_acc->storage();
            client->Start(storage, std::move(db_acc));
            // After start the storage <-> replica state should be READY or RECOVERING (if correctly started)
            // MAYBE_BEHIND isn't a statement of the current state, this is the default value
            // Failed to start due to branching of MAIN and REPLICA
            if (client->State() == storage::replication::ReplicaState::MAYBE_BEHIND) {
              spdlog::warn("Connection failed when registering replica {}. Replica will still be registered.",
                           instance_client.name_);
            }
            storage_clients.push_back(std::move(client));
            return {};
          });

      if (ret.HasError()) {
        MG_ASSERT(RegisterReplicaError::CONNECTION_FAILED != ret.GetError());
        LOG_FATAL("Failure when restoring replica {}: {}.", instance_client.name_,
                  RegisterReplicaErrorToString(ret.GetError()));
      }
      spdlog::info("Replica {} restored for {}.", instance_client.name_, db_acc->name());
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

#ifdef MG_ENTERPRISE
void CreateDatabaseHandler(DbmsHandler &dbms_handler, slk::Reader *req_reader, slk::Builder *res_builder) {
  memgraph::storage::replication::CreateDatabaseReq req;
  memgraph::slk::Load(&req, req_reader);

  namespace sr = memgraph::storage::replication;
  sr::CreateDatabaseRes res(sr::CreateDatabaseRes::Result::FAILURE);

  // TODO
  // Check epoch
  // Check ts

  try {
    // Create new
    auto new_db = dbms_handler.New(req.config);
    if (new_db.HasValue()) {
      // Successfully create db
      res = sr::CreateDatabaseRes(sr::CreateDatabaseRes::Result::SUCCESS);
    }
  } catch (...) {
    // Failure
  }

  memgraph::slk::Save(res, res_builder);
}

void SystemHeartbeatHandler(const uint64_t ts, slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::SystemHeartbeatReq req;
  replication::SystemHeartbeatReq::Load(&req, req_reader);
  memgraph::slk::Load(&req, req_reader);
  replication::SystemHeartbeatRes res(ts);
  memgraph::slk::Save(res, res_builder);
}

void SystemRecoveryHandler(DbmsHandler &dbms_handler, slk::Reader *req_reader, slk::Builder *res_builder) {
  // TODO Speed up
  memgraph::storage::replication::SystemRecoveryReq req;
  memgraph::slk::Load(&req, req_reader);

  namespace sr = memgraph::storage::replication;
  sr::SystemRecoveryRes res(sr::SystemRecoveryRes::Result::FAILURE);

  try {
    // Get all current dbs
    auto old = dbms_handler.All();
    // Check/create the incoming dbs
    for (const auto &config : req.database_configs) {
      try {
        dbms_handler.Get(config.uuid);
      } catch (const UnknownDatabaseException &) {
        // Missing db
        if (dbms_handler.New(config).HasError()) {
          spdlog::debug("Failed to create new database \"{}\".", config.name);
          throw;
        }
      }
      const auto it = std::find(old.begin(), old.end(), config.name);
      if (it != old.end()) old.erase(it);
    }
    // Delete all the leftover old dbs
    for (const auto &remove_db : old) {
      if (dbms_handler.Delete(remove_db).HasError()) {
        spdlog::debug("Failed to drop database \"{}\".", remove_db);
        throw;
      }
    }
    // Successfully recovered
    res = sr::SystemRecoveryRes(sr::SystemRecoveryRes::Result::SUCCESS);
  } catch (...) {
    // Failure
  }

  memgraph::slk::Save(res, res_builder);
}
#endif

void RegisterSystemRPC(replication::RoleReplicaData const &data, dbms::DbmsHandler &dbms_handler) {
#ifdef MG_ENTERPRISE
  data.server->rpc_server_.Register<storage::replication::CreateDatabaseRpc>(
      [&dbms_handler](auto *req_reader, auto *res_builder) {
        spdlog::debug("Received CreateDatabaseRpc");
        CreateDatabaseHandler(dbms_handler, req_reader, res_builder);
      });
  data.server->rpc_server_.Register<replication::SystemHeartbeatRpc>(
      [&dbms_handler](auto *req_reader, auto *res_builder) {
        spdlog::debug("Received SystemHeartbeatRpc");
        SystemHeartbeatHandler(dbms_handler.LastCommitedTS(), req_reader, res_builder);
      });
  data.server->rpc_server_.Register<storage::replication::SystemRecoveryRpc>(
      [&dbms_handler](auto *req_reader, auto *res_builder) {
        spdlog::debug("Received SystemRecoveryRpc");
        SystemRecoveryHandler(dbms_handler, req_reader, res_builder);
      });
#endif
}

}  // namespace memgraph::dbms
