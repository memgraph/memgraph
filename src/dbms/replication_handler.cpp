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
#include "dbms/global.hpp"
#include "dbms/inmemory/replication_handlers.hpp"
#include "dbms/replication_client.hpp"
#include "dbms/utils.hpp"
#include "replication/messages.hpp"
#include "replication/state.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/config.hpp"
#include "storage/v2/replication/rpc.hpp"
#include "utils/on_scope_exit.hpp"

using memgraph::replication::RoleMainData;
using memgraph::replication::RoleReplicaData;

namespace memgraph::dbms {

namespace {

std::string RegisterReplicaErrorToString(RegisterReplicaError error) {
  switch (error) {
    using enum RegisterReplicaError;
    case NAME_EXISTS:
      return "NAME_EXISTS";
    case ENDPOINT_EXISTS:
      return "ENDPOINT_EXISTS";
    case CONNECTION_FAILED:
      return "CONNECTION_FAILED";
    case COULD_NOT_BE_PERSISTED:
      return "COULD_NOT_BE_PERSISTED";
  }
}
}  // namespace

ReplicationHandler::ReplicationHandler(DbmsHandler &dbms_handler) : dbms_handler_(dbms_handler) {}

bool ReplicationHandler::SetReplicationRoleMain() {
  auto const main_handler = [](RoleMainData &) {
    // If we are already MAIN, we don't want to change anything
    return false;
  };

  auto const replica_handler = [this](RoleReplicaData const &) {
    return memgraph::dbms::DoReplicaToMainPromotion(dbms_handler_);
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
  dbms_handler_.ForEach([&](DatabaseAccess db_acc) {
    auto *storage = db_acc->storage();
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
                                   [this](RoleReplicaData const &data) { return StartRpcServer(dbms_handler_, data); }},
                 dbms_handler_.ReplicationState().ReplicationData());
  // TODO Handle error (restore to main?)
  return success;
}

auto ReplicationHandler::RegisterReplica(const memgraph::replication::ReplicationClientConfig &config)
    -> memgraph::utils::BasicResult<RegisterReplicaError> {
  MG_ASSERT(dbms_handler_.ReplicationState().IsMain(), "Only main instance can register a replica!");

  auto maybe_client = dbms_handler_.ReplicationState().RegisterReplica(config);
  if (maybe_client.HasError()) {
    switch (maybe_client.GetError()) {
      case memgraph::replication::RegisterReplicaError::NOT_MAIN:
        MG_ASSERT(false, "Only main instance can register a replica!");
        return {};
      case memgraph::replication::RegisterReplicaError::NAME_EXISTS:
        return memgraph::dbms::RegisterReplicaError::NAME_EXISTS;
      case memgraph::replication::RegisterReplicaError::ENDPOINT_EXISTS:
        return memgraph::dbms::RegisterReplicaError::ENDPOINT_EXISTS;
      case memgraph::replication::RegisterReplicaError::COULD_NOT_BE_PERSISTED:
        return memgraph::dbms::RegisterReplicaError::COULD_NOT_BE_PERSISTED;
      case memgraph::replication::RegisterReplicaError::SUCCESS:
        break;
    }
  }

  if (!allow_mt_repl && dbms_handler_.All().size() > 1) {
    spdlog::warn("Multi-tenant replication is currently not supported!");
  }

#ifdef MG_ENTERPRISE
  // Update system before enabling individual storage <-> replica clients
  dbms_handler_.SystemRestore(*maybe_client.GetValue());
#endif

  const auto dbms_error = memgraph::dbms::HandleRegisterReplicaStatus(maybe_client);
  if (dbms_error.has_value()) {
    return *dbms_error;
  }
  auto &instance_client_ptr = maybe_client.GetValue();
  const bool all_clients_good = memgraph::dbms::RegisterAllDatabasesClients(dbms_handler_, *instance_client_ptr);

  // NOTE Currently if any databases fails, we revert back
  if (!all_clients_good) {
    spdlog::error("Failed to register all databases on the REPLICA \"{}\"", config.name);
    UnregisterReplica(config.name);
    return RegisterReplicaError::CONNECTION_FAILED;
  }

  // No client error, start instance level client
  StartReplicaClient(dbms_handler_, *instance_client_ptr);
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
    dbms_handler_.ForEach([name](DatabaseAccess db_acc) {
      db_acc->storage()->repl_storage_state_.replication_clients_.WithLock([&name](auto &clients) {
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

auto ReplicationHandler::GetRole() const -> memgraph::replication_coordination_glue::ReplicationRole {
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
      const auto &ret = db_acc->storage()->repl_storage_state_.replication_clients_.WithLock(
          [&, db_acc](auto &storage_clients) mutable -> utils::BasicResult<RegisterReplicaError> {
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

namespace system_replication {
#ifdef MG_ENTERPRISE
void SystemHeartbeatHandler(const uint64_t ts, slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::SystemHeartbeatReq req;
  replication::SystemHeartbeatReq::Load(&req, req_reader);

  replication::SystemHeartbeatRes res(ts);
  memgraph::slk::Save(res, res_builder);
}

void CreateDatabaseHandler(DbmsHandler &dbms_handler, slk::Reader *req_reader, slk::Builder *res_builder) {
  memgraph::storage::replication::CreateDatabaseReq req;
  memgraph::slk::Load(&req, req_reader);

  using memgraph::storage::replication::CreateDatabaseRes;
  CreateDatabaseRes res(CreateDatabaseRes::Result::FAILURE);

  // Note: No need to check epoch, recovery mechanism is done by a full uptodate snapshot
  //       of the set of databases. Hence no history exists to maintain regarding epoch change.
  //       If MAIN has changed we need to check this new group_timestamp is consistent with
  //       what we have so far.

  if (req.expected_group_timestamp != dbms_handler.LastCommitedTS()) {
    spdlog::debug("CreateDatabaseHandler: bad expected timestamp {},{}", req.expected_group_timestamp,
                  dbms_handler.LastCommitedTS());
    memgraph::slk::Save(res, res_builder);
    return;
  }

  try {
    // Create new
    auto new_db = dbms_handler.Update(req.config);
    if (new_db.HasValue()) {
      // Successfully create db
      dbms_handler.SetLastCommitedTS(req.new_group_timestamp);
      res = CreateDatabaseRes(CreateDatabaseRes::Result::SUCCESS);
      spdlog::debug("CreateDatabaseHandler: SUCCESS updated LCTS to {}", req.new_group_timestamp);
    }
  } catch (...) {
    // Failure
  }

  memgraph::slk::Save(res, res_builder);
}

void DropDatabaseHandler(DbmsHandler &dbms_handler, slk::Reader *req_reader, slk::Builder *res_builder) {
  memgraph::storage::replication::DropDatabaseReq req;
  memgraph::slk::Load(&req, req_reader);

  using memgraph::storage::replication::DropDatabaseRes;
  DropDatabaseRes res(DropDatabaseRes::Result::FAILURE);

  // Note: No need to check epoch, recovery mechanism is done by a full uptodate snapshot
  //       of the set of databases. Hence no history exists to maintain regarding epoch change.
  //       If MAIN has changed we need to check this new group_timestamp is consistent with
  //       what we have so far.

  if (req.expected_group_timestamp != dbms_handler.LastCommitedTS()) {
    spdlog::debug("DropDatabaseHandler: bad expected timestamp {},{}", req.expected_group_timestamp,
                  dbms_handler.LastCommitedTS());
    memgraph::slk::Save(res, res_builder);
    return;
  }

  try {
    // NOTE: Single communication channel can exist at a time, no other database can be deleted/created at the moment.
    auto new_db = dbms_handler.Delete(req.uuid);
    if (new_db.HasError()) {
      if (new_db.GetError() == DeleteError::NON_EXISTENT) {
        // Nothing to drop
        dbms_handler.SetLastCommitedTS(req.new_group_timestamp);
        res = DropDatabaseRes(DropDatabaseRes::Result::NO_NEED);
      }
    } else {
      // Successfully drop db
      dbms_handler.SetLastCommitedTS(req.new_group_timestamp);
      res = DropDatabaseRes(DropDatabaseRes::Result::SUCCESS);
      spdlog::debug("DropDatabaseHandler: SUCCESS updated LCTS to {}", req.new_group_timestamp);
    }
  } catch (...) {
    // Failure
  }

  memgraph::slk::Save(res, res_builder);
}

void SystemRecoveryHandler(DbmsHandler &dbms_handler, slk::Reader *req_reader, slk::Builder *res_builder) {
  // TODO Speed up
  memgraph::storage::replication::SystemRecoveryReq req;
  memgraph::slk::Load(&req, req_reader);

  using memgraph::storage::replication::SystemRecoveryRes;
  SystemRecoveryRes res(SystemRecoveryRes::Result::FAILURE);

  utils::OnScopeExit send_on_exit([&]() { memgraph::slk::Save(res, res_builder); });

  // Get all current dbs
  auto old = dbms_handler.All();

  // Check/create the incoming dbs
  for (const auto &config : req.database_configs) {
    // Missing db
    try {
      if (dbms_handler.Update(config).HasError()) {
        spdlog::debug("SystemRecoveryHandler: Failed to update database \"{}\".", config.name);
        return;  // Send failure on exit
      }
    } catch (const UnknownDatabaseException &) {
      spdlog::debug("SystemRecoveryHandler: UnknownDatabaseException");
      return;  // Send failure on exit
    }
    const auto it = std::find(old.begin(), old.end(), config.name);
    if (it != old.end()) old.erase(it);
  }

  // Delete all the leftover old dbs
  for (const auto &remove_db : old) {
    const auto del = dbms_handler.Delete(remove_db);
    if (del.HasError()) {
      // Some errors are not terminal
      if (del.GetError() == DeleteError::DEFAULT_DB || del.GetError() == DeleteError::NON_EXISTENT) {
        spdlog::debug("SystemRecoveryHandler: Dropped database \"{}\".", remove_db);
        continue;
      }
      spdlog::debug("SystemRecoveryHandler: Failed to drop database \"{}\".", remove_db);
      return;  // Send failure on exit
    }
  }
  // Successfully recovered
  dbms_handler.SetLastCommitedTS(req.forced_group_timestamp);
  spdlog::debug("SystemRecoveryHandler: SUCCESS updated LCTS to {}", req.forced_group_timestamp);
  res = SystemRecoveryRes(SystemRecoveryRes::Result::SUCCESS);
}
#endif

void Register(replication::RoleReplicaData const &data, dbms::DbmsHandler &dbms_handler) {
#ifdef MG_ENTERPRISE
  data.server->rpc_server_.Register<replication::SystemHeartbeatRpc>(
      [&dbms_handler](auto *req_reader, auto *res_builder) {
        spdlog::debug("Received SystemHeartbeatRpc");
        SystemHeartbeatHandler(dbms_handler.LastCommitedTS(), req_reader, res_builder);
      });
  data.server->rpc_server_.Register<storage::replication::CreateDatabaseRpc>(
      [&dbms_handler](auto *req_reader, auto *res_builder) {
        spdlog::debug("Received CreateDatabaseRpc");
        CreateDatabaseHandler(dbms_handler, req_reader, res_builder);
      });
  data.server->rpc_server_.Register<storage::replication::DropDatabaseRpc>(
      [&dbms_handler](auto *req_reader, auto *res_builder) {
        spdlog::debug("Received DropDatabaseRpc");
        DropDatabaseHandler(dbms_handler, req_reader, res_builder);
      });
  data.server->rpc_server_.Register<storage::replication::SystemRecoveryRpc>(
      [&dbms_handler](auto *req_reader, auto *res_builder) {
        spdlog::debug("Received SystemRecoveryRpc");
        SystemRecoveryHandler(dbms_handler, req_reader, res_builder);
      });
#endif
}
}  // namespace system_replication

bool StartRpcServer(DbmsHandler &dbms_handler, const replication::RoleReplicaData &data) {
  // Register handlers
  InMemoryReplicationHandlers::Register(&dbms_handler, *data.server);
  system_replication::Register(data, dbms_handler);
  // Start server
  if (!data.server->Start()) {
    spdlog::error("Unable to start the replication server.");
    return false;
  }
  return true;
}
}  // namespace memgraph::dbms
