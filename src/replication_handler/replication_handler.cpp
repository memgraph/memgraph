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

#include "replication_handler/replication_handler.hpp"
#include "dbms/dbms_handler.hpp"
#include "replication_handler/system_replication.hpp"

namespace memgraph::replication {

namespace {
#ifdef MG_ENTERPRISE
void RecoverReplication(memgraph::replication::ReplicationState &repl_state, memgraph::system::System &system,
                        memgraph::dbms::DbmsHandler &dbms_handler, memgraph::auth::SynchedAuth &auth) {
  /*
   * REPLICATION RECOVERY AND STARTUP
   */

  // Startup replication state (if recovered at startup)
  auto replica = [&dbms_handler, &auth, &system](memgraph::replication::RoleReplicaData &data) {
    return memgraph::replication::StartRpcServer(dbms_handler, data, auth, system);
  };

  // Replication recovery and frequent check start
  auto main = [&system, &dbms_handler, &auth](memgraph::replication::RoleMainData &mainData) {
    for (auto &client : mainData.registered_replicas_) {
      if (client.try_set_uuid &&
          replication_coordination_glue::SendSwapMainUUIDRpc(client.rpc_client_, mainData.uuid_)) {
        client.try_set_uuid = false;
      }
      SystemRestore(client, system, dbms_handler, mainData.uuid_, auth);
    }
    // DBMS here
    dbms_handler.ForEach([&mainData](memgraph::dbms::DatabaseAccess db_acc) {
      dbms::DbmsHandler::RecoverStorageReplication(std::move(db_acc), mainData);
    });

    for (auto &client : mainData.registered_replicas_) {
      StartReplicaClient(client, system, dbms_handler, mainData.uuid_, auth);
    }

    // Warning
    if (dbms_handler.default_config().durability.snapshot_wal_mode ==
        memgraph::storage::Config::Durability::SnapshotWalMode::DISABLED) {
      spdlog::warn(
          "The instance has the MAIN replication role, but durability logs and snapshots are disabled. Please "
          "consider "
          "enabling durability by using --storage-snapshot-interval-sec and --storage-wal-enabled flags because "
          "without write-ahead logs this instance is not replicating any data.");
    }

    return true;
  };

  auto result = std::visit(memgraph::utils::Overloaded{replica, main}, repl_state.ReplicationData());
  MG_ASSERT(result, "Replica recovery failure!");
}
#else
void RecoverReplication(memgraph::replication::ReplicationState &repl_state,
                        memgraph::dbms::DbmsHandler &dbms_handler) {
  // Startup replication state (if recovered at startup)
  auto replica = [&dbms_handler](memgraph::replication::RoleReplicaData &data) {
    return memgraph::replication::StartRpcServer(dbms_handler, data);
  };

  // Replication recovery and frequent check start
  auto main = [&dbms_handler](memgraph::replication::RoleMainData &mainData) {
    dbms::DbmsHandler::RecoverStorageReplication(dbms_handler.Get(), mainData);

    for (auto &client : mainData.registered_replicas_) {
      if (client.try_set_uuid &&
          replication_coordination_glue::SendSwapMainUUIDRpc(client.rpc_client_, mainData.uuid_)) {
        client.try_set_uuid = false;
      }
      memgraph::replication::StartReplicaClient(client, dbms_handler, mainData.uuid_);
    }

    // Warning
    if (dbms_handler.default_config().durability.snapshot_wal_mode ==
        memgraph::storage::Config::Durability::SnapshotWalMode::DISABLED) {
      spdlog::warn(
          "The instance has the MAIN replication role, but durability logs and snapshots are disabled. Please "
          "consider "
          "enabling durability by using --storage-snapshot-interval-sec and --storage-wal-enabled flags because "
          "without write-ahead logs this instance is not replicating any data.");
    }

    return true;
  };

  auto result = std::visit(memgraph::utils::Overloaded{replica, main}, repl_state.ReplicationData());
  MG_ASSERT(result, "Replica recovery failure!");
}
#endif
}  // namespace

inline std::optional<query::RegisterReplicaError> HandleRegisterReplicaStatus(
    utils::BasicResult<replication::RegisterReplicaError, replication::ReplicationClient *> &instance_client) {
  if (instance_client.HasError()) switch (instance_client.GetError()) {
      case replication::RegisterReplicaError::NOT_MAIN:
        MG_ASSERT(false, "Only main instance can register a replica!");
        return {};
      case replication::RegisterReplicaError::NAME_EXISTS:
        return query::RegisterReplicaError::NAME_EXISTS;
      case replication::RegisterReplicaError::ENDPOINT_EXISTS:
        return query::RegisterReplicaError::ENDPOINT_EXISTS;
      case replication::RegisterReplicaError::COULD_NOT_BE_PERSISTED:
        return query::RegisterReplicaError::COULD_NOT_BE_PERSISTED;
      case replication::RegisterReplicaError::SUCCESS:
        break;
    }
  return {};
}

#ifdef MG_ENTERPRISE
void StartReplicaClient(replication::ReplicationClient &client, system::System &system, dbms::DbmsHandler &dbms_handler,
                        utils::UUID main_uuid, auth::SynchedAuth &auth) {
#else
void StartReplicaClient(replication::ReplicationClient &client, dbms::DbmsHandler &dbms_handler,
                        utils::UUID main_uuid) {
#endif
  // No client error, start instance level client
  auto const &endpoint = client.rpc_client_.Endpoint();
  spdlog::trace("Replication client started at: {}:{}", endpoint.address, endpoint.port);
  client.StartFrequentCheck([&, license = license::global_license_checker.IsEnterpriseValidFast(), main_uuid](
                                bool reconnect, replication::ReplicationClient &client) mutable {
    if (client.try_set_uuid &&
        memgraph::replication_coordination_glue::SendSwapMainUUIDRpc(client.rpc_client_, main_uuid)) {
      client.try_set_uuid = false;
    }
    // Working connection
    // Check if system needs restoration
    if (reconnect) {
      client.state_.WithLock([](auto &state) { state = memgraph::replication::ReplicationClient::State::BEHIND; });
    }
    // Check if license has changed
    const auto new_license = license::global_license_checker.IsEnterpriseValidFast();
    if (new_license != license) {
      license = new_license;
      client.state_.WithLock([](auto &state) { state = memgraph::replication::ReplicationClient::State::BEHIND; });
    }
#ifdef MG_ENTERPRISE
    SystemRestore<true>(client, system, dbms_handler, main_uuid, auth);
#endif
    // Check if any database has been left behind
    dbms_handler.ForEach([&name = client.name_, reconnect](dbms::DatabaseAccess db_acc) {
      // Specific database <-> replica client
      db_acc->storage()->repl_storage_state_.WithClient(name, [&](storage::ReplicationStorageClient *client) {
        if (reconnect || client->State() == storage::replication::ReplicaState::MAYBE_BEHIND) {
          // Database <-> replica might be behind, check and recover
          client->TryCheckReplicaStateAsync(db_acc->storage(), db_acc);
        }
      });
    });
  });
}

#ifdef MG_ENTERPRISE
ReplicationHandler::ReplicationHandler(memgraph::replication::ReplicationState &repl_state,
                                       memgraph::dbms::DbmsHandler &dbms_handler, memgraph::system::System &system,
                                       memgraph::auth::SynchedAuth &auth)
    : repl_state_{repl_state}, dbms_handler_{dbms_handler}, system_{system}, auth_{auth} {
  RecoverReplication(repl_state_, system_, dbms_handler_, auth_);
}
#else
ReplicationHandler::ReplicationHandler(replication::ReplicationState &repl_state, dbms::DbmsHandler &dbms_handler)
    : repl_state_{repl_state}, dbms_handler_{dbms_handler} {
  RecoverReplication(repl_state_, dbms_handler_);
}
#endif

bool ReplicationHandler::SetReplicationRoleMain() {
  auto const main_handler = [](memgraph::replication::RoleMainData &) {
    // If we are already MAIN, we don't want to change anything
    return false;
  };

  auto const replica_handler = [this](memgraph::replication::RoleReplicaData const &) {
    return DoReplicaToMainPromotion(utils::UUID{});
  };

  // TODO: under lock
  return std::visit(memgraph::utils::Overloaded{main_handler, replica_handler}, repl_state_.ReplicationData());
}

bool ReplicationHandler::SetReplicationRoleReplica(const memgraph::replication::ReplicationServerConfig &config,
                                                   const std::optional<utils::UUID> &main_uuid) {
  // We don't want to restart the server if we're already a REPLICA
  if (repl_state_.IsReplica()) {
    spdlog::trace("Instance has already has replica role.");
    return false;
  }

  // TODO StorageState needs to be synched. Could have a dangling reference if someone adds a database as we are
  //      deleting the replica.
  // Remove database specific clients
  dbms_handler_.ForEach([&](memgraph::dbms::DatabaseAccess db_acc) {
    auto *storage = db_acc->storage();
    storage->repl_storage_state_.replication_clients_.WithLock([](auto &clients) { clients.clear(); });
  });
  // Remove instance level clients
  std::get<memgraph::replication::RoleMainData>(repl_state_.ReplicationData()).registered_replicas_.clear();

  // Creates the server
  repl_state_.SetReplicationRoleReplica(config, main_uuid);

  // Start
  const auto success =
      std::visit(memgraph::utils::Overloaded{[](memgraph::replication::RoleMainData &) {
                                               // ASSERT
                                               return false;
                                             },
                                             [this](memgraph::replication::RoleReplicaData &data) {
#ifdef MG_ENTERPRISE
                                               return StartRpcServer(dbms_handler_, data, auth_, system_);
#else
                                               return StartRpcServer(dbms_handler_, data);
#endif
                                             }},
                 repl_state_.ReplicationData());
  // TODO Handle error (restore to main?)
  return success;
}

bool ReplicationHandler::DoReplicaToMainPromotion(const utils::UUID &main_uuid) {
  // STEP 1) bring down all REPLICA servers
  dbms_handler_.ForEach([](dbms::DatabaseAccess db_acc) {
    auto *storage = db_acc->storage();
    // Remember old epoch + storage timestamp association
    storage->PrepareForNewEpoch();
  });

  // STEP 2) Change to MAIN
  // TODO: restore replication servers if false?
  if (!repl_state_.SetReplicationRoleMain(main_uuid)) {
    // TODO: Handle recovery on failure???
    return false;
  }

  // STEP 3) We are now MAIN, update storage local epoch
  const auto &epoch = std::get<replication::RoleMainData>(std::as_const(repl_state_).ReplicationData()).epoch_;
  dbms_handler_.ForEach([&](dbms::DatabaseAccess db_acc) {
    auto *storage = db_acc->storage();
    storage->repl_storage_state_.epoch_ = epoch;
  });

  return true;
};

// as MAIN, define and connect to REPLICAs
auto ReplicationHandler::TryRegisterReplica(const memgraph::replication::ReplicationClientConfig &config,
                                            bool send_swap_uuid)
    -> memgraph::utils::BasicResult<memgraph::query::RegisterReplicaError> {
  return RegisterReplica_<false>(config, send_swap_uuid);
}

auto ReplicationHandler::RegisterReplica(const memgraph::replication::ReplicationClientConfig &config,
                                         bool send_swap_uuid)
    -> memgraph::utils::BasicResult<memgraph::query::RegisterReplicaError> {
  return RegisterReplica_<true>(config, send_swap_uuid);
}

auto ReplicationHandler::UnregisterReplica(std::string_view name) -> memgraph::query::UnregisterReplicaResult {
  auto const replica_handler =
      [](memgraph::replication::RoleReplicaData const &) -> memgraph::query::UnregisterReplicaResult {
    return memgraph::query::UnregisterReplicaResult::NOT_MAIN;
  };
  auto const main_handler =
      [this, name](memgraph::replication::RoleMainData &mainData) -> memgraph::query::UnregisterReplicaResult {
    if (!repl_state_.TryPersistUnregisterReplica(name)) {
      return memgraph::query::UnregisterReplicaResult::COULD_NOT_BE_PERSISTED;
    }
    // Remove database specific clients
    dbms_handler_.ForEach([name](memgraph::dbms::DatabaseAccess db_acc) {
      db_acc->storage()->repl_storage_state_.replication_clients_.WithLock([&name](auto &clients) {
        std::erase_if(clients, [name](const auto &client) { return client->Name() == name; });
      });
    });
    // Remove instance level clients
    auto const n_unregistered =
        std::erase_if(mainData.registered_replicas_, [name](auto const &client) { return client.name_ == name; });
    return n_unregistered != 0 ? memgraph::query::UnregisterReplicaResult::SUCCESS
                               : memgraph::query::UnregisterReplicaResult::CAN_NOT_UNREGISTER;
  };

  return std::visit(memgraph::utils::Overloaded{main_handler, replica_handler}, repl_state_.ReplicationData());
}

auto ReplicationHandler::GetRole() const -> memgraph::replication_coordination_glue::ReplicationRole {
  return repl_state_.GetRole();
}

auto ReplicationHandler::GetReplState() const -> const memgraph::replication::ReplicationState & { return repl_state_; }

auto ReplicationHandler::GetReplState() -> memgraph::replication::ReplicationState & { return repl_state_; }

bool ReplicationHandler::IsMain() const { return repl_state_.IsMain(); }

bool ReplicationHandler::IsReplica() const { return repl_state_.IsReplica(); }

}  // namespace memgraph::replication
