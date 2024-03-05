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
#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "replication/replication_client.hpp"
#include "replication_handler/system_replication.hpp"
#include "utils/functional.hpp"

namespace memgraph::replication {

namespace {
#ifdef MG_ENTERPRISE
void RecoverReplication(replication::ReplicationState &repl_state, system::System &system,
                        dbms::DbmsHandler &dbms_handler, auth::SynchedAuth &auth) {
  /*
   * REPLICATION RECOVERY AND STARTUP
   */

  // Startup replication state (if recovered at startup)
  auto replica = [&dbms_handler, &auth, &system](replication::RoleReplicaData &data) {
    return replication::StartRpcServer(dbms_handler, data, auth, system);
  };

  // Replication recovery and frequent check start
  auto main = [&system, &dbms_handler, &auth](replication::RoleMainData &mainData) {
    for (auto &client : mainData.registered_replicas_) {
      if (client.try_set_uuid &&
          replication_coordination_glue::SendSwapMainUUIDRpc(client.rpc_client_, mainData.uuid_)) {
        client.try_set_uuid = false;
      }
      SystemRestore(client, system, dbms_handler, mainData.uuid_, auth);
    }
    // DBMS here
    dbms_handler.ForEach([&mainData](dbms::DatabaseAccess db_acc) {
      dbms::DbmsHandler::RecoverStorageReplication(std::move(db_acc), mainData);
    });

    for (auto &client : mainData.registered_replicas_) {
      StartReplicaClient(client, system, dbms_handler, mainData.uuid_, auth);
    }

    // Warning
    if (dbms_handler.default_config().durability.snapshot_wal_mode ==
        storage::Config::Durability::SnapshotWalMode::DISABLED) {
      spdlog::warn(
          "The instance has the MAIN replication role, but durability logs and snapshots are disabled. Please "
          "consider "
          "enabling durability by using --storage-snapshot-interval-sec and --storage-wal-enabled flags because "
          "without write-ahead logs this instance is not replicating any data.");
    }

    return true;
  };

  auto result = std::visit(utils::Overloaded{replica, main}, repl_state.ReplicationData());
  MG_ASSERT(result, "Replica recovery failure!");
}
#else
void RecoverReplication(replication::ReplicationState &repl_state, dbms::DbmsHandler &dbms_handler) {
  // Startup replication state (if recovered at startup)
  auto replica = [&dbms_handler](replication::RoleReplicaData &data) {
    return replication::StartRpcServer(dbms_handler, data);
  };

  // Replication recovery and frequent check start
  auto main = [&dbms_handler](replication::RoleMainData &mainData) {
    dbms::DbmsHandler::RecoverStorageReplication(dbms_handler.Get(), mainData);

    for (auto &client : mainData.registered_replicas_) {
      if (client.try_set_uuid &&
          replication_coordination_glue::SendSwapMainUUIDRpc(client.rpc_client_, mainData.uuid_)) {
        client.try_set_uuid = false;
      }
      replication::StartReplicaClient(client, dbms_handler, mainData.uuid_);
    }

    // Warning
    if (dbms_handler.default_config().durability.snapshot_wal_mode ==
        storage::Config::Durability::SnapshotWalMode::DISABLED) {
      spdlog::warn(
          "The instance has the MAIN replication role, but durability logs and snapshots are disabled. Please "
          "consider "
          "enabling durability by using --storage-snapshot-interval-sec and --storage-wal-enabled flags because "
          "without write-ahead logs this instance is not replicating any data.");
    }

    return true;
  };

  auto result = std::visit(utils::Overloaded{replica, main}, repl_state.ReplicationData());
  MG_ASSERT(result, "Replica recovery failure!");
}
#endif
}  // namespace

inline std::optional<query::RegisterReplicaError> HandleRegisterReplicaStatus(
    utils::BasicResult<replication::RegisterReplicaError, replication::ReplicationClient *> &instance_client) {
  if (instance_client.HasError()) {
    switch (instance_client.GetError()) {
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
    if (client.try_set_uuid && replication_coordination_glue::SendSwapMainUUIDRpc(client.rpc_client_, main_uuid)) {
      client.try_set_uuid = false;
    }
    // Working connection
    // Check if system needs restoration
    if (reconnect) {
      client.state_.WithLock([](auto &state) { state = replication::ReplicationClient::State::BEHIND; });
    }
    // Check if license has changed
    const auto new_license = license::global_license_checker.IsEnterpriseValidFast();
    if (new_license != license) {
      license = new_license;
      client.state_.WithLock([](auto &state) { state = replication::ReplicationClient::State::BEHIND; });
    }
#ifdef MG_ENTERPRISE
    SystemRestore<true>(client, system, dbms_handler, main_uuid, auth);
#endif
    // Check if any database has been left behind
    dbms_handler.ForEach([&name = client.name_, reconnect](dbms::DatabaseAccess db_acc) {
      // Specific database <-> replica client
      db_acc->storage()->repl_storage_state_.WithClient(name, [&](storage::ReplicationStorageClient &client) {
        if (reconnect || client.State() == storage::replication::ReplicaState::MAYBE_BEHIND) {
          // Database <-> replica might be behind, check and recover
          client.TryCheckReplicaStateAsync(db_acc->storage(), db_acc);
        }
      });
    });
  });
}

#ifdef MG_ENTERPRISE
ReplicationHandler::ReplicationHandler(replication::ReplicationState &repl_state, dbms::DbmsHandler &dbms_handler,
                                       system::System &system, auth::SynchedAuth &auth)
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
  auto const main_handler = [](replication::RoleMainData &) {
    // If we are already MAIN, we don't want to change anything
    return false;
  };

  auto const replica_handler = [this](replication::RoleReplicaData const &) {
    return DoReplicaToMainPromotion(utils::UUID{});
  };

  // TODO: under lock
  return std::visit(utils::Overloaded{main_handler, replica_handler}, repl_state_.ReplicationData());
}

bool ReplicationHandler::SetReplicationRoleReplica(const replication::ReplicationServerConfig &config,
                                                   const std::optional<utils::UUID> &main_uuid) {
  return SetReplicationRoleReplica_<true>(config, main_uuid);
}

bool ReplicationHandler::TrySetReplicationRoleReplica(const memgraph::replication::ReplicationServerConfig &config,
                                                      const std::optional<utils::UUID> &main_uuid) {
  return SetReplicationRoleReplica_<false>(config, main_uuid);
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
auto ReplicationHandler::TryRegisterReplica(const memgraph::replication::ReplicationClientConfig &config)
    -> memgraph::utils::BasicResult<memgraph::query::RegisterReplicaError> {
  return RegisterReplica_<true>(config);
}

auto ReplicationHandler::RegisterReplica(const memgraph::replication::ReplicationClientConfig &config)
    -> memgraph::utils::BasicResult<memgraph::query::RegisterReplicaError> {
  return RegisterReplica_<false>(config);
}

auto ReplicationHandler::UnregisterReplica(std::string_view name) -> query::UnregisterReplicaResult {
  auto const replica_handler = [](replication::RoleReplicaData const &) -> query::UnregisterReplicaResult {
    return query::UnregisterReplicaResult::NOT_MAIN;
  };
  auto const main_handler = [this, name](replication::RoleMainData &mainData) -> query::UnregisterReplicaResult {
    if (!repl_state_.TryPersistUnregisterReplica(name)) {
      return query::UnregisterReplicaResult::COULD_NOT_BE_PERSISTED;
    }
    // Remove database specific clients
    dbms_handler_.ForEach([name](dbms::DatabaseAccess db_acc) {
      db_acc->storage()->repl_storage_state_.replication_clients_.WithLock([&name](auto &clients) {
        std::erase_if(clients, [name](const auto &client) { return client->Name() == name; });
      });
    });
    // Remove instance level clients
    auto const n_unregistered =
        std::erase_if(mainData.registered_replicas_, [name](auto const &client) { return client.name_ == name; });
    return n_unregistered != 0 ? query::UnregisterReplicaResult::SUCCESS
                               : query::UnregisterReplicaResult::CAN_NOT_UNREGISTER;
  };

  return std::visit(utils::Overloaded{main_handler, replica_handler}, repl_state_.ReplicationData());
}

auto ReplicationHandler::GetRole() const -> replication_coordination_glue::ReplicationRole {
  return repl_state_.GetRole();
}

auto ReplicationHandler::GetDatabasesHistories() -> replication_coordination_glue::DatabaseHistories {
  replication_coordination_glue::DatabaseHistories results;
  dbms_handler_.ForEach([&results](memgraph::dbms::DatabaseAccess db_acc) {
    auto &repl_storage_state = db_acc->storage()->repl_storage_state_;

    std::vector<std::pair<std::string, uint64_t>> history = utils::fmap(
        [](const auto &elem) { return std::make_pair(elem.first, elem.second); }, repl_storage_state.history);

    history.emplace_back(std::string(repl_storage_state.epoch_.id()), repl_storage_state.last_commit_timestamp_.load());
    replication_coordination_glue::DatabaseHistory repl{
        .db_uuid = utils::UUID{db_acc->storage()->uuid()}, .history = history, .name = std::string(db_acc->name())};
    results.emplace_back(repl);
  });

  return results;
}

auto ReplicationHandler::GetReplicaUUID() -> std::optional<utils::UUID> {
  MG_ASSERT(repl_state_.IsReplica(), "Instance is not replica");
  return std::get<RoleReplicaData>(repl_state_.ReplicationData()).uuid_;
}

auto ReplicationHandler::GetReplState() const -> const memgraph::replication::ReplicationState & { return repl_state_; }

auto ReplicationHandler::GetReplState() -> replication::ReplicationState & { return repl_state_; }

bool ReplicationHandler::IsMain() const { return repl_state_.IsMain(); }

bool ReplicationHandler::IsReplica() const { return repl_state_.IsReplica(); }
auto ReplicationHandler::ShowReplicas() const -> utils::BasicResult<query::ShowReplicaError, query::ReplicasInfos> {
  using res_t = utils::BasicResult<query::ShowReplicaError, query::ReplicasInfos>;
  auto main = [this](RoleMainData const &main) -> res_t {
    auto entries = std::vector<query::ReplicasInfo>{};
    entries.reserve(main.registered_replicas_.size());

    const bool full_info = license::global_license_checker.IsEnterpriseValidFast();

    for (auto const &replica : main.registered_replicas_) {
      // STEP 1: data_info
      auto data_info = std::map<std::string, query::ReplicaInfoState>{};
      this->dbms_handler_.ForEach([&](dbms::DatabaseAccess db_acc) {
        auto *storage = db_acc->storage();
        // ATM we only support IN_MEMORY_TRANSACTIONAL
        if (storage->storage_mode_ != storage::StorageMode::IN_MEMORY_TRANSACTIONAL) return;
        if (!full_info && storage->name() == dbms::kDefaultDB) return;
        auto ok =
            storage->repl_storage_state_.WithClient(replica.name_, [&](storage::ReplicationStorageClient &client) {
              auto ts_info = client.GetTimestampInfo(storage);
              auto state = client.State();

              data_info.emplace(storage->name(),
                                query::ReplicaInfoState{ts_info.current_timestamp_of_replica,
                                                        ts_info.current_number_of_timestamp_behind_main, state});
            });
        DMG_ASSERT(ok);
      });

// STEP 2: system_info
#ifdef MG_ENTERPRISE
      // Already locked on system transaction via the interpreter
      const auto ts = system_.LastCommittedSystemTimestamp();
      // NOTE: no system behind at the moment
      query::ReplicaSystemInfoState system_info{ts, 0 /* behind ts not implemented */, *replica.state_.ReadLock()};
#else
      query::ReplicaSystemInfoState system_info{};
#endif
      // STEP 3: add entry
      entries.emplace_back(replica.name_, replica.rpc_client_.Endpoint().SocketAddress(), replica.mode_, system_info,
                           std::move(data_info));
    }
    return query::ReplicasInfos{std::move(entries)};
  };
  auto replica = [](RoleReplicaData const &) -> res_t { return query::ShowReplicaError::NOT_MAIN; };

  return std::visit(utils::Overloaded{main, replica}, repl_state_.ReplicationData());
}

}  // namespace memgraph::replication
