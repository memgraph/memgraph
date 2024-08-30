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
#pragma once

#include <utility>

#include "auth/auth.hpp"
#include "dbms/dbms_handler.hpp"
#include "flags/coord_flag_env_handler.hpp"
#include "flags/experimental.hpp"
#include "replication/include/replication/state.hpp"
#include "replication_coordination_glue/common.hpp"
#include "replication_handler/system_replication.hpp"
#include "replication_handler/system_rpc.hpp"
#include "utils/result.hpp"

namespace memgraph::replication {

inline std::optional<query::RegisterReplicaError> HandleRegisterReplicaStatus(
    utils::BasicResult<replication::RegisterReplicaError, replication::ReplicationClient *> &instance_client);

#ifdef MG_ENTERPRISE
void StartReplicaClient(replication::ReplicationClient &client, system::System &system, dbms::DbmsHandler &dbms_handler,
                        utils::UUID main_uuid, auth::SynchedAuth &auth);
#else
void StartReplicaClient(replication::ReplicationClient &client, dbms::DbmsHandler &dbms_handler, utils::UUID main_uuid);
#endif

#ifdef MG_ENTERPRISE
// TODO: Split into 2 functions: dbms and auth
// When being called by interpreter no need to gain lock, it should already be under a system transaction
// But concurrently the FrequentCheck is running and will need to lock before reading last_committed_system_timestamp_
template <bool REQUIRE_LOCK = false>
void SystemRestore(replication::ReplicationClient &client, system::System &system, dbms::DbmsHandler &dbms_handler,
                   const utils::UUID &main_uuid, auth::SynchedAuth &auth) {
  // Check if system is up to date
  if (client.state_.WithLock(
          [](auto &state) { return state != memgraph::replication::ReplicationClient::State::BEHIND; }))
    return;

  // Try to recover...
  client.state_.WithLock(
      [](auto &state) { return state != memgraph::replication::ReplicationClient::State::RECOVERY; });
  {
    bool is_enterprise = license::global_license_checker.IsEnterpriseValidFast();
    // We still need to system replicate
    struct DbInfo {
      std::vector<storage::SalientConfig> configs;
      uint64_t last_committed_timestamp;
    };
    DbInfo db_info = std::invoke([&] {
      auto guard = std::invoke([&]() -> std::optional<memgraph::system::TransactionGuard> {
        if constexpr (REQUIRE_LOCK) {
          return system.GenTransactionGuard();
        }
        return std::nullopt;
      });

      if (is_enterprise) {
        auto configs = std::vector<storage::SalientConfig>{};
        dbms_handler.ForEach([&configs](dbms::DatabaseAccess acc) { configs.emplace_back(acc->config().salient); });
        // TODO: This is `SystemRestore` maybe DbInfo is incorrect as it will need Auth also
        return DbInfo{configs, system.LastCommittedSystemTimestamp()};
      }

      // No license -> send only default config
      return DbInfo{{dbms_handler.Get()->config().salient}, system.LastCommittedSystemTimestamp()};
    });
    try {
      auto stream = std::invoke([&]() {
        // Handle only default database is no license
        if (!is_enterprise) {
          return client.rpc_client_.Stream<replication::SystemRecoveryRpc>(
              main_uuid, db_info.last_committed_timestamp, std::move(db_info.configs), auth::Auth::Config{},
              std::vector<auth::User>{}, std::vector<auth::Role>{});
        }
        return auth.WithLock([&](auto &locked_auth) {
          return client.rpc_client_.Stream<replication::SystemRecoveryRpc>(
              main_uuid, db_info.last_committed_timestamp, std::move(db_info.configs), locked_auth.GetConfig(),
              locked_auth.AllUsers(), locked_auth.AllRoles());
        });
      });
      const auto response = stream.AwaitResponse();
      if (response.result == replication::SystemRecoveryRes::Result::FAILURE) {
        client.state_.WithLock([](auto &state) { state = memgraph::replication::ReplicationClient::State::BEHIND; });
        return;
      }
    } catch (memgraph::rpc::GenericRpcFailedException const &e) {
      client.state_.WithLock([](auto &state) { state = memgraph::replication::ReplicationClient::State::BEHIND; });
      return;
    }
  }

  // Successfully recovered
  client.state_.WithLock([](auto &state) { state = memgraph::replication::ReplicationClient::State::READY; });
}
#endif

/// A handler type that keep in sync current ReplicationState and the MAIN/REPLICA-ness of Storage
struct ReplicationHandler : public memgraph::query::ReplicationQueryHandler {
#ifdef MG_ENTERPRISE
  explicit ReplicationHandler(memgraph::replication::ReplicationState &repl_state,
                              memgraph::dbms::DbmsHandler &dbms_handler, memgraph::system::System &system,
                              memgraph::auth::SynchedAuth &auth);
#else
  explicit ReplicationHandler(memgraph::replication::ReplicationState &repl_state,
                              memgraph::dbms::DbmsHandler &dbms_handler);
#endif

  // as REPLICA, become MAIN
  bool SetReplicationRoleMain() override;

  // as MAIN, become REPLICA, can be called on MAIN and REPLICA
  bool SetReplicationRoleReplica(const memgraph::replication::ReplicationServerConfig &config,
                                 const std::optional<utils::UUID> &main_uuid) override;

  // as MAIN, become REPLICA, can be called only on MAIN
  bool TrySetReplicationRoleReplica(const memgraph::replication::ReplicationServerConfig &config,
                                    const std::optional<utils::UUID> &main_uuid) override;

  // as MAIN, define and connect to REPLICAs
  auto TryRegisterReplica(const memgraph::replication::ReplicationClientConfig &config)
      -> memgraph::utils::BasicResult<memgraph::query::RegisterReplicaError> override;

  auto RegisterReplica(const memgraph::replication::ReplicationClientConfig &config)
      -> memgraph::utils::BasicResult<memgraph::query::RegisterReplicaError> override;

  // as MAIN, remove a REPLICA connection
  auto UnregisterReplica(std::string_view name) -> memgraph::query::UnregisterReplicaResult override;

  bool DoReplicaToMainPromotion(const utils::UUID &main_uuid);

  // Helper pass-through (TODO: remove)
  auto GetRole() const -> memgraph::replication_coordination_glue::ReplicationRole override;
  bool IsMain() const override;
  bool IsReplica() const override;

  auto ShowReplicas() const
      -> utils::BasicResult<memgraph::query::ShowReplicaError, memgraph::query::ReplicasInfos> override;

  auto GetReplState() const -> const memgraph::replication::ReplicationState &;
  auto GetReplState() -> memgraph::replication::ReplicationState &;

  auto GetReplicaUUID() -> std::optional<utils::UUID>;

  auto GetDatabasesHistories() -> replication_coordination_glue::DatabaseHistories;

 private:
  template <bool SendSwapUUID>
  auto RegisterReplica_(const memgraph::replication::ReplicationClientConfig &config)
      -> memgraph::utils::BasicResult<memgraph::query::RegisterReplicaError> {
    MG_ASSERT(repl_state_.IsMain(), "Only main instance can register a replica!");
    auto maybe_client = repl_state_.RegisterReplica(config);
    if (maybe_client.HasError()) {
      switch (maybe_client.GetError()) {
        case memgraph::replication::RegisterReplicaError::NOT_MAIN:
          MG_ASSERT(false, "Only main instance can register a replica!");
          return {};
        case memgraph::replication::RegisterReplicaError::NAME_EXISTS:
          return memgraph::query::RegisterReplicaError::NAME_EXISTS;
        case memgraph::replication::RegisterReplicaError::ENDPOINT_EXISTS:
          return memgraph::query::RegisterReplicaError::ENDPOINT_EXISTS;
        case memgraph::replication::RegisterReplicaError::COULD_NOT_BE_PERSISTED:
          return memgraph::query::RegisterReplicaError::COULD_NOT_BE_PERSISTED;
        case memgraph::replication::RegisterReplicaError::SUCCESS:
          break;
      }
    }
    const auto main_uuid =
        std::get<memgraph::replication::RoleMainData>(dbms_handler_.ReplicationState().ReplicationData()).uuid_;
    if constexpr (SendSwapUUID) {
      if (!memgraph::replication_coordination_glue::SendSwapMainUUIDRpc(maybe_client.GetValue()->rpc_client_,
                                                                        main_uuid)) {
        return memgraph::query::RegisterReplicaError::ERROR_ACCEPTING_MAIN;
      }
    }
#ifdef MG_ENTERPRISE
    // Update system before enabling individual storage <-> replica clients
    SystemRestore(*maybe_client.GetValue(), system_, dbms_handler_, main_uuid, auth_);
#endif
    const auto dbms_error = HandleRegisterReplicaStatus(maybe_client);
    if (dbms_error.has_value()) {
      return *dbms_error;
    }
    auto &instance_client_ptr = maybe_client.GetValue();
    bool all_clients_good = true;
    // Add database specific clients (NOTE Currently all databases are connected to each replica)
    dbms_handler_.ForEach([&](dbms::DatabaseAccess db_acc) {
      auto *storage = db_acc->storage();
      // TODO: ATM only IN_MEMORY_TRANSACTIONAL, fix other modes
      if (storage->storage_mode_ != storage::StorageMode::IN_MEMORY_TRANSACTIONAL) return;
      all_clients_good &= storage->repl_storage_state_.replication_clients_.WithLock(
          [is_data_instance_managed_by_coord = flags::CoordinationSetupInstance().IsDataInstanceManagedByCoordinator(),
           storage, &instance_client_ptr, db_acc = std::move(db_acc),
           main_uuid](auto &storage_clients) mutable {  // NOLINT
            auto client = std::make_unique<storage::ReplicationStorageClient>(*instance_client_ptr, main_uuid);
            client->Start(storage, std::move(db_acc));
            bool const success = std::invoke([&is_data_instance_managed_by_coord, state = client->State()]() {
              // We force sync replicas in other situation
              if (state == storage::replication::ReplicaState::DIVERGED_FROM_MAIN) {
#ifdef MG_ENTERPRISE
                return is_data_instance_managed_by_coord;
#else
                return false;
#endif
              }
              return true;
            });

            if (success) {
              storage_clients.push_back(std::move(client));
            }
            return success;
          });
    });
    // NOTE Currently if any databases fails, we revert back
    if (!all_clients_good) {
      spdlog::error("Failed to register all databases on the REPLICA \"{}\"", config.name);
      UnregisterReplica(config.name);
      return memgraph::query::RegisterReplicaError::CONNECTION_FAILED;
    }
    // No client error, start instance level client
#ifdef MG_ENTERPRISE
    StartReplicaClient(*instance_client_ptr, system_, dbms_handler_, main_uuid, auth_);
#else
    StartReplicaClient(*instance_client_ptr, dbms_handler_, main_uuid);
#endif
    return {};
  }

  template <bool AllowIdempotency>
  bool SetReplicationRoleReplica_(const memgraph::replication::ReplicationServerConfig &config,
                                  const std::optional<utils::UUID> &main_uuid) {
    if (repl_state_.IsReplica()) {
      if (!AllowIdempotency) {
        return false;
      }
      // We don't want to restart the server if we're already a REPLICA with correct config
      auto &replica_data = std::get<memgraph::replication::RoleReplicaData>(repl_state_.ReplicationData());
      if (replica_data.config == config) {
        return true;
      }
      repl_state_.SetReplicationRoleReplica(config, main_uuid);
#ifdef MG_ENTERPRISE
      return StartRpcServer(dbms_handler_, replica_data, auth_, system_);
#else
      return StartRpcServer(dbms_handler_, replica_data);
#endif
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

    // Pause TTL
    dbms_handler_.ForEach([&](memgraph::dbms::DatabaseAccess db_acc) {
      auto &ttl = db_acc->ttl();
      ttl.Pause();
    });

    // TODO Handle error (restore to main?)
    return success;
  }

  memgraph::replication::ReplicationState &repl_state_;
  memgraph::dbms::DbmsHandler &dbms_handler_;

#ifdef MG_ENTERPRISE
  memgraph::system::System &system_;
  memgraph::auth::SynchedAuth &auth_;
#endif
};

}  // namespace memgraph::replication
