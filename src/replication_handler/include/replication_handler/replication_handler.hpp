// Copyright 2025 Memgraph Ltd.
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
#include "flags/experimental.hpp"
#include "replication/include/replication/state.hpp"
#include "replication_coordination_glue/common.hpp"
#include "replication_coordination_glue/handler.hpp"
#include "replication_handler/system_replication.hpp"
#include "replication_handler/system_rpc.hpp"
#include "utils/event_histogram.hpp"
#include "utils/metrics_timer.hpp"
#include "utils/result.hpp"

namespace memgraph::metrics {
extern const Event SystemRecoveryRpc_us;
}  // namespace memgraph::metrics

namespace memgraph::replication {

inline std::optional<query::RegisterReplicaError> HandleRegisterReplicaStatus(
    utils::BasicResult<RegisterReplicaStatus, ReplicationClient *> &instance_client);

#ifdef MG_ENTERPRISE
void StartReplicaClient(ReplicationClient &client, system::System &system, dbms::DbmsHandler &dbms_handler,
                        utils::UUID main_uuid, auth::SynchedAuth &auth);
#else
void StartReplicaClient(replication::ReplicationClient &client, dbms::DbmsHandler &dbms_handler, utils::UUID main_uuid);
#endif

#ifdef MG_ENTERPRISE
// TODO: Split into 2 functions: dbms and auth
// When being called by interpreter no need to gain lock, it should already be under a system transaction
// But concurrently the FrequentCheck is running and will need to lock before reading last_committed_system_timestamp_
template <bool REQUIRE_LOCK = false>
void SystemRestore(ReplicationClient &client, system::System &system, dbms::DbmsHandler &dbms_handler,
                   const utils::UUID &main_uuid, auth::SynchedAuth &auth) {
  // If the state was BEHIND, change it to RECOVERY, do the recovery process and change it to READY.
  // If the state was something else than BEHIND, return immediately.
  if (!client.state_.WithLock([](auto &state) {
        bool const is_behind = state == ReplicationClient::State::BEHIND;
        if (is_behind) {
          state = ReplicationClient::State::RECOVERY;
        }
        return is_behind;
      })) {
    return;
  }

  bool const is_enterprise = license::global_license_checker.IsEnterpriseValidFast();

  // We still need to system replicate
  struct DbInfo {
    std::vector<storage::SalientConfig> configs;
    uint64_t last_committed_timestamp;
  };

  DbInfo db_info = std::invoke([&] {
    auto guard = std::invoke([&]() -> std::optional<system::TransactionGuard> {
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
    utils::MetricsTimer const timer{metrics::SystemRecoveryRpc_us};
    auto stream = std::invoke([&]() {
      // Handle only default database is no license
      if (!is_enterprise) {
        return client.rpc_client_.Stream<SystemRecoveryRpc>(main_uuid, db_info.last_committed_timestamp,
                                                            std::move(db_info.configs), auth::Auth::Config{},
                                                            std::vector<auth::User>{}, std::vector<auth::Role>{});
      }
      return auth.WithLock([&](auto &locked_auth) {
        return client.rpc_client_.Stream<SystemRecoveryRpc>(main_uuid, db_info.last_committed_timestamp,
                                                            std::move(db_info.configs), locked_auth.GetConfig(),
                                                            locked_auth.AllUsers(), locked_auth.AllRoles());
      });
    });
    if (const auto response = stream.SendAndWait(); response.result == SystemRecoveryRes::Result::FAILURE) {
      client.state_.WithLock([](auto &state) { state = ReplicationClient::State::BEHIND; });
      return;
    }
  } catch (rpc::GenericRpcFailedException const &) {
    client.state_.WithLock([](auto &state) { state = ReplicationClient::State::BEHIND; });
    return;
  }

  // Successfully recovered
  client.state_.WithLock([](auto &state) { state = memgraph::replication::ReplicationClient::State::READY; });
}
#endif

/// A handler type that keep in sync current ReplicationState and the MAIN/REPLICA-ness of Storage
struct ReplicationHandler : public query::ReplicationQueryHandler {
#ifdef MG_ENTERPRISE
  explicit ReplicationHandler(utils::Synchronized<ReplicationState, utils::RWSpinLock> &repl_state,
                              memgraph::dbms::DbmsHandler &dbms_handler, memgraph::system::System &system,
                              memgraph::auth::SynchedAuth &auth);
#else
  explicit ReplicationHandler(utils::Synchronized<ReplicationState, utils::RWSpinLock> &repl_state,
                              memgraph::dbms::DbmsHandler &dbms_handler);
#endif

  // as REPLICA, become MAIN
  bool SetReplicationRoleMain() override;

  // as MAIN, become REPLICA, can be called on MAIN and REPLICA
  bool SetReplicationRoleReplica(const ReplicationServerConfig &config) override;

  // as MAIN, become REPLICA, can be called only on MAIN
  bool TrySetReplicationRoleReplica(const ReplicationServerConfig &config) override;

  // as MAIN, define and connect to REPLICAs
  auto TryRegisterReplica(const ReplicationClientConfig &config)
      -> utils::BasicResult<query::RegisterReplicaError> override;

  auto RegisterReplica(const ReplicationClientConfig &config)
      -> utils::BasicResult<query::RegisterReplicaError> override;

  // as MAIN, remove a REPLICA connection
  auto UnregisterReplica(std::string_view name) -> query::UnregisterReplicaResult override;

  bool DoToMainPromotion(const utils::UUID &main_uuid, bool force = true);

  // Helper pass-through (TODO: remove)
  auto GetRole() const -> replication_coordination_glue::ReplicationRole override;
  bool IsMain() const override;
  bool IsReplica() const override;

  auto ShowReplicas() const -> utils::BasicResult<query::ShowReplicaError, query::ReplicasInfos> override;

  auto GetReplState() const { return repl_state_.ReadLock(); }
  auto GetReplState() { return repl_state_.Lock(); }

#ifdef MG_ENTERPRISE
  auto GetDatabasesHistories() const -> replication_coordination_glue::InstanceInfo;
  auto GetReplicationLag() const -> coordination::ReplicationLagInfo;
#endif

 private:
  void ClientsShutdown(auto &locked_repl_state) const {
    spdlog::trace("Shutting down instance level clients.");

    auto &repl_clients = std::get<RoleMainData>(locked_repl_state->ReplicationData()).registered_replicas_;
    for (auto &client : repl_clients) {
      client.Shutdown();
    }

    spdlog::trace("Instance-level clients stopped, trying to destroy replication storage clients.");

    // TODO StorageState needs to be synced. Could have a dangling reference if someone adds a database as we are
    //      deleting the replica.
    // Remove database specific clients
    dbms_handler_.ForEach([&](dbms::DatabaseAccess db_acc) {
      auto *storage = db_acc->storage();
      storage->repl_storage_state_.replication_storage_clients_.WithLock([](auto &clients) { clients.clear(); });
    });

    spdlog::trace("Replication storage clients destroyed.");
  }

  template <bool SendSwapUUID>
  auto RegisterReplica_(auto &locked_repl_state, const ReplicationClientConfig &config)
      -> utils::BasicResult<query::RegisterReplicaError> {
    using query::RegisterReplicaError;
    using ClientRegisterReplicaStatus = RegisterReplicaStatus;

    auto maybe_client = locked_repl_state->RegisterReplica(config);
    if (maybe_client.HasError()) {
      switch (maybe_client.GetError()) {
        case ClientRegisterReplicaStatus::NOT_MAIN:
          return RegisterReplicaError::NOT_MAIN;
        case ClientRegisterReplicaStatus::NAME_EXISTS:
          return RegisterReplicaError::NAME_EXISTS;
        case ClientRegisterReplicaStatus::ENDPOINT_EXISTS:
          return RegisterReplicaError::ENDPOINT_EXISTS;
        case ClientRegisterReplicaStatus::COULD_NOT_BE_PERSISTED:
          return RegisterReplicaError::COULD_NOT_BE_PERSISTED;
        case ClientRegisterReplicaStatus::SUCCESS:
          break;
        default:
          LOG_FATAL("Unknown register replica status.");
      }
    }

    auto const main_uuid = std::get<RoleMainData>(locked_repl_state->ReplicationData()).uuid_;
    if constexpr (SendSwapUUID) {
      if (!replication_coordination_glue::SendSwapMainUUIDRpc(maybe_client.GetValue()->rpc_client_, main_uuid)) {
        return RegisterReplicaError::ERROR_ACCEPTING_MAIN;
      }
    }

#ifdef MG_ENTERPRISE
    // Update system before enabling individual storage <-> replica clients
    SystemRestore(*maybe_client.GetValue(), system_, dbms_handler_, main_uuid, auth_);
#endif

    if (const auto dbms_error = HandleRegisterReplicaStatus(maybe_client); dbms_error.has_value()) {
      return *dbms_error;
    }

    auto &instance_client_ptr = maybe_client.GetValue();
    // Add database specific clients (NOTE Currently all databases are connected to each replica)
    bool all_clients_good{true};
    dbms_handler_.ForEach([&](dbms::DatabaseAccess db_acc) {
      auto *storage = db_acc->storage();
      if (storage->storage_mode_ != storage::StorageMode::IN_MEMORY_TRANSACTIONAL) return;

      auto client = std::make_unique<storage::ReplicationStorageClient>(*instance_client_ptr, main_uuid);
      client->Start(storage, db_acc);

      all_clients_good &= storage->repl_storage_state_.replication_storage_clients_.WithLock(
          [client = std::move(client)](auto &storage_clients) mutable {  // NOLINT
            bool const success = std::invoke([state = client->State()]() {
              // We force sync replicas in other situation
              // DIVERGED_FROM_MAIN is only valid state in enterprise and community replication. HA will immediately
              // set the state to RECOVERY
              if (state == storage::replication::ReplicaState::DIVERGED_FROM_MAIN) {
                return false;
              }
              return true;
            });

            if (success) {
              storage_clients.push_back(std::move(client));
            }
            return success;
          });
    });

    if (!all_clients_good) {
      spdlog::error("Failed to register all databases for the replica {}. Started unregistering replica.", config.name);
      switch (UnregisterReplica(config.name)) {
        using query::UnregisterReplicaResult;
        case UnregisterReplicaResult::NO_ACCESS:
          spdlog::trace("Failed to unregister replica {} since we couldn't get unique access to ReplicationState.",
                        config.name);
          break;
        case UnregisterReplicaResult::NOT_MAIN:
          spdlog::trace(
              "Failed to unregister replica {} after failed registration process since the instance isn't main "
              "anymore. The instance left in inconsistent state, the administrator should manually delete the "
              "data and restart process.",
              config.name);
          break;
        case UnregisterReplicaResult::COULD_NOT_BE_PERSISTED:
          MG_ASSERT(
              false,
              "Failed to unregister replica {} after failed registration process since unregistration couldn't be "
              "persisted. The instance left in inconsistent state, the administrator should manually delete the data "
              "and restart process.",
              config.name);
        case UnregisterReplicaResult::CANNOT_UNREGISTER:
          spdlog::trace(
              "Failed to unregister replica {} after failed registration process since unregistration unsuccessful for "
              "all database clients. The instance left in inconsistent state, the administrator should manually delete "
              "the data and restart process.",
              config.name);
          break;
        case UnregisterReplicaResult::SUCCESS:
          spdlog::trace("Replica {} successfully unregistered after failed registration process.", config.name);
          break;
      }
      return RegisterReplicaError::CONNECTION_FAILED;
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
  bool SetReplicationRoleReplica_(auto &locked_repl_state, const ReplicationServerConfig &config) {
    if (locked_repl_state->IsReplica()) {
      if (!AllowIdempotency) {
        return false;
      }
      // We don't want to restart the server if we're already a REPLICA with correct config
      auto &replica_data = std::get<RoleReplicaData>(locked_repl_state->ReplicationData());
      if (replica_data.config == config) {
        return true;
      }
      locked_repl_state->SetReplicationRoleReplica(config);
#ifdef MG_ENTERPRISE
      return StartRpcServer(dbms_handler_, replica_data, auth_, system_);
#else
      return StartRpcServer(dbms_handler_, replica_data);
#endif
    }

    // Shutdown any clients we might have had
    ClientsShutdown(locked_repl_state);
    // Creates the server
    locked_repl_state->SetReplicationRoleReplica(config);
    spdlog::trace("Role set to replica, instance-level clients destroyed.");

    // Start
    const auto success = std::visit(utils::Overloaded{[](RoleMainData &) {
                                                        // ASSERT
                                                        return false;
                                                      },
                                                      [this](RoleReplicaData &data) {
#ifdef MG_ENTERPRISE
                                                        return StartRpcServer(dbms_handler_, data, auth_, system_);
#else
                                                        return StartRpcServer(dbms_handler_, data);
#endif
                                                      }},
                                    locked_repl_state->ReplicationData());

    // Pause TTL
    dbms_handler_.ForEach([&](dbms::DatabaseAccess db_acc) {
      auto &ttl = db_acc->ttl();
      ttl.Pause();
    });

    // TODO Handle error (restore to main?)
    return success;
  }

  utils::Synchronized<ReplicationState, utils::RWSpinLock> &repl_state_;
  dbms::DbmsHandler &dbms_handler_;

#ifdef MG_ENTERPRISE
  system::System &system_;
  auth::SynchedAuth &auth_;
#endif
};

}  // namespace memgraph::replication
