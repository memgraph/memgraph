// Copyright 2026 Memgraph Ltd.
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
#include "auth/profiles/user_profiles.hpp"
#include "coordination/coordinator_rpc.hpp"

#include "dbms/dbms_handler.hpp"
#include "flags/experimental.hpp"
#include "metrics/scoped_histogram_timer.hpp"
#include "parameters/parameters.hpp"
#include "replication/include/replication/state.hpp"
#include "replication_coordination_glue/common.hpp"
#include "replication_coordination_glue/handler.hpp"
#include "replication_handler/system_replication.hpp"
#include "replication_handler/system_rpc.hpp"

namespace memgraph::replication {

inline std::optional<query::RegisterReplicaError> HandleRegisterReplicaStatus(
    std::expected<ReplicationClient *, RegisterReplicaStatus> &instance_client);

#ifdef MG_ENTERPRISE
void StartReplicaClient(ReplicationClient &client, system::System &system, dbms::DbmsHandler &dbms_handler,
                        utils::UUID main_uuid, auth::SynchedAuth &auth, parameters::Parameters &parameters);
#else
void StartReplicaClient(replication::ReplicationClient &client, system::System &system, dbms::DbmsHandler &dbms_handler,
                        utils::UUID main_uuid, parameters::Parameters &parameters);
#endif

// TODO: Split into 2 functions: dbms and auth
// When being called by interpreter no need to gain lock, it should already be under a system transaction
// But concurrently the FrequentCheck is running and will need to lock before reading last_committed_system_timestamp_
#ifdef MG_ENTERPRISE
template <bool REQUIRE_LOCK = false>
void SystemRestore(ReplicationClient &client, system::System &system, dbms::DbmsHandler &dbms_handler,
                   const utils::UUID &main_uuid, auth::SynchedAuth &auth, parameters::Parameters &parameters) {
#else
template <bool REQUIRE_LOCK = false>
void SystemRestore(ReplicationClient &client, system::System &system, dbms::DbmsHandler &dbms_handler,
                   const utils::UUID &main_uuid, parameters::Parameters &parameters) {
#endif
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

  // We still need to system replicate.
  // NB: DbInfo is a local, in-process aggregate only — NOT a wire type, so it needs no versioning of
  // its own. Its fields are passed individually to Stream<SystemRecoveryRpc> below; the serialized
  // request is the versioned SystemRecoveryReq (V1 -> V2 adds parameters -> V3 adds cold_databases,
  // with server-side upgrade of an older peer's request). A future layout change bumps that RPC version.
  struct DbInfo {
    std::vector<storage::SalientConfig> configs;
    uint64_t last_committed_timestamp;
    // Hot/cold: COLD set carried so a reconnecting/lagging replica converges to {HOT ∪ COLD}.
    // One ColdTenantRecovery per suspended tenant (salient + stats); empty on non-enterprise /
    // no-license.
    std::vector<storage::ColdTenantRecovery> cold_databases;
  };

  const auto is_enterprise = license::global_license_checker.IsEnterpriseValidFast();

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
#ifdef MG_ENTERPRISE
      // Snapshot the COLD set inside the same system-transaction guard as the HOT ForEach so the two
      // are coherent as-of last_committed_timestamp.
      auto cold_databases = dbms_handler.SuspendedConfigsForRecovery();
      return DbInfo{std::move(configs), system.LastCommittedSystemTimestamp(), std::move(cold_databases)};
#else
      return DbInfo{std::move(configs), system.LastCommittedSystemTimestamp(), {}};
#endif
    }
    // No license -> send only default config
    return DbInfo{{dbms_handler.Get()->config().salient}, system.LastCommittedSystemTimestamp(), {}};
  });
  try {
    metrics::ScopedHistogramTimer const timer{metrics::Metrics().global.system_recovery_rpc_seconds};
    auto const params_snapshot = parameters.GetSnapshotForRecovery();
    auto stream = std::invoke([&]() {
#ifdef MG_ENTERPRISE
      if (!is_enterprise) {
        return client.rpc_client_.Stream<SystemRecoveryRpc>(main_uuid,
                                                            db_info.last_committed_timestamp,
                                                            std::move(db_info.configs),
                                                            auth::Auth::Config{},
                                                            std::vector<auth::User>{},
                                                            std::vector<auth::Role>{},
                                                            std::vector<auth::UserProfiles::Profile>{},
                                                            params_snapshot);
      }
      return auth.WithLock([&](auto &locked_auth) {
        return client.rpc_client_.Stream<SystemRecoveryRpc>(main_uuid,
                                                            db_info.last_committed_timestamp,
                                                            std::move(db_info.configs),
                                                            locked_auth.GetConfig(),
                                                            locked_auth.AllUsers(),
                                                            locked_auth.AllRoles(),
                                                            locked_auth.AllProfiles(),
                                                            params_snapshot,
                                                            std::move(db_info.cold_databases));
      });
#else
      return client.rpc_client_.Stream<SystemRecoveryRpc>(main_uuid,
                                                          db_info.last_committed_timestamp,
                                                          std::move(db_info.configs),
                                                          auth::Auth::Config{},
                                                          std::vector<auth::User>{},
                                                          std::vector<auth::Role>{},
                                                          std::vector<auth::UserProfiles::Profile>{},
                                                          params_snapshot);
#endif
    });
    auto const response = stream.SendAndWait();
    if (response.result == SystemRecoveryRes::Result::FAILURE) {
      // System recovery failed; do not record confirmations so the reset is re-advertised on the next
      // SystemRestore.
      client.state_.WithLock([](auto &state) { state = ReplicationClient::State::BEHIND; });
      return;
    }
  } catch (rpc::RpcFailedException const &) {  // intentionally RpcFailedException and not Generic because we want to
                                               // handle both Generic and timeout type of errors
    client.state_.WithLock([](auto &state) { state = ReplicationClient::State::BEHIND; });
    return;
  }
  // Successfully recovered
  client.state_.WithLock([](auto &state) { state = memgraph::replication::ReplicationClient::State::READY; });
}

/// A handler type that keep in sync current ReplicationState and the MAIN/REPLICA-ness of Storage
struct ReplicationHandler : public query::ReplicationQueryHandler {
#ifdef MG_ENTERPRISE
  explicit ReplicationHandler(utils::Synchronized<ReplicationState, utils::RWSpinLock> &repl_state,
                              memgraph::dbms::DbmsHandler &dbms_handler, memgraph::system::System &system,
                              memgraph::auth::SynchedAuth &auth, memgraph::parameters::Parameters &parameters);
#else
  explicit ReplicationHandler(utils::Synchronized<ReplicationState, utils::RWSpinLock> &repl_state,
                              memgraph::dbms::DbmsHandler &dbms_handler, memgraph::system::System &system,
                              memgraph::parameters::Parameters &parameters);
#endif

  // as REPLICA, become MAIN
  bool SetReplicationRoleMain() override;

  // as MAIN, become REPLICA, can be called on MAIN and REPLICA
  bool SetReplicationRoleReplica(const ReplicationServerConfig &config,
                                 std::optional<utils::UUID> const &maybe_main_uuid) override;

  // as MAIN, become REPLICA, can be called only on MAIN
  bool TrySetReplicationRoleReplica(const ReplicationServerConfig &config) override;

  // as MAIN, define and connect to REPLICAs
  auto TryRegisterReplica(const ReplicationClientConfig &config)
      -> std::expected<void, query::RegisterReplicaError> override;

  auto RegisterReplica(const ReplicationClientConfig &config)
      -> std::expected<void, query::RegisterReplicaError> override;

  // as MAIN, remove a REPLICA connection
  auto UnregisterReplica(std::string_view name) -> query::UnregisterReplicaResult override;

  bool DoToMainPromotion(const utils::UUID &main_uuid, bool force = true);

  // Helper pass-through (TODO: remove)
  auto GetRole() const -> replication_coordination_glue::ReplicationRole override;
  bool IsMain() const override;
  bool IsReplica() const override;

  auto ShowReplicas() const -> std::expected<query::ReplicasInfos, query::ShowReplicaError> override;

  auto GetReplState() const { return repl_state_.ReadLock(); }

  auto GetReplState() { return repl_state_.Lock(); }

#ifdef MG_ENTERPRISE
  std::variant<coordination::GetDatabaseHistoriesResV1, coordination::GetDatabaseHistoriesRes> GetDatabasesHistories(
      uint64_t request_version) const;

  auto GetReplicationLag() const -> coordination::ReplicationLagInfo;

  using ReplicasResT = std::map<std::string, std::map<std::string, int64_t>>;
  using MainResT = std::map<std::string, uint64_t>;
  std::pair<MainResT, ReplicasResT> GetNumCommittedTxns() const;

#endif

 private:
  void ClientsShutdown(auto &locked_repl_state) const {
    spdlog::info("Shutting down instance level clients.");

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

    spdlog::info("Replication storage clients destroyed.");
  }

  using LockedReplState = utils::Synchronized<ReplicationState, utils::RWSpinLock>::LockedPtr;

  // UnregisterReplica without the analytical-mode gate, for the rollback of a failed registration:
  // that rollback must succeed precisely when a database did turn analytical mid-registration.
  auto UnregisterReplica_(std::string_view name) -> query::UnregisterReplicaResult;

  // The same, driven by a hold the caller already owns. repl_state_ is not recursive, so a rollback
  // running inside RegisterReplica_ cannot reacquire it: UnregisterReplica_ fails its TryLock every time
  // and reports NO_ACCESS while leaving the replica registered.
  auto UnregisterReplicaLocked_(LockedReplState &locked_repl_state, std::string_view name)
      -> query::UnregisterReplicaResult;

  // Name of the first database found in analytical mode, if any. Registration and unregistration are
  // instance-wide operations, so a single analytical database blocks both.
  auto AnalyticalDatabase() const -> std::optional<std::string> {
    std::optional<std::string> analytical_db;
    dbms_handler_.ForEach([&analytical_db](dbms::DatabaseAccess db_acc) {
      if (!analytical_db && db_acc->storage()->storage_mode_ == storage::StorageMode::IN_MEMORY_ANALYTICAL) {
        analytical_db = db_acc->name();
      }
    });
    return analytical_db;
  }

  template <bool SendSwapUUID>
  auto RegisterReplica_(auto &locked_repl_state, const ReplicationClientConfig &config)
      -> std::expected<void, query::RegisterReplicaError> {
    using query::RegisterReplicaError;
    using ClientRegisterReplicaStatus = RegisterReplicaStatus;

    // Reject before any replication state is mutated: persisting the instance-level client while no
    // per-database client can be created leaves the replica permanently unattached, since every retry
    // then fails with NAME_EXISTS.
    if (auto const analytical_db = AnalyticalDatabase(); analytical_db.has_value()) {
      spdlog::error(
          "Cannot register replica {} while database \"{}\" is in analytical mode.", config.name, *analytical_db);
      return std::unexpected{RegisterReplicaError::ANALYTICAL_MODE};
    }

    auto maybe_client = locked_repl_state->RegisterReplica(config);
    if (!maybe_client) {
      switch (maybe_client.error()) {
        case ClientRegisterReplicaStatus::NOT_MAIN:
          return std::unexpected{RegisterReplicaError::NOT_MAIN};
        case ClientRegisterReplicaStatus::NAME_EXISTS:
          return std::unexpected{RegisterReplicaError::NAME_EXISTS};
        case ClientRegisterReplicaStatus::ENDPOINT_EXISTS:
          return std::unexpected{RegisterReplicaError::ENDPOINT_EXISTS};
        case ClientRegisterReplicaStatus::COULD_NOT_BE_PERSISTED:
          return std::unexpected{RegisterReplicaError::COULD_NOT_BE_PERSISTED};
        case ClientRegisterReplicaStatus::SUCCESS:
          break;
        default:
          LOG_FATAL("Unknown register replica status.");
      }
    }

    auto const main_uuid = std::get<RoleMainData>(locked_repl_state->ReplicationData()).uuid_;
    if constexpr (SendSwapUUID) {
      if (!replication_coordination_glue::SendSwapMainUUIDRpc((*maybe_client)->rpc_client_, main_uuid)) {
        return std::unexpected{RegisterReplicaError::ERROR_ACCEPTING_MAIN};
      }
    }

#ifdef MG_ENTERPRISE
    SystemRestore(*maybe_client.value(), system_, dbms_handler_, main_uuid, auth_, parameters_);
#else
    SystemRestore(*maybe_client.value(), system_, dbms_handler_, main_uuid, parameters_);
#endif

    if (const auto dbms_error = HandleRegisterReplicaStatus(maybe_client); dbms_error.has_value()) {
      return std::unexpected{*dbms_error};
    }

    auto &instance_client_ptr = maybe_client.value();
    // Add database specific clients (NOTE Currently all databases are connected to each replica)
    bool all_clients_good{true};
    dbms_handler_.ForEach([&](dbms::DatabaseAccess db_acc) {
      // One failed database rolls the whole registration back, and the Shutdown below has already torn the
      // instance client down, so every further Start would only block on an aborted RPC client.
      if (!all_clients_good) return;

      auto *storage = db_acc->storage();
      // Disk storage never participates in replication, so it is skipped rather than failed. Analytical
      // does fail: silently skipping it is what leaves the replica permanently unattached. The up-front
      // gate above already rejected that, so getting here means the mode flipped in between.
      if (storage->storage_mode_ == storage::StorageMode::ON_DISK_TRANSACTIONAL) return;
      if (storage->storage_mode_ != storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
        all_clients_good = false;
        return;
      }

      auto protector = dbms::DatabaseProtector{db_acc};

      auto client = std::make_unique<storage::ReplicationStorageClient>(*instance_client_ptr, main_uuid);
      client->Start(storage, protector);

      // Start runs the heartbeat synchronously but may leave a recovery task queued on the instance
      // client's thread pool, holding a raw pointer to this client. A rejected client therefore must not be
      // destroyed under the lock: ownership is handed back so it outlives the drain below.
      auto rejected = storage->repl_storage_state_.replication_storage_clients_.WithLock(
          [storage, client = std::move(client)](
              auto &storage_clients) mutable -> storage::ReplicationStorageState::ReplicationStorageClientPtr {
            // Re-read the mode under this lock. SetStorageMode stores IN_MEMORY_ANALYTICAL under the very
            // same lock, so "analytical with a live client" is unrepresentable rather than merely unlikely.
            if (storage->storage_mode_ != storage::StorageMode::IN_MEMORY_TRANSACTIONAL) return std::move(client);

            // We force sync replicas in other situation
            // DIVERGED_FROM_MAIN is only valid state in enterprise and community replication. HA will immediately
            // set the state to RECOVERY
            if (client->State() == storage::replication::ReplicaState::DIVERGED_FROM_MAIN) return std::move(client);

            storage_clients.push_back(std::move(client));
            return nullptr;
          });

      if (rejected) {
        all_clients_good = false;
        // Aborts the in-flight RPC, drops the queue and joins the worker, so no task can outlive `rejected`,
        // which is destroyed at the end of this iteration. Must run with the clients' lock released: the
        // task can be inside GetRecoverySteps waiting on engine_lock_, which a committing thread holds
        // while waiting for that very lock.
        instance_client_ptr->Shutdown();
      }
    });

    if (!all_clients_good) {
      spdlog::error("Failed to register all databases for the replica {}. Started unregistering replica.", config.name);
      switch (UnregisterReplicaLocked_(locked_repl_state, config.name)) {
        using query::UnregisterReplicaResult;
        case UnregisterReplicaResult::ANALYTICAL_MODE:
          LOG_FATAL("UnregisterReplicaLocked_ must not apply the analytical-mode gate.");
        case UnregisterReplicaResult::NO_ACCESS:
          LOG_FATAL("UnregisterReplicaLocked_ must not acquire ReplicationState; the caller already holds it.");
        case UnregisterReplicaResult::NOT_MAIN:
          spdlog::error(
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
          spdlog::error(
              "Failed to unregister replica {} after failed registration process since unregistration unsuccessful for "
              "all database clients. The instance left in inconsistent state, the administrator should manually delete "
              "the data and restart process.",
              config.name);
          break;
        case UnregisterReplicaResult::SUCCESS:
          spdlog::info("Replica {} successfully unregistered after failed registration process.", config.name);
          break;
      }
      return std::unexpected{RegisterReplicaError::CONNECTION_FAILED};
    }

#ifdef MG_ENTERPRISE
    StartReplicaClient(*instance_client_ptr, system_, dbms_handler_, main_uuid, auth_, parameters_);
#else
    StartReplicaClient(*instance_client_ptr, system_, dbms_handler_, main_uuid, parameters_);
#endif
    return {};
  }

  template <bool AllowIdempotency>
  bool SetReplicationRoleReplica_(auto &locked_repl_state, const ReplicationServerConfig &config,
                                  std::optional<utils::UUID> const &maybe_main_uuid = std::nullopt) {
    if (locked_repl_state->IsReplica()) {
      if constexpr (!AllowIdempotency) {
        return false;
      }
      // We don't want to restart the server if we're already a REPLICA with correct config
      auto &replica_data = std::get<RoleReplicaData>(locked_repl_state->ReplicationData());
      if (replica_data.config == config) {
        return true;
      }
      if (!locked_repl_state->SetReplicationRoleReplica(config, maybe_main_uuid)) {
        return false;
      }
#ifdef MG_ENTERPRISE
      return StartRpcServer(dbms_handler_, repl_state_, replica_data, auth_, system_, parameters_);
#else
      return StartRpcServer(dbms_handler_, repl_state_, replica_data, system_, parameters_);
#endif
    }

    // Shutdown any clients we might have had
    ClientsShutdown(locked_repl_state);
    // Creates the server
    if (!locked_repl_state->SetReplicationRoleReplica(config, maybe_main_uuid)) {
      return false;
    }
    spdlog::info("Role set to replica, instance-level clients destroyed.");

    // Start
    const auto success = std::visit(
        utils::Overloaded{[](RoleMainData &) {
                            // ASSERT
                            return false;
                          },
                          [this](RoleReplicaData &data) {
#ifdef MG_ENTERPRISE
                            return StartRpcServer(dbms_handler_, repl_state_, data, auth_, system_, parameters_);
#else
                            return StartRpcServer(dbms_handler_, repl_state_, data, system_, parameters_);
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
  system::System &system_;
#ifdef MG_ENTERPRISE
  auth::SynchedAuth &auth_;
#endif
  parameters::Parameters &parameters_;
};

}  // namespace memgraph::replication
