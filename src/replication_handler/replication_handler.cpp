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

#include "replication_handler/replication_handler.hpp"
#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "replication/replication_client.hpp"
#include "replication_handler/system_replication.hpp"
#include "replication_query_handler.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/functional.hpp"
#include "utils/synchronized.hpp"
#include "utils/timer.hpp"

#include <spdlog/spdlog.h>

namespace memgraph::replication {

using namespace std::chrono_literals;

namespace {
#ifdef MG_ENTERPRISE
void RecoverReplication(utils::Synchronized<ReplicationState, utils::RWSpinLock> &repl_state, system::System &system,
                        dbms::DbmsHandler &dbms_handler, auth::SynchedAuth &auth) {
  /*
   * REPLICATION RECOVERY AND STARTUP
   */

  // Startup replication state (if recovered at startup)
  auto replica = [&dbms_handler, &auth, &system](RoleReplicaData &data) {
    return StartRpcServer(dbms_handler, data, auth, system);
  };

  // Replication recovery and frequent check start
  auto main = [&system, &dbms_handler, &auth](RoleMainData &mainData) {
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

  auto const result = std::visit(utils::Overloaded{replica, main}, repl_state->ReplicationData());
  MG_ASSERT(result, "Replica recovery failure!");
}
#else
void RecoverReplication(utils::Synchronized<ReplicationState, utils::RWSpinLock> &repl_state,
                        dbms::DbmsHandler &dbms_handler) {
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

  auto result = std::visit(utils::Overloaded{replica, main}, repl_state->ReplicationData());
  MG_ASSERT(result, "Replica recovery failure!");
}
#endif
}  // namespace

inline std::optional<query::RegisterReplicaError> HandleRegisterReplicaStatus(
    utils::BasicResult<RegisterReplicaStatus, ReplicationClient *> &instance_client) {
  if (instance_client.HasError()) {
    switch (instance_client.GetError()) {
      case RegisterReplicaStatus::NOT_MAIN:
        MG_ASSERT(false, "Only main instance can register a replica!");
      case RegisterReplicaStatus::NAME_EXISTS:
        return query::RegisterReplicaError::NAME_EXISTS;
      case RegisterReplicaStatus::ENDPOINT_EXISTS:
        return query::RegisterReplicaError::ENDPOINT_EXISTS;
      case RegisterReplicaStatus::COULD_NOT_BE_PERSISTED:
        return query::RegisterReplicaError::COULD_NOT_BE_PERSISTED;
      case RegisterReplicaStatus::SUCCESS:
        break;
    }
  }
  return {};
}

#ifdef MG_ENTERPRISE
void StartReplicaClient(ReplicationClient &client, system::System &system, dbms::DbmsHandler &dbms_handler,
                        utils::UUID main_uuid, auth::SynchedAuth &auth) {
#else
void StartReplicaClient(replication::ReplicationClient &client, dbms::DbmsHandler &dbms_handler,
                        utils::UUID main_uuid) {
#endif
  // No client error, start instance level client
  auto const &endpoint = client.rpc_client_.Endpoint();
  spdlog::trace("Replication client started at: {}", endpoint.SocketAddress());  // non-resolved IP
  client.StartFrequentCheck(
      [&, license = license::global_license_checker.IsEnterpriseValidFast(),
       main_uuid](ReplicationClient &local_client) mutable {
        // Working connection
        if (local_client.try_set_uuid &&
            replication_coordination_glue::SendSwapMainUUIDRpc(local_client.rpc_client_, main_uuid)) {
          local_client.try_set_uuid = false;
        }
        // Check if license has changed
        if (const auto new_license = license::global_license_checker.IsEnterpriseValidFast(); new_license != license) {
          license = new_license;
          local_client.state_.WithLock([](auto &state) { state = ReplicationClient::State::BEHIND; });
        }
#ifdef MG_ENTERPRISE
        SystemRestore<true>(local_client, system, dbms_handler, main_uuid, auth);
#endif
        // Check if any database has been left behind
        dbms_handler.ForEach([&name = local_client.name_](dbms::DatabaseAccess db_acc) {
          // Specific database <-> replica client
          db_acc->storage()->repl_storage_state_.WithClient(name, [&](storage::ReplicationStorageClient &client) {
            if (client.State() == storage::replication::ReplicaState::MAYBE_BEHIND) {
              // Database <-> replica might be behind, check and recover
              client.TryCheckReplicaStateAsync(db_acc->storage(), dbms::DatabaseProtector{db_acc});
            }
          });
        });
      },
      [&](ReplicationClient &local_client) {
        // Connection lost, instance could be behind
        local_client.state_.WithLock([](auto &state) { state = ReplicationClient::State::BEHIND; });
        dbms_handler.ForEach([&name = local_client.name_](dbms::DatabaseAccess db_acc) {
          db_acc->storage()->repl_storage_state_.WithClient(name, [&](storage::ReplicationStorageClient &client) {
            // Specific database <-> replica client
            client.SetMaybeBehind();
          });
        });
      });
}

#ifdef MG_ENTERPRISE
ReplicationHandler::ReplicationHandler(utils::Synchronized<ReplicationState, utils::RWSpinLock> &repl_state,
                                       dbms::DbmsHandler &dbms_handler, system::System &system, auth::SynchedAuth &auth)
    : repl_state_{repl_state}, dbms_handler_{dbms_handler}, system_{system}, auth_{auth} {
  RecoverReplication(repl_state_, system_, dbms_handler_, auth_);
}
#else
ReplicationHandler::ReplicationHandler(utils::Synchronized<ReplicationState, utils::RWSpinLock> &repl_state,
                                       dbms::DbmsHandler &dbms_handler)
    : repl_state_{repl_state}, dbms_handler_{dbms_handler} {
  RecoverReplication(repl_state_, dbms_handler_);
}
#endif

bool ReplicationHandler::SetReplicationRoleMain() { return DoToMainPromotion({}, false); }

bool ReplicationHandler::SetReplicationRoleReplica(const ReplicationServerConfig &config) {
  try {
    auto locked_repl_state = repl_state_.TryLock();

    dbms_handler_.ForEach([](dbms::DatabaseAccess db_acc) {
      // Pause TTL
      db_acc->ttl().Pause();
      // Stop any snapshots
      auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->storage());
      storage->snapshot_runner_.Pause();
      storage->abort_snapshot_.store(true, std::memory_order_release);
    });

    std::map<std::string, std::unique_lock<std::mutex>> snapshot_locks;
    auto const timer = utils::Timer();
    while (true) {
      if (timer.Elapsed() > 10s) {
        spdlog::error("Failed to take snapshot lock on all DBs within 10s while demoting to replica.");
        return false;
      }
      if (snapshot_locks.size() == dbms_handler_.Count()) {
        break;
      }

      dbms_handler_.ForEach([&snapshot_locks](dbms::DatabaseAccess db_acc) {
        auto const lock_it = snapshot_locks.find(db_acc->name());
        if (lock_it != snapshot_locks.end()) {
          return;
        }
        auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->storage());
        auto db_lock = std::unique_lock{storage->snapshot_lock_, std::defer_lock};
        if (db_lock.try_lock()) {
          snapshot_locks.emplace(db_acc->name(), std::move(db_lock));
        }
      });
    }

    return SetReplicationRoleReplica_<true>(locked_repl_state, config);
  } catch (const utils::TryLockException & /* unused */) {
    return false;
  }
}

bool ReplicationHandler::TrySetReplicationRoleReplica(const ReplicationServerConfig &config) {
  try {
    auto locked_repl_state = repl_state_.TryLock();
    return SetReplicationRoleReplica_<false>(locked_repl_state, config);
  } catch (const utils::TryLockException & /* unused */) {
    return false;
  }
}

bool ReplicationHandler::DoToMainPromotion(const utils::UUID &main_uuid, bool const force) {
  try {
    auto locked_repl_state = repl_state_.TryLock();

    dbms_handler_.ForEach([](dbms::DatabaseAccess db_acc) {
      auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->storage());
      storage->abort_snapshot_.store(false, std::memory_order_release);
      storage->snapshot_runner_.Resume();
    });

    if (locked_repl_state->IsMain()) {
      if (!force) return false;
      // Forcing role update...
      // Shutdown any remaining client
      // Main can be promoted while being MAIN; we do this in order to update the uuid and epoch
      // Shutdown must be done after lock on repl_state is taken so that COMMIT and PROMOTION operations are serialized
      ClientsShutdown(locked_repl_state);
    } else {
      // Before preparing storage for new epoch, we need to finish everything from ReplicationServer (recovery)
      locked_repl_state->GetReplicaRole().server->Shutdown();
    }

    // STEP 1) bring down all REPLICA servers
    dbms_handler_.ForEach([](dbms::DatabaseAccess db_acc) {
      auto *storage = db_acc->storage();
      // Remember old epoch + storage timestamp association
      storage->PrepareForNewEpoch();
    });

    // STEP 2) Change to MAIN
    // TODO: restore replication servers if false?
    if (!locked_repl_state->SetReplicationRoleMain(main_uuid)) {
      // TODO: Handle recovery on failure???
      return false;
    }

    // All DBs should have the same epoch
    auto const new_epoch = ReplicationEpoch();
    spdlog::trace("Generated new epoch {}", new_epoch.id());

    // STEP 3) We are now MAIN, update storage local epoch
    dbms_handler_.ForEach([&](dbms::DatabaseAccess db_acc) {
      auto *storage = db_acc->storage();
      storage->repl_storage_state_.epoch_ = new_epoch;

      // Modifying storage->timestamp_ needs to be done under the engine lock.
      // Engine lock needs to be acquired after the repl state lock
      auto lock = std::lock_guard{storage->engine_lock_};

      // Durability is tracking last durable timestamp from MAIN, whereas timestamp_ is dependent on MVCC
      // We need to take bigger timestamp not to lose durability ordering
      if (auto const ldt = storage->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_;
          ldt >= storage->timestamp_) {
        // Mark all txns finished with IDs in range [old_storage_ts, global_ldt]
        static_cast<storage::InMemoryStorage *>(storage)->commit_log_->MarkFinishedInRange(storage->timestamp_, ldt);
        spdlog::trace("Txn IDs in ranges [{},{}] marked as finished", storage->timestamp_, ldt);
        storage->timestamp_ = ldt + 1;
      }
      spdlog::trace("New timestamp is {} for the database {}.", storage->timestamp_, db_acc->name());
    });

    // STEP 4) Resume TTL
    dbms_handler_.ForEach([](dbms::DatabaseAccess db_acc) {
      auto &ttl = db_acc->ttl();
      ttl.Resume();
    });

    return true;
  } catch (const utils::TryLockException & /* unused */) {
    return false;
  }
};

// as MAIN, define and connect to REPLICAS
auto ReplicationHandler::TryRegisterReplica(const ReplicationClientConfig &config)
    -> utils::BasicResult<query::RegisterReplicaError> {
  try {
    auto locked_repl_state = repl_state_.TryLock();
    return RegisterReplica_<true>(locked_repl_state, config);
  } catch (const utils::TryLockException & /* unused */) {
    return query::RegisterReplicaError::NO_ACCESS;
  }
}

auto ReplicationHandler::RegisterReplica(const ReplicationClientConfig &config)
    -> utils::BasicResult<query::RegisterReplicaError> {
  try {
    auto locked_repl_state = repl_state_.TryLock();
    return RegisterReplica_<false>(locked_repl_state, config);
  } catch (const utils::TryLockException & /* unused */) {
    return query::RegisterReplicaError::NO_ACCESS;
  }
}

auto ReplicationHandler::UnregisterReplica(std::string_view name) -> query::UnregisterReplicaResult {
  try {
    auto locked_repl_state = repl_state_.TryLock();

    auto const replica_handler = [](RoleReplicaData const &) -> query::UnregisterReplicaResult {
      return query::UnregisterReplicaResult::NOT_MAIN;
    };
    auto const main_handler = [this, name,
                               &locked_repl_state](RoleMainData &mainData) -> query::UnregisterReplicaResult {
      if (!locked_repl_state->TryPersistUnregisterReplica(name)) {
        return query::UnregisterReplicaResult::COULD_NOT_BE_PERSISTED;
      }
      // Remove database specific clients
      dbms_handler_.ForEach([name](dbms::DatabaseAccess db_acc) {
        db_acc->storage()->repl_storage_state_.replication_storage_clients_.WithLock([&name](auto &clients) {
          std::erase_if(clients, [name](const auto &client) { return client->Name() == name; });
        });
      });
      // Remove instance level clients
      auto const n_unregistered =
          std::erase_if(mainData.registered_replicas_, [name](auto const &client) { return client.name_ == name; });
      return n_unregistered != 0 ? query::UnregisterReplicaResult::SUCCESS
                                 : query::UnregisterReplicaResult::CANNOT_UNREGISTER;
    };

    return std::visit(utils::Overloaded{main_handler, replica_handler}, locked_repl_state->ReplicationData());
  } catch (const utils::TryLockException & /* unused */) {
    return query::UnregisterReplicaResult::NO_ACCESS;
  }
}

auto ReplicationHandler::GetRole() const -> replication_coordination_glue::ReplicationRole {
  return repl_state_.ReadLock()->GetRole();
}

#ifdef MG_ENTERPRISE
auto ReplicationHandler::GetDatabasesHistories() const -> replication_coordination_glue::InstanceInfo {
  replication_coordination_glue::InstanceInfo results;
  results.last_committed_system_timestamp = system_.LastCommittedSystemTimestamp();
  dbms_handler_.ForEach([&results](dbms::DatabaseAccess db_acc) {
    auto const &repl_storage_state = db_acc->storage()->repl_storage_state_;
    results.dbs_info.emplace_back(std::string{db_acc->storage()->uuid()},
                                  repl_storage_state.commit_ts_info_.load(std::memory_order_acquire).ldt_);
  });

  return results;
}

auto ReplicationHandler::GetReplicationLag() const -> coordination::ReplicationLagInfo {
  coordination::ReplicationLagInfo lag_info;

  dbms_handler_.ForEach([&lag_info](dbms::DatabaseAccess db_acc) {
    auto &repl_storage_state = db_acc->storage()->repl_storage_state_;
    auto const db_name = db_acc->name();
    auto const num_main_committed_txns =
        repl_storage_state.commit_ts_info_.load(std::memory_order_acquire).num_committed_txns_;
    lag_info.dbs_main_committed_txns_.emplace(db_name, num_main_committed_txns);

    repl_storage_state.replication_storage_clients_.WithLock([&db_name, &lag_info,
                                                              &num_main_committed_txns](auto &storage_clients) {
      for (auto &repl_storage_client : storage_clients) {
        auto const replica_name = repl_storage_client->Name();
        auto const num_committed_txns_repl = repl_storage_client->GetNumCommittedTxns();
        auto const replica_lag = num_main_committed_txns - num_committed_txns_repl;
        // Insert or find the already inserted element
        auto [replica_it, _] =
            lag_info.replicas_info_.try_emplace(replica_name, std::map<std::string, coordination::ReplicaDBLagData>{});
        replica_it->second.emplace(db_name,
                                   coordination::ReplicaDBLagData{.num_committed_txns_ = num_committed_txns_repl,
                                                                  .num_txns_behind_main_ = replica_lag});
      }
    });
  });
  return lag_info;
}

#endif

bool ReplicationHandler::IsMain() const { return repl_state_.ReadLock()->IsMain(); }

bool ReplicationHandler::IsReplica() const { return repl_state_.ReadLock()->IsReplica(); }

auto ReplicationHandler::ShowReplicas() const -> utils::BasicResult<query::ShowReplicaError, query::ReplicasInfos> {
  // TODO try lock
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
        if (!full_info && storage->name() != dbms::kDefaultDB) return;
        [[maybe_unused]] auto ok = storage->repl_storage_state_.WithClient(
            replica.name_, [&](const storage::ReplicationStorageClient &client) {
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
      query::ReplicaSystemInfoState const system_info{ts, 0 /* behind ts not implemented */,
                                                      *replica.state_.ReadLock()};
#else
      query::ReplicaSystemInfoState const system_info{};
#endif
      // STEP 3: add entry
      entries.emplace_back(replica.name_, replica.rpc_client_.Endpoint().SocketAddress(), replica.mode_, system_info,
                           std::move(data_info));
    }
    return query::ReplicasInfos{std::move(entries)};
  };
  auto replica = [](RoleReplicaData const &) -> res_t { return query::ShowReplicaError::NOT_MAIN; };

  return std::visit(utils::Overloaded{main, replica}, repl_state_.ReadLock()->ReplicationData());
}

}  // namespace memgraph::replication
