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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <system_error>

#include "auth/auth.hpp"
#include "constants.hpp"
#include "dbms/database.hpp"
#include "dbms/inmemory/replication_handlers.hpp"
#include "dbms/replication_handler.hpp"
#include "kvstore/kvstore.hpp"
#include "replication/replication_client.hpp"
#include "storage/v2/transaction.hpp"
#include "utils/string.hpp"
#ifdef MG_ENTERPRISE
#include "dbms/database_handler.hpp"
#endif
#include "dbms/replication_client.hpp"
#include "dbms/transaction.hpp"
#include "global.hpp"
#include "query/config.hpp"
#include "query/interpreter_context.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/isolation_level.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/logging.hpp"
#include "utils/result.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"
#include "utils/uuid.hpp"

namespace memgraph::dbms {

struct Statistics {
  uint64_t num_vertex;           //!< Sum of vertexes in every database
  uint64_t num_edges;            //!< Sum of edges in every database
  uint64_t triggers;             //!< Sum of triggers in every database
  uint64_t streams;              //!< Sum of streams in every database
  uint64_t users;                //!< Number of defined users
  uint64_t num_databases;        //!< Number of isolated databases
  uint64_t indices;              //!< Sum of indices in every database
  uint64_t constraints;          //!< Sum of constraints in every database
  uint64_t storage_modes[3];     //!< Number of databases in each storage mode [IN_MEM_TX, IN_MEM_ANA, ON_DISK_TX]
  uint64_t isolation_levels[3];  //!< Number of databases in each isolation level [SNAPSHOT, READ_COMM, READ_UNC]
  uint64_t snapshot_enabled;     //!< Number of databases with snapshots enabled
  uint64_t wal_enabled;          //!< Number of databases with WAL enabled
};

static inline nlohmann::json ToJson(const Statistics &stats) {
  nlohmann::json res;

  res["edges"] = stats.num_edges;
  res["vertices"] = stats.num_vertex;
  res["triggers"] = stats.triggers;
  res["streams"] = stats.streams;
  res["users"] = stats.users;
  res["databases"] = stats.num_databases;
  res["indices"] = stats.indices;
  res["constraints"] = stats.constraints;
  res["storage_modes"] = {{storage::StorageModeToString((storage::StorageMode)0), stats.storage_modes[0]},
                          {storage::StorageModeToString((storage::StorageMode)1), stats.storage_modes[1]},
                          {storage::StorageModeToString((storage::StorageMode)2), stats.storage_modes[2]}};
  res["isolation_levels"] = {{storage::IsolationLevelToString((storage::IsolationLevel)0), stats.isolation_levels[0]},
                             {storage::IsolationLevelToString((storage::IsolationLevel)1), stats.isolation_levels[1]},
                             {storage::IsolationLevelToString((storage::IsolationLevel)2), stats.isolation_levels[2]}};
  res["durability"] = {{"snapshot_enabled", stats.snapshot_enabled}, {"WAL_enabled", stats.wal_enabled}};

  return res;
}

/**
 * @brief Multi-database session contexts handler.
 */
class DbmsHandler {
 public:
  using LockT = utils::RWLock;
#ifdef MG_ENTERPRISE

  using NewResultT = utils::BasicResult<NewError, DatabaseAccess>;
  using DeleteResult = utils::BasicResult<DeleteError>;

  /**
   * @brief Initialize the handler.
   *
   * @param configs storage configuration
   * @param auth pointer to the global authenticator
   * @param recovery_on_startup restore databases (and its content) and authentication data
   */
  DbmsHandler(storage::Config config,
              memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth,
              bool recovery_on_startup);  // TODO If more arguments are added use a config struct
#else
  /**
   * @brief Initialize the handler. A single database is supported in community edition.
   *
   * @param configs storage configuration
   */
  DbmsHandler(storage::Config config)
      : repl_state_{ReplicationStateRootPath(config)},
        db_gatekeeper_{[&] {
                         config.salient.name = kDefaultDB;
                         return std::move(config);
                       }(),
                       repl_state_} {
    RecoverReplication(Get());
  }
#endif

#ifdef MG_ENTERPRISE
  /**
   * @brief Create a new Database associated with the "name" database
   *
   * @param name name of the database
   * @return NewResultT context on success, error on failure
   */
  NewResultT New(const std::string &name) {
    std::lock_guard<LockT> wr(lock_);
    const auto uuid = utils::UUID{};
    return New_(name, uuid);
  }
  /**
   * @brief Create a new Database using the passed configuration
   *
   * @param config configuration to be used
   * @return NewResultT context on success, error on failure
   */
  NewResultT New(const storage::SalientConfig &config) {
    std::lock_guard<LockT> wr(lock_);
    auto config_copy = default_config_;
    config_copy.salient = config;  // name, uuid, mode, etc
    UpdatePaths(config_copy, config_copy.durability.storage_directory / "database" / std::string{config.uuid});
    auto new_db = New_(config_copy);

    if (repl_state_.IsReplica()) {
      // If database exists, update the UUID
      if (new_db.HasError() && new_db.GetError() == NewError::EXISTS) {
        spdlog::debug("Trying to create db '{}' on replica which already exists.", config.name);
        auto db = Get_(config.name);
        if (db->uuid() != config.uuid) {
          spdlog::debug("Different UUIDs");

          // TODO: Fix this hack
          if (config.name == kDefaultDB) {
            spdlog::debug("Update default db's UUID");
            // Default db cannot be deleted and remade, have to just update the UUID
            db->storage()->config_.salient.uuid = config.uuid;
            UpdateDurability(db->storage()->config_, ".");
            return db;
          }

          spdlog::debug("Drop database and recreate with the correct UUID");
          // Defer drop
          (void)Delete_(db->name());
          // Second attempt
          return New_(std::move(config_copy));
        }
      }
    }

    return new_db;
  }

  void UpdateDurability(const storage::Config &config, std::optional<std::filesystem::path> rel_dir = {});

  /**
   * @brief Get the context associated with the "name" database
   *
   * @param name
   * @return DatabaseAccess
   * @throw UnknownDatabaseException if database not found
   */
  DatabaseAccess Get(std::string_view name = kDefaultDB) {
    std::shared_lock<LockT> rd(lock_);
    return Get_(name);
  }

  /**
   * @brief Get the context associated with the UUID database
   *
   * @param uuid
   * @return DatabaseAccess
   * @throw UnknownDatabaseException if database not found
   */
  DatabaseAccess Get(const utils::UUID &uuid) {
    std::shared_lock<LockT> rd(lock_);
    for (auto &[_, db_gk] : db_handler_) {
      auto acc = db_gk.access();
      if (acc->get()->uuid() == uuid) {
        return std::move(*acc);
      }
    }
    throw UnknownDatabaseException("Tried to retrieve an unknown database with UUID \"{}\".", std::string{uuid});
  }

#else
  /**
   * @brief Get the context associated with the default database
   *
   * @return DatabaseAccess
   */
  DatabaseAccess Get() {
    auto acc = db_gatekeeper_.access();
    MG_ASSERT(acc, "Failed to get default database!");
    return *acc;
  }
#endif

#ifdef MG_ENTERPRISE
  /**
   * @brief Attempt to delete database.
   *
   * @param db_name database name
   * @return DeleteResult error on failure
   */
  DeleteResult TryDelete(std::string_view db_name);

  /**
   * @brief Delete or defer deletion of database.
   *
   * @param db_name database name
   * @return DeleteResult error on failure
   */
  DeleteResult Delete(std::string_view db_name);
#endif

  /**
   * @brief Return all active databases.
   *
   * @return std::vector<std::string>
   */
  std::vector<std::string> All() const {
#ifdef MG_ENTERPRISE
    std::shared_lock<LockT> rd(lock_);
    return db_handler_.All();
#else
    return {db_gatekeeper_.access()->get()->name()};
#endif
  }

  replication::ReplicationState &ReplicationState() { return repl_state_; }
  replication::ReplicationState const &ReplicationState() const { return repl_state_; }

  bool IsMain() const { return repl_state_.IsMain(); }
  bool IsReplica() const { return repl_state_.IsReplica(); }

  /**
   * @brief Return the statistics all databases.
   *
   * @return Statistics
   */
  Statistics Stats() {
    auto const replication_role = repl_state_.GetRole();
    Statistics stats{};
    // TODO: Handle overflow?
#ifdef MG_ENTERPRISE
    std::shared_lock<LockT> rd(lock_);
    for (auto &[_, db_gk] : db_handler_) {
#else
    {
      auto &db_gk = db_gatekeeper_;
#endif
      auto db_acc_opt = db_gk.access();
      if (db_acc_opt) {
        auto &db_acc = *db_acc_opt;
        const auto &info = db_acc->GetInfo(false, replication_role);
        const auto &storage_info = info.storage_info;
        stats.num_vertex += storage_info.vertex_count;
        stats.num_edges += storage_info.edge_count;
        stats.triggers += info.triggers;
        stats.streams += info.streams;
        ++stats.num_databases;
        stats.indices += storage_info.label_indices + storage_info.label_property_indices;
        stats.constraints += storage_info.existence_constraints + storage_info.unique_constraints;
        ++stats.storage_modes[(int)storage_info.storage_mode];
        ++stats.isolation_levels[(int)storage_info.isolation_level];
        stats.snapshot_enabled += storage_info.durability_snapshot_enabled;
        stats.wal_enabled += storage_info.durability_wal_enabled;
      }
    }
    return stats;
  }

  /**
   * @brief Return a vector with all database info.
   *
   * @return std::vector<DatabaseInfo>
   */
  std::vector<DatabaseInfo> Info() {
    auto const replication_role = repl_state_.GetRole();
    std::vector<DatabaseInfo> res;
#ifdef MG_ENTERPRISE
    std::shared_lock<LockT> rd(lock_);
    res.reserve(std::distance(db_handler_.cbegin(), db_handler_.cend()));
    for (auto &[_, db_gk] : db_handler_) {
#else
    {
      auto &db_gk = db_gatekeeper_;
#endif
      auto db_acc_opt = db_gk.access();
      if (db_acc_opt) {
        auto &db_acc = *db_acc_opt;
        res.push_back(db_acc->GetInfo(false, replication_role));
      }
    }
    return res;
  }

  /**
   * @brief Restore triggers for all currently defined databases.
   * @note: Triggers can execute query procedures, so we need to reload the modules first and then the triggers
   *
   * @param ic global InterpreterContext
   */
  void RestoreTriggers(query::InterpreterContext *ic) {
#ifdef MG_ENTERPRISE
    std::lock_guard<LockT> wr(lock_);
    for (auto &[_, db_gk] : db_handler_) {
#else
    {
      auto &db_gk = db_gatekeeper_;
#endif
      auto db_acc_opt = db_gk.access();
      if (db_acc_opt) {
        auto &db_acc = *db_acc_opt;
        spdlog::debug("Restoring trigger for database \"{}\"", db_acc->name());
        auto storage_accessor = db_acc->Access();
        auto dba = memgraph::query::DbAccessor{storage_accessor.get()};
        db_acc->trigger_store()->RestoreTriggers(&ic->ast_cache, &dba, ic->config.query, ic->auth_checker);
      }
    }
  }

  /**
   * @brief Restore streams of all currently defined databases.
   * @note: Stream transformations are using modules, they have to be restored after the query modules are loaded.
   *
   * @param ic global InterpreterContext
   */
  void RestoreStreams(query::InterpreterContext *ic) {
#ifdef MG_ENTERPRISE
    std::lock_guard<LockT> wr(lock_);
    for (auto &[_, db_gk] : db_handler_) {
#else
    {
      auto &db_gk = db_gatekeeper_;
#endif
      auto db_acc = db_gk.access();
      if (db_acc) {
        auto *db = db_acc->get();
        spdlog::debug("Restoring streams for database \"{}\"", db->name());
        db->streams()->RestoreStreams(*db_acc, ic);
      }
    }
  }

  /**
   * @brief todo
   *
   * @param f
   */
  void ForEach(auto f) {
#ifdef MG_ENTERPRISE
    std::shared_lock<LockT> rd(lock_);
    for (auto &[_, db_gk] : db_handler_) {
#else
    {
      auto &db_gk = db_gatekeeper_;
#endif
      auto db_acc = db_gk.access();
      if (db_acc) {  // This isn't an error, just a defunct db
        if constexpr (std::is_invocable_v<decltype(f), DatabaseAccess>) {
          f(*db_acc);
        } else if constexpr (std::is_invocable_v<decltype(f), Database *>) {
          f(db_acc->get());
        }
      }
    }
  }

  /**
   * @brief todo
   *
   * @param f
   */
  void ForOne(auto f) {
#ifdef MG_ENTERPRISE
    std::shared_lock<LockT> rd(lock_);
    for (auto &[_, db_gk] : db_handler_) {
      auto db_acc = db_gk.access();
      if (!db_acc) continue;  // This isn't an error, just a defunct db
      if constexpr (std::is_invocable_v<decltype(f), DatabaseAccess>) {
        if (f(*db_acc)) break;  // Run until the first successful one
      } else if constexpr (std::is_invocable_v<decltype(f), Database *>) {
        if (f(db_acc->get())) break;  // Run until the first successful one
      }
    }
#else
    {
      auto db_acc = db_gatekeeper_.access();
      MG_ASSERT(db_acc, "Should always have the database");
      f(db_acc->get());
    }
#endif
  }

  void NewSystemTransaction() {
    DMG_ASSERT(!system_transaction_, "Already running a system transaction");
    system_transaction_.emplace(system_timestamp_++);
  }

  void ResetSystemTransaction() { system_transaction_.reset(); }

  void Commit() {
    if (system_transaction_ == std::nullopt || system_transaction_->delta == std::nullopt) return;  // Nothing to commit
    const auto &delta = *system_transaction_->delta;

    // TODO Create a system client that can handle all of this automatically
    switch (delta.action) {
      using enum SystemTransaction::Delta::Action;
      case CREATE_DATABASE: {
#ifdef MG_ENTERPRISE
        const auto &wal_key = system_wal_->GenKey(*system_transaction_);
        // WAL
        const auto &wal_val =
            system_wal_->GenVal(SystemTransaction::Delta::create_database, *system_transaction_->delta);
        system_wal_->Put(wal_key, wal_val);

        // Replication
        auto main_handler = [&](memgraph::replication::RoleMainData &main_data) {
          // TODO: data race issue? registered_replicas_ access not protected
          // This is sync in any case, as this is the startup
          for (auto &client : main_data.registered_replicas_) {
            try {
              auto stream = client.rpc_client_.Stream<storage::replication::CreateDatabaseRpc>(
                  std::string(main_data.epoch_.id()), system_transaction_->system_timestamp, delta.config);
              const auto response = stream.AwaitResponse();
              if (response.result == storage::replication::CreateDatabaseRes::Result::FAILURE) {
                // TODO This replica needs SYSTEM recovery
                client.behind_ = true;
                return;
              }
            } catch (memgraph::rpc::GenericRpcFailedException const &e) {
              // TODO This replica needs SYSTEM recovery
              client.behind_ = true;
              return;
            }
          }
          // Sync database with REPLICAs
          RecoverReplication(Get_(delta.config.name));
        };
        auto replica_handler = [](memgraph::replication::RoleReplicaData &) { /* Nothing to do */ };
        std::visit(utils::Overloaded{main_handler, replica_handler}, repl_state_.ReplicationData());
#endif
      } break;
    }
    last_commited_system_timestamp_ = system_transaction_->system_timestamp;
    ResetSystemTransaction();
  }

  uint64_t LastCommitedTS() const { return last_commited_system_timestamp_; }

  std::pair<kvstore::KVStore::iterator, kvstore::KVStore::iterator> SystemWAL(
      const std::optional<uint64_t> first_ts) const {
    auto itr = system_wal_->begin("delta:");
    auto end = system_wal_->end("delta:");

    if (first_ts) {
      // Find the specific commit wanted by the user
      for (; itr != end; ++itr) {
        std::vector<std::string> out;
        utils::Split(&out, itr->first, ":", 2);
        const auto system_timestamp = std::stoul(out[1]);
        if (system_timestamp >= *first_ts) break;
      }
    }

    return std::make_pair(std::move(itr), std::move(end));
  }

#ifdef MG_ENTERPRISE
  void SystemRestore(replication::ReplicationClient &client) {
    // Check if system is up to date
    if (!client.behind_) return;

    uint64_t replica_ts = storage::kTimestampInitialId;
    try {
      auto stream{client.rpc_client_.Stream<memgraph::replication::SystemHeartbeatRpc>()};
      const auto res = stream.AwaitResponse();
      replica_ts = res.system_timestamp;
    } catch (...) {
      client.behind_ = true;
    }

    // TODO Check for branching....
    const auto &epoch = std::get<replication::RoleMainData>(std::as_const(ReplicationState()).ReplicationData()).epoch_;

    // Try to recover...
    {
      ForEach([&client, &epoch, ts = last_commited_system_timestamp_.load()](DatabaseAccess acc) {
        try {
          const auto &storage = acc->storage();
          auto stream = client.rpc_client_.Stream<storage::replication::CreateDatabaseRpc>(std::string(epoch.id()), ts,
                                                                                           storage->config_.salient);
          const auto response = stream.AwaitResponse();
          if (response.result == storage::replication::CreateDatabaseRes::Result::FAILURE) {
            client.behind_ = true;
            return;
          }
        } catch (memgraph::rpc::GenericRpcFailedException const &e) {
          client.behind_ = true;
          return;
        }
      });
    }

    // Successfully went through the deltas, we are up to date
    client.behind_ = false;
  }
#endif

 private:
#ifdef MG_ENTERPRISE
  /**
   * @brief return the storage directory of the associated database
   *
   * @param name Database name
   * @return std::optional<std::filesystem::path>
   */
  std::optional<std::filesystem::path> StorageDir_(std::string_view name) {
    const auto conf = db_handler_.GetConfig(name);
    if (conf) {
      return conf->durability.storage_directory;
    }
    spdlog::debug("Failed to find storage dir for database \"{}\"", name);
    return {};
  }

  /**
   * @brief Create a new Database associated with the "name" database
   *
   * @param name name of the database
   * @param uuid undelying RocksDB directory
   * @return NewResultT context on success, error on failure
   */
  NewResultT New_(std::string_view name, utils::UUID uuid, std::optional<std::filesystem::path> rel_dir = {}) {
    auto config_copy = default_config_;
    config_copy.salient.name = name;
    config_copy.salient.uuid = uuid;
    spdlog::debug("Creating database '{}' - '{}'", name, std::string{uuid});
    if (rel_dir) {
      storage::UpdatePaths(config_copy, default_config_.durability.storage_directory / *rel_dir);
    } else {
      storage::UpdatePaths(config_copy, default_config_.durability.storage_directory / "databases" / std::string{uuid});
    }
    return New_(std::move(config_copy));
  }

  /**
   * @brief Create a new Database associated with the "name" database
   *
   * @param storage_config storage configuration
   * @return NewResultT context on success, error on failure
   */
  NewResultT New_(storage::Config storage_config);

  DeleteResult Delete_(std::string_view db_name);

  /**
   * @brief Create a new Database associated with the default database
   *
   * @return NewResultT context on success, error on failure
   */
  void SetupDefault_() {
    try {
      Get(kDefaultDB);
    } catch (const UnknownDatabaseException &) {
      // No default DB restored, create it
      MG_ASSERT(New_(kDefaultDB, {/* random UUID */}, ".").HasValue(), "Failed while creating the default database");
    }

    // For back-compatibility...
    // Recreate the dbms layout for the default db and symlink to the root
    const auto dir = StorageDir_(kDefaultDB);
    MG_ASSERT(dir, "Failed to find storage path.");
    const auto main_dir = *dir / "databases" / kDefaultDB;

    if (!std::filesystem::exists(main_dir)) {
      std::filesystem::create_directory(main_dir);
    }

    // Force link on-disk directories
    const auto conf = db_handler_.GetConfig(kDefaultDB);
    MG_ASSERT(conf, "No configuration for the default database.");
    const auto &tmp_conf = conf->disk;
    std::vector<std::filesystem::path> to_link{
        tmp_conf.main_storage_directory,         tmp_conf.label_index_directory,
        tmp_conf.label_property_index_directory, tmp_conf.unique_constraints_directory,
        tmp_conf.name_id_mapper_directory,       tmp_conf.id_name_mapper_directory,
        tmp_conf.durability_directory,           tmp_conf.wal_directory,
    };

    // Add in-memory paths
    // Some directories are redundant (skip those)
    const std::vector<std::string> skip{".lock", "audit_log", "auth", "databases", "internal_modules", "settings"};
    for (auto const &item : std::filesystem::directory_iterator{*dir}) {
      const auto dir_name = std::filesystem::relative(item.path(), item.path().parent_path());
      if (std::find(skip.begin(), skip.end(), dir_name) != skip.end()) continue;
      to_link.push_back(item.path());
    }

    // Symlink to root dir
    for (auto const &item : to_link) {
      const auto dir_name = std::filesystem::relative(item, item.parent_path());
      const auto link = main_dir / dir_name;
      const auto to = std::filesystem::relative(item, main_dir);
      if (!std::filesystem::is_symlink(link) && !std::filesystem::exists(link)) {
        std::filesystem::create_directory_symlink(to, link);
      } else {  // Check existing link
        std::error_code ec;
        const auto test_link = std::filesystem::read_symlink(link, ec);
        if (ec || test_link != to) {
          MG_ASSERT(false,
                    "Memgraph storage directory incompatible with new version.\n"
                    "Please use a clean directory or remove \"{}\" and try again.",
                    link.string());
        }
      }
    }
  }

  /**
   * @brief Get the DatabaseAccess for the database associated with the "name"
   *
   * @param name
   * @return DatabaseAccess
   * @throw UnknownDatabaseException if trying to get unknown database
   */
  DatabaseAccess Get_(std::string_view name) {
    auto db = db_handler_.Get(name);
    if (db) {
      return *db;
    }
    throw UnknownDatabaseException("Tried to retrieve an unknown database \"{}\".", name);
  }
#endif

  void RecoverReplication(DatabaseAccess db_acc) {
    if (allow_mt_repl || db_acc->name() == dbms::kDefaultDB) {
      // Handle global replication state
      spdlog::info("Replication configuration will be stored and will be automatically restored in case of a crash.");
      // RECOVER REPLICA CONNECTIONS
      memgraph::dbms::RestoreReplication(repl_state_, std::move(db_acc));
    } else if (const ::memgraph::replication::RoleMainData *data =
                   std::get_if<::memgraph::replication::RoleMainData>(&repl_state_.ReplicationData());
               data && !data->registered_replicas_.empty()) {
      spdlog::warn("Multi-tenant replication is currently not supported!");
    }
  }

#ifdef MG_ENTERPRISE
  mutable LockT lock_{utils::RWLock::Priority::READ};  //!< protective lock
  storage::Config default_config_;                     //!< Storage configuration used when creating new databases
  DatabaseHandler db_handler_;                         //!< multi-tenancy storage handler
  std::unique_ptr<kvstore::KVStore> durability_;       //!< list of active dbs (pointer so we can postpone its creation)
#endif
  std::optional<SystemTransaction> system_transaction_;      //!< Current system transaction (only one at a time)
  uint64_t system_timestamp_{storage::kTimestampInitialId};  //!< System timestamp
  std::atomic_uint64_t last_commited_system_timestamp_{
      storage::kTimestampInitialId};          //!< Last commited system timestamp
  std::unique_ptr<WAL> system_wal_;           //!< System WAL
  replication::ReplicationState repl_state_;  //!< Global replication state
#ifndef MG_ENTERPRISE
  mutable utils::Gatekeeper<Database> db_gatekeeper_;  //!< Single databases gatekeeper
#endif
};  // namespace memgraph::dbms

}  // namespace memgraph::dbms
