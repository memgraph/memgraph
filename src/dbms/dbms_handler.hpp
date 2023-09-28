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

#pragma once

#include <algorithm>
#include <concepts>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <system_error>
#include <unordered_map>

#include "auth/auth.hpp"
#include "constants.hpp"
#include "dbms/database_handler.hpp"
#include "global.hpp"
#include "query/config.hpp"
#include "query/interpreter_context.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/paths.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/logging.hpp"
#include "utils/result.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"
#include "utils/uuid.hpp"

namespace memgraph::dbms {

#ifdef MG_ENTERPRISE

using DeleteResult = utils::BasicResult<DeleteError>;

/**
 * @brief Multi-database session contexts handler.
 */
class DbmsHandler {
 public:
  using LockT = utils::RWLock;
  using NewResultT = utils::BasicResult<NewError, DatabaseAccess>;

  struct Statistics {
    uint64_t num_vertex;     //!< Sum of vertexes in every database
    uint64_t num_edges;      //!< Sum of edges in every database
    uint64_t num_databases;  //! number of isolated databases
  };

  /**
   * @brief Initialize the handler.
   *
   * @param configs storage and interpreter configurations
   * @param auth pointer to the global authenticator
   * @param recovery_on_startup restore databases (and its content) and authentication data
   * @param delete_on_drop when dropping delete any associated directories on disk
   */
  DbmsHandler(storage::Config config, auto *auth, bool recovery_on_startup, bool delete_on_drop)
      : lock_{utils::RWLock::Priority::READ}, default_config_{std::move(config)}, delete_on_drop_(delete_on_drop) {
    // TODO: Decouple storage config from dbms config
    // TODO: Save individual db configs inside the kvstore and restore from there
    storage::UpdatePaths(*default_config_, default_config_->durability.storage_directory / "databases");
    const auto &db_dir = default_config_->durability.storage_directory;
    const auto durability_dir = db_dir / ".durability";
    utils::EnsureDirOrDie(db_dir);
    utils::EnsureDirOrDie(durability_dir);
    durability_ = std::make_unique<kvstore::KVStore>(durability_dir);

    // Generate the default database
    MG_ASSERT(!NewDefault_().HasError(), "Failed while creating the default DB.");

    // Recover previous databases
    if (recovery_on_startup) {
      for (const auto &[name, _] : *durability_) {
        if (name == kDefaultDB) continue;  // Already set
        spdlog::info("Restoring database {}.", name);
        MG_ASSERT(!New_(name).HasError(), "Failed while creating database {}.", name);
        spdlog::info("Database {} restored.", name);
      }
    } else {  // Clear databases from the durability list and auth
      auto locked_auth = auth->Lock();
      for (const auto &[name, _] : *durability_) {
        if (name == kDefaultDB) continue;
        locked_auth->DeleteDatabase(name);
        durability_->Delete(name);
      }
    }
  }

  /**
   * @brief Create a new Database associated with the "name" database
   *
   * @param name name of the database
   * @return NewResultT context on success, error on failure
   */
  NewResultT New(const std::string &name) {
    std::lock_guard<LockT> wr(lock_);
    return New_(name, name);
  }

  /**
   * @brief Get the context associated with the "name" database
   *
   * @param name
   * @return DatabaseAccess
   * @throw UnknownDatabaseException if database not found
   */
  DatabaseAccess Get(std::string_view name) {
    std::shared_lock<LockT> rd(lock_);
    return Get_(name);
  }

  /**
   * @brief Delete database.
   *
   * @param db_name database name
   * @return DeleteResult error on failure
   */
  DeleteResult Delete(const std::string &db_name) {
    std::lock_guard<LockT> wr(lock_);
    if (db_name == kDefaultDB) {
      // MSG cannot delete the default db
      return DeleteError::DEFAULT_DB;
    }

    const auto storage_path = StorageDir_(db_name);
    if (!storage_path) return DeleteError::NON_EXISTENT;

    // Check if db exists
    try {
      // Low level handlers
      if (!db_handler_.Delete(db_name)) {
        return DeleteError::USING;
      }
    } catch (utils::BasicException &) {
      return DeleteError::NON_EXISTENT;
    }

    // Remove from durability list
    if (durability_) durability_->Delete(db_name);

    // Delete disk storage
    if (delete_on_drop_) {
      std::error_code ec;
      (void)std::filesystem::remove_all(*storage_path, ec);
      if (ec) {
        spdlog::error("Failed to clean disk while deleting database \"{}\".", db_name);
        defunct_dbs_.emplace(db_name);
        return DeleteError::DISK_FAIL;
      }
    }

    // Delete from defunct_dbs_ (in case a second delete call was successful)
    defunct_dbs_.erase(db_name);

    return {};  // Success
  }

  /**
   * @brief Return all active databases.
   *
   * @return std::vector<std::string>
   */
  std::vector<std::string> All() const {
    std::shared_lock<LockT> rd(lock_);
    return db_handler_.All();
  }

  /**
   * @brief Return the number of vertex across all databases.
   *
   * @return uint64_t
   */
  Statistics Info() {
    // TODO: Handle overflow?
    uint64_t nv = 0;
    uint64_t ne = 0;
    std::shared_lock<LockT> rd(lock_);
    const uint64_t ndb = std::distance(db_handler_.cbegin(), db_handler_.cend());
    for (auto &[_, db_gk] : db_handler_) {
      auto db_acc_opt = db_gk.access();
      if (!db_acc_opt) continue;
      auto &db_acc = *db_acc_opt;
      const auto &info = db_acc->GetInfo();
      nv += info.vertex_count;
      ne += info.edge_count;
    }
    return {nv, ne, ndb};
  }

  /**
   * @brief Restore triggers for all currently defined databases.
   * @note: Triggers can execute query procedures, so we need to reload the modules first and then the triggers
   *
   * @param ic global InterpreterContext
   */
  void RestoreTriggers(query::InterpreterContext *ic) {
    std::lock_guard<LockT> wr(lock_);
    for (auto &[_, db_gk] : db_handler_) {
      auto db_acc_opt = db_gk.access();
      if (!db_acc_opt) continue;
      auto &db_acc = *db_acc_opt;
      spdlog::debug("Restoring trigger for database \"{}\"", db_acc->id());
      auto storage_accessor = db_acc->Access();
      auto dba = memgraph::query::DbAccessor{storage_accessor.get()};
      db_acc->trigger_store()->RestoreTriggers(&ic->ast_cache, &dba, ic->config.query, ic->auth_checker);
    }
  }

  /**
   * @brief Restore streams of all currently defined databases.
   * @note: Stream transformations are using modules, they have to be restored after the query modules are loaded.
   *
   * @param ic global InterpreterContext
   */
  void RestoreStreams(query::InterpreterContext *ic) {
    std::lock_guard<LockT> wr(lock_);
    for (auto &[_, db_gk] : db_handler_) {
      auto db_acc = db_gk.access();
      if (!db_acc) continue;
      auto *db = db_acc->get();
      spdlog::debug("Restoring streams for database \"{}\"", db->id());
      db->streams()->RestoreStreams(*db_acc, ic);
    }
  }

 private:
  /**
   * @brief return the storage directory of the associated database
   *
   * @param name Database name
   * @return std::optional<std::filesystem::path>
   */
  std::optional<std::filesystem::path> StorageDir_(const std::string &name) {
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
   * @return NewResultT context on success, error on failure
   */
  NewResultT New_(const std::string &name) { return New_(name, name); }

  /**
   * @brief Create a new Database associated with the "name" database
   *
   * @param name name of the database
   * @param storage_subdir undelying RocksDB directory
   * @return NewResultT context on success, error on failure
   */
  NewResultT New_(const std::string &name, std::filesystem::path storage_subdir) {
    if (default_config_) {
      auto config_copy = *default_config_;
      storage::UpdatePaths(config_copy, default_config_->durability.storage_directory / storage_subdir);
      return New_(name, config_copy);
    }
    spdlog::info("Trying to generate session context without any configurations.");
    return NewError::NO_CONFIGS;
  }

  /**
   * @brief Create a new Database associated with the "name" database
   *
   * @param name name of the database
   * @param storage_config storage configuration
   * @return NewResultT context on success, error on failure
   */
  NewResultT New_(const std::string &name, storage::Config &storage_config) {
    if (defunct_dbs_.contains(name)) {
      spdlog::warn("Failed to generate database due to the unknown state of the previously defunct database \"{}\".",
                   name);
      return NewError::DEFUNCT;
    }

    auto new_db = db_handler_.New(name, storage_config);
    if (new_db.HasValue()) {
      // Success
      if (durability_) durability_->Put(name, "ok");  // TODO: Serialize the configuration?
      return new_db.GetValue();
    }
    return new_db.GetError();
  }

  /**
   * @brief Create a new Database associated with the default database
   *
   * @return NewResultT context on success, error on failure
   */
  NewResultT NewDefault_() {
    // Create the default DB in the root (this is how it was done pre multi-tenancy)
    auto res = New_(kDefaultDB, "..");
    if (res.HasValue()) {
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
    return res;
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

  // Should storage objects ever be deleted?
  mutable LockT lock_;                             //!< protective lock
  DatabaseHandler db_handler_;                     //!< multi-tenancy storage handler
  std::optional<storage::Config> default_config_;  //!< Storage configuration used when creating new databases
  std::unique_ptr<kvstore::KVStore> durability_;   //!< list of active dbs (pointer so we can postpone its creation)
  std::set<std::string> defunct_dbs_;              //!< Databases that are in an unknown state due to various failures
  bool delete_on_drop_;                            //!< Flag defining if dropping storage also deletes its directory
};
#endif

}  // namespace memgraph::dbms
