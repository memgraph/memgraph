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

#include "constants.hpp"
#include "global.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
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

#include "dbms/database_handler.hpp"

#include "handler.hpp"

#ifdef MG_ENTERPRISE
#else
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"
#endif

namespace memgraph::dbms {

#ifdef MG_ENTERPRISE

using DeleteResult = utils::BasicResult<DeleteError>;

/**
 * @brief Multi-database session contexts handler.
 */
class NewSessionHandler {
 public:
  using LockT = utils::RWLock;
  using NewResultT = utils::BasicResult<NewError, std::shared_ptr<Database>>;

  // struct Statistics {
  //   uint64_t num_vertex;     //!< Sum of vertexes in every database
  //   uint64_t num_edges;      //!< Sum of edges in every database
  //   uint64_t num_databases;  //! number of isolated databases
  // };

  /**
   * @brief Initialize the handler.
   *
   * @param audit_log pointer to the audit logger (ENTERPRISE only)
   * @param configs storage and interpreter configurations
   * @param recovery_on_startup restore databases (and its content) and authentication data
   */
  NewSessionHandler(storage::Config config)
      : lock_{utils::RWLock::Priority::READ}, default_config_{std::move(config)}, run_id_{utils::GenerateUUID()} {
    // Update
    const auto &root = default_config_->durability.storage_directory;
    utils::EnsureDirOrDie(root);
    // Verify that the user that started the process is the same user that is
    // the owner of the storage directory.
    storage::durability::VerifyStorageDirectoryOwnerAndProcessUserOrDie(root);

    // Create the lock file and open a handle to it. This will crash the
    // database if it can't open the file for writing or if any other process is
    // holding the file opened.
    lock_file_path_ = root / ".lock";
    lock_file_handle_.Open(lock_file_path_, utils::OutputFile::Mode::OVERWRITE_EXISTING);
    MG_ASSERT(lock_file_handle_.AcquireLock(),
              "Couldn't acquire lock on the storage directory {}"
              "!\nAnother Memgraph process is currently running with the same "
              "storage directory, please stop it first before starting this "
              "process!",
              root);

    // TODO: Decouple storage config from dbms config
    // TODO: Save individual db configs inside the kvstore and restore from there
    storage::UpdatePaths(*default_config_, default_config_->durability.storage_directory / "databases");
    const auto &db_dir = default_config_->durability.storage_directory;
    const auto durability_dir = db_dir / ".durability";
    utils::EnsureDirOrDie(db_dir);

    // Generate the default database
    MG_ASSERT(!NewDefault_().HasError(), "Failed while creating the default DB.");
  }

  //   void Shutdown() {
  //     for (auto &ic : interp_handler_) memgraph::query::Shutdown(ic.second.get().get());
  //   }

  /**
   * @brief Create a new SessionContext associated with the "name" database
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
   * @return SessionContext
   * @throw UnknownDatabaseException if getting unknown database
   */
  std::shared_ptr<Database> Get(std::string_view name) {
    std::shared_lock<LockT> rd(lock_);
    return Get_(name);
  }

  //   /**
  //    * @brief Set the undelying database for a particular session.
  //    *
  //    * @param uuid unique session identifier
  //    * @param db_name unique database name
  //    * @return SetForResult enum
  //    * @throws UnknownDatabaseException, UnknownSessionException or anything OnChange throws
  //    */
  //   SetForResult SetFor(const std::string &uuid, const std::string &db_name) {
  //     std::shared_lock<LockT> rd(lock_);
  //     (void)Get_(
  //         db_name);  // throws if db doesn't exist (TODO: Better to pass it via OnChange - but injecting dependency)
  //     try {
  //       auto &s = sessions_.at(uuid);
  //       return s.OnChange(db_name);
  //     } catch (std::out_of_range &) {
  //       throw UnknownSessionException("Unknown session \"{}\"", uuid);
  //     }
  //   }

  //   /**
  //    * @brief Set the undelying database from a session itself. SessionContext handler.
  //    *
  //    * @param db_name unique database name
  //    * @param handler function that gets called in place with the appropriate SessionContext
  //    * @return SetForResult enum
  //    */
  //   template <typename THandler>
  //   requires std::invocable<THandler, SessionContext> SetForResult SetInPlace(const std::string &db_name,
  //                                                                             THandler handler) {
  //     std::shared_lock<LockT> rd(lock_);
  //     return handler(Get_(db_name));
  //   }

  //   /**
  //    * @brief Call void handler under a shared lock.
  //    *
  //    * @param handler function that gets called in place
  //    */
  //   template <typename THandler>
  //   requires std::invocable<THandler>
  //   void CallInPlace(THandler handler) {
  //     std::shared_lock<LockT> rd(lock_);
  //     handler();
  //   }

  //   /**
  //    * @brief Register an active session (used to handle callbacks).
  //    *
  //    * @param session
  //    * @return true on success
  //    */
  //   bool Register(SessionInterface &session) {
  //     std::lock_guard<LockT> wr(lock_);
  //     auto [_, success] = sessions_.emplace(session.UUID(), session);
  //     return success;
  //   }

  //   /**
  //    * @brief Delete a session.
  //    *
  //    * @param session
  //    */
  //   bool Delete(const SessionInterface &session) {
  //     std::lock_guard<LockT> wr(lock_);
  //     return sessions_.erase(session.UUID()) > 0;
  //   }

  //   /**
  //    * @brief Delete database.
  //    *
  //    * @param db_name database name
  //    * @return DeleteResult error on failure
  //    */
  //   DeleteResult Delete(const std::string &db_name) {
  //     std::lock_guard<LockT> wr(lock_);
  //     if (db_name == kDefaultDB) {
  //       // MSG cannot delete the default db
  //       return DeleteError::DEFAULT_DB;
  //     }
  //     // Check if db exists
  //     try {
  //       auto sc = Get_(db_name);
  //       // Check if a session is using the db
  //       if (!sc.interpreter_context->interpreters->empty()) {
  //         return DeleteError::USING;
  //       }
  //     } catch (UnknownDatabaseException &) {
  //       return DeleteError::NON_EXISTENT;
  //     }

  //     // High level handlers
  //     for (auto &[_, s] : sessions_) {
  //       if (!s.OnDelete(db_name)) {
  //         spdlog::error("Partial failure while deleting database \"{}\".", db_name);
  //         defunct_dbs_.emplace(db_name);
  //         return DeleteError::FAIL;
  //       }
  //     }

  //     // Low level handlers
  //     const auto storage_path = StorageDir_(db_name);
  //     MG_ASSERT(storage_path, "Missing storage for {}", db_name);
  //     if (!db_handler_.Delete(db_name) || !interp_handler_.Delete(db_name)) {
  //       spdlog::error("Partial failure while deleting database \"{}\".", db_name);
  //       defunct_dbs_.emplace(db_name);
  //       return DeleteError::FAIL;
  //     }

  //     // Remove from auth
  //     auth_->Lock()->DeleteDatabase(db_name);
  //     // Remove from durability list
  //     if (durability_) durability_->Delete(db_name);

  //     // Delete disk storage
  //     if (delete_on_drop_) {
  //       std::error_code ec;
  //       (void)std::filesystem::remove_all(*storage_path, ec);
  //       if (ec) {
  //         spdlog::error("Failed to clean disk while deleting database \"{}\".", db_name);
  //         defunct_dbs_.emplace(db_name);
  //         return DeleteError::DISK_FAIL;
  //       }
  //     }

  //     // Delete from defunct_dbs_ (in case a second delete call was successful)
  //     defunct_dbs_.erase(db_name);

  //     return {};  // Success
  //   }

  // /**
  //  * @brief Set the default configurations.
  //  *
  //  * @param configs storage, interpreter and authorization configurations
  //  */
  // void SetDefaultConfigs(storage::Config configs) {
  //   std::lock_guard<LockT> wr(lock_);
  //   default_config_ = std::move(configs);
  // }

  // /**
  //  * @brief Get the default configurations.
  //  *
  //  * @return std::optional<Config>
  //  */
  // std::optional<storage::Config> GetDefaultConfigs() const {
  //   std::shared_lock<LockT> rd(lock_);
  //   return default_config_;
  // }

  //   /**
  //    * @brief Return all active databases.
  //    *
  //    * @return std::vector<std::string>
  //    */
  //   std::vector<std::string> All() const {
  //     std::shared_lock<LockT> rd(lock_);
  //     return db_handler_.All();
  //   }

  //   /**
  //    * @brief Return the number of vertex across all databases.
  //    *
  //    * @return uint64_t
  //    */
  //   Statistics Info() const {
  //     // TODO: Handle overflow
  //     uint64_t nv = 0;
  //     uint64_t ne = 0;
  //     std::shared_lock<LockT> rd(lock_);
  //     const uint64_t ndb = std::distance(db_handler_.cbegin(), db_handler_.cend());
  //     for (const auto &[_, db] : db_handler_) {
  //       const auto &info = db->GetInfo();
  //       nv += info.vertex_count;
  //       ne += info.edge_count;
  //     }
  //     return {nv, ne, ndb};
  //   }

  //   /**
  //    * @brief Return the currently active database for a particular session.
  //    *
  //    * @param uuid session's unique identifier
  //    * @return std::string name of the database
  //    * @throw
  //    */
  //   std::string Current(const std::string &uuid) const {
  //     std::shared_lock<LockT> rd(lock_);
  //     return sessions_.at(uuid).GetDatabaseName();
  //   }

  //   /**
  //    * @brief Restore triggers for all currently defined databases.
  //    * @note: Triggers can execute query procedures, so we need to reload the modules first and then the triggers
  //    */
  //   void RestoreTriggers() {
  //     std::lock_guard<LockT> wr(lock_);
  //     for (auto &[_, ic] : interp_handler_) {
  //       spdlog::debug("Restoring trigger for database \"{}\"", ic->db->id());
  //       auto storage_accessor = ic->db->Access();
  //       auto dba = memgraph::query::DbAccessor{storage_accessor.get()};
  //       ic->trigger_store.RestoreTriggers(&ic->ast_cache, &dba, ic->config.query, ic->auth_checker);
  //     }
  //   }

  //   /**
  //    * @brief Restore streams of all currently defined databases.
  //    * @note: Stream transformations are using modules, they have to be restored after the query modules are loaded.
  //    */
  //   void RestoreStreams() {
  //     std::lock_guard<LockT> wr(lock_);
  //     for (auto &[_, ic] : interp_handler_) {
  //       spdlog::debug("Restoring streams for database \"{}\"", ic->db->id());
  //       ic->streams.RestoreStreams();
  //     }
  //   }

  //   void SwitchMemoryDevice(std::string_view name) {
  //     std::shared_lock<LockT> rd(lock_);
  //     try {
  //       // Check if db exists
  //       auto sc = Get_(name.data());
  //       auto &ic = sc.interpreter_context;
  //       auto &db = sc.db;
  //       auto db_config = db->config_;
  //       // interpreters is a global list of active interpreters
  //       // lock it and check if there are any active, if we are the only one,
  //       // keep it locked during the switch to protect from any other user accessing the same database
  //       ic->interpreters.WithLock([&](const auto &interpreters_) {
  //         if (interpreters_.size() > 1) {
  //           throw utils::BasicException(
  //               "You cannot switch from an in-memory storage mode to the on-disk storage mode when there are "
  //               "multiple sessions active. Close all other sessions and try again. As Memgraph Lab uses "
  //               "multiple sessions to run queries in parallel, "
  //               "it is currently impossible to switch to the on-disk storage mode within Lab. "
  //               "Close it, connect to the instance with mgconsole "
  //               "and change the storage mode to on-disk from there. Then, you can reconnect with the Lab "
  //               "and continue to use the instance as usual.");
  //         }
  //         std::unique_lock main_guard{db->main_lock_};
  //         if (auto vertex_cnt_approx = db->GetInfo().vertex_count; vertex_cnt_approx > 0) {
  //           throw utils::BasicException(
  //               "You cannot switch from an in-memory storage mode to the on-disk storage mode when the database "
  //               "contains data. Delete all entries from the database, run FREE MEMORY and then repeat this "
  //               "query. ");
  //         }
  //         main_guard.unlock();
  //         // Delete old database
  //         if (!db_handler_.Delete(name.data())) {
  //           spdlog::error("Partial failure while switching to mode for database \"{}\".", name);
  //           defunct_dbs_.emplace(name);
  //           throw utils::BasicException(
  //               "Partial failure while switching modes, database \"{}\" is now defunct. Try deleting it and creating
  //               it " "again.", name);
  //         }
  //         // Create new OnDisk databases
  //         db_config.force_on_disk = true;
  //         auto new_db = db_handler_.New(name.data(), db_config);
  //         if (new_db.HasError()) {
  //           throw utils::BasicException("Failed to create disk storage.");
  //         }
  //         ic->db = new_db.GetValue().get();
  //       });
  //     } catch (UnknownDatabaseException &) {
  //       throw utils::BasicException("Tried to switch modes of an unknown database \"{}\"", name);
  //     }
  //   }

 private:
  std::optional<std::filesystem::path> StorageDir_(const std::string &name) const {
    const auto conf = db_handler_.GetConfig(name);
    if (conf) {
      return conf->durability.storage_directory;
    }
    spdlog::debug("Failed to find storage dir for database \"{}\"", name);
    return {};
  }

  /**
   * @brief Create a new SessionContext associated with the "name" database
   *
   * @param name name of the database
   * @return NewResultT context on success, error on failure
   */
  NewResultT New_(const std::string &name) { return New_(name, name); }

  /**
   * @brief Create a new SessionContext associated with the "name" database
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
   * @brief Create a new SessionContext associated with the "name" database
   *
   * @param name name of the database
   * @param storage_config storage configuration
   * @param inter_config interpreter configuration
   * @return NewResultT context on success, error on failure
   */
  NewResultT New_(const std::string &name, storage::Config &storage_config/*,
                    const std::string &ah_flags*/) {
    // MG_ASSERT(auth_handler_, "No high level AuthQueryHandler has been supplied.");
    // MG_ASSERT(auth_checker_, "No high level AuthChecker has been supplied.");

    // if (defunct_dbs_.contains(name)) {
    //   spdlog::warn("Failed to generate database due to the unknown state of the previously defunct database
    //     \"{}\".", name);
    //   return NewError::DEFUNCT;
    // }

    auto new_db = db_handler_.New(name, storage_config);
    if (new_db.HasValue()) {
      // Success
      // if (durability_) durability_->Put(name, "ok");
      return new_db.GetValue();  // shared_ptr to Database
    }
    return new_db.GetError();
  }

  /**
   * @brief Create a new SessionContext associated with the default database
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
   * @brief Get the context associated with the "name" database
   *
   * @param name
   * @return SessionContext
   * @throw UnknownDatabaseException if trying to get unknown database
   */
  std::shared_ptr<Database> Get_(std::string_view name) {
    auto db = db_handler_.Get(name);
    if (db) {
      return *db;
    }
    throw UnknownDatabaseException("Tried to retrieve an unknown database \"{}\".", name);
  }

  // Should storage objects ever be deleted?
  mutable LockT lock_;                    //!< protective lock
  std::filesystem::path lock_file_path_;  //!< Lock file protecting the main storage
  utils::OutputFile lock_file_handle_;    //!< Handler the lock (crash if already open)
  DatabaseHandler db_handler_;            //!< multi-tenancy storage handler
  std::optional<storage::Config> default_config_;
  // std::unique_ptr<utils::Synchronized<auth::Auth, utils::WritePrioritizedRWLock>> auth_;
  // std::unique_ptr<query::AuthQueryHandler> auth_handler_;
  // std::unique_ptr<query::AuthChecker> auth_checker_;
  const std::string run_id_;  //!< run's unique identifier (auto generated)
  // memgraph::audit::Log *audit_log_;                               //!< pointer to the audit logger
  // std::unordered_map<std::string, SessionInterface &> sessions_;  //!< map of active/registered sessions
  // std::unique_ptr<kvstore::KVStore> durability_;  //!< list of active dbs (pointer so we can postpone its creation)

  // std::set<std::string> defunct_dbs_;  //!< Databases that are in an unknown state due to various failures
  // bool delete_on_drop_;                //!< Flag defining if dropping storage also deletes its directory
  //  public:
  //   static SessionContextHandler &ExtractSCH(query::InterpreterContext *interpreter_context) {
  //     return static_cast<typename decltype(interp_handler_)::InterpContextT *>(interpreter_context)->sc_handler_;
  //   }
};

#else
/**
 * @brief Initialize the handler.
 *
 * @param auth pointer to the authenticator
 * @param configs storage and interpreter configurations
 */
static inline SessionContext Init(storage::Config &storage_config, query::InterpreterConfig &interp_config,
                                  utils::Synchronized<auth::Auth, utils::WritePrioritizedRWLock> *auth,
                                  query::AuthQueryHandler *auth_handler, query::AuthChecker *auth_checker) {
  MG_ASSERT(auth, "Passed a nullptr auth");
  MG_ASSERT(auth_handler, "Passed a nullptr auth_handler");
  MG_ASSERT(auth_checker, "Passed a nullptr auth_checker");

  storage_config.name = kDefaultDB;
  std::shared_ptr<storage::Storage> db;
  if (storage_config.force_on_disk || utils::DirExists(storage_config.disk.main_storage_directory)) {
    std::make_shared<storage::DiskStorage>(storage_config);
  } else {
    std::make_unique<storage::InMemoryStorage>(storage_config);
  }
  auto interp_context = std::make_shared<query::InterpreterContext>(
      db.get(), interp_config, storage_config.durability.storage_directory, auth_handler, auth_checker);
  MG_ASSERT(interp_context, "Failed to construct main interpret context.");

  return SessionContext{db, interp_context, utils::GenerateUUID(), auth};
}
#endif

}  // namespace memgraph::dbms
