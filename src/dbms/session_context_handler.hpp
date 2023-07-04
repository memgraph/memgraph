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
#include "interp_handler.hpp"
#include "query/auth_checker.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "session_context.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage_handler.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/logging.hpp"
#include "utils/result.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"
#include "utils/uuid.hpp"

#include "handler.hpp"

namespace memgraph::dbms {

#ifdef MG_ENTERPRISE

using DeleteResult = utils::BasicResult<DeleteError>;

/**
 * @brief Multi-database session contexts handler.
 */
class SessionContextHandler {
 public:
  using StorageT = storage::Storage;
  using StorageConfigT = storage::Config;
  // using InterpT = decltype(interp_handler_)::ExpandedInterpContext;
  // using InterpConfigT = query::InterpreterConfig;
  using LockT = utils::RWLock;
  using NewResultT = utils::BasicResult<NewError, SessionContext>;

  struct Config {
    StorageConfigT storage_config;           //!< Storage configuration
    query::InterpreterConfig interp_config;  //!< Interpreter context configuration
    // std::string ah_flags;                    //!< glue::AuthHandler setup flags
    std::function<void(utils::Synchronized<auth::Auth, utils::WritePrioritizedRWLock> *,
                       std::unique_ptr<query::AuthQueryHandler> &, std::unique_ptr<query::AuthChecker> &)>
        glue_auth;
  };

  /**
   * @brief Initialize the handler.
   *
   * @param audit_log pointer to the audit logger (ENTERPRISE only)
   * @param configs storage and interpreter configurations
   * @param recovery_on_startup restore databases (and its content) and authentication data
   */
  SessionContextHandler(memgraph::audit::Log &audit_log, Config configs, bool recovery_on_startup, bool delete_on_drop)
      : lock_{utils::RWLock::Priority::READ},
        default_configs_(configs),
        run_id_{utils::GenerateUUID()},
        audit_log_(&audit_log),
        delete_on_drop_(delete_on_drop) {
    const auto &root = configs.storage_config.durability.storage_directory;
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

    // TODO: Figure out if this is needed/wanted
    // Clear auth database since we are not recovering
    // if (!recovery_on_startup) {
    //   const auto &auth_dir = root / "auth";
    //   // Backup if auth present
    //   if (utils::DirExists(auth_dir)) {
    //     auto backup_dir = root / storage::durability::kBackupDirectory;
    //     std::error_code error_code;
    //     utils::EnsureDirOrDie(backup_dir);
    //     std::error_code ec;
    //     const auto now = std::chrono::system_clock::now();
    //     std::ostringstream os;
    //     os << now.time_since_epoch().count();
    //     std::filesystem::rename(auth_dir, backup_dir / ("auth-" + os.str()), ec);
    //     MG_ASSERT(!ec, "Couldn't backup auth directory because of: {}", ec.message());
    //     spdlog::warn(
    //         "Since Memgraph was not supposed to recover on startup the authentication files will be "
    //         "overwritten. To prevent important data loss, Memgraph has stored those files into .backup directory "
    //         "inside the storage directory.");
    //   }

    //   // Clear
    //   if (std::filesystem::exists(auth_dir)) {
    //     std::filesystem::remove_all(auth_dir);
    //   }
    // }

    // Lazy initialization of auth_
    auth_ = std::make_unique<utils::Synchronized<auth::Auth, utils::WritePrioritizedRWLock>>(root / "auth");
    configs.glue_auth(auth_.get(), auth_handler_, auth_checker_);

    // TODO: Decouple storage config from dbms config
    // TODO: Save individual db configs inside the kvstore and restore from there
    auto &db_dir = default_configs_->storage_config.durability.storage_directory;
    db_dir /= "databases";  // Move database root one level up
    const auto durability_dir = db_dir / ".durability";
    utils::EnsureDirOrDie(db_dir);
    utils::EnsureDirOrDie(durability_dir);
    durability_ = std::make_unique<kvstore::KVStore>(durability_dir);

    // Generate the default database
    MG_ASSERT(!NewDefault_().HasError(), "Failed while creating the default DB.");

    // Recover previous databases
    if (recovery_on_startup) {
      for (const auto &[name, sts] : *durability_) {
        if (name == kDefaultDB) continue;  // Already set
        spdlog::info("Restoring database {}.", name);
        MG_ASSERT(!New_(name).HasError(), "Failed while creating database {}.", name);
        spdlog::info("Database {} restored.", name);
      }
    }
  }

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
   * @throw UnknownDatabase if getting unknown database
   */
  SessionContext Get(const std::string &name) {
    std::shared_lock<LockT> rd(lock_);
    return Get_(name);
  }

  /**
   * @brief Set the undelying database for a particular session.
   *
   * @param uuid unique session identifier
   * @param db_name unique database name
   * @return SetForResult enum
   * @throws UnknownDatabase, UnknownSession or anything OnChange throws
   */
  SetForResult SetFor(const std::string &uuid, const std::string &db_name) {
    std::shared_lock<LockT> rd(lock_);
    (void)Get_(
        db_name);  // throws if db doesn't exist (TODO: Better to pass it via OnChange - but injecting dependency)
    try {
      auto &s = sessions_.at(uuid);
      return s.OnChange(db_name);
    } catch (std::out_of_range &) {
      throw UnknownSession("Unknown session \"{}\"", uuid);
    }
  }

  /**
   * @brief Set the undelying database from a session itself. SessionContext handler.
   *
   * @param db_name unique database name
   * @param handler function that gets called in place with the appropriate SessionContext
   * @return SetForResult enum
   */
  template <typename THandler>
  requires std::invocable<THandler, SessionContext> SetForResult SetInPlace(const std::string &db_name,
                                                                            THandler handler) {
    std::shared_lock<LockT> rd(lock_);
    return handler(Get_(db_name));
  }

  /**
   * @brief Call void handler under a shared lock.
   *
   * @param handler function that gets called in place
   */
  template <typename THandler>
  requires std::invocable<THandler>
  void CallInPlace(THandler handler) {
    std::shared_lock<LockT> rd(lock_);
    handler();
  }

  /**
   * @brief Register an active session (used to handle callbacks).
   *
   * @param session
   * @return true on success
   */
  bool Register(SessionInterface &session) {
    std::lock_guard<LockT> wr(lock_);
    auto [_, success] = sessions_.emplace(session.UUID(), session);
    return success;
  }

  /**
   * @brief Delete a session.
   *
   * @param session
   */
  bool Delete(const SessionInterface &session) {
    std::lock_guard<LockT> wr(lock_);
    return sessions_.erase(session.UUID()) > 0;
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
    // Check if db exists
    try {
      auto sc = Get_(db_name);
      // Check if a session is using the db
      if (!sc.interpreter_context->interpreters->empty()) {
        return DeleteError::USING;
      }
    } catch (UnknownDatabase &) {
      return DeleteError::NON_EXISTENT;
    }

    // High level handlers
    for (auto &[_, s] : sessions_) {
      if (!s.OnDelete(db_name)) {
        spdlog::error("Partial failure while deleting database \"{}\".", db_name);
        defunct_dbs_.emplace(db_name);
        return DeleteError::FAIL;
      }
    }

    // Low level handlers
    const auto storage_path = StorageDir_(db_name);
    MG_ASSERT(storage_path, "Missing storage for {}", db_name);
    if (!interp_handler_.Delete(db_name) || !storage_handler_.Delete(db_name)) {
      spdlog::error("Partial failure while deleting database \"{}\".", db_name);
      defunct_dbs_.emplace(db_name);
      return DeleteError::FAIL;
    }

    // Remove from auth
    auth_->Lock()->DeleteDatabase(db_name);
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
   * @brief Set the default configurations.
   *
   * @param configs storage, interpreter and authorization configurations
   */
  void SetDefaultConfigs(Config configs) {
    std::lock_guard<LockT> wr(lock_);
    default_configs_ = configs;
  }

  /**
   * @brief Get the default configurations.
   *
   * @return std::optional<Config>
   */
  std::optional<Config> GetDefaultConfigs() const {
    std::shared_lock<LockT> rd(lock_);
    return default_configs_;
  }

  /**
   * @brief Return all active databases.
   *
   * @return std::vector<std::string>
   */
  std::vector<std::string> All() const {
    std::shared_lock<LockT> rd(lock_);
    return storage_handler_.All();
  }

  /**
   * @brief Return the currently active database for a particular session.
   *
   * @param uuid session's unique identifier
   * @return std::string name of the database
   * @throw
   */
  std::string Current(const std::string &uuid) const {
    std::shared_lock<LockT> rd(lock_);
    return sessions_.at(uuid).GetID();
  }

  /**
   * @brief Restore triggers for all currently defined databases.
   * @note: Triggers can execute query procedures, so we need to reload the modules first and then the triggers
   */
  void RestoreTriggers() {
    std::lock_guard<LockT> wr(lock_);
    for (auto &ic_itr : interp_handler_) {
      auto ic = ic_itr.second.get();
      spdlog::debug("Restoring trigger for database \"{}\"", ic->db->id());
      auto storage_accessor = ic->db->Access();
      auto dba = memgraph::query::DbAccessor{&storage_accessor};
      ic->trigger_store.RestoreTriggers(&ic->ast_cache, &dba, ic->config.query, ic->auth_checker);
    }
  }

  /**
   * @brief Restore streams of all currently defined databases.
   * @note: Stream transformations are using modules, they have to be restored after the query modules are loaded.
   */
  void RestoreStreams() {
    std::lock_guard<LockT> wr(lock_);
    for (auto &ic_itr : interp_handler_) {
      auto ic = ic_itr.second.get();
      spdlog::debug("Restoring streams for database \"{}\"", ic->db->id());
      ic->streams.RestoreStreams();
    }
  }

 private:
  std::optional<std::filesystem::path> StorageDir_(const std::string &name) const {
    const auto conf = storage_handler_.GetConfig(name);
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
    if (default_configs_) {
      auto storage = default_configs_->storage_config;
      storage.durability.storage_directory /= storage_subdir;
      return New_(name, storage, default_configs_->interp_config);
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
  NewResultT New_(const std::string &name, StorageConfigT &storage_config, query::InterpreterConfig &inter_config/*,
                  const std::string &ah_flags*/) {
    MG_ASSERT(auth_handler_, "No high level AuthQueryHandler has been supplied.");
    MG_ASSERT(auth_checker_, "No high level AuthChecker has been supplied.");

    if (defunct_dbs_.contains(name)) {
      spdlog::warn("Failed to generate database due to the unknown state of the previously defunct database \"{}\".",
                   name);
      return NewError::DEFUNCT;
    }

    auto new_storage = storage_handler_.New(name, storage_config);
    if (new_storage.HasValue()) {
      // auto new_auth = auth_handler_.New(name, storage_config.durability.storage_directory, ah_flags);
      // if (new_auth.HasValue()) {
      // auto &auth_context = new_auth.GetValue();
      auto new_interp =
          interp_handler_.New(name, *this, *new_storage.GetValue(), inter_config,
                              storage_config.durability.storage_directory, *auth_handler_, *auth_checker_);

      if (new_interp.HasValue()) {
        // Success
        if (durability_) durability_->Put(name, "ok");
        return SessionContext{new_storage.GetValue(), new_interp.GetValue(), run_id_, auth_.get(), audit_log_};
      }
      // Partial error: Clear storage and exit
      spdlog::warn("Partial failure while generating new database.");
      new_storage.GetValue().reset();
      if (!storage_handler_.Delete(name)) {
        spdlog::error(
            "Failed to handle partial error while generating new database \"{}\". Database might be in an unknown "
            "state!",
            name);
      }
      return new_interp.GetError();
      // }
      // return new_auth.GetError();
    }
    // Error: Nothing to clear
    return new_storage.GetError();
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
      const auto main_dir = *dir / "databases";
      const auto to_root = std::filesystem::relative(*dir, main_dir);

      const auto link = main_dir / kDefaultDB;
      if (!std::filesystem::exists(link)) {
        std::filesystem::create_directory_symlink(to_root, link);
      } else {  // Check existing link
        std::error_code ec;
        const auto test_link = std::filesystem::read_symlink(link, ec);
        if (ec || test_link != to_root) {
          MG_ASSERT(false,
                    "Memgraph storage directory incompatible with new version.\n"
                    "Please use a clean directory or remove \"{}\" and try again.",
                    link.string());
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
   * @throw UnknownDatabase unknown database
   */
  SessionContext Get_(const std::string &name) {
    auto storage = storage_handler_.Get(name);
    if (storage) {
      // auto auth = auth_handler_.Get(name);
      // if (auth) {
      auto interp = interp_handler_.Get(name);
      if (interp) {
        return SessionContext{*storage, *interp, run_id_, auth_.get(), audit_log_};
      }
      // }
    }
    throw UnknownDatabase("Tried to retrieve an unknown database \"{}\".", name);
  }

  // Should storage objects ever be deleted?
  mutable LockT lock_;                                          //!< protective lock
  std::filesystem::path lock_file_path_;                        //!< Lock file protecting the main storage
  utils::OutputFile lock_file_handle_;                          //!< Handler the lock (crash if already open)
  StorageHandler storage_handler_;                              //!< multi-tenancy storage handler
  InterpContextHandler<SessionContextHandler> interp_handler_;  //!< multi-tenancy interpreter handler
  // AuthContextHandler auth_handler_; //!< multi-tenancy authorization handler (currently we use a single global
  // auth)
  std::unique_ptr<utils::Synchronized<auth::Auth, utils::WritePrioritizedRWLock>> auth_;
  std::unique_ptr<query::AuthQueryHandler> auth_handler_;
  std::unique_ptr<query::AuthChecker> auth_checker_;
  std::optional<Config> default_configs_;                         //!< default storage and interpreter configurations
  const std::string run_id_;                                      //!< run's unique identifier (auto generated)
  memgraph::audit::Log *audit_log_;                               //!< pointer to the audit logger
  std::unordered_map<std::string, SessionInterface &> sessions_;  //!< map of active/registered sessions
  std::unique_ptr<kvstore::KVStore> durability_;  //!< list of active dbs (pointer so we can postpone its creation)

  std::set<std::string> defunct_dbs_;  //!< Databases that are in an unknown state due to various failures
  bool delete_on_drop_;                //!< Flag defining if dropping storage also deletes its directory
 public:
  using InterpContextT = decltype(interp_handler_)::InterpContextT;
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
  MG_ASSERT(auth, "Pass a nullptr auth");
  MG_ASSERT(auth_handler, "Pass a nullptr auth_handler");
  MG_ASSERT(auth_checker, "Pass a nullptr auth_checker");

  auto storage = std::make_shared<storage::Storage>(storage_config);
  MG_ASSERT(storage, "Failed to allocate main storage.");

  auto interp_context = std::make_shared<query::InterpreterContext>(
      storage.get(), interp_config, storage_config.durability.storage_directory, auth_handler, auth_checker);
  MG_ASSERT(interp_context, "Failed to construct main interpret context.");

  return SessionContext{storage, interp_context, utils::GenerateUUID(), auth};
}
#endif

}  // namespace memgraph::dbms
