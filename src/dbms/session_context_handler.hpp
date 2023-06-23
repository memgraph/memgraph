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
// TODO: Check if comment above is ok
#pragma once

#include <algorithm>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <system_error>
#include <unordered_map>

#include "constants.hpp"
#include "global.hpp"
#include "glue/auth_checker.hpp"
#include "glue/auth_handler.hpp"
#include "interp_handler.hpp"
#include "query/auth_checker.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "session_context.hpp"
#include "storage/v2/durability/durability.hpp"
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

/**
 * SessionContext Exception
 *
 * Used to indicate that something went wrong while handling multiple sessions.
 */
class SessionContextException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
};

using DeleteResult = utils::BasicResult<DeleteError>;

/**
 * @brief Multi-tenancy handler of session contexts. @note singleton
 */
class SessionContextHandler {
 public:
  using StorageT = storage::Storage;
  using StorageConfigT = storage::Config;
  // using InterpT = decltype(interp_handler_)::ExpandedInterpContext;
  // using InterpConfigT = query::InterpreterConfig;
  using LockT = utils::RWLock;
  using NewResultT = utils::BasicResult<NewError, SessionContext>;

  SessionContextHandler(const SessionContextHandler &) = delete;
  SessionContextHandler &operator=(const SessionContextHandler &) = delete;
  SessionContextHandler(SessionContextHandler &&) = delete;
  SessionContextHandler &operator=(SessionContextHandler &&) = delete;

  struct Config {
    StorageConfigT storage_config;           //!< Storage configuration
    query::InterpreterConfig interp_config;  //!< Interpreter context configuration
    std::string ah_flags;                    //!< glue::AuthHandler setup flags
  };

  /**
   * @brief Initialize the handler.
   *
   * @param audit_log pointer to the audit logger (ENTERPRISE only)
   * @param configs storage and interpreter configurations
   */
  SessionContextHandler(memgraph::audit::Log &audit_log, Config configs, bool tenant_recovery)
      : lock_{utils::RWLock::Priority::READ}, initialized_{false}, run_id_{utils::GenerateUUID()} {
    // std::lock_guard<LockT> wr(lock_);
    // MG_ASSERT(!initialized_, "Tried to reinitialize SessionContextHandler.");

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

    audit_log_ = &audit_log;
    default_configs_ = configs;

    // TODO: Decouple storage config from dbms config
    // TODO: Save individual db configs inside the kvstore and restore from there
    auto &db_dir = default_configs_->storage_config.durability.storage_directory;
    db_dir /= "databases";  // Move database root one level up
    const auto durability_dir = db_dir / ".durability";
    utils::EnsureDirOrDie(db_dir);
    utils::EnsureDirOrDie(durability_dir);
    durability_ = std::make_unique<kvstore::KVStore>(durability_dir);

    MG_ASSERT(!NewDefault_().HasError(), "Failed while creating the default DB.");

    if (tenant_recovery) {
      for (const auto &[name, sts] : *durability_) {
        if (name == kDefaultDB) continue;  // Already set
        spdlog::info("Restoring database {}.", name);
        MG_ASSERT(!New_(name).HasError(), "Failed while creating database {}.", name);
        spdlog::info("Database {} restored.", name);
      }
    }

    initialized_ = true;
  }

  ~SessionContextHandler() {}

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
   * @throw SessionContextException unknown database
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
   * @return true on success
   * @throws if uuid unknown or OnChange throws
   */
  SetForResult SetFor(const std::string &uuid, const std::string &db_name) {
    std::shared_lock<LockT> rd(lock_);
    (void)Get_(db_name);  // throw if db doesn't exist (TODO: Better to pass it via OnChange - but injecting dependency)
    auto &s = sessions_.at(uuid);
    return s.OnChange(db_name);
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
      (void)Get_(db_name);
    } catch (SessionContextException &) {
      return DeleteError::NON_EXISTENT;
    }
    // Check if a session is using the db
    for (const auto &[_, s] : sessions_) {
      if (s.GetDB() == db_name) {
        return DeleteError::USING;
      }
    }
    // High level handlers
    for (auto &[_, s] : sessions_) {
      if (!s.OnDelete(db_name)) {
        // TODO Handle
        return DeleteError::FAIL;
      }
    }
    // Low level handlers
    const auto storage_path = StorageDir_(db_name);
    MG_ASSERT(storage_path, "Missing storage for {}", db_name);
    if (!interp_handler_.Delete(db_name) || !auth_handler_.Delete(db_name) || !storage_handler_.Delete(db_name)) {
      return DeleteError::FAIL;
    }
    // Remove from durability list
    if (durability_) durability_->Delete(db_name);
    // Delete disk storage (TODO: Add a config to enable/disable this)
    std::error_code ec;
    (void)std::filesystem::remove_all(*storage_path, ec);
    if (ec) {
      return DeleteError::DISK_FAIL;
    }
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
    return sessions_.at(uuid).GetDB();
  }

 private:
  // SessionContextHandler() : lock_{utils::RWLock::Priority::READ}, initialized_{false}, run_id_{utils::GenerateUUID()}
  // {}
  // ~SessionContextHandler() {}

  std::optional<std::filesystem::path> StorageDir_(const std::string &name) const {
    try {
      const auto conf = storage_handler_.GetConfig(name);
      if (conf) {
        return conf->durability.storage_directory;
      }
    } catch (std::out_of_range &) {
    }
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
      return New_(name, storage, default_configs_->interp_config, default_configs_->ah_flags);
    }
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
  NewResultT New_(const std::string &name, StorageConfigT &storage_config, query::InterpreterConfig &inter_config,
                  const std::string &ah_flags) {
    auto new_storage = storage_handler_.New(name, storage_config);
    if (new_storage.HasValue()) {
      auto new_auth = auth_handler_.New(name, storage_config.durability.storage_directory, ah_flags);
      if (new_auth.HasValue()) {
        auto &auth_context = new_auth.GetValue();
        auto new_interp = interp_handler_.New(name, *this, *new_storage.GetValue(), inter_config,
                                              storage_config.durability.storage_directory, auth_context->auth_handler,
                                              auth_context->auth_checker);

        if (new_interp.HasValue()) {
          // Success
          if (durability_) durability_->Put(name, "ok");
          return SessionContext{new_storage.GetValue(), new_interp.GetValue(), run_id_, auth_context, audit_log_};
        }
        // TODO: Handler partial success
        return new_interp.GetError();
      }
      return new_auth.GetError();
    }
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
      const auto main_dir = *dir / "databases" / kDefaultDB;

      if (!std::filesystem::exists(main_dir)) {
        std::filesystem::create_directory(main_dir);
      }

      // Some directories are redundant (skip those)
      const std::vector<std::string> skip{".lock",    "audit_log", "databases", "internal_modules",
                                          "settings", kDefaultDB};

      // Symlink to root dir
      for (auto const &item : std::filesystem::directory_iterator{*dir}) {
        const auto dir_name = std::filesystem::relative(item.path(), item.path().parent_path());
        if (std::find(skip.begin(), skip.end(), dir_name) != skip.end()) continue;
        const auto link = main_dir / dir_name;
        const auto to = std::filesystem::relative(item.path(), main_dir);
        if (!std::filesystem::exists(link)) {
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
   * @throw SessionContextException unknown database
   */
  SessionContext Get_(const std::string &name) {
    auto storage = storage_handler_.Get(name);
    if (storage) {
      auto auth = auth_handler_.Get(name);
      if (auth) {
        auto interp = interp_handler_.Get(name);
        if (interp) {
          return SessionContext{*storage, *interp, run_id_, *auth, audit_log_};
        }
      }
    }
    throw SessionContextException("Tried to retrieve an unknown database.");
  }

  // Should storage objects ever be deleted?
  mutable LockT lock_;                    //!< protective lock
  std::atomic_bool initialized_;          //!< initialized flag (safeguard against multiple init calls)
  std::filesystem::path lock_file_path_;  //!< Lock file protecting the main storage
  utils::OutputFile lock_file_handle_;    //!< Handler the lock (crash if already open)
  StorageHandler storage_handler_;        //!< multi-tenancy storage handler
  InterpContextHandler<SessionContextHandler> interp_handler_;    //!< multi-tenancy interpreter handler
  AuthContextHandler auth_handler_;                               //!< multi-tenancy authorization handler
  std::optional<Config> default_configs_;                         //!< default storage and interpreter configurations
  const std::string run_id_;                                      //!< run's unique identifier (auto generated)
  memgraph::audit::Log *audit_log_;                               //!< pointer to the audit logger
  std::unordered_map<std::string, SessionInterface &> sessions_;  //!< map of active/registered sessions
  std::unique_ptr<kvstore::KVStore> durability_;  //!< list of active dbs (pointer so we can postpone its creation)

 public:
  using InterpContextT = decltype(interp_handler_)::ExpandedInterpContext;
};

#else
/**
 * @brief Initialize the handler.
 *
 * @param auth pointer to the authenticator
 * @param configs storage and interpreter configurations
 */
static inline SessionContext Init(storage::Config &storage_config, query::InterpreterConfig &interp_config,
                                  const std::string &ah_flags) {
  auto storage = std::make_shared<storage::Storage>(storage_config);
  MG_ASSERT(storage, "Failed to allocate main storage.");

  auto auth = std::make_shared<AuthContextHandler::AuthContext>(storage_config.durability.storage_directory, ah_flags);
  MG_ASSERT(auth, "Failed to generate authentication.");

  auto interp_context = std::make_shared<query::InterpreterContext>(storage.get(), interp_config,
                                                                    storage_config.durability.storage_directory,
                                                                    &auth->auth_handler, &auth->auth_checker);
  MG_ASSERT(interp_context, "Failed to construct main interpret context.");

  return SessionContext{storage, interp_context, utils::GenerateUUID(), auth};
}
#endif

}  // namespace memgraph::dbms
