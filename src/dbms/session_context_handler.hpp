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

#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>

#include "constants.hpp"
#include "global.hpp"
#include "interp_handler.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "session_context.hpp"
#include "storage_handler.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/result.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"
#include "utils/uuid.hpp"

namespace memgraph::dbms {

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
  using InterpT = query::InterpreterContext;
  using InterpConfigT = query::InterpreterConfig;
  using ConfigT = std::pair<StorageConfigT, InterpConfigT>;
  using LockT = utils::RWLock;
  using NewResultT = utils::BasicResult<NewError, SessionContext>;

  SessionContextHandler(const SessionContextHandler &) = delete;
  SessionContextHandler &operator=(const SessionContextHandler &) = delete;
  SessionContextHandler(SessionContextHandler &&) = delete;
  SessionContextHandler &operator=(SessionContextHandler &&) = delete;

  /**
   * @brief Singleton's access API.
   *
   * @return SessionContextHandler&
   */
  static SessionContextHandler &get() {
    static SessionContextHandler sd;
    return sd;
  }

  /**
   * @brief Initialize the handler.
   *
   * @param auth pointer to the authenticator
   * @param audit_log pointer to the audit logger
   * @param configs storage and interpreter configurations
   */
#if MG_ENTERPRISE
  void Init(memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth,
            memgraph::audit::Log *audit_log, ConfigT configs) {
#else
  void Init(memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth,
            ConfigT configs) {
#endif
    std::lock_guard<LockT> wr(lock_);
    MG_ASSERT(!initialized_, "Tried to reinitialize SessionContextHandler.");
    default_configs_ = configs;
    auth_ = auth;
#if MG_ENTERPRISE
    audit_log_ = audit_log;
#endif
    MG_ASSERT(!NewDefault_().HasError(), "Failed while creating the default DB.");
    initialized_ = true;
  }

  /**
   * @brief Create a new SessionContext associated with the "name" database
   *
   * @param name name of the database
   * @return NewResultT context on success, error on failure
   */
  NewResultT New(std::string_view name) {
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
  void Delete(const SessionInterface &session) {
    std::lock_guard<LockT> wr(lock_);
    const auto &uuid = session.UUID();
    sessions_.erase(uuid);
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
        return DeleteError::FAIL;
      }
    }
    // Low level handlers
    if (!interp_handler_.Delete(db_name) || !storage_handler_.Delete(db_name)) {
      return DeleteError::FAIL;
    }
    return {};  // Success
  }

  /**
   * @brief Set the default configurations.
   *
   * @param configs std::pair of storage and interpreter configurations
   */
  void SetDefaultConfigs(ConfigT configs) {
    std::lock_guard<LockT> wr(lock_);
    default_configs_ = configs;
  }

  /**
   * @brief Get the default configurations.
   *
   * @return std::optional<ConfigT>
   */
  std::optional<ConfigT> GetDefaultConfigs() const {
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
  SessionContextHandler() : lock_{utils::RWLock::Priority::READ}, initialized_{false}, run_id_{utils::GenerateUUID()} {}
  ~SessionContextHandler() {}

  std::optional<std::filesystem::path> StorageDir(const std::string &name) const {
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
  NewResultT New_(std::string_view name) { return New_(name, name); }

  /**
   * @brief Create a new SessionContext associated with the "name" database
   *
   * @param name name of the database
   * @param storage_subdir undelying RocksDB directory
   * @return NewResultT context on success, error on failure
   */
  NewResultT New_(std::string_view name, std::filesystem::path storage_subdir) {
    if (default_configs_) {
      auto storage = default_configs_->first;
      storage.durability.storage_directory /= storage_subdir;
      return New_(name, storage, default_configs_->second);
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
  NewResultT New_(std::string_view name, StorageConfigT &storage_config, InterpConfigT &inter_config) {
    auto new_storage = storage_handler_.New(name, storage_config);
    if (new_storage.HasValue()) {
      // storage config can be something else if storage already exists, or return false or reread config
      auto new_interp = interp_handler_.New(name, new_storage.GetValue().get(), inter_config,
                                            storage_config.durability.storage_directory);
      if (new_interp.HasValue()) {
        return SessionContext {
          new_storage.GetValue(), new_interp.GetValue(), run_id_, auth_
#if MG_ENTERPRISE
              ,
              audit_log_
#endif
        };
      }
      // TODO: Storage succeeded, but interpreter failed... How to handle?
      return new_interp.GetError();
    }
    return new_storage.GetError();
  }

  /**
   * @brief Create a new SessionContext associated with the default database
   *
   * @return NewResultT context on success, error on failure
   */
  NewResultT NewDefault_() {
    auto res = New_(kDefaultDB);
    if (res.HasValue()) {
      // Symlink to support back-compatibility
      const auto dir = StorageDir(kDefaultDB);
      MG_ASSERT(dir, "Failed to find storage path.");
      const auto main_dir = dir->parent_path();
      for (auto const &item : std::filesystem::directory_iterator{*dir}) {
        const auto dir_name = std::filesystem::relative(item.path(), item.path().parent_path());
        const auto link = main_dir / dir_name;
        const auto to = std::filesystem::relative(item.path(), main_dir);
        if (!std::filesystem::exists(link)) {
          std::filesystem::create_directory_symlink(to, link);
        } else {  // Check existing link
          std::error_code ec;
          const auto test_link = std::filesystem::read_symlink(link, ec);
          if (ec || test_link != to) {
            MG_ASSERT(
                false,
                "Memgraph storage directory incompatible with new version.\n"
                "Please use a clean directory or move directory \"{}\" under a new directory called \"memgraph\".",
                dir_name.string());
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
      auto interp = interp_handler_.Get(name);
      if (interp) {
        return SessionContext {
          *storage, *interp, run_id_, auth_
#if MG_ENTERPRISE
              ,
              audit_log_
#endif
        };
      }
    }
    throw SessionContextException("Tried to retrieve an unknown database.");
  }

  // Should storage objects ever be deleted?
  mutable LockT lock_;            //!< protective lock
  std::atomic_bool initialized_;  //!< initialized flag (safeguard against multiple init calls)
  StorageHandler<StorageT, StorageConfigT> storage_handler_;     //!< multi-tenancy storage handler
  InterpContextHandler<InterpT, InterpConfigT> interp_handler_;  //!< multi-tenancy interpreter handler
  std::optional<ConfigT> default_configs_;                       //!< default storage and interpreter configurations
  const std::string run_id_;                                     //!< run's unique identifier (auto generated)
  memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock>
      *auth_;  //!< pointer to the authorizer
#if MG_ENTERPRISE
  memgraph::audit::Log *audit_log_;  //!< pointer to the audit logger
#endif
  std::unordered_map<std::string, SessionInterface &> sessions_;  //!< map of active/registered sessions
};

}  // namespace memgraph::dbms
