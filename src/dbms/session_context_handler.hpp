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

#include <memory>
#include <mutex>
#include <unordered_map>

#include "constants.hpp"
#include "global.hpp"
#include "interp_handler.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "session_context.hpp"
#include "storage_handler.hpp"
#include "utils/result.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"
#include "utils/uuid.hpp"

namespace memgraph::dbms {

template <typename T>
concept WithUUID = requires(T v) {
  { v.UUID() } -> std::same_as<std::string>;
};

using DeleteResult = utils::BasicResult<DeleteError>;

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

  static SessionContextHandler &get() {
    static SessionContextHandler sd;
    return sd;
  }

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
    MG_ASSERT(!New_(kDefaultDB).HasError(), "Failed while creating the default DB.");
    initialized_ = true;
  }

  NewResultT New(std::string_view name) {
    std::lock_guard<LockT> wr(lock_);
    return New_(name, name);
  }

  // TODO rethink if we want a raw pointer here
  //  in theory delete is protected by USING, but I still don't like it
  //  Can throw
  SessionContext *GetPtr(const std::string &name) {
    std::shared_lock<LockT> rd(lock_);
    // TODO checks
    return &db_context_.at(name);
  }

  // Can throw
  bool SetFor(const std::string &uuid, const std::string &db_name) {
    std::shared_lock<LockT> rd(lock_);
    if (db_context_.find(db_name) != db_context_.end()) {
      const auto &name = get_db_name_.at(uuid)();
      if (name != db_name) {
        // todo checks
        return on_change_cb_.at(uuid)(db_name);
      }
      return true;
    }
    return false;
  }

  template <WithUUID T>
  bool RegisterOnChange(const T &session, std::function<bool(const std::string &)> cb) {
    std::lock_guard<LockT> wr(lock_);
    auto [_, success] = on_change_cb_.emplace(session.UUID(), cb);
    if (!success) std::cout << "FAILED TO EMPLACE!" << std::endl;
    return true;
    // todo checks
  }

  template <WithUUID T>
  bool RegisterGetDB(const T &session, std::function<std::string()> cb) {
    std::lock_guard<LockT> wr(lock_);
    auto [_, success] = get_db_name_.emplace(session.UUID(), cb);
    if (!success) std::cout << "FAILED TO EMPLACE!" << std::endl;
    return true;
    // todo checks
  }

  template <WithUUID T>
  void Delete(const T &session) {
    std::lock_guard<LockT> wr(lock_);
    const auto &uuid = session.UUID();
    get_db_name_.erase(uuid);
    on_change_cb_.erase(uuid);
  }

  DeleteResult Delete(const std::string &db_name) {
    std::lock_guard<LockT> wr(lock_);
    if (db_name == kDefaultDB) {
      // MSG cannot delete the default db
      return DeleteError::DEFAULT_DB;
    }
    if (db_context_.find(db_name) != db_context_.end()) {
      for (const auto &itr : get_db_name_) {
        const auto &db = itr.second();
        if (db == db_name) {
          // At least one session is using the db
          return DeleteError::USING;
        }
      }
      db_context_.erase(db_name);
      return {};  // Success
    }
    return DeleteError::NON_EXISTENT;
  }

  void SetDefaultConfigs(ConfigT configs) {
    std::lock_guard<LockT> wr(lock_);
    default_configs_ = configs;
  }

  std::optional<ConfigT> GetDefaultConfigs() {
    std::shared_lock<LockT> rd(lock_);
    return default_configs_;
  }

  std::vector<std::string> All() {
    std::shared_lock<LockT> rd(lock_);
    std::vector<std::string> names;
    names.reserve(db_context_.size());
    for (const auto &[name, _] : db_context_) {
      names.emplace_back(name);
    }
    return names;
  }

  // can throw
  std::string Current(const std::string &uuid) {
    std::shared_lock<LockT> rd(lock_);
    return get_db_name_.at(uuid)();
  }

 private:
  SessionContextHandler() : lock_{utils::RWLock::Priority::READ}, initialized_{false}, run_id_{utils::GenerateUUID()} {}
  ~SessionContextHandler() {}

  NewResultT New_(std::string_view name) { return New_(name, name); }

  NewResultT New_(std::string_view name, std::filesystem::path storage_subdir) {
    if (default_configs_) {
      auto storage = default_configs_->first;
      storage.durability.storage_directory /= storage_subdir;
      return New_(name, storage, default_configs_->second);
    }
    return NewError::NO_CONFIGS;
  }

  NewResultT New_(std::string_view name, StorageConfigT &storage_config, InterpConfigT &inter_config) {
    auto new_storage = storage_handler_.New(name, storage_config);
    if (new_storage.HasValue()) {
      // storage config can be something else if storage already exists, or return false or reread config
      auto new_interp =
          interp_handler_.New(name, new_storage.GetValue(), inter_config, storage_config.durability.storage_directory);
      if (new_interp.HasValue()) {
#if MG_ENTERPRISE
        SessionContext sd{*new_storage, new_interp.GetValue(), auth_, audit_log_};
#else
        SessionContext sd{*new_storage, new_interp.GetValue(), auth_};
#endif
        sd.run_id = run_id_;
        db_context_.emplace(name, sd);
        return sd;
      }
      // TODO: Storage succeeded, but interpreter failed... How to handle?
      return new_interp.GetError();
    }
    return new_storage.GetError();
  }

  // Should storage objects ever be deleted?
  LockT lock_;
  std::atomic_bool initialized_;
  StorageHandler<StorageT, StorageConfigT> storage_handler_;
  InterpContextHandler<InterpT, InterpConfigT> interp_handler_;
  std::optional<ConfigT> default_configs_;
  std::unordered_map<std::string, SessionContext> db_context_;
  const std::string run_id_;
  memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth_;
#if MG_ENTERPRISE
  memgraph::audit::Log *audit_log_;
#endif
  std::unordered_map<std::string, std::function<bool(const std::string &)>> on_change_cb_;
  std::unordered_map<std::string, std::function<std::string()>> get_db_name_;
};

}  // namespace memgraph::dbms
