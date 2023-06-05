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
#include <unordered_map>

#include "constants.hpp"
#include "global.hpp"
#include "interp_handler.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "session_data.hpp"
#include "storage_handler.hpp"
#include "utils/result.hpp"
#include "utils/uuid.hpp"

namespace memgraph::dbms {

template <typename T>
concept WithUUID = requires(T v) {
  { v.UUID() } -> std::same_as<std::string>;
};

using DeleteResult = utils::BasicResult<DeleteError>;

class SessionDataHandler {
 public:
  using StorageT = storage::Storage;
  using StorageConfigT = storage::Config;
  using InterpT = query::InterpreterContext;
  using InterpConfigT = query::InterpreterConfig;
  using config_type = std::pair<StorageConfigT, InterpConfigT>;
  using NewResultT = utils::BasicResult<NewError, SessionData>;

  SessionDataHandler(const SessionDataHandler &) = delete;
  SessionDataHandler &operator=(const SessionDataHandler &) = delete;
  SessionDataHandler(SessionDataHandler &&) = delete;
  SessionDataHandler &operator=(SessionDataHandler &&) = delete;

  static SessionDataHandler &get() {
    static SessionDataHandler sd;
    return sd;
  }

#if MG_ENTERPRISE
  void Init(memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth,
            memgraph::audit::Log *audit_log, config_type configs) {
    default_configs_ = configs;
    auth_ = auth;
    audit_log_ = audit_log;
    // Always create the default DB
    MG_ASSERT(!New(kDefaultDB).HasError(), "Failed while creating the default DB.");
  }
#else
  static void Init(memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth,
                   config_type configs) {
    default_configs_ = configs;
    auth_ = auth;
    // Always create the default DB
    MG_ASSERT(!New(kDefaultDB).HasError(), "Failed while creating the default DB.");
  }
#endif

  // TODO: Think about what New returns. Maybe it's easier to return false on error (because how are we suppose to
  // handle errors if we don't know if they happened)
  NewResultT New(std::string_view name, StorageConfigT &storage_config, InterpConfigT &inter_config) {
    auto new_storage = storage_handler_.New(name, storage_config);
    if (new_storage.HasValue()) {
      // storage config can be something else if storage already exists, or return false or reread config
      auto new_interp =
          interp_handler_.New(name, new_storage.GetValue(), inter_config, storage_config.durability.storage_directory);
      if (new_interp.HasValue()) {
#if MG_ENTERPRISE
        SessionData sd{*new_storage, *new_interp, auth_, audit_log_};
#else
        SessionData sd{*new_storage, new_interp.GetValue(), auth_};
#endif
        sd.run_id = run_id_;
        session_data_.emplace(name, sd);
        return sd;
      }
      // TODO: Storage succeeded, but interpreter failed... How to handle?
      return new_interp.GetError();
    }
    return new_storage.GetError();
  }

  NewResultT New(std::string_view name, std::filesystem::path storage_subdir) {
    if (default_configs_) {
      auto storage = default_configs_->first;
      storage.durability.storage_directory /= storage_subdir;
      return New(name, storage, default_configs_->second);
    }
    return NewError::NO_CONFIGS;
  }

  NewResultT New(std::string_view name) { return New(name, name); }

  // Can throw
  SessionData *GetPtr(const std::string &name) { return &session_data_.at(name); }

  // Can throw
  bool SetFor(const std::string &uuid, const std::string &db_name) {
    // if (const auto found = session_data_.find(uuid); found != session_data_.end()) {
    const auto *ptr = GetPtr(db_name);
    if (ptr) {
      const auto &current = get_db_name_.at(uuid)();
      if (current != db_name) {
        // todo checks
        return on_change_cb_.at(uuid)(db_name);
      }
      return true;
    }
    return false;
  }

  template <WithUUID T>
  bool RegisterOnChange(const T &session, std::function<bool(const std::string &)> cb) {
    on_change_cb_[session.UUID()] = cb;
    return true;
    // todo checks
  }

  template <WithUUID T>
  bool RegisterGetDB(const T &session, std::function<std::string()> cb) {
    get_db_name_[session.UUID()] = cb;
    return true;
    // todo checks
  }

  template <WithUUID T>
  void Delete(const T &session) {
    const auto &uuid = session.UUID();
    on_change_cb_.erase(uuid);
    get_db_name_.erase(uuid);
  }

  std::unordered_map<std::string, std::function<bool(const std::string &)>> on_change_cb_;
  std::unordered_map<std::string, std::function<std::string()>> get_db_name_;

  DeleteResult Delete(const std::string &db_name) {
    // TODO better
    if (db_name == kDefaultDB) {
      // MSG cannot delete the default db
      return DeleteError::DEFAULT_DB;
    }
    try {
      const auto *ptr = GetPtr(db_name);
      if (ptr) {
        for (const auto &itr : get_db_name_) {
          const auto &db = itr.second();
          if (db == db_name) {
            // At least one session is using the db
            return DeleteError::USING;
          }
        }
        session_data_.erase(db_name);
        return {};  // Success
      }
      return DeleteError::NON_EXISTENT;
    } catch (...) {
      return DeleteError::NON_EXISTENT;
    }
  }

  void SetDefaultConfigs(config_type configs) { default_configs_ = configs; }
  std::optional<config_type> GetDefaultConfigs() { return default_configs_; }

  std::vector<std::string> All() {
    std::vector<std::string> names;
    names.reserve(session_data_.size());
    for (const auto &sd : session_data_) {
      names.emplace_back(sd.first);
    }
    return names;
  }

  // can throw
  std::string Current(const std::string &uuid) { return get_db_name_.at(uuid)(); }

 private:
  SessionDataHandler() : run_id_(utils::GenerateUUID()) {}
  ~SessionDataHandler() {}

  // Are storage objects ever deleted?
  // shared_ptr and custom destructor if we are destroying it
  // unique and raw ptrs if we are not destroying it
  // Create drop and create with same name?
  std::unordered_map<std::string, SessionData> session_data_;
  StorageHandler<StorageT, StorageConfigT> storage_handler_;
  InterpContextHandler<InterpT, InterpConfigT> interp_handler_;
  std::optional<config_type> default_configs_;
  const std::string run_id_;
  memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth_;
#if MG_ENTERPRISE
  memgraph::audit::Log *audit_log_;
#endif
};

}  // namespace memgraph::dbms
