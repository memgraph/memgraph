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

#include <unordered_map>

#include "constants.hpp"
#include "interp_handler.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "session_data.hpp"
#include "storage_handler.hpp"
#include "utils/uuid.hpp"

namespace memgraph::dbms {

template <typename T>
concept get_uuid = requires(T v) {
  { v.UUID() } -> std::same_as<std::string>;
};

template <typename TStorage = storage::Storage, typename TStorageConfig = storage::Config,
          typename TInterp = query::InterpreterContext, typename TInterpConfig = query::InterpreterConfig>
class SessionDataHandler {
  using config_type = std::pair<TStorageConfig, TInterpConfig>;

 public:
#if MG_ENTERPRISE
  SessionDataHandler(memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth,
                     memgraph::audit::Log *audit_log, config_type configs)
      : default_configs_(configs), run_id_(utils::GenerateUUID()), auth_(auth), audit_log_(audit_log) {
    // Always create the default DB
    New(kDefaultDB);
  }
#else
  explicit SessionDataHandler(
      memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth,
      config_type configs)
      : default_configs_(configs), run_id_(utils::GenerateUUID()), auth_(auth) {
    // Always create the default DB
    New(kDefaultDB);
  }
#endif

  // TODO: Think about what New returns. Maybe it's easier to return false on error (because how are we suppose to
  // handle errors if we don't know if they happened)
  std::optional<SessionData> New(std::string_view name, TStorageConfig &storage_config, TInterpConfig &inter_config) {
    auto new_storage = storage_handler_.New(name, storage_config);
    if (new_storage) {
      // storage config can be something else if storage already exists, or return false or reread config
      auto new_interp =
          interp_handler_.New(name, *new_storage, inter_config, storage_config.durability.storage_directory, *this);
      if (new_interp) {
#if MG_ENTERPRISE
        SessionData sd{*new_storage, *new_interp, auth_, audit_log_};
#else
        SessionData sd{*new_storage, *new_interp, auth_};
#endif
        sd.run_id = run_id_;
        session_data_.emplace(name, sd);
        return sd;
      }
      // TODO: Storage succeeded, but interpreter failed... How to handle?
      return {};
    }
    return {};
  }

  std::optional<SessionData> New(std::string_view name, std::filesystem::path storage_subdir) {
    if (default_configs_) {
      auto storage = default_configs_->first;
      storage.durability.storage_directory /= storage_subdir;
      return New(name, storage, default_configs_->second);
    }
    return {};
  }

  std::optional<SessionData> New(std::string_view name) { return New(name, name); }

  // Can throw
  SessionData *GetPtr(const std::string &name) { return &session_data_.at(name); }

  // Can throw
  bool SetFor(const std::string &uuid, const std::string &db_name) {
    // if (const auto found = session_data_.find(uuid); found != session_data_.end()) {
    const auto *ptr = GetPtr(db_name);
    auto &current = using_[uuid];
    if (current.empty()) current = kDefaultDB;  // TODO better initialization....
    if (ptr && current != db_name) {
      current = db_name;
      pending_change_[uuid] = true;
      return true;
    }
    return false;
  }

  template <get_uuid T>
  std::optional<std::string> ToUpdate(const T &session) {
    const auto uuid = session.UUID();
    auto &change = pending_change_[uuid];
    if (change) {
      change = false;
      return using_[uuid];
    }
    return {};
  }

  bool Delete(std::string_view name) {
    // TODO
    return false;
  }

  void SetDefaultConfigs(config_type configs) { default_configs_ = configs; }
  std::optional<config_type> GetDefaultConfigs() { return default_configs_; }

 private:
  // Are storage objects ever deleted?
  // shared_ptr and custom destructor if we are destroying it
  // unique and raw ptrs if we are not destroying it
  // Create drop and create with same name?
  std::unordered_map<std::string, SessionData> session_data_;
  StorageHandler<TStorage, TStorageConfig> storage_handler_;
  InterpContextHandler<TInterp, TInterpConfig> interp_handler_;
  std::optional<config_type> default_configs_;
  // TODO: Add a session registration to this (the using_ is kinda bad)
  std::unordered_map<std::string, std::string> using_;
  std::unordered_map<std::string, bool> pending_change_;
  const std::string run_id_;
  memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth_;
#if MG_ENTERPRISE
  memgraph::audit::Log *audit_log_;
#endif
};

}  // namespace memgraph::dbms
