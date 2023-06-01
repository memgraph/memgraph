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
#include "interp_handler.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "session_data.hpp"
#include "storage_handler.hpp"
#include "utils/uuid.hpp"

namespace memgraph::dbms {

template <typename TStorage = storage::Storage, typename TStorageConfig = storage::Config,
          typename TInterp = query::InterpreterContext, typename TInterpConfig = query::InterpreterConfig>
class SessionDataHandler {
  using config_type = std::pair<TStorageConfig, TInterpConfig>;

 public:
#if MG_ENTERPRISE
  SessionDataHandler(memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth,
                     memgraph::audit::Log *audit_log)
      : run_id_(utils::GenerateUUID()), auth_(auth), audit_log_(audit_log) {}
#else
  explicit SessionDataHandler(
      memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth)
      : run_id_(utils::GenerateUUID()), auth_(auth) {}
#endif

  // TODO: Think about what New returns. Maybe it's easier to return false on error (because how are we suppose to
  // handle errors if we don't know if they happened)
  std::optional<SessionData> New(std::string_view name, TStorageConfig &storage_config, TInterpConfig &inter_config) {
    auto new_storage = storage_handler_.New(name, storage_config);
    if (new_storage) {
      // storage config can be something else if storage already exists, or return false or reread config
      auto new_interp =
          interp_handler_.New(name, *new_storage, inter_config, storage_config.durability.storage_directory);
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

  std::optional<SessionData> New(std::string_view name) {
    if (default_configs_) {
      return New(name, default_configs_->first, default_configs_->second);
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

  std::optional<SessionData> Get(std::string_view name) {
    auto storage = storage_handler_.Get(name);
    if (storage) {
      auto interp = interp_handler_.Get(name);
      if (interp) {
#if MG_ENTERPRISE
        return {*storage, *interp, auth_, audit_log_, run_id_};
#else
        return {*storage, *interp, auth_, run_id_};
#endif
      }
    }
    return {};
  }

  SessionData *GetPtr(const std::string &name) { return &session_data_.at(name); }

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
  const std::string run_id_;
  memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth_;
#if MG_ENTERPRISE
  memgraph::audit::Log *audit_log_;
#endif
};

}  // namespace memgraph::dbms
