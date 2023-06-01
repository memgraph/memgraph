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

#include "query/config.hpp"
#include "query/interpreter.hpp"

#include <filesystem>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>

namespace memgraph::dbms {

template <typename TContext = query::InterpreterContext, typename TConfig = query::InterpreterConfig>
class InterpContextHandler {
 public:
  InterpContextHandler() {}

  std::optional<TContext *> New(std::string_view name, storage::Storage *db, const TConfig &config,
                                const std::filesystem::path &data_directory) {
    // Control that the new configuration does not conflict with the previous ones
    if (false) {  // TODO: Is there anything that can conflict
      // LOG
      return {};
    }
    // Create storage
    auto [itr, _] =
        storage_.emplace(name, std::make_pair(std::make_unique<TContext>(db, config, data_directory), config));
    return itr->second.first.get();
    // TODO: Handle errors and return {}?
  }

  std::optional<TContext *> New(std::string_view name, storage::Storage *db,
                                const std::filesystem::path &data_directory) {
    if (default_config_) {
      return New(name, db, *default_config_, data_directory);
    }
    return {};
  }

  std::optional<TContext *> Get(std::string_view name) {
    if (auto search = storage_.find(name); search != storage_.end()) {
      return search->second.first.get();
    }
    return {};
  }

  bool Delete(std::string_view name) {
    // TODO: Are we deleting the storage or just "deleting" its content?
    return false;
  }

  void SetDefaultConfig(TConfig config) { default_config_ = config; }
  std::optional<TConfig> GetDefaultConfig() { return default_config_; }

 private:
  // Are storage objects ever deleted?
  // shared_ptr and custom destructor if we are destroying it
  // unique and raw ptrs if we are not destroying it
  // Create drop and create with same name?
  std::unordered_map<std::string, std::pair<std::unique_ptr<TContext>, TConfig>> storage_;
  std::optional<TConfig> default_config_;
};

}  // namespace memgraph::dbms
