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
#include <optional>
#include <string_view>
#include <unordered_map>

#include "global.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "storage/v2/storage.hpp"

// TODO: Fix this
namespace memgraph::query {
struct InterpreterContext;
}
namespace memgraph::dbms {

template <typename TContext = query::InterpreterContext, typename TConfig = query::InterpreterConfig>
class InterpContextHandler {
 public:
  using NewResult = utils::BasicResult<NewError, std::shared_ptr<TContext>>;

  InterpContextHandler() {}

  NewResult New(std::string_view name, storage::Storage *db, const TConfig &config,
                const std::filesystem::path &data_directory) {
    // Control that the new configuration does not conflict with the previous ones
    // TODO: Is there anything that can conflict
    // Create storage
    auto [itr, success] =
        storage_.emplace(name, std::make_pair(std::make_shared<TContext>(db, config, data_directory), config));
    if (success) return itr->second.first;
    // TODO: Handle errors and return {}?
    return NewError::EXISTS;
  }

  // std::optional<TContext *> New(std::string_view name, storage::Storage *db,
  //                               const std::filesystem::path &data_directory) {
  //   if (default_config_) {
  //     return New(name, db, *default_config_, data_directory);
  //   }
  //   return {};
  // }

  std::optional<std::shared_ptr<TContext>> Get(std::string_view name) {
    if (auto search = storage_.find(name); search != storage_.end()) {
      return search->second.first;
    }
    return {};
  }

  bool Delete(const std::string &name) {
    if (auto itr = storage_.find(name); itr != storage_.end()) {
      storage_.erase(itr);
      return true;
    }
    return false;
  }

  void SetDefaultConfig(TConfig config) { default_config_ = config; }
  std::optional<TConfig> GetDefaultConfig() { return default_config_; }

 private:
  // Are storage objects ever deleted?
  // shared_ptr and custom destructor if we are destroying it
  // unique and raw ptrs if we are not destroying it
  // Create drop and create with same name?
  std::unordered_map<std::string, std::pair<std::shared_ptr<TContext>, TConfig>> storage_;
  std::optional<TConfig> default_config_;
};

}  // namespace memgraph::dbms
