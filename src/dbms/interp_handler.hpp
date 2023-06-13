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

#include <filesystem>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>

#include "global.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "storage/v2/storage.hpp"
#include "utils.hpp"

namespace memgraph::dbms {

/**
 * @brief Multi-tenancy handler of interpreter context.
 *
 * @tparam TContext
 * @tparam TConfig
 */
template <typename TContext = query::InterpreterContext, typename TConfig = query::InterpreterConfig>
class InterpContextHandler {
 public:
  using NewResult = utils::BasicResult<NewError, std::shared_ptr<TContext>>;

  InterpContextHandler() {}

  /**
   * @brief Create a new interpreter context associated to the "name" database.
   *
   * @param name name of the database
   * @param db database storage pointer
   * @param config interpreter configuration
   * @param data_directory underlying RocksDB directory
   * @return NewResult pointer to context on success, error on failure
   */
  NewResult New(std::string_view name, storage::Storage *db, const TConfig &config,
                const std::filesystem::path &data_directory) {
    // TODO: Is there anything that can conflict
    // Control that the new configuration does not conflict with the previous ones
    // Create storage
    auto [itr, success] = interp_.emplace(std::piecewise_construct, std::forward_as_tuple(name),
                                          std::forward_as_tuple(config, db, config, data_directory));
    if (success) return itr->second.ptr_;
    return NewError::EXISTS;
  }

  std::optional<std::shared_ptr<TContext>> Get(const std::string &name) {
    if (auto search = interp_.find(name); search != interp_.end()) {
      return search->second.ptr_;
    }
    return {};
  }

  /**
   * @brief Delete the interpreter context associated with the "name" database
   *
   * @param name name of the database
   * @return true on success
   */
  bool Delete(const std::string &name) {
    if (auto itr = interp_.find(name); itr != interp_.end()) {
      interp_.erase(itr);
      return true;
    }
    return false;
  }

  /**
   * @brief Set the default configuration
   *
   * @param config
   */
  void SetDefaultConfig(TConfig config) { default_config_ = config; }

  /**
   * @brief Get the default configuration
   *
   * @return std::optional<TConfig>
   */
  std::optional<TConfig> GetDefaultConfig() { return default_config_; }

 private:
  std::unordered_map<std::string, SyncPtr<TContext, TConfig>> interp_;  //!< map to all active interpreters
  std::optional<TConfig> default_config_;                               //!< default configuration to use
};

}  // namespace memgraph::dbms
