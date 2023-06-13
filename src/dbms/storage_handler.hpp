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
#include "storage/v2/storage.hpp"
#include "utils/result.hpp"

namespace memgraph::dbms {

/**
 * @brief Multi-tenancy handler of storage.
 *
 * @tparam TStorage
 * @tparam TConfig
 */
template <typename TStorage = memgraph::storage::Storage, typename TConfig = memgraph::storage::Config>
class StorageHandler {
 public:
  using NewResult = utils::BasicResult<NewError, std::shared_ptr<TStorage>>;

  StorageHandler() {}

  /**
   * @brief Create a new storage associated to the "name" database.
   *
   * @param name name of the database
   * @param config storage configuration
   * @return NewResult pointer to storage on success, error on failure
   */
  NewResult New(std::string_view name, const TConfig &config) {
    // Control that no one is using the same data directory
    if (storage_.find(std::string(name)) != storage_.end()) {
      return NewError::EXISTS;
    }
    // TODO better check
    if (std::any_of(storage_.begin(), storage_.end(), [&](const auto &elem) {
          return elem.second.second.durability.storage_directory == config.durability.storage_directory;
        })) {
      // LOG
      return NewError::EXISTS;
    }
    // Create storage
    auto [itr, success] = storage_.emplace(name, std::make_pair(std::make_shared<TStorage>(config), config));
    if (success) return itr->second.first;
    return NewError::EXISTS;
  }

  /**
   * @brief Create a new storage associated to the "name" database.
   *
   * @param name name of the database
   * @param storage_subdir undelying RocksDB directory
   * @return NewResult pointer to storage on success, error on failure
   */
  NewResult New(std::string_view name, std::filesystem::path storage_subdir) {
    if (default_config_) {
      auto config = default_config_;
      config->durability.storage_directory /= storage_subdir;
      return New(name, *default_config_);
    }
    return NewError::NO_CONFIGS;
  }

  /**
   * @brief Create a new storage associated to the "name" database.
   *
   * @param name name of the database
   * @return NewResult pointer to storage on success, error on failure
   */
  NewResult New(std::string_view name) { return New(name, name); }

  /**
   * @brief Get storage associated with "name" database
   *
   * @param name name of the database
   * @return std::optional<std::shared_ptr<TStorage>>
   */
  std::optional<std::shared_ptr<TStorage>> Get(const std::string &name) {
    if (auto search = storage_.find(name); search != storage_.end()) {
      return search->second.first;
    }
    return {};
  }

  /**
   * @brief Get the storage configuration associated with "name" database
   *
   * @param name name of the database
   * @return std::optional<TConfig>
   */
  std::optional<TConfig> GetConfig(std::string_view name) {
    if (auto search = storage_.find(name); search != storage_.end()) {
      return search->second.second;
    }
    return {};
  }

  /**
   * @brief Delete the storage associated with the "name" database
   *
   * @param name name of the database
   * @return true on success
   */
  bool Delete(const std::string &name) {
    if (auto itr = storage_.find(name); itr != storage_.end()) {
      storage_.erase(itr);
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

  /**
   * @brief Return all active databases.
   *
   * @return std::vector<std::string>
   */
  std::vector<std::string> All() const {
    std::vector<std::string> res;
    res.reserve(storage_.size());
    for (const auto &[name, _] : storage_) {
      res.emplace_back(name);
    }
    return res;
  }

 private:
  std::unordered_map<std::string, std::pair<std::shared_ptr<TStorage>, TConfig>>
      storage_;                            //!< map to all active storages
  std::optional<TConfig> default_config_;  //!< default configuration
};

}  // namespace memgraph::dbms
