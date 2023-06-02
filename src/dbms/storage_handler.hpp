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

#include "storage/v2/storage.hpp"

namespace memgraph::dbms {

template <typename TStorage = memgraph::storage::Storage, typename TConfig = memgraph::storage::Config>
class StorageHandler {
 public:
  StorageHandler() {}

  std::optional<TStorage *> New(std::string_view name, const TConfig &config) {
    // Control that no one is using the same data directory
    if (std::any_of(storage_.begin(), storage_.end(), [&](const auto &elem) {
          return elem.second.second.durability.storage_directory == config.durability.storage_directory;
        })) {
      // LOG
      return {};
    }
    // Create storage
    auto [itr, _] = storage_.emplace(name, std::make_pair(std::make_unique<TStorage>(config), config));
    return itr->second.first.get();
    // TODO: Handle errors and return {}?
  }

  std::optional<TStorage *> New(std::string_view name) {
    if (default_config_) {
      return New(name, *default_config_);
    }
    return {};
  }

  std::optional<TStorage *> New(std::string_view name, std::filesystem::path storage_subdir) {
    if (default_config_) {
      auto config = default_config_;
      config->durability.storage_directory /= storage_subdir;
      return New(name, *default_config_);
    }
    return {};
  }

  std::optional<TStorage *> Get(std::string_view name) {
    if (auto search = storage_.find(name); search != storage_.end()) {
      return search->second.first.get();
    }
    return {};
  }

  std::optional<TConfig> GetConfig(std::string_view name) {
    if (auto search = storage_.find(name); search != storage_.end()) {
      return search->second.second;
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
  std::unordered_map<std::string, std::pair<std::unique_ptr<TStorage>, TConfig>> storage_;
  std::optional<TConfig> default_config_;
};

}  // namespace memgraph::dbms
