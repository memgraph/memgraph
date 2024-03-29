// Copyright 2024 Memgraph Ltd.
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

#include <functional>
#include <optional>
#include <unordered_map>

#include "kvstore/kvstore.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::utils {
struct Settings {
  using OnChangeCallback = std::function<void()>;
  using Validation = std::function<bool(std::string_view)>;

  void Initialize(std::filesystem::path storage_path);
  // RocksDB depends on statically allocated objects so we need to delete it before the static destruction kicks in
  void Finalize();

  void RegisterSetting(
      std::string name, const std::string &default_value, OnChangeCallback callback,
      Validation validation = [](auto) { return true; });
  std::optional<std::string> GetValue(const std::string &setting_name) const;
  bool SetValue(const std::string &setting_name, const std::string &new_value);
  std::vector<std::pair<std::string, std::string>> AllSettings() const;

 private:
  mutable utils::RWLock settings_lock_{RWLock::Priority::WRITE};
  std::unordered_map<std::string, OnChangeCallback> on_change_callbacks_;
  std::unordered_map<std::string, Validation> validations_;
  std::optional<kvstore::KVStore> storage_;
};

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extern Settings global_settings;
}  // namespace memgraph::utils
