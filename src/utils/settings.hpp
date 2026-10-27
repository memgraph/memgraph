// Copyright 2026 Memgraph Ltd.
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

#include <cstdint>
#include <expected>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kvstore/kvstore.hpp"
#include "utils/rw_lock.hpp"

namespace memgraph::utils {

/// Current values of every setting live in memory. Only settings registered as persisted are also written to the
/// on-disk store and read back from it on the next start.
struct Settings {
  using OnChangeCallback = std::function<void()>;
  using ValidatorResult = std::expected<void, std::string>;
  using Validation = std::function<ValidatorResult(std::string_view)>;

  enum class Persistence : uint8_t {
    /// Loaded from the store at registration and written through on every change.
    kPersisted,
    /// Never touches the store; a leftover entry written by an older version is deleted at registration.
    kRuntimeOnly,
  };

  explicit Settings(std::filesystem::path storage_path);

  /// Returns the leftover store value that a run-time only registration discarded, if there was one.
  std::optional<std::string> RegisterSetting(
      std::string name, const std::string &default_value, Persistence persistence, OnChangeCallback callback,
      Validation validation = [](auto) -> ValidatorResult { return {}; });
  std::optional<std::string> GetValue(const std::string &setting_name) const;
  bool SetValue(const std::string &setting_name, const std::string &new_value);
  // Set without validation and without the on-change callback.
  // Use only for internal system writes (e.g. persisting the winning license back to storage).
  void SetValueForce(const std::string &setting_name, const std::string &new_value);
  std::vector<std::pair<std::string, std::string>> AllSettings() const;

 private:
  struct Entry {
    std::string value;
    Persistence persistence;
    OnChangeCallback on_change;
    Validation validation;
  };

  mutable utils::RWLock settings_lock_{RWLock::Priority::WRITE};
  std::unordered_map<std::string, Entry> settings_;
  kvstore::KVStore storage_;
};

}  // namespace memgraph::utils
