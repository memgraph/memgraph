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

#include <fmt/format.h>

#include "utils/logging.hpp"
#include "utils/settings.hpp"

namespace memgraph::utils {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
Settings global_settings;

void Settings::Initialize(std::filesystem::path storage_path) {
  std::lock_guard settings_guard{settings_lock_};
  storage_.emplace(std::move(storage_path));
}

void Settings::Finalize() {
  std::lock_guard settings_guard{settings_lock_};
  storage_.reset();
  on_change_callbacks_.clear();
  validations_.clear();
}

void Settings::RegisterSetting(std::string name, const std::string &default_value, OnChangeCallback callback,
                               Validation validation) {
  std::lock_guard settings_guard{settings_lock_};
  MG_ASSERT(storage_);
  MG_ASSERT(validation(default_value), "\"{}\"'s default value does not satisfy the validation condition.", name);

  if (const auto maybe_value = storage_->Get(name); maybe_value) {
    SPDLOG_INFO("The setting with name {} already exists!", name);
  } else {
    MG_ASSERT(storage_->Put(name, default_value), "Failed to register a setting");
  }
  {
    const auto [_, inserted] = on_change_callbacks_.emplace(name, callback);
    MG_ASSERT(inserted, "Settings storage is out of sync");
  }
  {
    const auto [_, inserted] = validations_.emplace(std::move(name), validation);
    MG_ASSERT(inserted, "Settings storage is out of sync");
  }
}

std::optional<std::string> Settings::GetValue(const std::string &setting_name) const {
  std::shared_lock settings_guard{settings_lock_};
  MG_ASSERT(storage_);
  auto maybe_value = storage_->Get(setting_name);
  return maybe_value;
}

bool Settings::SetValue(const std::string &setting_name, const std::string &new_value) {
  const auto settings_change_callback = std::invoke([&, this]() -> std::optional<OnChangeCallback> {
    std::lock_guard settings_guard{settings_lock_};
    MG_ASSERT(storage_);

    if (const auto maybe_value = storage_->Get(setting_name); !maybe_value) {
      return std::nullopt;
    }

    const auto val = validations_.find(setting_name);
    MG_ASSERT(val != validations_.end(), "Settings storage is out of sync");
    if (!val->second(new_value)) {
      throw utils::BasicException("'{}' not valid for '{}'", new_value, setting_name);
    }

    MG_ASSERT(storage_->Put(setting_name, new_value), "Failed to modify the setting");

    const auto it = on_change_callbacks_.find(setting_name);
    MG_ASSERT(it != on_change_callbacks_.end(), "Settings storage is out of sync");
    return it->second;
  });

  if (!settings_change_callback) {
    return false;
  }

  (*settings_change_callback)();
  return true;
}

std::vector<std::pair<std::string, std::string>> Settings::AllSettings() const {
  std::shared_lock settings_guard{settings_lock_};

  MG_ASSERT(storage_);

  std::vector<std::pair<std::string, std::string>> settings;
  settings.reserve(storage_->Size());
  for (const auto &[k, v] : *storage_) {
    settings.emplace_back(k, v);
  }

  return settings;
}
}  // namespace memgraph::utils
