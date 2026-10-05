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

#include <spdlog/spdlog.h>
#include <algorithm>
#include <mutex>
#include <shared_mutex>

#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/settings.hpp"

namespace memgraph::utils {

Settings::Settings(std::filesystem::path storage_path) : storage_(std::move(storage_path)) {}

void Settings::RegisterSetting(std::string name, const std::string &default_value, Persistence persistence,
                               OnChangeCallback callback, Validation validation) {
  std::lock_guard settings_guard{settings_lock_};
  MG_ASSERT(
      validation(default_value).has_value(), "\"{}\"'s default value does not satisfy the validation condition.", name);

  auto [it, inserted] = settings_.try_emplace(name,
                                              Entry{.value = default_value,
                                                    .persistence = persistence,
                                                    .on_change = std::move(callback),
                                                    .validation = std::move(validation)});
  MG_ASSERT(inserted, "Setting '{}' is already registered", name);

  switch (persistence) {
    case Persistence::kPersisted: {
      if (const auto stored = storage_.Get(name); stored) {
        it->second.value = *stored;
      } else {
        MG_ASSERT(storage_.Put(name, default_value), "Failed to register a setting");
      }
      break;
    }
    case Persistence::kRuntimeOnly: {
      if (storage_.Get(name)) {
        MG_ASSERT(storage_.Delete(name), "Failed to delete a stale setting");
      }
      break;
    }
    case Persistence::kDeprecatedRestore:
      break;
  }
}

std::optional<std::string> Settings::GetValue(const std::string &setting_name) const {
  std::shared_lock settings_guard{settings_lock_};
  const auto it = settings_.find(setting_name);
  if (it == settings_.end()) return std::nullopt;
  return it->second.value;
}

bool Settings::SetValue(const std::string &setting_name, const std::string &new_value) {
  const auto settings_change_callback = std::invoke([&, this]() -> std::optional<OnChangeCallback> {
    std::lock_guard settings_guard{settings_lock_};
    const auto it = settings_.find(setting_name);
    if (it == settings_.end()) return std::nullopt;

    auto &entry = it->second;
    if (const auto msg = entry.validation(new_value); !msg.has_value()) {
      throw utils::BasicException("Cannot update setting '{}': {}", setting_name, msg.error());
    }

    if (entry.persistence == Persistence::kPersisted) {
      MG_ASSERT(storage_.Put(setting_name, new_value), "Failed to modify the setting");
    }
    entry.value = new_value;
    return entry.on_change;
  });

  if (!settings_change_callback) {
    return false;
  }

  (*settings_change_callback)();
  return true;
}

void Settings::SetValueForce(const std::string &setting_name, const std::string &new_value) {
  const std::lock_guard settings_guard{settings_lock_};
  const auto it = settings_.find(setting_name);
  if (it == settings_.end()) {
    spdlog::error("SetValueForce called for unregistered setting '{}'", setting_name);
    return;
  }
  auto &entry = it->second;
  if (entry.persistence == Persistence::kPersisted && !storage_.Put(setting_name, new_value)) {
    spdlog::error("Failed to force-set setting '{}'", setting_name);
    return;
  }
  entry.value = new_value;
}

std::vector<std::pair<std::string, std::string>> Settings::AllSettings() const {
  std::shared_lock settings_guard{settings_lock_};

  std::vector<std::pair<std::string, std::string>> settings;
  settings.reserve(settings_.size());
  for (const auto &[name, entry] : settings_) {
    settings.emplace_back(name, entry.value);
  }
  std::ranges::sort(settings);
  return settings;
}

std::optional<std::string> Settings::StoredValue(const std::string &setting_name) const {
  std::shared_lock settings_guard{settings_lock_};
  return storage_.Get(setting_name);
}

void Settings::DropStoredValue(const std::string &setting_name) {
  std::lock_guard settings_guard{settings_lock_};
  const auto it = settings_.find(setting_name);
  MG_ASSERT(it != settings_.end() && it->second.persistence != Persistence::kPersisted,
            "Only a non-persisted setting can drop its stored value");
  if (!storage_.Delete(setting_name)) {
    spdlog::error("Failed to delete the stored value of setting '{}'", setting_name);
  }
}

}  // namespace memgraph::utils
