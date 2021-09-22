#include <fmt/format.h>

#include "utils/logging.hpp"
#include "utils/settings.hpp"

namespace utils {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
Settings global_settings;

void Settings::Initialize(std::filesystem::path storage_path) {
  std::lock_guard settings_guard{settings_lock_};
  storage_.emplace(std::move(storage_path));
}

void Settings::Finalize() {
  std::lock_guard settings_guard{settings_lock_};
  storage_.reset();
}

void Settings::RegisterSetting(std::string name, const std::string &default_value, OnChangeCallback callback) {
  std::lock_guard settings_guard{settings_lock_};
  MG_ASSERT(storage_);

  if (const auto maybe_value = storage_->Get(name); maybe_value) {
    SPDLOG_INFO("The setting with name {} already exists!", name);
  } else {
    MG_ASSERT(storage_->Put(name, default_value), "Failed to register a setting");
  }

  const auto [it, inserted] = on_change_callbacks_.emplace(std::move(name), callback);
  MG_ASSERT(inserted, "Settings storage is out of sync");
}

std::optional<std::string> Settings::GetValue(const std::string &setting_name) const {
  std::shared_lock settings_guard{settings_lock_};
  MG_ASSERT(storage_);
  auto maybe_value = storage_->Get(setting_name);
  return maybe_value;
}

bool Settings::SetValue(const std::string &setting_name, const std::string &new_value) {
  const auto callback = std::invoke([&, this]() -> std::optional<OnChangeCallback> {
    std::lock_guard settings_guard{settings_lock_};
    MG_ASSERT(storage_);

    if (const auto maybe_value = storage_->Get(setting_name); !maybe_value) {
      return std::nullopt;
    }

    MG_ASSERT(storage_->Put(setting_name, new_value), "Failed to modify the setting");

    const auto it = on_change_callbacks_.find(setting_name);
    MG_ASSERT(it != on_change_callbacks_.end(), "Settings storage is out of sync");
    return it->second;
  });

  if (!callback) {
    return false;
  }

  (*callback)();
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
}  // namespace utils
