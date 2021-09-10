#include <fmt/format.h>

#include "utils/logging.hpp"
#include "utils/settings.hpp"

namespace utils {
Settings::Settings(std::filesystem::path storage_path) : storage_{std::move(storage_path)} {}

void Settings::RegisterSetting(std::string name, std::string default_value) {
  auto storage_locked = storage_.Lock();
  if (const auto maybe_value = storage_locked->Get(name); maybe_value) {
    SPDLOG_INFO("The setting with name {} already exists!", name);
    return;
  }

  MG_ASSERT(storage_locked->Put(name, default_value), "Failed to register a setting");
}

std::string Settings::GetValueFor(const std::string &setting_name) {
  auto storage_locked = storage_.Lock();
  auto maybe_value = storage_locked->Get(setting_name);
  MG_ASSERT(maybe_value, "Invalid setting used in the code");
  return std::move(*maybe_value);
}

bool Settings::SetValueFor(const std::string &setting_name, std::string new_value) {
  auto storage_locked = storage_.Lock();
  if (const auto maybe_value = storage_locked->Get(setting_name); maybe_value) {
    return false;
  }

  MG_ASSERT(storage_locked->Put(setting_name, new_value), "Failed to modify the setting");
  return true;
}
}  // namespace utils
