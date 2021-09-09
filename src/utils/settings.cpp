#include <fmt/format.h>

#include "utils/logging.hpp"
#include "utils/settings.hpp"

namespace utils {
void Settings::RegisterSetting(std::string name, std::string default_value) {
  auto settings_locked = settings_.Lock();
  const auto [it, inserted] = settings_locked->try_emplace(std::move(name), std::move(default_value));
  MG_ASSERT(inserted, fmt::format("Tried to register same setting twice ({})!", name));
}

std::string Settings::GetValueFor(const std::string &setting_name) {
  auto settings_locked = settings_.Lock();
  const auto it = settings_locked->find(setting_name);
  MG_ASSERT(it != settings_locked->end(), "Invalid setting used in the code");
  return it->second;
}

bool Settings::SetValueFor(const std::string &setting_name, std::string new_value) {
  auto settings_locked = settings_.Lock();

  const auto it = settings_locked->find(setting_name);
  if (it == settings_locked->end()) {
    return false;
  }

  it->second = std::move(new_value);
  return true;
}
}  // namespace utils
