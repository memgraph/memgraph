#pragma once

#include <optional>
#include <unordered_map>

#include "utils/synchronized.hpp"

namespace utils {

struct Settings {
  void RegisterSetting(std::string name, std::string default_value);
  std::string GetValueFor(const std::string &setting_name);
  bool SetValueFor(const std::string &setting_name, std::string new_value);

 private:
  // TODO(antonio2368): Add support for multiple types, description, validation...
  utils::Synchronized<std::unordered_map<std::string, std::string>> settings_;
};
}  // namespace utils
