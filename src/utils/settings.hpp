#pragma once

#include <optional>
#include <unordered_map>

#include "kvstore/kvstore.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace utils {

struct Settings {
  explicit Settings(std::filesystem::path storage_path);
  void RegisterSetting(std::string name, std::string default_value);
  std::string GetValueFor(const std::string &setting_name);
  bool SetValueFor(const std::string &setting_name, std::string new_value);

 private:
  utils::Synchronized<kvstore::KVStore, utils::SpinLock> storage_;
};
}  // namespace utils
