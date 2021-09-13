#pragma once

#include <optional>
#include <unordered_map>

#include "kvstore/kvstore.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

namespace utils {

struct Settings {
  explicit Settings(std::filesystem::path storage_path);

  void RegisterSetting(std::string name, std::string default_value);
  std::optional<std::string> GetValueFor(const std::string &setting_name) const;
  bool SetValueFor(const std::string &setting_name, std::string new_value);
  std::vector<std::pair<std::string, std::string>> AllSettings() const;

 private:
  utils::Synchronized<kvstore::KVStore, utils::WritePrioritizedRWLock> storage_;
};
}  // namespace utils
