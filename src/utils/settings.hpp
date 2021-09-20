#pragma once

#include <optional>
#include <unordered_map>

#include "kvstore/kvstore.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

namespace utils {

struct Settings {
  static Settings &GetInstance() {
    static Settings settings;
    return settings;
  }

  void Initialize(std::filesystem::path storage_path);
  // RocksDB depends on statically allocated objects so we need to delete it before the static destruction kicks in
  void Finalize();
  void RegisterSetting(std::string name, std::string default_value);
  std::optional<std::string> GetValue(const std::string &setting_name) const;
  bool SetValue(const std::string &setting_name, std::string new_value);
  std::vector<std::pair<std::string, std::string>> AllSettings() const;

 private:
  std::optional<utils::Synchronized<kvstore::KVStore, utils::WritePrioritizedRWLock>> storage_;
};
}  // namespace utils
