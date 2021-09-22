#pragma once

#include <optional>
#include <unordered_map>

#include "kvstore/kvstore.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

namespace utils {
struct Settings {
  using OnChangeCallback = std::function<void()>;

  void Initialize(std::filesystem::path storage_path);
  // RocksDB depends on statically allocated objects so we need to delete it before the static destruction kicks in
  void Finalize();

  void RegisterSetting(std::string name, const std::string &default_value, OnChangeCallback callback);
  std::optional<std::string> GetValue(const std::string &setting_name) const;
  bool SetValue(const std::string &setting_name, const std::string &new_value);
  std::vector<std::pair<std::string, std::string>> AllSettings() const;

 private:
  mutable utils::RWLock settings_lock_{RWLock::Priority::WRITE};
  std::unordered_map<std::string, OnChangeCallback> on_change_callbacks_;
  std::optional<kvstore::KVStore> storage_;
};

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extern Settings global_settings;
}  // namespace utils
