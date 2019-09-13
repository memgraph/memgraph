/// @file API for loading and registering plugins providing custom oC procedures
#pragma once

#include <filesystem>
#include <functional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "utils/rw_lock.hpp"

struct mg_value;
struct mg_graph;
struct mg_result;

namespace query::plugin {

struct Plugin final {
  /// Path as requested for loading the plugin from a library.
  std::filesystem::path file_path;
  /// System handle to shared library.
  void *handle;
  /// Entry-point for plugin's custom procedure.
  std::function<void(const mg_value **, int64_t, const mg_graph *, mg_result *)>
      main_fn;
  /// Optional initialization function called on plugin load.
  std::function<int()> init_fn;
  /// Optional shutdown function called on plugin unload.
  std::function<int()> shutdown_fn;
};


/// Proxy for a registered Plugin, acquires a read lock from PluginRegistry.
class PluginPtr final {
  const Plugin *plugin_{nullptr};
  std::shared_lock<utils::RWLock> lock_;

 public:
  PluginPtr() = default;
  PluginPtr(std::nullptr_t) {}
  PluginPtr(const Plugin *plugin, std::shared_lock<utils::RWLock> lock)
      : plugin_(plugin), lock_(std::move(lock)) {}

  explicit operator bool() const { return static_cast<bool>(plugin_); }

  const Plugin &operator*() const { return *plugin_; }
  const Plugin *operator->() const { return plugin_; }
};

/// Thread-safe registration of plugins from libraries, uses utils::RWLock.
class PluginRegistry final {
  std::unordered_map<std::string, Plugin> plugins_;
  utils::RWLock lock_{utils::RWLock::Priority::WRITE};

 public:
  /// Load a plugin from the given path and return true if successful.
  ///
  /// A write lock is taken during the execution of this method. Loading a
  /// plugin is done through `dlopen` facility and path is resolved accordingly.
  /// The plugin is registered using the filename part of the path, with the
  /// extension removed. If a plugin with the same name already exists, the
  /// function does nothing.
  bool LoadPluginLibrary(std::filesystem::path path);

  /// Find a plugin with given name or return nullptr.
  /// Takes a read lock.
  PluginPtr GetPluginNamed(const std::string_view &name);

  /// Reload a plugin with given name and return true if successful.
  /// Takes a write lock. If false was returned, then the plugin is no longer
  /// registered.
  bool ReloadPluginNamed(const std::string_view &name);

  /// Reload all loaded plugins and return true if successful.
  /// Takes a write lock. If false was returned, the plugin which failed to
  /// reload is no longer registered. Remaining plugins may or may not be
  /// reloaded, but are valid and registered.
  bool ReloadAllPlugins();

  /// Remove all loaded plugins.
  /// Takes a write lock.
  void UnloadAllPlugins();
};

/// Single, global plugin registry.
extern PluginRegistry gPluginRegistry;

}  // namespace query::plugin
