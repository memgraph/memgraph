/// @file
/// API for loading and registering modules providing custom oC procedures
#pragma once

#include <filesystem>
#include <functional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "query/procedure/mg_procedure_impl.hpp"
#include "utils/rw_lock.hpp"

namespace query::procedure {

struct Module final {
  /// Path as requested for loading the module from a library.
  std::filesystem::path file_path;
  /// System handle to shared library.
  void *handle;
  /// Required initialization function called on module load.
  std::function<int(mgp_module *, mgp_memory *)> init_fn;
  /// Optional shutdown function called on module unload.
  std::function<int()> shutdown_fn;
  /// Registered procedures
  std::map<std::string, mgp_proc, std::less<>> procedures;
};

/// Proxy for a registered Module, acquires a read lock from ModuleRegistry.
class ModulePtr final {
  const Module *module_{nullptr};
  std::shared_lock<utils::RWLock> lock_;

 public:
  ModulePtr() = default;
  ModulePtr(std::nullptr_t) {}
  ModulePtr(const Module *module, std::shared_lock<utils::RWLock> lock)
      : module_(module), lock_(std::move(lock)) {}

  explicit operator bool() const { return static_cast<bool>(module_); }

  const Module &operator*() const { return *module_; }
  const Module *operator->() const { return module_; }
};

/// Thread-safe registration of modules from libraries, uses utils::RWLock.
class ModuleRegistry final {
  std::unordered_map<std::string, Module> modules_;
  utils::RWLock lock_{utils::RWLock::Priority::WRITE};

 public:
  /// Load a module from the given path and return true if successful.
  ///
  /// A write lock is taken during the execution of this method. Loading a
  /// module is done through `dlopen` facility and path is resolved accordingly.
  /// The module is registered using the filename part of the path, with the
  /// extension removed. If a module with the same name already exists, the
  /// function does nothing.
  bool LoadModuleLibrary(std::filesystem::path path);

  /// Find a module with given name or return nullptr.
  /// Takes a read lock.
  ModulePtr GetModuleNamed(const std::string_view &name);

  /// Reload a module with given name and return true if successful.
  /// Takes a write lock. If false was returned, then the module is no longer
  /// registered.
  bool ReloadModuleNamed(const std::string_view &name);

  /// Reload all loaded modules and return true if successful.
  /// Takes a write lock. If false was returned, the module which failed to
  /// reload is no longer registered. Remaining modules may or may not be
  /// reloaded, but are valid and registered.
  bool ReloadAllModules();

  /// Remove all loaded modules.
  /// Takes a write lock.
  void UnloadAllModules();
};

/// Single, global module registry.
extern ModuleRegistry gModuleRegistry;

}  // namespace query::procedure
