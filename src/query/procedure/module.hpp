// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// @file
/// API for loading and registering modules providing custom oC procedures
#pragma once

#include <filesystem>
#include <functional>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "query/procedure/mg_procedure_impl.hpp"
#include "utils/memory.hpp"
#include "utils/rw_lock.hpp"

class CypherMainVisitorTest;

namespace query::procedure {

class Module {
 public:
  Module() {}
  virtual ~Module();
  Module(const Module &) = delete;
  Module(Module &&) = delete;
  Module &operator=(const Module &) = delete;
  Module &operator=(Module &&) = delete;

  /// Invokes the (optional) shutdown function and closes the module.
  virtual bool Close() = 0;

  /// Returns registered procedures of this module
  virtual const std::map<std::string, mgp_proc, std::less<>> *Procedures() const = 0;
  /// Returns registered transformations of this module
  virtual const std::map<std::string, mgp_trans, std::less<>> *Transformations() const = 0;

  virtual std::optional<std::filesystem::path> Path() const = 0;
};

/// Proxy for a registered Module, acquires a read lock from ModuleRegistry.
class ModulePtr final {
  const Module *module_{nullptr};
  std::shared_lock<utils::RWLock> lock_;

 public:
  ModulePtr() = default;
  ModulePtr(std::nullptr_t) {}
  ModulePtr(const Module *module, std::shared_lock<utils::RWLock> lock) : module_(module), lock_(std::move(lock)) {}

  explicit operator bool() const { return static_cast<bool>(module_); }

  const Module &operator*() const { return *module_; }
  const Module *operator->() const { return module_; }
};

/// Thread-safe registration of modules from libraries, uses utils::RWLock.
class ModuleRegistry final {
  friend CypherMainVisitorTest;

  std::map<std::string, std::unique_ptr<Module>, std::less<>> modules_;
  mutable utils::RWLock lock_{utils::RWLock::Priority::WRITE};
  std::unique_ptr<utils::MemoryResource> shared_{std::make_unique<utils::ResourceWithOutOfMemoryException>()};

  bool RegisterModule(const std::string_view &name, std::unique_ptr<Module> module);

  void DoUnloadAllModules();

  /// Loads the module if it's in the modules_dir directory
  /// @return Whether the module was loaded
  bool LoadModuleIfFound(const std::filesystem::path &modules_dir, std::string_view name);

  void LoadModulesFromDirectory(const std::filesystem::path &modules_dir);

 public:
  ModuleRegistry();

  /// Set the modules directories that will be used when (re)loading modules.
  void SetModulesDirectory(std::vector<std::filesystem::path> modules_dir, const std::filesystem::path &data_directory);
  const std::vector<std::filesystem::path> &GetModulesDirectory() const;

  /// Atomically load or reload a module with a particular name from the given
  /// directory.
  ///
  /// Takes a write lock. If the module exists it is reloaded. Otherwise, the
  /// module is loaded from the file whose filename, without the extension,
  /// matches the module's name. If multiple such files exist, only one is
  /// chosen, in an unspecified manner. If loading of the chosen file fails, no
  /// other files are tried.
  ///
  /// Return true if the module was loaded or reloaded successfully, false
  /// otherwise.
  bool LoadOrReloadModuleFromName(const std::string_view name);

  /// Atomically unload all modules and then load all possible modules from the
  /// set directories.
  ///
  /// Takes a write lock.
  void UnloadAndLoadModulesFromDirectories();

  /// Find a module with given name or return nullptr.
  /// Takes a read lock.
  ModulePtr GetModuleNamed(const std::string_view &name) const;

  /// Remove all loaded (non-builtin) modules.
  /// Takes a write lock.
  void UnloadAllModules();

  /// Returns the shared memory allocator used by modules
  utils::MemoryResource &GetSharedMemoryResource() noexcept;

  bool RegisterMgProcedure(std::string_view name, mgp_proc proc);

  const std::filesystem::path &InternalModuleDir() const noexcept;

 private:
  std::vector<std::filesystem::path> modules_dirs_;
  std::filesystem::path internal_module_dir_;
};

/// Single, global module registry.
extern ModuleRegistry gModuleRegistry;

/// Return the ModulePtr and `mgp_proc *` of the found procedure after resolving
/// `fully_qualified_procedure_name`. `memory` is used for temporary allocations
/// inside this function. ModulePtr must be kept alive to make sure it won't be
/// unloaded.
std::optional<std::pair<procedure::ModulePtr, const mgp_proc *>> FindProcedure(
    const ModuleRegistry &module_registry, const std::string_view fully_qualified_procedure_name,
    utils::MemoryResource *memory);

/// Return the ModulePtr and `mgp_trans *` of the found transformation after resolving
/// `fully_qualified_transformation_name`. `memory` is used for temporary allocations
/// inside this function. ModulePtr must be kept alive to make sure it won't be
/// unloaded.
std::optional<std::pair<procedure::ModulePtr, const mgp_trans *>> FindTransformation(
    const ModuleRegistry &module_registry, const std::string_view fully_qualified_transformation_name,
    utils::MemoryResource *memory);
}  // namespace query::procedure
