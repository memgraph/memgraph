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

#include <dlfcn.h>
#include <filesystem>
#include <functional>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "query/procedure/cypher_types.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "utils/memory.hpp"
#include "utils/rw_lock.hpp"

class CypherMainVisitorTest;

namespace memgraph::query::procedure {

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
  // /// Returns registered functions of this module
  virtual const std::map<std::string, mgp_func, std::less<>> *Functions() const = 0;

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

  bool RegisterModule(std::string_view name, std::unique_ptr<Module> module);

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
  bool LoadOrReloadModuleFromName(std::string_view name);

  /// Atomically unload all modules and then load all possible modules from the
  /// set directories.
  ///
  /// Takes a write lock.
  void UnloadAndLoadModulesFromDirectories();

  /// Find a module with given name or return nullptr.
  /// Takes a read lock.
  ModulePtr GetModuleNamed(std::string_view name) const;

  /// Remove all loaded (non-builtin) modules.
  /// Takes a write lock.
  void UnloadAllModules();

  /// Returns the shared memory allocator used by modules
  utils::MemoryResource &GetSharedMemoryResource() noexcept;

  bool RegisterMgProcedure(std::string_view name, mgp_proc proc);

  const std::filesystem::path &InternalModuleDir() const noexcept;

 private:
  class SharedLibraryHandle {
   public:
    SharedLibraryHandle(const std::string &shared_library, int mode) : handle_{dlopen(shared_library.c_str(), mode)} {}
    SharedLibraryHandle(const SharedLibraryHandle &) = delete;
    SharedLibraryHandle(SharedLibraryHandle &&) = delete;
    SharedLibraryHandle operator=(const SharedLibraryHandle &) = delete;
    SharedLibraryHandle operator=(SharedLibraryHandle &&) = delete;

    ~SharedLibraryHandle() {
      if (handle_) {
        dlclose(handle_);
      }
    }

   private:
    void *handle_;
  };

#if __has_feature(address_sanitizer)
  // This is why we need RTLD_NODELETE and we must not use RTLD_DEEPBIND with
  // ASAN: https://github.com/google/sanitizers/issues/89
  SharedLibraryHandle libstd_handle{"libstdc++.so.6", RTLD_NOW | RTLD_LOCAL | RTLD_NODELETE};
#else
  // The reason behind opening share library during runtime is to avoid issues
  // with loading symbols from stdlib. We have encounter issues with locale
  // that cause std::cout not being printed and issues when python libraries
  // would call stdlib (e.g. pytorch).
  // The way that those issues were solved was
  // by using RTLD_DEEPBIND. RTLD_DEEPBIND ensures that the lookup for the
  // mentioned library will be first performed in the already existing bound
  // libraries and then the global namespace.
  // RTLD_DEEPBIND => https://linux.die.net/man/3/dlopen
  SharedLibraryHandle libstd_handle{"libstdc++.so.6", RTLD_NOW | RTLD_LOCAL | RTLD_DEEPBIND};
#endif
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
    const ModuleRegistry &module_registry, std::string_view fully_qualified_procedure_name,
    utils::MemoryResource *memory);

/// Return the ModulePtr and `mgp_trans *` of the found transformation after resolving
/// `fully_qualified_transformation_name`. `memory` is used for temporary allocations
/// inside this function. ModulePtr must be kept alive to make sure it won't be
/// unloaded.
std::optional<std::pair<procedure::ModulePtr, const mgp_trans *>> FindTransformation(
    const ModuleRegistry &module_registry, std::string_view fully_qualified_transformation_name,
    utils::MemoryResource *memory);

/// Return the ModulePtr and `mgp_func *` of the found function after resolving
/// `fully_qualified_function_name` if found. If there is no such function
/// std::nullopt is returned. `memory` is used for temporary allocations
/// inside this function. ModulePtr must be kept alive to make sure it won't be unloaded.
std::optional<std::pair<procedure::ModulePtr, const mgp_func *>> FindFunction(
    const ModuleRegistry &module_registry, std::string_view fully_qualified_function_name,
    utils::MemoryResource *memory);

template <typename T>
concept IsCallable = utils::SameAsAnyOf<T, mgp_proc, mgp_func>;

template <IsCallable TCall>
void ConstructArguments(const std::vector<TypedValue> &args, const TCall &callable,
                        const std::string_view fully_qualified_name, mgp_list &args_list, mgp_graph &graph) {
  const auto n_args = args.size();
  const auto c_args_sz = callable.args.size();
  const auto c_opt_args_sz = callable.opt_args.size();

  if (n_args < c_args_sz || (n_args - c_args_sz > c_opt_args_sz)) {
    if (callable.args.empty() && callable.opt_args.empty()) {
      throw QueryRuntimeException("'{}' requires no arguments.", fully_qualified_name);
    }

    if (callable.opt_args.empty()) {
      throw QueryRuntimeException("'{}' requires exactly {} {}.", fully_qualified_name, c_args_sz,
                                  c_args_sz == 1U ? "argument" : "arguments");
    }

    throw QueryRuntimeException("'{}' requires between {} and {} arguments.", fully_qualified_name, c_args_sz,
                                c_args_sz + c_opt_args_sz);
  }
  args_list.elems.reserve(n_args);

  auto is_not_optional_arg = [c_args_sz](int i) { return c_args_sz > i; };
  for (size_t i = 0; i < n_args; ++i) {
    auto arg = args[i];
    std::string_view name;
    const query::procedure::CypherType *type;
    if (is_not_optional_arg(i)) {
      name = callable.args[i].first;
      type = callable.args[i].second;
    } else {
      name = std::get<0>(callable.opt_args[i - c_args_sz]);
      type = std::get<1>(callable.opt_args[i - c_args_sz]);
    }
    if (!type->SatisfiesType(arg)) {
      throw QueryRuntimeException("'{}' argument named '{}' at position {} must be of type {}.", fully_qualified_name,
                                  name, i, type->GetPresentableName());
    }
    args_list.elems.emplace_back(std::move(arg), &graph);
  }
  // Fill missing optional arguments with their default values.
  const size_t passed_in_opt_args = n_args - c_args_sz;
  for (size_t i = passed_in_opt_args; i < c_opt_args_sz; ++i) {
    args_list.elems.emplace_back(std::get<2>(callable.opt_args[i]), &graph);
  }
}
}  // namespace memgraph::query::procedure
