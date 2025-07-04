// Copyright 2025 Memgraph Ltd.
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

#include "query/exceptions.hpp"
#include "query/procedure/cypher_types.hpp"
#include "query/procedure/module_fwd.hpp"
#include "utils/concepts.hpp"
#include "utils/memory.hpp"
#include "utils/rw_lock.hpp"

#include <dlfcn.h>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>

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

/// Thread-safe registration of modules from libraries, uses utils::RWLock.
class ModuleRegistry final {
  friend CypherMainVisitorTest;

 private:
  std::map<std::string, std::shared_ptr<Module>, std::less<>> modules_;
  mutable utils::RWLock lock_{utils::RWLock::Priority::WRITE};
  std::unique_ptr<utils::MemoryResource> shared_{std::make_unique<utils::ResourceWithOutOfMemoryException>()};

  /// requires unique lock on registry
  bool TryEraseModule(std::string_view name);

  /// requires unique lock on registry
  bool TryEraseAllModules();

  /// requires lock on registry
  bool RegisterModule(std::string_view name, std::unique_ptr<Module> module);

  /// requires lock
  void DoUnloadAllModules();

  /// Loads the module if it's in the modules_dir directory
  /// @return Whether the module was loaded
  bool LoadModuleIfFound(const std::filesystem::path &modules_dir, std::string_view name);

  /// requires lock on registry
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
  auto GetModuleNamed(std::string_view name) const -> std::shared_ptr<Module>;

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
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extern ModuleRegistry gModuleRegistry;

template <typename T>
using find_result = std::optional<std::pair<std::shared_ptr<Module>, const T *>>;

/// Return the ModulePtr and `mgp_proc *` of the found procedure after resolving
/// `fully_qualified_procedure_name`. `memory` is used for temporary allocations
/// inside this function. ModulePtr must be kept alive to make sure it won't be
/// unloaded.
find_result<mgp_proc> FindProcedure(const ModuleRegistry &module_registry,
                                    std::string_view fully_qualified_procedure_name);

/// Return the ModulePtr and `mgp_trans *` of the found transformation after resolving
/// `fully_qualified_transformation_name`. `memory` is used for temporary allocations
/// inside this function. ModulePtr must be kept alive to make sure it won't be
/// unloaded.
find_result<mgp_trans> FindTransformation(const ModuleRegistry &module_registry,
                                          std::string_view fully_qualified_transformation_name);

/// Return the ModulePtr and `mgp_func *` of the found function after resolving
/// `fully_qualified_function_name` if found. If there is no such function
/// std::nullopt is returned. `memory` is used for temporary allocations
/// inside this function. ModulePtr must be kept alive to make sure it won't be unloaded.
find_result<mgp_func> FindFunction(const ModuleRegistry &module_registry,
                                   std::string_view fully_qualified_function_name);

template <typename T>
concept IsCallable = utils::SameAsAnyOf<T, mgp_proc, mgp_func>;

/// Validates the args.
/// Ensures right number of required + optional args
/// Ensures args are of the right type
template <IsCallable TCall>
void ValidateArguments(std::span<TypedValue const> args, const TCall &callable,
                       const std::string_view fully_qualified_name) {
  const auto n_args = args.size();
  const auto c_args_sz = callable.args.size();
  const auto c_opt_args_sz = callable.opt_args.size();

  if (n_args < c_args_sz || (n_args - c_args_sz > c_opt_args_sz)) [[unlikely]] {
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

  for (size_t i = 0; i < n_args; ++i) {
    auto is_required_arg = i < c_args_sz;
    auto const *type = is_required_arg ? callable.args[i].second : std::get<1>(callable.opt_args[i - c_args_sz]);
    if (!type->SatisfiesType(args[i])) [[unlikely]] {
      auto name =
          std::string_view{is_required_arg ? callable.args[i].first : std::get<0>(callable.opt_args[i - c_args_sz])};
      throw QueryRuntimeException("'{}' argument named '{}' at position {} must be of type {}.", fully_qualified_name,
                                  name, i, type->GetPresentableName());
    }
  }
}

/// build mgp_list with copies of the args
/// also populates the missing optional args
void ConstructArguments(std::span<TypedValue const> args, const mgp_func &callable, mgp_list &args_list,
                        mgp_graph &graph);

void ConstructArguments(std::span<TypedValue const> args, const mgp_proc &callable, mgp_list &args_list,
                        mgp_graph &graph);
}  // namespace memgraph::query::procedure
