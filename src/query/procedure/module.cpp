#include "query/procedure/module.hpp"

extern "C" {
#include <dlfcn.h>
}

#include <optional>

namespace query::procedure {

ModuleRegistry gModuleRegistry;

namespace {

std::optional<Module> LoadModuleFromSharedLibrary(std::filesystem::path path) {
  LOG(INFO) << "Loading module " << path << " ...";
  Module module{path};
  dlerror();  // Clear any existing error.
  module.handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (!module.handle) {
    LOG(ERROR) << "Unable to load module " << path << "; " << dlerror();
    return std::nullopt;
  }
  // Get required mgp_init_module
  module.init_fn = reinterpret_cast<int (*)(mgp_module *, mgp_memory *)>(
      dlsym(module.handle, "mgp_init_module"));
  const char *error = dlerror();
  if (!module.init_fn || error) {
    LOG(ERROR) << "Unable to load module " << path << "; " << error;
    dlclose(module.handle);
    return std::nullopt;
  }
  // We probably don't need more than 256KB for module initialazation.
  constexpr size_t stack_bytes = 256 * 1024;
  unsigned char stack_memory[stack_bytes];
  utils::MonotonicBufferResource monotonic_memory(stack_memory, stack_bytes);
  mgp_memory memory{&monotonic_memory};
  mgp_module module_def{memory.impl};
  // Run mgp_init_module which must succeed.
  int init_res = module.init_fn(&module_def, &memory);
  if (init_res != 0) {
    LOG(ERROR) << "Unable to load module " << path
               << "; mgp_init_module returned " << init_res;
    dlclose(module.handle);
    return std::nullopt;
  }
  // Copy procedures into our memory.
  for (const auto &proc : module_def.procedures)
    module.procedures.emplace(proc);
  // Get optional mgp_shutdown_module
  module.shutdown_fn =
      reinterpret_cast<int (*)()>(dlsym(module.handle, "mgp_shutdown_module"));
  error = dlerror();
  if (error) LOG(WARNING) << "When loading module " << path << "; " << error;
  LOG(INFO) << "Loaded module " << path;
  return module;
}

bool CloseModule(Module *module) {
  LOG(INFO) << "Closing module " << module->file_path << " ...";
  if (module->shutdown_fn) {
    int shutdown_res = module->shutdown_fn();
    if (shutdown_res != 0) {
      LOG(WARNING) << "When closing module " << module->file_path
                   << "; mgp_shutdown_module returned " << shutdown_res;
    }
  }
  if (dlclose(module->handle) != 0) {
    LOG(ERROR) << "Failed to close module " << module->file_path << "; "
               << dlerror();
    return false;
  }
  LOG(INFO) << "Closed module " << module->file_path;
  return true;
}

// Return true if the module is builtin, i.e. not loaded from dynamic lib.
// Builtin modules cannot be reloaded nor unloaded.
bool IsBuiltinModule(const Module &module) { return module.handle == nullptr; }

void RegisterMgReload(ModuleRegistry *module_registry, utils::RWLock *lock,
                      Module *module) {
  // Reloading relies on the fact that regular procedure invocation through
  // CallProcedureCursor::Pull takes ModuleRegistry::lock_ with READ access. To
  // reload modules we have to upgrade our READ access to WRITE access,
  // therefore we release the READ lock and invoke the reload function which
  // takes the WRITE lock. Obviously, some other thread may take a READ or WRITE
  // lock during our transition when we hold no such lock. In this case it is
  // fine, because our builtin module cannot be unloaded and we are ok with
  // using the new state of module_registry when we manage to acquire the lock
  // we desire. Note, deadlock between threads should not be possible, because a
  // single thread may only take either a READ or a WRITE lock, it's not
  // possible for a thread to hold both. If a thread tries to do that, it will
  // deadlock immediately (no other thread needs to do anything).
  auto with_unlock_shared = [lock](const auto &reload_function) {
    lock->unlock_shared();
    try {
      reload_function();
      // There's no finally in C++, but we have to return our original READ lock
      // state in any possible case.
    } catch (...) {
      lock->lock_shared();
      throw;
    }
    lock->lock_shared();
  };
  auto reload_all_cb = [module_registry, with_unlock_shared](
                           const mgp_list *, const mgp_graph *, mgp_result *res,
                           mgp_memory *) {
    bool succ = false;
    with_unlock_shared([&]() { succ = module_registry->ReloadAllModules(); });
    if (!succ) mgp_result_set_error_msg(res, "Failed to reload all modules.");
  };
  mgp_proc reload_all("reload_all", reload_all_cb, utils::NewDeleteResource());
  module->procedures.emplace("reload_all", std::move(reload_all));
  auto reload_cb = [module_registry, with_unlock_shared](
                       const mgp_list *args, const mgp_graph *, mgp_result *res,
                       mgp_memory *) {
    CHECK(mgp_list_size(args) == 1U) << "Should have been type checked already";
    const mgp_value *arg = mgp_list_at(args, 0);
    CHECK(mgp_value_is_string(arg)) << "Should have been type checked already";
    bool succ = false;
    with_unlock_shared([&]() {
      succ = module_registry->ReloadModuleNamed(mgp_value_get_string(arg));
    });
    if (!succ)
      mgp_result_set_error_msg(
          res, "Failed to reload the module; it is no longer loaded.");
  };
  mgp_proc reload("reload", reload_cb, utils::NewDeleteResource());
  mgp_proc_add_arg(&reload, "module_name", mgp_type_string());
  module->procedures.emplace("reload", std::move(reload));
}

void RegisterMgProcedures(
    // We expect modules to be sorted by name.
    const std::map<std::string, Module, std::less<>> *all_modules,
    Module *module) {
  auto procedures_cb = [all_modules](const mgp_list *, const mgp_graph *,
                                     mgp_result *result, mgp_memory *memory) {
    // Iterating over all_modules assumes that the standard mechanism of custom
    // procedure invocations takes the ModuleRegistry::lock_ with READ access.
    // For details on how the invocation is done, take a look at the
    // CallProcedureCursor::Pull implementation.
    for (const auto &[module_name, module] : *all_modules) {
      // Return the results in sorted order by module and by procedure.
      static_assert(
          std::is_same_v<decltype(module.procedures),
                         std::map<std::string, mgp_proc, std::less<>>>,
          "Expected module procedures to be sorted by name");
      for (const auto &[proc_name, proc] : module.procedures) {
        auto *record = mgp_result_new_record(result);
        if (!record) {
          mgp_result_set_error_msg(result, "Not enough memory!");
          return;
        }
        utils::pmr::string full_name(module_name, memory->impl);
        full_name.append(1, '.');
        full_name.append(proc_name);
        auto *name_value = mgp_value_make_string(full_name.c_str(), memory);
        if (!name_value) {
          mgp_result_set_error_msg(result, "Not enough memory!");
          return;
        }
        std::stringstream ss;
        ss << module_name << ".";
        PrintProcSignature(proc, &ss);
        const auto signature = ss.str();
        auto *signature_value =
            mgp_value_make_string(signature.c_str(), memory);
        if (!signature_value) {
          mgp_value_destroy(name_value);
          mgp_result_set_error_msg(result, "Not enough memory!");
          return;
        }
        int succ1 = mgp_result_record_insert(record, "name", name_value);
        int succ2 =
            mgp_result_record_insert(record, "signature", signature_value);
        mgp_value_destroy(name_value);
        mgp_value_destroy(signature_value);
        if (!succ1 || !succ2) {
          mgp_result_set_error_msg(result, "Unable to set the result!");
          return;
        }
      }
    }
  };
  mgp_proc procedures("procedures", procedures_cb, utils::NewDeleteResource());
  mgp_proc_add_result(&procedures, "name", mgp_type_string());
  mgp_proc_add_result(&procedures, "signature", mgp_type_string());
  module->procedures.emplace("procedures", std::move(procedures));
}

}  // namespace

ModuleRegistry::ModuleRegistry() {
  Module module{.handle = nullptr};
  RegisterMgProcedures(&modules_, &module);
  RegisterMgReload(this, &lock_, &module);
  modules_.emplace("mg", std::move(module));
}

bool ModuleRegistry::LoadModuleLibrary(std::filesystem::path path) {
  std::unique_lock<utils::RWLock> guard(lock_);
  std::string module_name(path.stem());
  if (modules_.find(module_name) != modules_.end()) {
    LOG(ERROR) << "Unable to overwrite an already loaded module " << path;
    return false;
  }
  auto maybe_module = LoadModuleFromSharedLibrary(path);
  if (!maybe_module) return false;
  modules_[module_name] = std::move(*maybe_module);
  return true;
}

ModulePtr ModuleRegistry::GetModuleNamed(const std::string_view &name) {
  std::shared_lock<utils::RWLock> guard(lock_);
  auto found_it = modules_.find(name);
  if (found_it == modules_.end()) return nullptr;
  return ModulePtr(&found_it->second, std::move(guard));
}

bool ModuleRegistry::ReloadModuleNamed(const std::string_view &name) {
  std::unique_lock<utils::RWLock> guard(lock_);
  auto found_it = modules_.find(name);
  if (found_it == modules_.end()) {
    LOG(ERROR) << "Trying to reload module '" << name
               << "' which is not loaded.";
    return false;
  }
  auto &module = found_it->second;
  if (IsBuiltinModule(module)) return true;
  if (!CloseModule(&module)) {
    modules_.erase(found_it);
    return false;
  }
  auto maybe_module = LoadModuleFromSharedLibrary(module.file_path);
  if (!maybe_module) {
    modules_.erase(found_it);
    return false;
  }
  module = std::move(*maybe_module);
  return true;
}

bool ModuleRegistry::ReloadAllModules() {
  std::unique_lock<utils::RWLock> guard(lock_);
  for (auto &[name, module] : modules_) {
    if (IsBuiltinModule(module)) continue;
    if (!CloseModule(&module)) {
      modules_.erase(name);
      return false;
    }
    auto maybe_module = LoadModuleFromSharedLibrary(module.file_path);
    if (!maybe_module) {
      modules_.erase(name);
      return false;
    }
    module = std::move(*maybe_module);
  }
  return true;
}

void ModuleRegistry::UnloadAllModules() {
  std::unique_lock<utils::RWLock> guard(lock_);
  for (auto &name_and_module : modules_) {
    if (IsBuiltinModule(name_and_module.second)) continue;
    CloseModule(&name_and_module.second);
  }
  modules_.clear();
}

}  // namespace query::procedure
