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
  // Get required mgp_main
  module.main_fn = reinterpret_cast<void (*)(
      const mgp_list *, const mgp_graph *, mgp_result *, mgp_memory *)>(
      dlsym(module.handle, "mgp_main"));
  const char *error = dlerror();
  if (!module.main_fn || error) {
    LOG(ERROR) << "Unable to load module " << path << "; " << error;
    dlclose(module.handle);
    return std::nullopt;
  }
  // Get optional mgp_init_module
  module.init_fn =
      reinterpret_cast<int (*)()>(dlsym(module.handle, "mgp_init_module"));
  error = dlerror();
  if (error) LOG(WARNING) << "When loading module " << path << "; " << error;
  // Run mgp_init_module which must succeed.
  if (module.init_fn) {
    int init_res = module.init_fn();
    if (init_res != 0) {
      LOG(ERROR) << "Unable to load module " << path
                 << "; mgp_init_module returned " << init_res;
      dlclose(module.handle);
      return std::nullopt;
    }
  }
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

}  // namespace

bool ModuleRegistry::LoadModuleLibrary(std::filesystem::path path) {
  std::unique_lock<utils::RWLock> guard(lock_);
  std::string module_name(path.stem());
  if (modules_.find(module_name) != modules_.end()) return true;
  auto maybe_module = LoadModuleFromSharedLibrary(path);
  if (!maybe_module) return false;
  modules_[module_name] = std::move(*maybe_module);
  return true;
}

ModulePtr ModuleRegistry::GetModuleNamed(const std::string_view &name) {
  std::shared_lock<utils::RWLock> guard(lock_);
  // NOTE: std::unordered_map::find cannot work with std::string_view :(
  auto found_it = modules_.find(std::string(name));
  if (found_it == modules_.end()) return nullptr;
  return ModulePtr(&found_it->second, std::move(guard));
}

bool ModuleRegistry::ReloadModuleNamed(const std::string_view &name) {
  std::unique_lock<utils::RWLock> guard(lock_);
  // NOTE: std::unordered_map::find cannot work with std::string_view :(
  auto found_it = modules_.find(std::string(name));
  if (found_it == modules_.end()) {
    LOG(ERROR) << "Trying to reload module '" << name
               << "' which is not loaded.";
    return false;
  }
  auto &module = found_it->second;
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
  for (auto &name_and_module : modules_) CloseModule(&name_and_module.second);
  modules_.clear();
}

}  // namespace query::procedure
