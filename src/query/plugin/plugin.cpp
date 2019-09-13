#include "query/plugin/plugin.hpp"

extern "C" {
#include <dlfcn.h>
}

#include <optional>

namespace query::plugin {

PluginRegistry gPluginRegistry;

namespace {

std::optional<Plugin> LoadPluginFromSharedLibrary(std::filesystem::path path) {
  LOG(INFO) << "Loading plugin " << path << " ...";
  Plugin plugin{path};
  dlerror();  // Clear any existing error.
  plugin.handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (!plugin.handle) {
    LOG(ERROR) << "Unable to load plugin " << path << "; " << dlerror();
    return std::nullopt;
  }
  // Get required mg_main
  plugin.main_fn =
      reinterpret_cast<void (*)(const mg_value **, int64_t, const mg_graph *,
                                mg_result *)>(dlsym(plugin.handle, "mg_main"));
  const char *error = dlerror();
  if (!plugin.main_fn || error) {
    LOG(ERROR) << "Unable to load plugin " << path << "; " << error;
    dlclose(plugin.handle);
    return std::nullopt;
  }
  // Get optional mg_init_module
  plugin.init_fn =
      reinterpret_cast<int (*)()>(dlsym(plugin.handle, "mg_init_module"));
  error = dlerror();
  if (error) LOG(WARNING) << "When loading plugin " << path << "; " << error;
  // Run mg_init_module which must succeed.
  if (plugin.init_fn) {
    int init_res = plugin.init_fn();
    if (init_res != 0) {
      LOG(ERROR) << "Unable to load plugin " << path
                 << "; mg_init_module returned " << init_res;
      dlclose(plugin.handle);
      return std::nullopt;
    }
  }
  // Get optional mg_shutdown_module
  plugin.shutdown_fn =
      reinterpret_cast<int (*)()>(dlsym(plugin.handle, "mg_shutdown_module"));
  error = dlerror();
  if (error) LOG(WARNING) << "When loading plugin " << path << "; " << error;
  LOG(INFO) << "Loaded plugin " << path;
  return plugin;
}

bool ClosePlugin(Plugin *plugin) {
  LOG(INFO) << "Closing plugin " << plugin->file_path << " ...";
  if (plugin->shutdown_fn) {
    int shutdown_res = plugin->shutdown_fn();
    if (shutdown_res != 0) {
      LOG(WARNING) << "When closing plugin " << plugin->file_path
                   << "; mg_shutdown_module returned " << shutdown_res;
    }
  }
  if (dlclose(plugin->handle) != 0) {
    LOG(ERROR) << "Failed to close plugin " << plugin->file_path << "; "
               << dlerror();
    return false;
  }
  LOG(INFO) << "Closed plugin " << plugin->file_path;
  return true;
}

}  // namespace

bool PluginRegistry::LoadPluginLibrary(std::filesystem::path path) {
  std::unique_lock<utils::RWLock> guard(lock_);
  std::string plugin_name(path.stem());
  if (plugins_.find(plugin_name) != plugins_.end()) return true;
  auto maybe_plugin = LoadPluginFromSharedLibrary(path);
  if (!maybe_plugin) return false;
  plugins_[plugin_name] = std::move(*maybe_plugin);
  return true;
}

PluginPtr PluginRegistry::GetPluginNamed(const std::string_view &name) {
  std::shared_lock<utils::RWLock> guard(lock_);
  // NOTE: std::unordered_map::find cannot work with std::string_view :(
  auto found_it = plugins_.find(std::string(name));
  if (found_it == plugins_.end()) return nullptr;
  return PluginPtr(&found_it->second, std::move(guard));
}

bool PluginRegistry::ReloadPluginNamed(const std::string_view &name) {
  std::unique_lock<utils::RWLock> guard(lock_);
  // NOTE: std::unordered_map::find cannot work with std::string_view :(
  auto found_it = plugins_.find(std::string(name));
  if (found_it == plugins_.end()) {
    LOG(ERROR) << "Trying to reload plugin '" << name
               << "' which is not loaded.";
    return false;
  }
  auto &plugin = found_it->second;
  if (!ClosePlugin(&plugin)) {
    plugins_.erase(found_it);
    return false;
  }
  auto maybe_plugin = LoadPluginFromSharedLibrary(plugin.file_path);
  if (!maybe_plugin) {
    plugins_.erase(found_it);
    return false;
  }
  plugin = std::move(*maybe_plugin);
  return true;
}

bool PluginRegistry::ReloadAllPlugins() {
  std::unique_lock<utils::RWLock> guard(lock_);
  for (auto &[name, plugin] : plugins_) {
    if (!ClosePlugin(&plugin)) {
      plugins_.erase(name);
      return false;
    }
    auto maybe_plugin = LoadPluginFromSharedLibrary(plugin.file_path);
    if (!maybe_plugin) {
      plugins_.erase(name);
      return false;
    }
    plugin = std::move(*maybe_plugin);
  }
  return true;
}

void PluginRegistry::UnloadAllPlugins() {
  std::unique_lock<utils::RWLock> guard(lock_);
  for (auto &name_and_plugin : plugins_) ClosePlugin(&name_and_plugin.second);
  plugins_.clear();
}

}  // namespace query::plugin
