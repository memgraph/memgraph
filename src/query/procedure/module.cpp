#include "query/procedure/module.hpp"
#include "utils/memory.hpp"

extern "C" {
#include <dlfcn.h>
}

#include <optional>

#include "fmt/format.h"
#include "py/py.hpp"
#include "query/procedure/py_module.hpp"
#include "utils/file.hpp"
#include "utils/logging.hpp"
#include "utils/pmr/vector.hpp"
#include "utils/string.hpp"

namespace query::procedure {

ModuleRegistry gModuleRegistry;

Module::~Module() {}

class BuiltinModule final : public Module {
 public:
  BuiltinModule();
  ~BuiltinModule() override;
  BuiltinModule(const BuiltinModule &) = delete;
  BuiltinModule(BuiltinModule &&) = delete;
  BuiltinModule &operator=(const BuiltinModule &) = delete;
  BuiltinModule &operator=(BuiltinModule &&) = delete;

  bool Close() override;

  const std::map<std::string, mgp_proc, std::less<>> *Procedures() const override;

  const std::map<std::string, mgp_trans, std::less<>> *Transformations() const override;

  void AddProcedure(std::string_view name, mgp_proc proc);

  void AddTransformation(std::string_view name, mgp_trans trans);

 private:
  /// Registered procedures
  std::map<std::string, mgp_proc, std::less<>> procedures_;
  std::map<std::string, mgp_trans, std::less<>> transformations_;
};

BuiltinModule::BuiltinModule() {}

BuiltinModule::~BuiltinModule() {}

bool BuiltinModule::Close() { return true; }

const std::map<std::string, mgp_proc, std::less<>> *BuiltinModule::Procedures() const { return &procedures_; }

const std::map<std::string, mgp_trans, std::less<>> *BuiltinModule::Transformations() const {
  return &transformations_;
}

void BuiltinModule::AddProcedure(std::string_view name, mgp_proc proc) { procedures_.emplace(name, std::move(proc)); }

void BuiltinModule::AddTransformation(std::string_view name, mgp_trans trans) {
  transformations_.emplace(name, std::move(trans));
}

namespace {

void RegisterMgLoad(ModuleRegistry *module_registry, utils::RWLock *lock, BuiltinModule *module) {
  // Loading relies on the fact that regular procedure invocation through
  // CallProcedureCursor::Pull takes ModuleRegistry::lock_ with READ access. To
  // load modules we have to upgrade our READ access to WRITE access,
  // therefore we release the READ lock and invoke the load function which
  // takes the WRITE lock. Obviously, some other thread may take a READ or WRITE
  // lock during our transition when we hold no such lock. In this case it is
  // fine, because our builtin module cannot be unloaded and we are ok with
  // using the new state of module_registry when we manage to acquire the lock
  // we desire. Note, deadlock between threads should not be possible, because a
  // single thread may only take either a READ or a WRITE lock, it's not
  // possible for a thread to hold both. If a thread tries to do that, it will
  // deadlock immediately (no other thread needs to do anything).
  auto with_unlock_shared = [lock](const auto &load_function) {
    lock->unlock_shared();
    try {
      load_function();
      // There's no finally in C++, but we have to return our original READ lock
      // state in any possible case.
    } catch (...) {
      lock->lock_shared();
      throw;
    }
    lock->lock_shared();
  };
  auto load_all_cb = [module_registry, with_unlock_shared](mgp_list *, mgp_graph *, mgp_result *, mgp_memory *) {
    with_unlock_shared([&]() { module_registry->UnloadAndLoadModulesFromDirectories(); });
  };
  mgp_proc load_all("load_all", load_all_cb, utils::NewDeleteResource());
  module->AddProcedure("load_all", std::move(load_all));
  auto load_cb = [module_registry, with_unlock_shared](mgp_list *args, mgp_graph *, mgp_result *res, mgp_memory *) {
    MG_ASSERT(Call<size_t>(mgp_list_size, args) == 1U, "Should have been type checked already");
    const auto *arg = Call<mgp_value *>(mgp_list_at, args, 0);
    MG_ASSERT(CallBool(mgp_value_is_string, arg), "Should have been type checked already");
    bool succ = false;
    with_unlock_shared([&]() {
      const char *arg_as_string{nullptr};
      if (const auto err = mgp_value_get_string(arg, &arg_as_string); err != MGP_ERROR_NO_ERROR) {
        succ = false;
      } else {
        succ = module_registry->LoadOrReloadModuleFromName(arg_as_string);
      }
    });
    if (!succ) {
      MG_ASSERT(mgp_result_set_error_msg(res, "Failed to (re)load the module.") == MGP_ERROR_NO_ERROR);
    }
  };
  mgp_proc load("load", load_cb, utils::NewDeleteResource());
  MG_ASSERT(mgp_proc_add_arg(&load, "module_name", Call<const mgp_type *>(mgp_type_string)) == MGP_ERROR_NO_ERROR);
  module->AddProcedure("load", std::move(load));
}

void RegisterMgProcedures(
    // We expect modules to be sorted by name.
    const std::map<std::string, std::unique_ptr<Module>, std::less<>> *all_modules, BuiltinModule *module) {
  auto procedures_cb = [all_modules](mgp_list *, mgp_graph *, mgp_result *result, mgp_memory *memory) {
    // Iterating over all_modules assumes that the standard mechanism of custom
    // procedure invocations takes the ModuleRegistry::lock_ with READ access.
    // For details on how the invocation is done, take a look at the
    // CallProcedureCursor::Pull implementation.
    for (const auto &[module_name, module] : *all_modules) {
      // Return the results in sorted order by module and by procedure.
      static_assert(
          std::is_same_v<decltype(module->Procedures()), const std::map<std::string, mgp_proc, std::less<>> *>,
          "Expected module procedures to be sorted by name");
      for (const auto &[proc_name, proc] : *module->Procedures()) {
        mgp_result_record *record{nullptr};
        if (const auto err = mgp_result_new_record(result, &record); err == MGP_ERROR_UNABLE_TO_ALLOCATE) {
          static_cast<void>(mgp_result_set_error_msg(result, "Not enough memory!"));
          return;
        } else if (err != MGP_ERROR_NO_ERROR) {
          static_cast<void>(mgp_result_set_error_msg(result, "Unexpected error"));
          return;
        }

        utils::pmr::string full_name(module_name, memory->impl);
        full_name.append(1, '.');
        full_name.append(proc_name);
        MgpUniquePtr<mgp_value> name_value{nullptr, mgp_value_destroy};
        if (const auto err = CreateMgpObject(name_value, mgp_value_make_string, full_name.c_str(), memory);
            err == MGP_ERROR_UNABLE_TO_ALLOCATE) {
          static_cast<void>(mgp_result_set_error_msg(result, "Not enough memory!"));
          return;
        } else if (err != MGP_ERROR_NO_ERROR) {
          static_cast<void>(mgp_result_set_error_msg(result, "Unexpected error"));
          return;
        }
        std::stringstream ss;
        ss << module_name << ".";
        PrintProcSignature(proc, &ss);
        const auto signature = ss.str();
        MgpUniquePtr<mgp_value> signature_value{nullptr, mgp_value_destroy};
        if (const auto err = CreateMgpObject(signature_value, mgp_value_make_string, full_name.c_str(), memory);
            err == MGP_ERROR_UNABLE_TO_ALLOCATE) {
          static_cast<void>(mgp_result_set_error_msg(result, "Not enough memory!"));
          return;
        } else if (err != MGP_ERROR_NO_ERROR) {
          static_cast<void>(mgp_result_set_error_msg(result, "Unexpected error"));
          return;
        }
        const auto err1 = mgp_result_record_insert(record, "name", name_value.get());
        const auto err2 = mgp_result_record_insert(record, "signature", signature_value.get());
        if (err1 != MGP_ERROR_NO_ERROR || err2 != MGP_ERROR_NO_ERROR) {
          static_cast<void>(mgp_result_set_error_msg(result, "Unable to set the result!"));
          return;
        }
      }
    }
  };
  mgp_proc procedures("procedures", procedures_cb, utils::NewDeleteResource());
  MG_ASSERT(mgp_proc_add_result(&procedures, "name", Call<const mgp_type *>(mgp_type_string)) == MGP_ERROR_NO_ERROR);
  MG_ASSERT(mgp_proc_add_result(&procedures, "signature", Call<const mgp_type *>(mgp_type_string)) ==
            MGP_ERROR_NO_ERROR);
  module->AddProcedure("procedures", std::move(procedures));
}

void RegisterMgTransformations(const std::map<std::string, std::unique_ptr<Module>, std::less<>> *all_modules,
                               BuiltinModule *module) {
  auto procedures_cb = [all_modules](const mgp_list * /*unused*/, const mgp_graph * /*unused*/, mgp_result *result,
                                     mgp_memory *memory) {
    for (const auto &[module_name, module] : *all_modules) {
      // Return the results in sorted order by module and by transformation.
      static_assert(
          std::is_same_v<decltype(module->Transformations()), const std::map<std::string, mgp_trans, std::less<>> *>,
          "Expected module transformations to be sorted by name");
      for (const auto &[trans_name, proc] : *module->Transformations()) {
        mgp_result_record *record{nullptr};
        if (const auto err = mgp_result_new_record(result, &record); err == MGP_ERROR_UNABLE_TO_ALLOCATE) {
          static_cast<void>(mgp_result_set_error_msg(result, "Not enough memory!"));
          return;
        } else if (err != MGP_ERROR_NO_ERROR) {
          static_cast<void>(mgp_result_set_error_msg(result, "Unexpected error"));
          return;
        }

        utils::pmr::string full_name(module_name, memory->impl);
        full_name.append(1, '.');
        full_name.append(trans_name);

        MgpUniquePtr<mgp_value> name_value{nullptr, mgp_value_destroy};
        if (const auto err = CreateMgpObject(name_value, mgp_value_make_string, full_name.c_str(), memory);
            err == MGP_ERROR_UNABLE_TO_ALLOCATE) {
          static_cast<void>(mgp_result_set_error_msg(result, "Not enough memory!"));
          return;
        } else if (err != MGP_ERROR_NO_ERROR) {
          static_cast<void>(mgp_result_set_error_msg(result, "Unexpected error"));
          return;
        }

        if (const auto err = mgp_result_record_insert(record, "name", name_value.get()); err != MGP_ERROR_NO_ERROR) {
          static_cast<void>(mgp_result_set_error_msg(result, "Unable to set the result!"));
          return;
        }
      }
    }
  };
  mgp_proc procedures("transformations", procedures_cb, utils::NewDeleteResource());
  MG_ASSERT(mgp_proc_add_result(&procedures, "name", Call<const mgp_type *>(mgp_type_string)) == MGP_ERROR_NO_ERROR);
  module->AddProcedure("transformations", std::move(procedures));
}

// Run `fun` with `mgp_module *` and `mgp_memory *` arguments. If `fun` returned
// a `true` value, store the `mgp_module::procedures` and
// `mgp_module::transformations into `proc_map`. The return value of WithModuleRegistration
// is the same as that of `fun`. Note, the return value need only be convertible to `bool`,
// it does not have to be `bool` itself.
template <class TProcMap, class TTransMap, class TFun>
auto WithModuleRegistration(TProcMap *proc_map, TTransMap *trans_map, const TFun &fun) {
  // We probably don't need more than 256KB for module initialization.
  constexpr size_t stack_bytes = 256 * 1024;
  unsigned char stack_memory[stack_bytes];
  utils::MonotonicBufferResource monotonic_memory(stack_memory, stack_bytes);
  mgp_memory memory{&monotonic_memory};
  mgp_module module_def{memory.impl};
  auto res = fun(&module_def, &memory);
  if (res) {
    // Copy procedures into resulting proc_map.
    for (const auto &proc : module_def.procedures) proc_map->emplace(proc);
    // Copy transformations into resulting trans_map.
    for (const auto &trans : module_def.transformations) trans_map->emplace(trans);
  }
  return res;
}

}  // namespace

class SharedLibraryModule final : public Module {
 public:
  SharedLibraryModule();
  ~SharedLibraryModule() override;
  SharedLibraryModule(const SharedLibraryModule &) = delete;
  SharedLibraryModule(SharedLibraryModule &&) = delete;
  SharedLibraryModule &operator=(const SharedLibraryModule &) = delete;
  SharedLibraryModule &operator=(SharedLibraryModule &&) = delete;

  bool Load(const std::filesystem::path &file_path);

  bool Close() override;

  const std::map<std::string, mgp_proc, std::less<>> *Procedures() const override;

  const std::map<std::string, mgp_trans, std::less<>> *Transformations() const override;

 private:
  /// Path as requested for loading the module from a library.
  std::filesystem::path file_path_;
  /// System handle to shared library.
  void *handle_;
  /// Required initialization function called on module load.
  std::function<int(mgp_module *, mgp_memory *)> init_fn_;
  /// Optional shutdown function called on module unload.
  std::function<int()> shutdown_fn_;
  /// Registered procedures
  std::map<std::string, mgp_proc, std::less<>> procedures_;
  /// Registered transformations
  std::map<std::string, mgp_trans, std::less<>> transformations_;
};

SharedLibraryModule::SharedLibraryModule() : handle_(nullptr) {}

SharedLibraryModule::~SharedLibraryModule() {
  if (handle_) Close();
}

bool SharedLibraryModule::Load(const std::filesystem::path &file_path) {
  MG_ASSERT(!handle_, "Attempting to load an already loaded module...");
  spdlog::info("Loading module {}...", file_path);
  file_path_ = file_path;
  dlerror();  // Clear any existing error.
  handle_ = dlopen(file_path.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (!handle_) {
    spdlog::error("Unable to load module {}; {}", file_path, dlerror());
    return false;
  }
  // Get required mgp_init_module
  init_fn_ = reinterpret_cast<int (*)(mgp_module *, mgp_memory *)>(dlsym(handle_, "mgp_init_module"));
  char *dl_errored = dlerror();
  if (!init_fn_ || dl_errored) {
    spdlog::error("Unable to load module {}; {}", file_path, dl_errored);
    dlclose(handle_);
    handle_ = nullptr;
    return false;
  }
  auto module_cb = [&](auto *module_def, auto *memory) {
    // Run mgp_init_module which must succeed.
    int init_res = init_fn_(module_def, memory);
    auto with_error = [this](std::string_view error_msg) {
      spdlog::error(error_msg);
      dlclose(handle_);
      handle_ = nullptr;
      return false;
    };

    if (init_res != 0) {
      const auto error = fmt::format("Unable to load module {}; mgp_init_module_returned {} ", file_path, init_res);
      return with_error(error);
    }
    for (auto &trans : module_def->transformations) {
      const bool was_result_added = MgpTransAddFixedResult(&trans.second);
      if (!was_result_added) {
        const auto error =
            fmt::format("Unable to add result to transformation in module {}; add result failed", file_path);
        return with_error(error);
      }
    }
    return true;
  };
  if (!WithModuleRegistration(&procedures_, &transformations_, module_cb)) {
    return false;
  }
  // Get optional mgp_shutdown_module
  shutdown_fn_ = reinterpret_cast<int (*)()>(dlsym(handle_, "mgp_shutdown_module"));
  dl_errored = dlerror();
  if (dl_errored) spdlog::warn("When loading module {}; {}", file_path, dl_errored);
  spdlog::info("Loaded module {}", file_path);
  return true;
}

bool SharedLibraryModule::Close() {
  MG_ASSERT(handle_, "Attempting to close a module that has not been loaded...");
  spdlog::info("Closing module {}...", file_path_);
  // non-existent shutdown function is semantically the same as a shutdown
  // function that does nothing.
  int shutdown_res = 0;
  if (shutdown_fn_) shutdown_res = shutdown_fn_();
  if (shutdown_res != 0) {
    spdlog::warn("When closing module {}; mgp_shutdown_module returned {}", file_path_, shutdown_res);
  }
  if (dlclose(handle_) != 0) {
    spdlog::error("Failed to close module {}; {}", file_path_, dlerror());
    return false;
  }
  spdlog::info("Closed module {}", file_path_);
  handle_ = nullptr;
  procedures_.clear();
  return true;
}

const std::map<std::string, mgp_proc, std::less<>> *SharedLibraryModule::Procedures() const {
  MG_ASSERT(handle_,
            "Attempting to access procedures of a module that has not "
            "been loaded...");
  return &procedures_;
}

const std::map<std::string, mgp_trans, std::less<>> *SharedLibraryModule::Transformations() const {
  MG_ASSERT(handle_,
            "Attempting to access procedures of a module that has not "
            "been loaded...");
  return &transformations_;
}

class PythonModule final : public Module {
 public:
  PythonModule();
  ~PythonModule() override;
  PythonModule(const PythonModule &) = delete;
  PythonModule(PythonModule &&) = delete;
  PythonModule &operator=(const PythonModule &) = delete;
  PythonModule &operator=(PythonModule &&) = delete;

  bool Load(const std::filesystem::path &file_path);

  bool Close() override;

  const std::map<std::string, mgp_proc, std::less<>> *Procedures() const override;
  const std::map<std::string, mgp_trans, std::less<>> *Transformations() const override;

 private:
  std::filesystem::path file_path_;
  py::Object py_module_;
  std::map<std::string, mgp_proc, std::less<>> procedures_;
  std::map<std::string, mgp_trans, std::less<>> transformations_;
};

PythonModule::PythonModule() {}

PythonModule::~PythonModule() {
  if (py_module_) Close();
}

bool PythonModule::Load(const std::filesystem::path &file_path) {
  MG_ASSERT(!py_module_, "Attempting to load an already loaded module...");
  spdlog::info("Loading module {}...", file_path);
  file_path_ = file_path;
  auto gil = py::EnsureGIL();
  auto maybe_exc = py::AppendToSysPath(file_path.parent_path().c_str());
  if (maybe_exc) {
    spdlog::error("Unable to load module {}; {}", file_path, *maybe_exc);
    return false;
  }
  bool succ = true;
  auto module_cb = [&](auto *module_def, auto *memory) {
    auto result = ImportPyModule(file_path.stem().c_str(), module_def);
    for (auto &trans : module_def->transformations) {
      succ = MgpTransAddFixedResult(&trans.second);
      if (!succ) return result;
    };
    return result;
  };
  py_module_ = WithModuleRegistration(&procedures_, &transformations_, module_cb);
  if (py_module_) {
    spdlog::info("Loaded module {}", file_path);

    if (!succ) {
      spdlog::error("Unable to add result to transformation");
      return false;
    }
    return true;
  }
  auto exc_info = py::FetchError().value();
  spdlog::error("Unable to load module {}; {}", file_path, exc_info);
  return false;
}

bool PythonModule::Close() {
  MG_ASSERT(py_module_, "Attempting to close a module that has not been loaded...");
  spdlog::info("Closing module {}...", file_path_);
  // The procedures and transformations are closures which hold references to the Python callbacks.
  // Releasing these references might result in deallocations so we need to take the GIL.
  auto gil = py::EnsureGIL();
  procedures_.clear();
  transformations_.clear();
  // Delete the module from the `sys.modules` directory so that the module will
  // be properly imported if imported again.
  py::Object sys(PyImport_ImportModule("sys"));
  if (PyDict_DelItemString(sys.GetAttr("modules").Ptr(), file_path_.stem().c_str()) != 0) {
    spdlog::warn("Failed to remove the module from sys.modules");
    py_module_ = py::Object(nullptr);
    return false;
  }
  py_module_ = py::Object(nullptr);
  spdlog::info("Closed module {}", file_path_);
  return true;
}

const std::map<std::string, mgp_proc, std::less<>> *PythonModule::Procedures() const {
  MG_ASSERT(py_module_,
            "Attempting to access procedures of a module that has "
            "not been loaded...");
  return &procedures_;
}

const std::map<std::string, mgp_trans, std::less<>> *PythonModule::Transformations() const {
  MG_ASSERT(py_module_,
            "Attempting to access procedures of a module that has "
            "not been loaded...");
  return &transformations_;
}
namespace {

std::unique_ptr<Module> LoadModuleFromFile(const std::filesystem::path &path) {
  const auto &ext = path.extension();
  if (ext != ".so" && ext != ".py") {
    spdlog::warn("Unknown query module file {}", path);
    return nullptr;
  }
  std::unique_ptr<Module> module;
  if (path.extension() == ".so") {
    auto lib_module = std::make_unique<SharedLibraryModule>();
    if (!lib_module->Load(path)) return nullptr;
    module = std::move(lib_module);
  } else if (path.extension() == ".py") {
    auto py_module = std::make_unique<PythonModule>();
    if (!py_module->Load(path)) return nullptr;
    module = std::move(py_module);
  }
  return module;
}

}  // namespace

bool ModuleRegistry::RegisterModule(const std::string_view &name, std::unique_ptr<Module> module) {
  MG_ASSERT(!name.empty(), "Module name cannot be empty");
  MG_ASSERT(module, "Tried to register an invalid module");
  if (modules_.find(name) != modules_.end()) {
    spdlog::error("Unable to overwrite an already loaded module {}", name);
    return false;
  }
  modules_.emplace(name, std::move(module));
  return true;
}

void ModuleRegistry::DoUnloadAllModules() {
  MG_ASSERT(modules_.find("mg") != modules_.end(), "Expected the builtin \"mg\" module to be present.");
  // This is correct because the destructor will close each module. However,
  // we don't want to unload the builtin "mg" module.
  auto module = std::move(modules_["mg"]);
  modules_.clear();
  modules_.emplace("mg", std::move(module));
}

ModuleRegistry::ModuleRegistry() {
  auto module = std::make_unique<BuiltinModule>();
  RegisterMgProcedures(&modules_, module.get());
  RegisterMgTransformations(&modules_, module.get());
  RegisterMgLoad(this, &lock_, module.get());
  modules_.emplace("mg", std::move(module));
}

void ModuleRegistry::SetModulesDirectory(std::vector<std::filesystem::path> modules_dirs) {
  modules_dirs_ = std::move(modules_dirs);
}

bool ModuleRegistry::LoadModuleIfFound(const std::filesystem::path &modules_dir, const std::string_view name) {
  if (!utils::DirExists(modules_dir)) {
    spdlog::error("Module directory {} doesn't exist", modules_dir);
    return false;
  }
  for (const auto &entry : std::filesystem::directory_iterator(modules_dir)) {
    const auto &path = entry.path();
    if (entry.is_regular_file() && path.stem() == name) {
      auto module = LoadModuleFromFile(path);
      if (!module) return false;
      return RegisterModule(name, std::move(module));
    }
  }
  return false;
}

bool ModuleRegistry::LoadOrReloadModuleFromName(const std::string_view name) {
  if (modules_dirs_.empty()) return false;
  if (name.empty()) return false;
  std::unique_lock<utils::RWLock> guard(lock_);
  auto found_it = modules_.find(name);
  if (found_it != modules_.end()) {
    if (!found_it->second->Close()) {
      spdlog::warn("Failed to close module {}", found_it->first);
    }
    modules_.erase(found_it);
  }

  for (const auto &module_dir : modules_dirs_) {
    if (LoadModuleIfFound(module_dir, name)) {
      return true;
    }
  }
  return false;
}

void ModuleRegistry::LoadModulesFromDirectory(const std::filesystem::path &modules_dir) {
  if (modules_dir.empty()) return;
  if (!utils::DirExists(modules_dir)) {
    spdlog::error("Module directory {} doesn't exist", modules_dir);
    return;
  }
  for (const auto &entry : std::filesystem::directory_iterator(modules_dir)) {
    const auto &path = entry.path();
    if (entry.is_regular_file()) {
      std::string name = path.stem();
      if (name.empty()) continue;
      auto module = LoadModuleFromFile(path);
      if (!module) continue;
      RegisterModule(name, std::move(module));
    }
  }
}

void ModuleRegistry::UnloadAndLoadModulesFromDirectories() {
  std::unique_lock<utils::RWLock> guard(lock_);
  DoUnloadAllModules();
  for (const auto &module_dir : modules_dirs_) {
    LoadModulesFromDirectory(module_dir);
  }
}

ModulePtr ModuleRegistry::GetModuleNamed(const std::string_view &name) const {
  std::shared_lock<utils::RWLock> guard(lock_);
  auto found_it = modules_.find(name);
  if (found_it == modules_.end()) return nullptr;
  return ModulePtr(found_it->second.get(), std::move(guard));
}

void ModuleRegistry::UnloadAllModules() {
  std::unique_lock<utils::RWLock> guard(lock_);
  DoUnloadAllModules();
}

utils::MemoryResource &ModuleRegistry::GetSharedMemoryResource() noexcept { return *shared_; }

namespace {

/// This function returns a pair of either
//      ModuleName | Prop
/// 1. <ModuleName,  ProcedureName>
/// 2. <ModuleName,  TransformationName>
std::optional<std::pair<std::string_view, std::string_view>> FindModuleNameAndProp(
    const ModuleRegistry &module_registry, std::string_view fully_qualified_name, utils::MemoryResource *memory) {
  utils::pmr::vector<std::string_view> name_parts(memory);
  utils::Split(&name_parts, fully_qualified_name, ".");
  if (name_parts.size() == 1U) return std::nullopt;
  auto last_dot_pos = fully_qualified_name.find_last_of('.');
  MG_ASSERT(last_dot_pos != std::string_view::npos);

  const auto &module_name = fully_qualified_name.substr(0, last_dot_pos);
  const auto &name = name_parts.back();
  return std::make_pair(module_name, name);
}

template <typename T>
concept ModuleProperties = utils::SameAsAnyOf<T, mgp_proc, mgp_trans>;

template <ModuleProperties T>
std::optional<std::pair<procedure::ModulePtr, const T *>> MakePairIfPropFound(const ModuleRegistry &module_registry,
                                                                              std::string_view fully_qualified_name,
                                                                              utils::MemoryResource *memory) {
  auto prop_fun = [](auto &module) {
    if constexpr (std::is_same_v<T, mgp_proc>) {
      return module->Procedures();
    } else {
      return module->Transformations();
    }
  };
  auto result = FindModuleNameAndProp(module_registry, fully_qualified_name, memory);
  if (!result) return std::nullopt;
  auto [module_name, prop_name] = *result;
  auto module = module_registry.GetModuleNamed(module_name);
  if (!module) return std::nullopt;
  auto *prop = prop_fun(module);
  const auto &prop_it = prop->find(prop_name);
  if (prop_it == prop->end()) return std::nullopt;
  return std::make_pair(std::move(module), &prop_it->second);
}

}  // namespace

std::optional<std::pair<procedure::ModulePtr, const mgp_proc *>> FindProcedure(
    const ModuleRegistry &module_registry, std::string_view fully_qualified_procedure_name,
    utils::MemoryResource *memory) {
  return MakePairIfPropFound<mgp_proc>(module_registry, fully_qualified_procedure_name, memory);
}

std::optional<std::pair<procedure::ModulePtr, const mgp_trans *>> FindTransformation(
    const ModuleRegistry &module_registry, std::string_view fully_qualified_transformation_name,
    utils::MemoryResource *memory) {
  return MakePairIfPropFound<mgp_trans>(module_registry, fully_qualified_transformation_name, memory);
}

}  // namespace query::procedure
