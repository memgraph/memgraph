#include "query/procedure/module.hpp"

extern "C" {
#include <dlfcn.h>
}

#include <optional>

#include "py/py.hpp"
#include "query/procedure/py_module.hpp"
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

  bool Reload() override;

  const std::map<std::string, mgp_proc, std::less<>> *Procedures()
      const override;

  void AddProcedure(std::string_view name, mgp_proc proc);

 private:
  /// Registered procedures
  std::map<std::string, mgp_proc, std::less<>> procedures_;
};

BuiltinModule::BuiltinModule() {}

BuiltinModule::~BuiltinModule() {}

bool BuiltinModule::Reload() { return true; }

bool BuiltinModule::Close() { return true; }

const std::map<std::string, mgp_proc, std::less<>> *BuiltinModule::Procedures()
    const {
  return &procedures_;
}

void BuiltinModule::AddProcedure(std::string_view name, mgp_proc proc) {
  procedures_.emplace(name, std::move(proc));
}

namespace {

void RegisterMgReload(ModuleRegistry *module_registry, utils::RWLock *lock,
                      BuiltinModule *module) {
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
  module->AddProcedure("reload_all", std::move(reload_all));
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
  module->AddProcedure("reload", std::move(reload));
}

void RegisterMgProcedures(
    // We expect modules to be sorted by name.
    const std::map<std::string, std::unique_ptr<Module>, std::less<>>
        *all_modules,
    BuiltinModule *module) {
  auto procedures_cb = [all_modules](const mgp_list *, const mgp_graph *,
                                     mgp_result *result, mgp_memory *memory) {
    // Iterating over all_modules assumes that the standard mechanism of custom
    // procedure invocations takes the ModuleRegistry::lock_ with READ access.
    // For details on how the invocation is done, take a look at the
    // CallProcedureCursor::Pull implementation.
    for (const auto &[module_name, module] : *all_modules) {
      // Return the results in sorted order by module and by procedure.
      static_assert(
          std::is_same_v<decltype(module->Procedures()),
                         const std::map<std::string, mgp_proc, std::less<>> *>,
          "Expected module procedures to be sorted by name");
      for (const auto &[proc_name, proc] : *module->Procedures()) {
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
  module->AddProcedure("procedures", std::move(procedures));
}

// Run `fun` with `mgp_module *` and `mgp_memory *` arguments. If `fun` returned
// a `true` value, store the `mgp_module::procedures` into `proc_map`. The
// return value of WithModuleRegistration is the same as that of `fun`. Note,
// the return value need only be convertible to `bool`, it does not have to be
// `bool` itself.
template <class TProcMap, class TFun>
auto WithModuleRegistration(TProcMap *proc_map, const TFun &fun) {
  // We probably don't need more than 256KB for module initialization.
  constexpr size_t stack_bytes = 256 * 1024;
  unsigned char stack_memory[stack_bytes];
  utils::MonotonicBufferResource monotonic_memory(stack_memory, stack_bytes);
  mgp_memory memory{&monotonic_memory};
  mgp_module module_def{memory.impl};
  auto res = fun(&module_def, &memory);
  if (res)
    // Copy procedures into resulting proc_map.
    for (const auto &proc : module_def.procedures) proc_map->emplace(proc);
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

  bool Load(std::filesystem::path file_path);

  bool Close() override;

  bool Reload() override;

  const std::map<std::string, mgp_proc, std::less<>> *Procedures()
      const override;

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
};

SharedLibraryModule::SharedLibraryModule() : handle_(nullptr) {}

SharedLibraryModule::~SharedLibraryModule() {
  if (handle_) Close();
}

bool SharedLibraryModule::Load(std::filesystem::path file_path) {
  CHECK(!handle_) << "Attempting to load an already loaded module...";
  LOG(INFO) << "Loading module " << file_path << " ...";
  file_path_ = file_path;
  dlerror();  // Clear any existing error.
  handle_ = dlopen(file_path.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (!handle_) {
    LOG(ERROR) << "Unable to load module " << file_path << "; " << dlerror();
    return false;
  }
  // Get required mgp_init_module
  init_fn_ = reinterpret_cast<int (*)(mgp_module *, mgp_memory *)>(
      dlsym(handle_, "mgp_init_module"));
  const char *error = dlerror();
  if (!init_fn_ || error) {
    LOG(ERROR) << "Unable to load module " << file_path << "; " << error;
    dlclose(handle_);
    handle_ = nullptr;
    return false;
  }
  if (!WithModuleRegistration(
          &procedures_, [&](auto *module_def, auto *memory) {
            // Run mgp_init_module which must succeed.
            int init_res = init_fn_(module_def, memory);
            if (init_res != 0) {
              LOG(ERROR) << "Unable to load module " << file_path
                         << "; mgp_init_module returned " << init_res;
              dlclose(handle_);
              handle_ = nullptr;
              return false;
            }
            return true;
          })) {
    return false;
  }
  // Get optional mgp_shutdown_module
  shutdown_fn_ =
      reinterpret_cast<int (*)()>(dlsym(handle_, "mgp_shutdown_module"));
  error = dlerror();
  if (error)
    LOG(WARNING) << "When loading module " << file_path << "; " << error;
  LOG(INFO) << "Loaded module " << file_path;
  return true;
}

bool SharedLibraryModule::Close() {
  CHECK(handle_) << "Attempting to close a module that has not been loaded...";
  LOG(INFO) << "Closing module " << file_path_ << " ...";
  // non-existent shutdown function is semantically the same as a shutdown
  // function that does nothing.
  int shutdown_res = 0;
  if (shutdown_fn_) shutdown_res = shutdown_fn_();
  if (shutdown_res != 0) {
    LOG(WARNING) << "When closing module " << file_path_
                 << "; mgp_shutdown_module returned " << shutdown_res;
  }
  if (dlclose(handle_) != 0) {
    LOG(ERROR) << "Failed to close module " << file_path_ << "; "
               << dlerror();
    return false;
  }
  LOG(INFO) << "Closed module " << file_path_;
  handle_ = nullptr;
  procedures_.clear();
  return true;
}

bool SharedLibraryModule::Reload() {
  CHECK(handle_) << "Attempting to reload a module that has not been loaded...";
  if (!Close()) return false;
  return Load(file_path_);
}

const std::map<std::string, mgp_proc, std::less<>>
    *SharedLibraryModule::Procedures() const {
  CHECK(handle_) << "Attempting to access procedures of a module that has not "
                    "been loaded...";
  return &procedures_;
}

class PythonModule final : public Module {
 public:
  PythonModule();
  ~PythonModule() override;
  PythonModule(const PythonModule &) = delete;
  PythonModule(PythonModule &&) = delete;
  PythonModule &operator=(const PythonModule &) = delete;
  PythonModule &operator=(PythonModule &&) = delete;

  bool Load(std::filesystem::path file_path);

  bool Close() override;

  bool Reload() override;

  const std::map<std::string, mgp_proc, std::less<>> *Procedures()
      const override;

 private:
  py::Object py_module_;
  std::map<std::string, mgp_proc, std::less<>> procedures_;
};

PythonModule::PythonModule() {}

PythonModule::~PythonModule() {
  if (py_module_) Close();
}

bool PythonModule::Load(std::filesystem::path file_path) {
  CHECK(!py_module_) << "Attempting to load an already loaded module...";
  LOG(INFO) << "Loading module " << file_path << " ...";
  auto gil = py::EnsureGIL();
  auto *py_path = PySys_GetObject("path");
  CHECK(py_path);
  py::Object import_dir(PyUnicode_FromString(file_path.parent_path().c_str()));
  int import_dir_in_path = PySequence_Contains(py_path, import_dir);
  if (import_dir_in_path == -1) {
    LOG(ERROR) << "Unexpected error when loading module " << file_path;
    return false;
  }
  if (import_dir_in_path == 0) {
    if (PyList_Append(py_path, import_dir) != 0) {
      auto exc_info = py::FetchError().value();
      LOG(ERROR) << "Unable to load module " << file_path << "; " << exc_info;
      return false;
    }
  }
  py_module_ =
      WithModuleRegistration(&procedures_, [&](auto *module_def, auto *memory) {
        return ImportPyModule(file_path.stem().c_str(), module_def);
      });
  if (py_module_) return true;
  auto exc_info = py::FetchError().value();
  LOG(ERROR) << "Unable to load module " << file_path << "; " << exc_info;
  return false;
}

bool PythonModule::Close() {
  CHECK(py_module_)
      << "Attempting to close a module that has not been loaded...";
  // Deleting procedures will probably release PyObject closures, so we need to
  // take the GIL.
  auto gil = py::EnsureGIL();
  procedures_.clear();
  py_module_ = py::Object(nullptr);
  return true;
}

bool PythonModule::Reload() {
  CHECK(py_module_)
      << "Attempting to reload a module that has not been loaded...";
  auto gil = py::EnsureGIL();
  procedures_.clear();
  py_module_ =
      WithModuleRegistration(&procedures_, [&](auto *module_def, auto *memory) {
        return ReloadPyModule(py_module_, module_def);
      });
  if (py_module_) return true;
  auto exc_info = py::FetchError().value();
  LOG(ERROR) << "Unable to reload module; " << exc_info;
  return false;
}

const std::map<std::string, mgp_proc, std::less<>> *PythonModule::Procedures()
    const {
  CHECK(py_module_) << "Attempting to access procedures of a module that has "
                       "not been loaded...";
  return &procedures_;
}

ModuleRegistry::ModuleRegistry() {
  auto module = std::make_unique<BuiltinModule>();
  RegisterMgProcedures(&modules_, module.get());
  RegisterMgReload(this, &lock_, module.get());
  modules_.emplace("mg", std::move(module));
}

bool ModuleRegistry::LoadModuleLibrary(std::filesystem::path path) {
  std::unique_lock<utils::RWLock> guard(lock_);
  std::string module_name(path.stem());
  if (modules_.find(module_name) != modules_.end()) {
    LOG(ERROR) << "Unable to overwrite an already loaded module " << path;
    return false;
  }
  if (path.extension() == ".so") {
    auto module = std::make_unique<SharedLibraryModule>();
    bool loaded = module->Load(path);
    if (!loaded) return false;
    modules_[module_name] = std::move(module);
  } else if (path.extension() == ".py") {
    auto module = std::make_unique<PythonModule>();
    bool loaded = module->Load(path);
    if (!loaded) return false;
    modules_[module_name] = std::move(module);
  } else {
    LOG(ERROR) << "Unknown query module file " << path;
    return false;
  }
  return true;
}

ModulePtr ModuleRegistry::GetModuleNamed(const std::string_view &name) const {
  std::shared_lock<utils::RWLock> guard(lock_);
  auto found_it = modules_.find(name);
  if (found_it == modules_.end()) return nullptr;
  return ModulePtr(found_it->second.get(), std::move(guard));
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
  LOG(INFO) << "Reloading module '" << name << "' ...";
  if (!module->Reload()) {
    modules_.erase(found_it);
    return false;
  }
  return true;
}

bool ModuleRegistry::ReloadAllModules() {
  std::unique_lock<utils::RWLock> guard(lock_);
  for (auto &[name, module] : modules_) {
    LOG(INFO) << "Reloading module '" << name << "' ...";
    if (!module->Reload()) {
      modules_.erase(name);
      return false;
    }
  }
  return true;
}

void ModuleRegistry::UnloadAllModules() {
  std::unique_lock<utils::RWLock> guard(lock_);
  // This is correct because the destructor will close each module.
  modules_.clear();
}

std::optional<std::pair<procedure::ModulePtr, const mgp_proc *>> FindProcedure(
    const ModuleRegistry &module_registry,
    const std::string_view &fully_qualified_procedure_name,
    utils::MemoryResource *memory) {
  utils::pmr::vector<std::string_view> name_parts(memory);
  utils::Split(&name_parts, fully_qualified_procedure_name, ".");
  if (name_parts.size() == 1U) return std::nullopt;
  auto last_dot_pos = fully_qualified_procedure_name.find_last_of('.');
  CHECK(last_dot_pos != std::string_view::npos);
  const auto &module_name =
      fully_qualified_procedure_name.substr(0, last_dot_pos);
  const auto &proc_name = name_parts.back();
  auto module = module_registry.GetModuleNamed(module_name);
  if (!module) return std::nullopt;
  const auto procedures = module->Procedures();
  const auto &proc_it = procedures->find(proc_name);
  if (proc_it == procedures->end()) return std::nullopt;
  return std::make_pair(std::move(module), &proc_it->second);
}

}  // namespace query::procedure
