// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/procedure/module.hpp"

#include <filesystem>
#include <optional>

extern "C" {
#include <dlfcn.h>
}

#include <fmt/format.h>
#include <unistd.h>

#include "py/py.hpp"
#include "query/procedure/mg_procedure_helpers.hpp"
#include "query/procedure/py_module.hpp"
#include "utils/file.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/message.hpp"
#include "utils/pmr/vector.hpp"
#include "utils/string.hpp"

namespace memgraph::query::procedure {

constexpr const char *func_code =
    "import ast\n\n"
    "no_removals = ['collections', 'abc', 'sys', 'torch', 'torch_geometric', 'igraph']\n"
    "modules = set()\n\n"
    "def visit_Import(node):\n"
    "  for name in node.names:\n"
    "    mod_name = name.name.split('.')[0]\n"
    "    if mod_name not in no_removals:\n"
    "      modules.add(mod_name)\n\n"
    "def visit_ImportFrom(node):\n"
    "  if node.module is not None and node.level == 0:\n"
    "    mod_name = node.module.split('.')[0]\n"
    "    if mod_name not in no_removals:\n"
    "      modules.add(mod_name)\n"
    "node_iter = ast.NodeVisitor()\n"
    "node_iter.visit_Import = visit_Import\n"
    "node_iter.visit_ImportFrom = visit_ImportFrom\n"
    "node_iter.visit(ast.parse(code))\n";

void ProcessFileDependencies(std::filesystem::path file_path_, const char *module_path, const char *func_code,
                             PyObject *sys_mod_ref);

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

  const std::map<std::string, mgp_func, std::less<>> *Functions() const override;

  void AddProcedure(std::string_view name, mgp_proc proc);

  void AddTransformation(std::string_view name, mgp_trans trans);

  std::optional<std::filesystem::path> Path() const override { return std::nullopt; }

 private:
  /// Registered procedures
  std::map<std::string, mgp_proc, std::less<>> procedures_;
  std::map<std::string, mgp_trans, std::less<>> transformations_;
  std::map<std::string, mgp_func, std::less<>> functions_;
};

BuiltinModule::BuiltinModule() {}

BuiltinModule::~BuiltinModule() {}

bool BuiltinModule::Close() { return true; }

const std::map<std::string, mgp_proc, std::less<>> *BuiltinModule::Procedures() const { return &procedures_; }

const std::map<std::string, mgp_trans, std::less<>> *BuiltinModule::Transformations() const {
  return &transformations_;
}
const std::map<std::string, mgp_func, std::less<>> *BuiltinModule::Functions() const { return &functions_; }

void BuiltinModule::AddProcedure(std::string_view name, mgp_proc proc) { procedures_.emplace(name, std::move(proc)); }

void BuiltinModule::AddTransformation(std::string_view name, mgp_trans trans) {
  transformations_.emplace(name, std::move(trans));
}

namespace {

auto WithUpgradedLock(auto *lock, const auto &function) {
  lock->unlock_shared();
  utils::OnScopeExit shared_lock{[&] { lock->lock_shared(); }};
  function();
};

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
  auto load_all_cb = [module_registry, lock](mgp_list * /*args*/, mgp_graph * /*graph*/, mgp_result * /*result*/,
                                             mgp_memory * /*memory*/) {
    WithUpgradedLock(lock, [&]() { module_registry->UnloadAndLoadModulesFromDirectories(); });
  };
  mgp_proc load_all("load_all", load_all_cb, utils::NewDeleteResource());
  module->AddProcedure("load_all", std::move(load_all));
  auto load_cb = [module_registry, lock](mgp_list *args, mgp_graph * /*graph*/, mgp_result *result,
                                         mgp_memory * /*memory*/) {
    MG_ASSERT(Call<size_t>(mgp_list_size, args) == 1U, "Should have been type checked already");
    auto *arg = Call<mgp_value *>(mgp_list_at, args, 0);
    MG_ASSERT(CallBool(mgp_value_is_string, arg), "Should have been type checked already");
    bool succ = false;
    WithUpgradedLock(lock, [&]() {
      const char *arg_as_string{nullptr};
      if (const auto err = mgp_value_get_string(arg, &arg_as_string); err != mgp_error::MGP_ERROR_NO_ERROR) {
        succ = false;
      } else {
        succ = module_registry->LoadOrReloadModuleFromName(arg_as_string);
      }
    });
    if (!succ) {
      MG_ASSERT(mgp_result_set_error_msg(result, "Failed to (re)load the module.") == mgp_error::MGP_ERROR_NO_ERROR);
    }
  };
  mgp_proc load("load", load_cb, utils::NewDeleteResource());
  MG_ASSERT(mgp_proc_add_arg(&load, "module_name", Call<mgp_type *>(mgp_type_string)) == mgp_error::MGP_ERROR_NO_ERROR);
  module->AddProcedure("load", std::move(load));
}

namespace {
[[nodiscard]] bool IsFileEditable(const std::optional<std::filesystem::path> &path) {
  return path && access(path->c_str(), W_OK) == 0;
}

std::string GetPathString(const std::optional<std::filesystem::path> &path) {
  if (!path) {
    return "builtin";
  }

  return std::filesystem::canonical(*path).generic_string();
}
}  // namespace

void RegisterMgProcedures(
    // We expect modules to be sorted by name.
    const std::map<std::string, std::unique_ptr<Module>, std::less<>> *all_modules, BuiltinModule *module) {
  auto procedures_cb = [all_modules](mgp_list * /*args*/, mgp_graph * /*graph*/, mgp_result *result,
                                     mgp_memory *memory) {
    // Iterating over all_modules assumes that the standard mechanism of custom
    // procedure invocations takes the ModuleRegistry::lock_ with READ access.
    // For details on how the invocation is done, take a look at the
    // CallProcedureCursor::Pull implementation.
    for (const auto &[module_name, module] : *all_modules) {
      // Return the results in sorted order by module and by procedure.
      static_assert(
          std::is_same_v<decltype(module->Procedures()), const std::map<std::string, mgp_proc, std::less<>> *>,
          "Expected module procedures to be sorted by name");

      const auto path = module->Path();
      const auto path_string = GetPathString(path);
      const auto is_editable = IsFileEditable(path);

      for (const auto &[proc_name, proc] : *module->Procedures()) {
        mgp_result_record *record{nullptr};
        if (!TryOrSetError([&] { return mgp_result_new_record(result, &record); }, result)) {
          return;
        }

        const auto path_value = GetStringValueOrSetError(path_string.c_str(), memory, result);
        if (!path_value) {
          return;
        }

        MgpUniquePtr<mgp_value> is_editable_value{nullptr, mgp_value_destroy};
        if (!TryOrSetError([&] { return CreateMgpObject(is_editable_value, mgp_value_make_bool, is_editable, memory); },
                           result)) {
          return;
        }

        utils::pmr::string full_name(module_name, memory->impl);
        full_name.append(1, '.');
        full_name.append(proc_name);
        const auto name_value = GetStringValueOrSetError(full_name.c_str(), memory, result);
        if (!name_value) {
          return;
        }

        std::stringstream ss;
        ss << module_name << ".";
        PrintProcSignature(proc, &ss);
        const auto signature = ss.str();
        const auto signature_value = GetStringValueOrSetError(signature.c_str(), memory, result);
        if (!signature_value) {
          return;
        }

        MgpUniquePtr<mgp_value> is_write_value{nullptr, mgp_value_destroy};
        if (!TryOrSetError(
                [&, &proc = proc] {
                  return CreateMgpObject(is_write_value, mgp_value_make_bool, proc.info.is_write ? 1 : 0, memory);
                },
                result)) {
          return;
        }

        if (!InsertResultOrSetError(result, record, "name", name_value.get())) {
          return;
        }

        if (!InsertResultOrSetError(result, record, "signature", signature_value.get())) {
          return;
        }

        if (!InsertResultOrSetError(result, record, "is_write", is_write_value.get())) {
          return;
        }

        if (!InsertResultOrSetError(result, record, "path", path_value.get())) {
          return;
        }

        if (!InsertResultOrSetError(result, record, "is_editable", is_editable_value.get())) {
          return;
        }
      }
    }
  };
  mgp_proc procedures("procedures", procedures_cb, utils::NewDeleteResource());
  MG_ASSERT(mgp_proc_add_result(&procedures, "name", Call<mgp_type *>(mgp_type_string)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  MG_ASSERT(mgp_proc_add_result(&procedures, "signature", Call<mgp_type *>(mgp_type_string)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  MG_ASSERT(mgp_proc_add_result(&procedures, "is_write", Call<mgp_type *>(mgp_type_bool)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  MG_ASSERT(mgp_proc_add_result(&procedures, "path", Call<mgp_type *>(mgp_type_string)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  MG_ASSERT(mgp_proc_add_result(&procedures, "is_editable", Call<mgp_type *>(mgp_type_bool)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  module->AddProcedure("procedures", std::move(procedures));
}

void RegisterMgTransformations(const std::map<std::string, std::unique_ptr<Module>, std::less<>> *all_modules,
                               BuiltinModule *module) {
  auto transformations_cb = [all_modules](mgp_list * /*unused*/, mgp_graph * /*unused*/, mgp_result *result,
                                          mgp_memory *memory) {
    for (const auto &[module_name, module] : *all_modules) {
      // Return the results in sorted order by module and by transformation.
      static_assert(
          std::is_same_v<decltype(module->Transformations()), const std::map<std::string, mgp_trans, std::less<>> *>,
          "Expected module transformations to be sorted by name");

      const auto path = module->Path();
      const auto path_string = GetPathString(path);
      const auto is_editable = IsFileEditable(path);

      for (const auto &[trans_name, proc] : *module->Transformations()) {
        mgp_result_record *record{nullptr};
        if (!TryOrSetError([&] { return mgp_result_new_record(result, &record); }, result)) {
          return;
        }

        const auto path_value = GetStringValueOrSetError(path_string.c_str(), memory, result);
        if (!path_value) {
          return;
        }

        MgpUniquePtr<mgp_value> is_editable_value{nullptr, mgp_value_destroy};
        if (!TryOrSetError([&] { return CreateMgpObject(is_editable_value, mgp_value_make_bool, is_editable, memory); },
                           result)) {
          return;
        }

        utils::pmr::string full_name(module_name, memory->impl);
        full_name.append(1, '.');
        full_name.append(trans_name);

        const auto name_value = GetStringValueOrSetError(full_name.c_str(), memory, result);
        if (!name_value) {
          return;
        }

        if (!InsertResultOrSetError(result, record, "name", name_value.get())) {
          return;
        }

        if (!InsertResultOrSetError(result, record, "path", path_value.get())) {
          return;
        }

        if (!InsertResultOrSetError(result, record, "is_editable", is_editable_value.get())) {
          return;
        }
      }
    }
  };
  mgp_proc procedures("transformations", transformations_cb, utils::NewDeleteResource());
  MG_ASSERT(mgp_proc_add_result(&procedures, "name", Call<mgp_type *>(mgp_type_string)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  MG_ASSERT(mgp_proc_add_result(&procedures, "path", Call<mgp_type *>(mgp_type_string)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  MG_ASSERT(mgp_proc_add_result(&procedures, "is_editable", Call<mgp_type *>(mgp_type_bool)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  module->AddProcedure("transformations", std::move(procedures));
}

void RegisterMgFunctions(
    // We expect modules to be sorted by name.
    const std::map<std::string, std::unique_ptr<Module>, std::less<>> *all_modules, BuiltinModule *module) {
  auto functions_cb = [all_modules](mgp_list * /*args*/, mgp_graph * /*graph*/, mgp_result *result,
                                    mgp_memory *memory) {
    // Iterating over all_modules assumes that the standard mechanism of magic
    // functions invocations takes the ModuleRegistry::lock_ with READ access.
    for (const auto &[module_name, module] : *all_modules) {
      // Return the results in sorted order by module and by function_name.
      static_assert(std::is_same_v<decltype(module->Functions()), const std::map<std::string, mgp_func, std::less<>> *>,
                    "Expected module magic functions to be sorted by name");

      const auto path = module->Path();
      const auto path_string = GetPathString(path);
      const auto is_editable = IsFileEditable(path);

      for (const auto &[func_name, func] : *module->Functions()) {
        mgp_result_record *record{nullptr};

        if (!TryOrSetError([&] { return mgp_result_new_record(result, &record); }, result)) {
          return;
        }

        const auto path_value = GetStringValueOrSetError(path_string.c_str(), memory, result);
        if (!path_value) {
          return;
        }

        MgpUniquePtr<mgp_value> is_editable_value{nullptr, mgp_value_destroy};
        if (!TryOrSetError([&] { return CreateMgpObject(is_editable_value, mgp_value_make_bool, is_editable, memory); },
                           result)) {
          return;
        }

        utils::pmr::string full_name(module_name, memory->impl);
        full_name.append(1, '.');
        full_name.append(func_name);
        const auto name_value = GetStringValueOrSetError(full_name.c_str(), memory, result);
        if (!name_value) {
          return;
        }

        std::stringstream ss;
        ss << module_name << ".";
        PrintFuncSignature(func, ss);
        const auto signature = ss.str();
        const auto signature_value = GetStringValueOrSetError(signature.c_str(), memory, result);
        if (!signature_value) {
          return;
        }

        if (!InsertResultOrSetError(result, record, "name", name_value.get())) {
          return;
        }

        if (!InsertResultOrSetError(result, record, "signature", signature_value.get())) {
          return;
        }

        if (!InsertResultOrSetError(result, record, "path", path_value.get())) {
          return;
        }

        if (!InsertResultOrSetError(result, record, "is_editable", is_editable_value.get())) {
          return;
        }
      }
    }
  };
  mgp_proc functions("functions", functions_cb, utils::NewDeleteResource());
  MG_ASSERT(mgp_proc_add_result(&functions, "name", Call<mgp_type *>(mgp_type_string)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  MG_ASSERT(mgp_proc_add_result(&functions, "signature", Call<mgp_type *>(mgp_type_string)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  MG_ASSERT(mgp_proc_add_result(&functions, "path", Call<mgp_type *>(mgp_type_string)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  MG_ASSERT(mgp_proc_add_result(&functions, "is_editable", Call<mgp_type *>(mgp_type_bool)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  module->AddProcedure("functions", std::move(functions));
}
namespace {
bool IsAllowedExtension(const auto &extension) {
  static constexpr std::array<std::string_view, 1> allowed_extensions{".py"};
  return std::any_of(allowed_extensions.begin(), allowed_extensions.end(),
                     [&](const auto allowed_extension) { return allowed_extension == extension; });
}

bool IsSubPath(const auto &base, const auto &destination) {
  const auto relative = std::filesystem::relative(destination, base);
  return !relative.empty() && *relative.begin() != "..";
}

std::optional<std::string> ReadFile(const auto &path) {
  std::ifstream file(path);
  if (!file.is_open()) {
    return std::nullopt;
  }

  const auto size = std::filesystem::file_size(path);
  std::string content(size, '\0');
  file.read(content.data(), static_cast<std::streamsize>(size));
  return std::move(content);
}

// Return the module directory that contains the `path`
utils::BasicResult<const char *, std::filesystem::path> ParentModuleDirectory(const ModuleRegistry &module_registry,
                                                                              const std::filesystem::path &path) {
  const auto &module_directories = module_registry.GetModulesDirectory();

  auto longest_parent_directory = module_directories.end();
  auto max_length = std::numeric_limits<uint64_t>::min();
  for (auto it = module_directories.begin(); it != module_directories.end(); ++it) {
    if (IsSubPath(*it, path)) {
      const auto length = std::filesystem::canonical(*it).string().size();
      if (length > max_length) {
        longest_parent_directory = it;
        max_length = length;
      }
    }
  }

  if (longest_parent_directory == module_directories.end()) {
    return "The specified file isn't contained in any of the module directories.";
  }

  return *longest_parent_directory;
}
}  // namespace

void RegisterMgGetModuleFiles(ModuleRegistry *module_registry, BuiltinModule *module) {
  auto get_module_files_cb = [module_registry](mgp_list * /*args*/, mgp_graph * /*unused*/, mgp_result *result,
                                               mgp_memory *memory) {
    for (const auto &module_directory : module_registry->GetModulesDirectory()) {
      for (const auto &dir_entry : std::filesystem::recursive_directory_iterator(module_directory)) {
        if (dir_entry.is_regular_file() && IsAllowedExtension(dir_entry.path().extension())) {
          mgp_result_record *record{nullptr};
          if (!TryOrSetError([&] { return mgp_result_new_record(result, &record); }, result)) {
            return;
          }

          const auto path_string = GetPathString(dir_entry);
          const auto is_editable = IsFileEditable(dir_entry);

          const auto path_value = GetStringValueOrSetError(path_string.c_str(), memory, result);
          if (!path_value) {
            return;
          }

          MgpUniquePtr<mgp_value> is_editable_value{nullptr, mgp_value_destroy};
          if (!TryOrSetError(
                  [&] { return CreateMgpObject(is_editable_value, mgp_value_make_bool, is_editable, memory); },
                  result)) {
            return;
          }

          if (!InsertResultOrSetError(result, record, "path", path_value.get())) {
            return;
          }

          if (!InsertResultOrSetError(result, record, "is_editable", is_editable_value.get())) {
            return;
          }
        }
      }
    }
  };

  mgp_proc get_module_files("get_module_files", get_module_files_cb, utils::NewDeleteResource(),
                            {.required_privilege = AuthQuery::Privilege::MODULE_READ});
  MG_ASSERT(mgp_proc_add_result(&get_module_files, "path", Call<mgp_type *>(mgp_type_string)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  MG_ASSERT(mgp_proc_add_result(&get_module_files, "is_editable", Call<mgp_type *>(mgp_type_bool)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  module->AddProcedure("get_module_files", std::move(get_module_files));
}

void RegisterMgGetModuleFile(ModuleRegistry *module_registry, BuiltinModule *module) {
  auto get_module_file_cb = [module_registry](mgp_list *args, mgp_graph * /*unused*/, mgp_result *result,
                                              mgp_memory *memory) {
    MG_ASSERT(Call<size_t>(mgp_list_size, args) == 1U, "Should have been type checked already");
    auto *arg = Call<mgp_value *>(mgp_list_at, args, 0);
    MG_ASSERT(CallBool(mgp_value_is_string, arg), "Should have been type checked already");
    const char *path_str{nullptr};
    if (!TryOrSetError([&] { return mgp_value_get_string(arg, &path_str); }, result)) {
      return;
    }

    const std::filesystem::path path{path_str};

    if (!path.is_absolute()) {
      static_cast<void>(mgp_result_set_error_msg(result, "The path should be an absolute path."));
      return;
    }

    if (!IsAllowedExtension(path.extension())) {
      static_cast<void>(mgp_result_set_error_msg(result, "The specified file isn't in the supported format."));
      return;
    }

    if (!std::filesystem::exists(path)) {
      static_cast<void>(mgp_result_set_error_msg(result, "The specified file doesn't exist."));
      return;
    }

    if (auto maybe_error_msg = ParentModuleDirectory(*module_registry, path); maybe_error_msg.HasError()) {
      static_cast<void>(mgp_result_set_error_msg(result, maybe_error_msg.GetError()));
      return;
    }

    const auto maybe_content = ReadFile(path);
    if (!maybe_content) {
      static_cast<void>(mgp_result_set_error_msg(result, "Couldn't read the content of the file."));
      return;
    }

    mgp_result_record *record{nullptr};
    if (!TryOrSetError([&] { return mgp_result_new_record(result, &record); }, result)) {
      return;
    }

    const auto content_value = GetStringValueOrSetError(maybe_content->c_str(), memory, result);
    if (!content_value) {
      return;
    }

    if (!InsertResultOrSetError(result, record, "content", content_value.get())) {
      return;
    }
  };
  mgp_proc get_module_file("get_module_file", std::move(get_module_file_cb), utils::NewDeleteResource(),
                           {.required_privilege = AuthQuery::Privilege::MODULE_READ});
  MG_ASSERT(mgp_proc_add_arg(&get_module_file, "path", Call<mgp_type *>(mgp_type_string)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  MG_ASSERT(mgp_proc_add_result(&get_module_file, "content", Call<mgp_type *>(mgp_type_string)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  module->AddProcedure("get_module_file", std::move(get_module_file));
}

namespace {
utils::BasicResult<std::string> WriteToFile(const std::filesystem::path &file, const std::string_view content) {
  std::ofstream output_file{file};
  if (!output_file.is_open()) {
    return fmt::format("Failed to open the file at location {}", file);
  }
  output_file.write(content.data(), static_cast<std::streamsize>(content.size()));
  output_file.flush();
  return {};
}
}  // namespace

void RegisterMgCreateModuleFile(ModuleRegistry *module_registry, utils::RWLock *lock, BuiltinModule *module) {
  auto create_module_file_cb = [module_registry, lock](mgp_list *args, mgp_graph * /*unused*/, mgp_result *result,
                                                       mgp_memory *memory) {
    MG_ASSERT(Call<size_t>(mgp_list_size, args) == 2U, "Should have been type checked already");
    auto *filename_arg = Call<mgp_value *>(mgp_list_at, args, 0);
    MG_ASSERT(CallBool(mgp_value_is_string, filename_arg), "Should have been type checked already");
    const char *filename_str{nullptr};
    if (!TryOrSetError([&] { return mgp_value_get_string(filename_arg, &filename_str); }, result)) {
      return;
    }

    const auto file_path = module_registry->InternalModuleDir() / filename_str;

    if (!IsSubPath(module_registry->InternalModuleDir(), file_path)) {
      static_cast<void>(mgp_result_set_error_msg(
          result,
          "Invalid relative path defined. The module file cannot be define outside the internal modules directory."));
      return;
    }

    if (!IsAllowedExtension(file_path.extension())) {
      static_cast<void>(mgp_result_set_error_msg(result, "The specified file isn't in the supported format."));
      return;
    }

    if (std::filesystem::exists(file_path)) {
      static_cast<void>(mgp_result_set_error_msg(result, "File with the same name already exists!"));
      return;
    }

    utils::EnsureDir(file_path.parent_path());

    auto *content_arg = Call<mgp_value *>(mgp_list_at, args, 1);
    MG_ASSERT(CallBool(mgp_value_is_string, content_arg), "Should have been type checked already");
    const char *content_str{nullptr};
    if (!TryOrSetError([&] { return mgp_value_get_string(content_arg, &content_str); }, result)) {
      return;
    }

    if (auto maybe_error = WriteToFile(file_path, {content_str, std::strlen(content_str)}); maybe_error.HasError()) {
      static_cast<void>(mgp_result_set_error_msg(result, maybe_error.GetError().c_str()));
      return;
    }

    mgp_result_record *record{nullptr};
    if (!TryOrSetError([&] { return mgp_result_new_record(result, &record); }, result)) {
      return;
    }

    const auto path_value = GetStringValueOrSetError(std::filesystem::canonical(file_path).c_str(), memory, result);
    if (!path_value) {
      return;
    }

    if (!InsertResultOrSetError(result, record, "path", path_value.get())) {
      return;
    }

    WithUpgradedLock(lock, [&]() { module_registry->UnloadAndLoadModulesFromDirectories(); });
  };
  mgp_proc create_module_file("create_module_file", std::move(create_module_file_cb), utils::NewDeleteResource(),
                              {.required_privilege = AuthQuery::Privilege::MODULE_WRITE});
  MG_ASSERT(mgp_proc_add_arg(&create_module_file, "filename", Call<mgp_type *>(mgp_type_string)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  MG_ASSERT(mgp_proc_add_arg(&create_module_file, "content", Call<mgp_type *>(mgp_type_string)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  MG_ASSERT(mgp_proc_add_result(&create_module_file, "path", Call<mgp_type *>(mgp_type_string)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  module->AddProcedure("create_module_file", std::move(create_module_file));
}

void RegisterMgUpdateModuleFile(ModuleRegistry *module_registry, utils::RWLock *lock, BuiltinModule *module) {
  auto update_module_file_cb = [module_registry, lock](mgp_list *args, mgp_graph * /*unused*/, mgp_result *result,
                                                       mgp_memory * /*memory*/) {
    MG_ASSERT(Call<size_t>(mgp_list_size, args) == 2U, "Should have been type checked already");
    auto *path_arg = Call<mgp_value *>(mgp_list_at, args, 0);
    MG_ASSERT(CallBool(mgp_value_is_string, path_arg), "Should have been type checked already");
    const char *path_str{nullptr};
    if (!TryOrSetError([&] { return mgp_value_get_string(path_arg, &path_str); }, result)) {
      return;
    }

    const std::filesystem::path path{path_str};

    if (!path.is_absolute()) {
      static_cast<void>(mgp_result_set_error_msg(result, "The path should be an absolute path."));
      return;
    }

    if (!IsAllowedExtension(path.extension())) {
      static_cast<void>(mgp_result_set_error_msg(result, "The specified file isn't in the supported format."));
      return;
    }

    if (!std::filesystem::exists(path)) {
      static_cast<void>(mgp_result_set_error_msg(result, "The specified file doesn't exist."));
      return;
    }

    if (auto maybe_error_msg = ParentModuleDirectory(*module_registry, path); maybe_error_msg.HasError()) {
      static_cast<void>(mgp_result_set_error_msg(result, maybe_error_msg.GetError()));
      return;
    }

    auto *content_arg = Call<mgp_value *>(mgp_list_at, args, 1);
    MG_ASSERT(CallBool(mgp_value_is_string, content_arg), "Should have been type checked already");
    const char *content_str{nullptr};
    if (!TryOrSetError([&] { return mgp_value_get_string(content_arg, &content_str); }, result)) {
      return;
    }

    if (auto maybe_error = WriteToFile(path, {content_str, std::strlen(content_str)}); maybe_error.HasError()) {
      static_cast<void>(mgp_result_set_error_msg(result, maybe_error.GetError().c_str()));
      return;
    }

    WithUpgradedLock(lock, [&]() { module_registry->UnloadAndLoadModulesFromDirectories(); });
  };
  mgp_proc update_module_file("update_module_file", std::move(update_module_file_cb), utils::NewDeleteResource(),
                              {.required_privilege = AuthQuery::Privilege::MODULE_WRITE});
  MG_ASSERT(mgp_proc_add_arg(&update_module_file, "path", Call<mgp_type *>(mgp_type_string)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  MG_ASSERT(mgp_proc_add_arg(&update_module_file, "content", Call<mgp_type *>(mgp_type_string)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  module->AddProcedure("update_module_file", std::move(update_module_file));
}

void RegisterMgDeleteModuleFile(ModuleRegistry *module_registry, utils::RWLock *lock, BuiltinModule *module) {
  auto delete_module_file_cb = [module_registry, lock](mgp_list *args, mgp_graph * /*unused*/, mgp_result *result,
                                                       mgp_memory * /*memory*/) {
    MG_ASSERT(Call<size_t>(mgp_list_size, args) == 1U, "Should have been type checked already");
    auto *path_arg = Call<mgp_value *>(mgp_list_at, args, 0);
    MG_ASSERT(CallBool(mgp_value_is_string, path_arg), "Should have been type checked already");
    const char *path_str{nullptr};
    if (!TryOrSetError([&] { return mgp_value_get_string(path_arg, &path_str); }, result)) {
      return;
    }

    const std::filesystem::path path{path_str};

    if (!path.is_absolute()) {
      static_cast<void>(mgp_result_set_error_msg(result, "The path should be an absolute path."));
      return;
    }

    if (!IsAllowedExtension(path.extension())) {
      static_cast<void>(mgp_result_set_error_msg(result, "The specified file isn't in the supported format."));
      return;
    }

    if (!std::filesystem::exists(path)) {
      static_cast<void>(mgp_result_set_error_msg(result, "The specified file doesn't exist."));
      return;
    }

    const auto parent_module_directory = ParentModuleDirectory(*module_registry, path);
    if (parent_module_directory.HasError()) {
      static_cast<void>(mgp_result_set_error_msg(result, parent_module_directory.GetError()));
      return;
    }

    std::error_code ec;
    if (!std::filesystem::remove(path, ec)) {
      static_cast<void>(
          mgp_result_set_error_msg(result, fmt::format("Failed to delete the module: {}", ec.message()).c_str()));
      return;
    }

    auto parent_path = path.parent_path();
    while (!std::filesystem::is_symlink(parent_path) && std::filesystem::is_empty(parent_path) &&
           !std::filesystem::equivalent(*parent_module_directory, parent_path)) {
      std::filesystem::remove(parent_path);
      parent_path = parent_path.parent_path();
    }

    WithUpgradedLock(lock, [&]() { module_registry->UnloadAndLoadModulesFromDirectories(); });
  };
  mgp_proc delete_module_file("delete_module_file", std::move(delete_module_file_cb), utils::NewDeleteResource(),
                              {.required_privilege = AuthQuery::Privilege::MODULE_WRITE});
  MG_ASSERT(mgp_proc_add_arg(&delete_module_file, "path", Call<mgp_type *>(mgp_type_string)) ==
            mgp_error::MGP_ERROR_NO_ERROR);
  module->AddProcedure("delete_module_file", std::move(delete_module_file));
}

// Run `fun` with `mgp_module *` and `mgp_memory *` arguments. If `fun` returned
// a `true` value, store the `mgp_module::procedures` and
// `mgp_module::transformations into `proc_map`. The return value of WithModuleRegistration
// is the same as that of `fun`. Note, the return value need only be convertible to `bool`,
// it does not have to be `bool` itself.
template <class TProcMap, class TTransMap, class TFuncMap, class TFun>
auto WithModuleRegistration(TProcMap *proc_map, TTransMap *trans_map, TFuncMap *func_map, const TFun &fun) {
  // We probably don't need more than 256KB for module initialization.
  static constexpr size_t stack_bytes = 256UL * 1024UL;
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
    // Copy functions into resulting func_map.
    for (const auto &func : module_def.functions) func_map->emplace(func);
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

  const std::map<std::string, mgp_func, std::less<>> *Functions() const override;

  std::optional<std::filesystem::path> Path() const override { return file_path_; }

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
  /// Registered functions
  std::map<std::string, mgp_func, std::less<>> functions_;
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
  // NOLINTNEXTLINE(hicpp-signed-bitwise)
  handle_ = dlopen(file_path.c_str(), RTLD_NOW | RTLD_LOCAL | RTLD_DEEPBIND);
  if (!handle_) {
    spdlog::error(
        utils::MessageWithLink("Unable to load module {}; {}.", file_path, dlerror(), "https://memgr.ph/modules"));
    return false;
  }
  // Get required mgp_init_module
  init_fn_ = reinterpret_cast<int (*)(mgp_module *, mgp_memory *)>(dlsym(handle_, "mgp_init_module"));
  char *dl_errored = dlerror();
  if (!init_fn_ || dl_errored) {
    spdlog::error(
        utils::MessageWithLink("Unable to load module {}; {}.", file_path, dl_errored, "https://memgr.ph/modules"));
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
      const bool success = mgp_error::MGP_ERROR_NO_ERROR == MgpTransAddFixedResult(&trans.second);
      if (!success) {
        const auto error =
            fmt::format("Unable to add result to transformation in module {}; add result failed", file_path);
        return with_error(error);
      }
    }
    return true;
  };
  if (!WithModuleRegistration(&procedures_, &transformations_, &functions_, module_cb)) {
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
  // nonexistent shutdown function is semantically the same as a shutdown
  // function that does nothing.
  int shutdown_res = 0;
  if (shutdown_fn_) shutdown_res = shutdown_fn_();
  if (shutdown_res != 0) {
    spdlog::warn("When closing module {}; mgp_shutdown_module returned {}", file_path_, shutdown_res);
  }
  if (dlclose(handle_) != 0) {
    spdlog::error(
        utils::MessageWithLink("Failed to close module {}; {}.", file_path_, dlerror(), "https://memgr.ph/modules"));
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

const std::map<std::string, mgp_func, std::less<>> *SharedLibraryModule::Functions() const {
  MG_ASSERT(handle_,
            "Attempting to access functions of a module that has not "
            "been loaded...");
  return &functions_;
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
  const std::map<std::string, mgp_func, std::less<>> *Functions() const override;
  std::optional<std::filesystem::path> Path() const override { return file_path_; }

 private:
  std::filesystem::path file_path_;
  py::Object py_module_;
  std::map<std::string, mgp_proc, std::less<>> procedures_;
  std::map<std::string, mgp_trans, std::less<>> transformations_;
  std::map<std::string, mgp_func, std::less<>> functions_;
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
    spdlog::error(
        utils::MessageWithLink("Unable to load module {}; {}.", file_path, *maybe_exc, "https://memgr.ph/modules"));
    return false;
  }
  bool succ = true;
  auto module_cb = [&](auto *module_def, auto * /*memory*/) {
    auto result = ImportPyModule(file_path.stem().c_str(), module_def);
    for (auto &trans : module_def->transformations) {
      succ = MgpTransAddFixedResult(&trans.second) == mgp_error::MGP_ERROR_NO_ERROR;
      if (!succ) {
        return result;
      }
    };
    return result;
  };
  py_module_ = WithModuleRegistration(&procedures_, &transformations_, &functions_, module_cb);
  if (py_module_) {
    spdlog::info("Loaded module {}", file_path);

    if (!succ) {
      spdlog::error("Unable to add result to transformation");
      return false;
    }
    return true;
  }
  auto exc_info = py::FetchError().value();
  spdlog::error(
      utils::MessageWithLink("Unable to load module {}; {}.", file_path, exc_info, "https://memgr.ph/modules"));
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
  functions_.clear();

  // Get the reference to sys.modules dictionary
  py::Object sys(PyImport_ImportModule("sys"));
  PyObject *sys_mod_ref = sys.GetAttr("modules").Ptr();

  std::string stem = file_path_.stem().string();

  ProcessFileDependencies(file_path_, file_path_.stem().c_str(), func_code, sys_mod_ref);

  std::vector<std::filesystem::path> submodules;

  for (auto it = std::filesystem::recursive_directory_iterator(file_path_.parent_path());
       it != std::filesystem::recursive_directory_iterator(); ++it) {
    std::string dir_entry_stem = it->path().stem().string();
    if (it->is_regular_file() || dir_entry_stem == "__pycache__") continue;
    if (dir_entry_stem.find(stem) != std::string_view::npos) {
      it.disable_recursion_pending();
      submodules.emplace_back(it->path());
    }
  }

  for (const auto &submodule : submodules) {
    if (std::filesystem::exists(submodule)) {
      std::filesystem::remove_all(submodule / "__pycache__");
      for (auto const &rec_dir_entry : std::filesystem::recursive_directory_iterator(submodule)) {
        std::string rec_dir_entry_stem = rec_dir_entry.path().stem().string();
        if (rec_dir_entry.is_directory() && rec_dir_entry_stem != "__pycache__") {
          std::filesystem::remove_all(rec_dir_entry.path() / "__pycache__");
        }
        std::string rec_dir_entry_ext = rec_dir_entry.path().extension().string();
        if (!rec_dir_entry.is_regular_file() || rec_dir_entry_ext != ".py") continue;
        ProcessFileDependencies(rec_dir_entry.path().c_str(), file_path_.stem().c_str(), func_code, sys_mod_ref);
      }
    }
  }

  // first throw out of cache file
  if (PyDict_DelItemString(sys_mod_ref, file_path_.stem().c_str()) != 0) {
    spdlog::warn("Failed to remove the module {} from sys.modules", file_path_.stem().c_str());
    py_module_ = py::Object(nullptr);
    return false;
  }

  // Remove the cached bytecode if it's present
  std::filesystem::remove_all(file_path_.parent_path() / "__pycache__");
  py_module_ = py::Object(nullptr);
  spdlog::info("Closed module {}", file_path_);
  return true;
}

void ProcessFileDependencies(std::filesystem::path file_path_, const char *module_path, const char *func_code,
                             PyObject *sys_mod_ref) {
  const auto maybe_content =
      ReadFile(file_path_);  // this is already done at Load so it can somehow be optimized but not sure how yet

  if (maybe_content) {
    const char *content_value = maybe_content->c_str();
    if (content_value) {
      PyObject *py_main = PyImport_ImportModule("__main__");
      PyObject *py_global_dict = PyModule_GetDict(py_main);

      PyDict_SetItemString(py_global_dict, "code", PyUnicode_FromString(content_value));
      PyRun_String(func_code, Py_file_input, py_global_dict, py_global_dict);
      PyObject *py_res = PyDict_GetItemString(py_global_dict, "modules");

      PyObject *iterator = PyObject_GetIter(py_res);
      PyObject *module = nullptr;

      if (iterator != nullptr) {
        while ((module = PyIter_Next(iterator))) {
          const char *module_name = PyUnicode_AsUTF8(module);
          auto module_name_str = std::string(module_name);
          PyObject *sys_iterator = PyObject_GetIter(PyDict_Keys(sys_mod_ref));
          if (sys_iterator == nullptr) {
            spdlog::warn("Cannot get reference to the sys.modules.keys()");
            break;
          }
          PyObject *sys_mod_key = nullptr;
          while ((sys_mod_key = PyIter_Next(sys_iterator))) {
            const char *sys_mod_key_name = PyUnicode_AsUTF8(sys_mod_key);
            auto sys_mod_key_name_str = std::string(sys_mod_key_name);
            if (sys_mod_key_name_str.rfind(module_name_str, 0) == 0 && sys_mod_key_name_str.compare(module_path) != 0) {
              PyDict_DelItemString(sys_mod_ref, sys_mod_key_name);  // don't test output
            }
            Py_DECREF(sys_mod_key);
          }
          Py_DECREF(sys_iterator);
          Py_DECREF(module);
        }
        Py_DECREF(iterator);
      }
    }
  }
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

const std::map<std::string, mgp_func, std::less<>> *PythonModule::Functions() const {
  MG_ASSERT(py_module_,
            "Attempting to access functions of a module that has "
            "not been loaded...");
  return &functions_;
}
namespace {

std::unique_ptr<Module> LoadModuleFromFile(const std::filesystem::path &path) {
  const auto &ext = path.extension();
  if (ext != ".so" && ext != ".py") {
    spdlog::warn(utils::MessageWithLink("Unknown query module file {}.", path, "https://memgr.ph/modules"));
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

bool ModuleRegistry::RegisterModule(const std::string_view name, std::unique_ptr<Module> module) {
  MG_ASSERT(!name.empty(), "Module name cannot be empty");
  MG_ASSERT(module, "Tried to register an invalid module");
  if (modules_.find(name) != modules_.end()) {
    spdlog::error(
        utils::MessageWithLink("Unable to overwrite an already loaded module {}.", name, "https://memgr.ph/modules"));
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
  RegisterMgFunctions(&modules_, module.get());
  RegisterMgLoad(this, &lock_, module.get());
  RegisterMgGetModuleFiles(this, module.get());
  RegisterMgGetModuleFile(this, module.get());
  RegisterMgCreateModuleFile(this, &lock_, module.get());
  RegisterMgUpdateModuleFile(this, &lock_, module.get());
  RegisterMgDeleteModuleFile(this, &lock_, module.get());
  modules_.emplace("mg", std::move(module));
}

void ModuleRegistry::SetModulesDirectory(std::vector<std::filesystem::path> modules_dirs,
                                         const std::filesystem::path &data_directory) {
  internal_module_dir_ = data_directory / "internal_modules";
  utils::EnsureDirOrDie(internal_module_dir_);
  modules_dirs_ = std::move(modules_dirs);
  modules_dirs_.push_back(internal_module_dir_);
}

const std::vector<std::filesystem::path> &ModuleRegistry::GetModulesDirectory() const { return modules_dirs_; }

bool ModuleRegistry::LoadModuleIfFound(const std::filesystem::path &modules_dir, const std::string_view name) {
  if (!utils::DirExists(modules_dir)) {
    spdlog::error(
        utils::MessageWithLink("Module directory {} doesn't exist.", modules_dir, "https://memgr.ph/modules"));
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
    spdlog::error(
        utils::MessageWithLink("Module directory {} doesn't exist.", modules_dir, "https://memgr.ph/modules"));
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

ModulePtr ModuleRegistry::GetModuleNamed(const std::string_view name) const {
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

bool ModuleRegistry::RegisterMgProcedure(const std::string_view name, mgp_proc proc) {
  std::unique_lock<utils::RWLock> guard(lock_);
  if (auto module = modules_.find("mg"); module != modules_.end()) {
    auto *builtin_module = dynamic_cast<BuiltinModule *>(module->second.get());
    builtin_module->AddProcedure(name, std::move(proc));
    return true;
  }
  return false;
}

const std::filesystem::path &ModuleRegistry::InternalModuleDir() const noexcept { return internal_module_dir_; }

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
concept ModuleProperties = utils::SameAsAnyOf<T, mgp_proc, mgp_trans, mgp_func>;

template <ModuleProperties T>
std::optional<std::pair<ModulePtr, const T *>> MakePairIfPropFound(const ModuleRegistry &module_registry,
                                                                   std::string_view fully_qualified_name,
                                                                   utils::MemoryResource *memory) {
  auto prop_fun = [](auto &module) {
    if constexpr (std::is_same_v<T, mgp_proc>) {
      return module->Procedures();
    } else if constexpr (std::is_same_v<T, mgp_trans>) {
      return module->Transformations();
    } else if constexpr (std::is_same_v<T, mgp_func>) {
      return module->Functions();
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

std::optional<std::pair<ModulePtr, const mgp_proc *>> FindProcedure(const ModuleRegistry &module_registry,
                                                                    std::string_view fully_qualified_procedure_name,
                                                                    utils::MemoryResource *memory) {
  return MakePairIfPropFound<mgp_proc>(module_registry, fully_qualified_procedure_name, memory);
}

std::optional<std::pair<ModulePtr, const mgp_trans *>> FindTransformation(
    const ModuleRegistry &module_registry, std::string_view fully_qualified_transformation_name,
    utils::MemoryResource *memory) {
  return MakePairIfPropFound<mgp_trans>(module_registry, fully_qualified_transformation_name, memory);
}

std::optional<std::pair<ModulePtr, const mgp_func *>> FindFunction(const ModuleRegistry &module_registry,
                                                                   std::string_view fully_qualified_function_name,
                                                                   utils::MemoryResource *memory) {
  return MakePairIfPropFound<mgp_func>(module_registry, fully_qualified_function_name, memory);
}

}  // namespace memgraph::query::procedure
