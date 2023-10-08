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

#include <filesystem>
#include <fstream>

#include <gflags/gflags.h>
#include <mgclient.hpp>

#include "utils/file.hpp"
#include "utils/logging.hpp"
#include "utils/timer.hpp"

DEFINE_uint64(bolt_port, 7687, "Bolt port");
DEFINE_uint64(timeout, 120, "Timeout seconds");
DEFINE_bool(multi_db, false, "Run test in multi db environment");

namespace {
auto GetClient() {
  auto client =
      mg::Client::Connect({.host = "127.0.0.1", .port = static_cast<uint16_t>(FLAGS_bolt_port), .use_ssl = false});
  MG_ASSERT(client, "Failed to connect!");

  return client;
}

std::vector<std::filesystem::path> GetModuleFiles(auto &client) {
  MG_ASSERT(client->Execute("CALL mg.get_module_files() YIELD path"));

  const auto result_rows = client->FetchAll();
  MG_ASSERT(result_rows, "Failed to get results");

  std::vector<std::filesystem::path> result;
  result.reserve(result_rows->size());

  for (const auto &row : *result_rows) {
    MG_ASSERT(row.size() == 1, "Invalid result received from mg.get_module_files");
    result.emplace_back(row[0].ValueString());
  }

  return result;
}

bool ModuleFileExists(auto &client, const auto &path) {
  const auto module_files = GetModuleFiles(client);

  return std::any_of(module_files.begin(), module_files.end(),
                     [&](const auto &module_file) { return module_file == path; });
}

void AssertModuleFileExists(auto &client, const auto &path) {
  MG_ASSERT(ModuleFileExists(client, path), "Module file {} is missing", path.string());
}

void AssertModuleFileNotExists(auto &client, const auto &path) {
  MG_ASSERT(!ModuleFileExists(client, path), "Invalid module file {} is present", path.string());
}

bool ProcedureExists(auto &client, const std::string_view procedure_name,
                     std::optional<std::filesystem::path> path = std::nullopt) {
  MG_ASSERT(client->Execute("CALL mg.procedures() YIELD name, path"));

  const auto result_rows = client->FetchAll();
  MG_ASSERT(result_rows, "Failed to get results for mg.procedures()");

  return std::find_if(result_rows->begin(), result_rows->end(), [&, procedure_name](const auto &row) {
           MG_ASSERT(row.size() == 2, "Invalid result received from mg.procedures()");
           if (row[0].ValueString() == procedure_name) {
             if (path) {
               return row[1].ValueString() == std::filesystem::canonical(*path).generic_string();
             }
             return true;
           }
           return false;
         }) != result_rows->end();
}

void AssertProcedureExists(auto &client, const std::string_view procedure_name,
                           std::optional<std::filesystem::path> path = std::nullopt) {
  MG_ASSERT(ProcedureExists(client, procedure_name, path), "Procedure {} is missing", procedure_name);
}

void AssertProcedureNotExists(auto &client, const std::string_view procedure_name) {
  MG_ASSERT(!ProcedureExists(client, procedure_name), "Invalid procedure ('{}') is present", procedure_name);
}

template <typename TException>
void AssertQueryFails(auto &client, const std::string &query, std::optional<std::string> expected_message) {
  spdlog::info("Asserting query '{}' fails", query);
  MG_ASSERT(client->Execute(query));
  try {
    client->FetchAll();
  } catch (const TException &exception) {
    if (expected_message) {
      MG_ASSERT(*expected_message == exception.what(),
                "Exception with a different message was thrown.\n\t\tExpected: {}\n\t\tActual: {}", *expected_message,
                exception.what());
    }
    return;
  }

  LOG_FATAL("Didn't receive expected exception");
}

std::string CreateModuleFileQuery(const std::string_view filename, const std::string_view content) {
  return fmt::format("CALL mg.create_module_file('{}', '{}') YIELD path", filename, content);
}

std::filesystem::path CreateModuleFile(auto &client, const std::string_view filename, const std::string_view content) {
  spdlog::info("Creating module file '{}' with content:\n{}", filename, content);
  MG_ASSERT(client->Execute(CreateModuleFileQuery(filename, content)));

  const auto result_row = client->FetchOne();
  MG_ASSERT(result_row && result_row->size() == 1, "Received invalid result from mg.create_module_file");
  MG_ASSERT(!client->FetchOne().has_value(), "Too many results received from mg.create_module_file");

  return result_row->at(0).ValueString();
}

std::string GetModuleFileQuery(const std::filesystem::path &path) {
  return fmt::format("CALL mg.get_module_file({}) YIELD content", path.string());
}

std::string GetModuleFile(auto &client, const std::filesystem::path &path) {
  spdlog::info("Getting content of module file '{}'", path.string());
  MG_ASSERT(client->Execute(GetModuleFileQuery(path)));

  const auto result_row = client->FetchOne();
  MG_ASSERT(result_row && result_row->size() == 1, "Received invalid result from mg.get_module_file");
  MG_ASSERT(!client->FetchOne().has_value(), "Too many results received from mg.get_module_file");

  return std::string{result_row->at(0).ValueString()};
}

std::string UpdateModuleFileQuery(const std::filesystem::path &path, const std::string_view content) {
  return fmt::format("CALL mg.update_module_file({}, '{}')", path.string(), content);
}

void UpdateModuleFile(auto &client, const std::filesystem::path &path, const std::string_view content) {
  spdlog::info("Updating module file {} with content:\n{}", path.string(), content);
  MG_ASSERT(client->Execute(UpdateModuleFileQuery(path, content)));
  MG_ASSERT(client->FetchAll().has_value());
}

std::string DeleteModuleFileQuery(const std::filesystem::path &path) {
  return fmt::format("CALL mg.delete_module_file({})", path.string());
}

void DeleteModuleFile(auto &client, const std::filesystem::path &path) {
  spdlog::info("Deleting module file {}", path.string());
  MG_ASSERT(client->Execute(DeleteModuleFileQuery(path)));
  MG_ASSERT(client->FetchAll().has_value());
}

inline constexpr std::string_view module_content1 = R"(import mgp

@mgp.read_proc
def simple1(ctx: mgp.ProcCtx) -> mgp.Record(result=bool):
    return mgp.Record(mutable=True))";

inline constexpr std::string_view module_content2 = R"(import mgp

@mgp.read_proc
def simple2(ctx: mgp.ProcCtx) -> mgp.Record(result=bool):
    return mgp.Record(mutable=True))";

}  // namespace

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph E2E Isolation Levels");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memgraph::logging::RedirectToStderr();

  mg::Client::Init();
  auto client = GetClient();

  if (FLAGS_multi_db) {
    client->Execute("CREATE DATABASE clean;");
    client->DiscardAll();
    client->Execute("USE DATABASE clean;");
    client->DiscardAll();
    client->Execute("MATCH (n) DETACH DELETE n;");
    client->DiscardAll();
  }

  AssertQueryFails<mg::ClientException>(client, CreateModuleFileQuery("some.cpp", "some content"),
                                        "mg.create_module_file: The specified file isn't in the supported format.");

  AssertQueryFails<mg::ClientException>(client, CreateModuleFileQuery("../some.cpp", "some content"),
                                        "mg.create_module_file: Invalid relative path defined. The module file cannot "
                                        "be define outside the internal modules directory.");

  AssertProcedureNotExists(client, "some.simple1");
  const auto module_path = CreateModuleFile(client, "some.py", module_content1);
  AssertQueryFails<mg::ClientException>(client, CreateModuleFileQuery("some.py", "some content"),
                                        "mg.create_module_file: File with the same name already exists!");

  AssertProcedureExists(client, "some.simple1", module_path);
  AssertModuleFileExists(client, module_path);
  MG_ASSERT(GetModuleFile(client, module_path) == module_content1,
            "Content received from mg.get_module_file is incorrect");

  AssertQueryFails<mg::ClientException>(client, GetModuleFileQuery("some.py"),
                                        "mg.get_module_file: The path should be an absolute path.");

  AssertQueryFails<mg::ClientException>(client, GetModuleFileQuery(module_path.parent_path() / "some.cpp"),
                                        "mg.get_module_file: The specified file isn't in the supported format.");

  AssertQueryFails<mg::ClientException>(client, GetModuleFileQuery(module_path.parent_path() / "some2.py"),
                                        "mg.get_module_file: The specified file doesn't exist.");

  AssertQueryFails<mg::ClientException>(client, UpdateModuleFileQuery("some.py", "some content"),
                                        "mg.update_module_file: The path should be an absolute path.");

  AssertQueryFails<mg::ClientException>(client,
                                        UpdateModuleFileQuery(module_path.parent_path() / "some.cpp", "some content"),
                                        "mg.update_module_file: The specified file isn't in the supported format.");

  AssertQueryFails<mg::ClientException>(client,
                                        UpdateModuleFileQuery(module_path.parent_path() / "some2.py", "some content"),
                                        "mg.update_module_file: The specified file doesn't exist.");

  UpdateModuleFile(client, module_path, module_content2);
  AssertProcedureNotExists(client, "some.simple1");
  AssertProcedureExists(client, "some.simple2", module_path);
  AssertModuleFileExists(client, module_path);
  MG_ASSERT(GetModuleFile(client, module_path) == module_content2,
            "Content received from mg.get_module_file is incorrect");

  AssertQueryFails<mg::ClientException>(client, DeleteModuleFileQuery("some.py"),
                                        "mg.delete_module_file: The path should be an absolute path.");

  AssertQueryFails<mg::ClientException>(client, DeleteModuleFileQuery(module_path.parent_path() / "some.cpp"),
                                        "mg.delete_module_file: The specified file isn't in the supported format.");

  AssertQueryFails<mg::ClientException>(client, DeleteModuleFileQuery(module_path.parent_path() / "some2.py"),
                                        "mg.delete_module_file: The specified file doesn't exist.");

  DeleteModuleFile(client, module_path);
  AssertProcedureNotExists(client, "some.simple1");
  AssertProcedureNotExists(client, "some.simple2");
  AssertModuleFileNotExists(client, module_path);

  const auto non_module_directory =
      std::filesystem::temp_directory_path() / "module_file_manager_e2e_non_module_directory";
  memgraph::utils::EnsureDirOrDie(non_module_directory);
  const auto non_module_file_path{non_module_directory / "something.py"};

  {
    std::ofstream non_module_file{non_module_file_path};
    MG_ASSERT(non_module_file.is_open(), "Failed to open {} for writing", non_module_file_path.string());
    static constexpr std::string_view content = "import mgp";
    non_module_file.write(content.data(), content.size());
    non_module_file.flush();
  }

  AssertQueryFails<mg::ClientException>(
      client, GetModuleFileQuery(non_module_file_path),
      "mg.get_module_file: The specified file isn't contained in any of the module directories.");

  AssertQueryFails<mg::ClientException>(
      client, UpdateModuleFileQuery(non_module_file_path, "some content"),
      "mg.update_module_file: The specified file isn't contained in any of the module directories.");

  AssertQueryFails<mg::ClientException>(
      client, DeleteModuleFileQuery(non_module_file_path),
      "mg.delete_module_file: The specified file isn't contained in any of the module directories.");

  MG_ASSERT(std::filesystem::remove_all(non_module_directory), "Failed to cleanup directories");

  return 0;
}
