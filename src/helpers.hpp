// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <gflags/gflags.h>

#include "utils/logging.hpp"

/// Reads the memgraph configuration files.
///
/// Load flags in this order, the last one has the highest priority:
///   1) /etc/memgraph/memgraph.conf
///   2) ~/.memgraph/config
///   3) env - MEMGRAPH_CONFIG
inline void LoadConfig(const std::string &product_name) {
  namespace fs = std::filesystem;
  std::vector<fs::path> configs = {fs::path("/etc/memgraph/memgraph.conf")};
  if (getenv("HOME") != nullptr) configs.emplace_back(fs::path(getenv("HOME")) / fs::path(".memgraph/config"));
  {
    auto *memgraph_config = getenv("MEMGRAPH_CONFIG");
    if (memgraph_config != nullptr) {
      auto path = fs::path(memgraph_config);
      MG_ASSERT(fs::exists(path), "MEMGRAPH_CONFIG environment variable set to nonexisting path: {}",
                path.generic_string());
      configs.emplace_back(path);
    }
  }

  std::vector<std::string> flagfile_arguments;
  for (const auto &config : configs)
    if (fs::exists(config)) {
      flagfile_arguments.emplace_back(std::string("--flag-file=" + config.generic_string()));
    }

  int custom_argc = static_cast<int>(flagfile_arguments.size()) + 1;
  char **custom_argv = new char *[custom_argc];

  custom_argv[0] = strdup(product_name.c_str());
  for (int i = 0; i < static_cast<int>(flagfile_arguments.size()); ++i) {
    custom_argv[i + 1] = strdup(flagfile_arguments[i].c_str());
  }

  // setup flags from config flags
  gflags::ParseCommandLineFlags(&custom_argc, &custom_argv, false);

  // unconsumed arguments have to be freed to avoid memory leak since they are
  // strdup-ed.
  for (int i = 0; i < custom_argc; ++i) free(custom_argv[i]);
  delete[] custom_argv;
}

inline std::pair<std::string, std::string> LoadUsernameAndPassword(const std::string &pass_file) {
  std::ifstream file(pass_file);
  if (file.fail()) {
    spdlog::warn("Problem with opening MG_PASSFILE, memgraph server will start without user");
    return {};
  }
  std::vector<std::string> result;

  std::string line;
  std::getline(file, line);
  size_t pos = 0;
  std::string token;
  static constexpr std::string_view delimiter{":"};
  while ((pos = line.find(delimiter)) != std::string::npos) {
    if (line[pos - 1] == '\\') {
      line.erase(pos - 1, 1);
      token += line.substr(0, pos);
      line.erase(0, pos);

    } else {
      token += line.substr(0, pos);
      result.push_back(token);
      line.erase(0, pos + delimiter.length());
      token = "";
    }
  }
  result.push_back(line);
  file.close();

  if (result.size() != 2) {
    spdlog::warn(
        "Wrong data format. Data should be store in format: username:password, memgraph server will start without "
        "user");
    return {};
  }

  return {result[0], result[1]};
}
