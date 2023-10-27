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

#include <cstddef>
#include <filesystem>
#include <optional>

#include <unistd.h>

#include "spdlog/spdlog.h"
#include "utils/file.hpp"
#include "utils/string.hpp"

namespace memgraph::utils {

std::optional<std::string> exec(const char *cmd) {
  std::array<char, 128> buffer;
  std::string result;
  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd, "r"), pclose);
  if (!pipe) {
    spdlog::trace("Can't read process output.");
    return std::nullopt;
  }
  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    result += buffer.data();
  }
  return result;
}

/// Returns the number of bytes the process is using in the memory.
uint64_t GetMemoryUsage() {
  // Get PID of entire process.
  pid_t pid = getpid();
  uint64_t memory = 0;
  auto statm_data = utils::ReadLines(fmt::format("/proc/{}/statm", pid));
  if (!statm_data.empty()) {
    auto split = utils::Split(statm_data[0]);
    if (split.size() >= 2) {
      memory = std::stoull(split[1]) * sysconf(_SC_PAGESIZE);
    }
  }
  return memory;
}

/// Returns the size of vm.max_map_count
uint64_t GetVmMaxMapCount() {
  uint64_t max_map_count = 0;
  auto statm_data = exec("sysctl vm.max_map_count");
  if (!statm_data) {
    return -1;
  }
  if (!statm_data->empty()) {
    const auto parts{utils::Split(*statm_data)};
    if (parts.size() == 3) {
      max_map_count = std::stoull(parts[2]);
    }
  }
  return max_map_count;
}
}  // namespace memgraph::utils
