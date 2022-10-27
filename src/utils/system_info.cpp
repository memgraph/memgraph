// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/system_info.hpp"

#include <string>

#include <gflags/gflags.h>
#include <sys/utsname.h>

#include "utils/file.hpp"
#include "utils/string.hpp"

namespace memgraph::utils {

std::string GetMachineId() {
#ifdef MG_TELEMETRY_ID_OVERRIDE
  return MG_TELEMETRY_ID_OVERRIDE;
#else
  // We assume we're on linux and we need to read the machine id from /etc/machine-id
  const auto machine_id_lines = memgraph::utils::ReadLines("/etc/machine-id");
  if (machine_id_lines.size() != 1) {
    return "UNKNOWN";
  }
  return machine_id_lines[0];
#endif
}

MemoryInfo GetMemoryInfo() {
  // Parse `/proc/meminfo`.
  uint64_t memory{0};
  uint64_t swap{0};
  auto mem_data = utils::ReadLines("/proc/meminfo");
  for (auto &row : mem_data) {
    auto tmp = utils::Trim(row);
    if (utils::StartsWith(tmp, "MemTotal")) {
      auto split = utils::Split(tmp);
      if (split.size() < 2) continue;
      memory = std::stoull(split[1]);
    } else if (utils::StartsWith(tmp, "SwapTotal")) {
      auto split = utils::Split(tmp);
      if (split.size() < 2) continue;
      swap = std::stoull(split[1]);
    }
  }
  memory *= 1024;
  swap *= 1024;
  return {memory, swap};
}

CPUInfo GetCPUInfo() {
  // Parse `/proc/cpuinfo`.
  std::string cpu_model;
  uint64_t cpu_count{0};
  auto cpu_data = utils::ReadLines("/proc/cpuinfo");
  for (auto &row : cpu_data) {
    auto tmp = utils::Trim(row);
    if (tmp.empty()) {
      ++cpu_count;
    } else if (utils::StartsWith(tmp, "model name")) {
      auto split = utils::Split(tmp, ":");
      if (split.size() != 2) continue;
      cpu_model = utils::Trim(split[1]);
    }
  }
  return {cpu_model, cpu_count};
}

nlohmann::json GetSystemInfo() {
  // Get `uname`.
  struct utsname info;
  if (uname(&info) != 0) return {};

  // Parse `/etc/os-release`.
  std::string os_name;
  std::string os_version;
  std::string os_full;
  auto os_data = utils::ReadLines("/etc/os-release");
  for (auto &row : os_data) {
    auto split = utils::Split(row, "=");
    if (split.size() < 2) continue;
    if (split[0] == "NAME") {
      os_name = utils::Trim(split[1], "\"");
    } else if (split[0] == "VERSION") {
      os_version = utils::Trim(split[1], "\"");
    }
    os_full = fmt::format("{} {}", os_name, os_version);
  }

  const auto cpu_info = GetCPUInfo();
  const auto mem_info = GetMemoryInfo();

  return {{"architecture", info.machine},    {"cpu_count", cpu_info.cpu_count},
          {"cpu_model", cpu_info.cpu_model}, {"kernel", fmt::format("{} {}", info.release, info.version)},
          {"memory", mem_info.memory},       {"os", os_full},
          {"swap", mem_info.swap},           {"version", gflags::VersionString()}};
}

}  // namespace memgraph::utils
