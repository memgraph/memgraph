// Copyright 2025 Memgraph Ltd.
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

#include <nlohmann/json.hpp>

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

std::unordered_set<std::string> ExtractCPUFlags(const std::vector<std::string> &cpu_data) {
  std::unordered_set<std::string> flags;

  for (const auto &row : cpu_data) {
    auto tmp = utils::Trim(row);
    if (utils::StartsWith(tmp, "flags")) {
      auto split = utils::Split(tmp, ":");
      if (split.size() != 2) continue;
      auto flag_str = utils::Trim(split[1]);
      auto flag_list = utils::Split(flag_str, " ");
      flags.insert(flag_list.begin(), flag_list.end());
      // assume that all CPUs have the same flags
      // TODO (matt): do we need to check for heterogenous flags?
      break;
    }
  }

  return flags;
}

bool HasCPUFlag(const std::unordered_set<std::string> &flags, const std::string &flag) {
  return flags.find(flag) != flags.end();
}

uint8_t DetectX86LevelFromFlags(const std::unordered_set<std::string> &flags) {
  // see for definitions used by `ld.so`
  // https://codebrowser.dev/glibc/glibc/elf/elf.h.html#1385

  // v1: SSE2 is always baseline x86-64, so skip check
  // `sse3` is also known as `pni`
  if (!((HasCPUFlag(flags, "sse3") || HasCPUFlag(flags, "pni")) && HasCPUFlag(flags, "ssse3") &&
        HasCPUFlag(flags, "sse4_1") && HasCPUFlag(flags, "sse4_2") && HasCPUFlag(flags, "popcnt")))
    return 1;

  // Note that for AMD `lxcnt` is called `abm`
  // see:
  // https://github.com/google/cpu_features/blob/d3b2440fcfc25fe8e6d0d4a85f06d68e98312f5b/include/cpuinfo_x86.h#L103
  if (!(HasCPUFlag(flags, "avx") && HasCPUFlag(flags, "avx2") && HasCPUFlag(flags, "bmi1") &&
        HasCPUFlag(flags, "bmi2") && HasCPUFlag(flags, "f16c") && HasCPUFlag(flags, "fma") &&
        (HasCPUFlag(flags, "lzcnt") || HasCPUFlag(flags, "abm")) && HasCPUFlag(flags, "movbe") &&
        HasCPUFlag(flags, "xsave")))
    return 2;

  if (!(HasCPUFlag(flags, "avx512f") && HasCPUFlag(flags, "avx512dq") && HasCPUFlag(flags, "avx512cd") &&
        HasCPUFlag(flags, "avx512bw") && HasCPUFlag(flags, "avx512vl")))
    return 3;

  return 4;
}

uint8_t DetectArmArchitectureLevel(const std::vector<std::string> &cpu_data) {
  for (auto &row : cpu_data) {
    auto tmp = utils::Trim(row);
    if (utils::StartsWith(tmp, "CPU architecture")) {
      auto split = utils::Split(tmp, ":");
      if (split.size() != 2) continue;
      return std::stoi(split[1]);
    }
  }
  return 0;
}

CPUInfo GetCPUInfo(const std::string &machine) {
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
  uint8_t microarch_level = 0;
  if (machine == "x86_64") {
    auto flags = ExtractCPUFlags(cpu_data);
    microarch_level = DetectX86LevelFromFlags(flags);
  }

  if (machine == "aarch64") {
    microarch_level = DetectArmArchitectureLevel(cpu_data);
  }

  return {cpu_model, cpu_count, microarch_level};
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

  const auto cpu_info = GetCPUInfo(info.machine);
  const auto mem_info = GetMemoryInfo();

  return {{"architecture", info.machine},
          {"cpu_count", cpu_info.cpu_count},
          {"cpu_model", cpu_info.cpu_model},
          {"kernel", fmt::format("{} {}", info.release, info.version)},
          {"memory", mem_info.memory},
          {"os", os_full},
          {"swap", mem_info.swap},
          {"version", gflags::VersionString()},
          {"microarch_level", cpu_info.microarch_level}};
}

}  // namespace memgraph::utils
