// Copyright 2026 Memgraph Ltd.
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

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <sys/utsname.h>
#include <algorithm>
#include <charconv>
#include <cstdlib>
#include <filesystem>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "utils/file.hpp"
#include "utils/string.hpp"
#include "utils/sysinfo/memory.hpp"

namespace memgraph::utils {

std::string GetMachineId() {
  // Set MEMGRAPH_TELEMETRY_ID=DOCKER in the Dockerfile.
  if (const char *override_id = std::getenv("MEMGRAPH_TELEMETRY_ID");
      override_id != nullptr && override_id[0] != '\0') {
    return override_id;
  }
  // We assume we're on linux and we need to read the machine id from /etc/machine-id
  const auto machine_id_lines = memgraph::utils::ReadLines("/etc/machine-id");
  if (machine_id_lines.size() != 1) {
    return "UNKNOWN";
  }
  return machine_id_lines[0];
}

MemoryInfo GetMemoryInfo() {
  // sysinfo reports KiB; MemoryInfo is in bytes.
  const auto capacity = sysinfo::InstalledMemory().value_or(sysinfo::MemoryCapacity{});
  return {.memory = capacity.ram_kib * 1024, .swap = capacity.swap_kib * 1024};
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

bool HasCPUFlag(const std::unordered_set<std::string> &flags, const std::string &flag) { return flags.contains(flag); }

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
  for (const auto &row : cpu_data) {
    auto tmp = utils::Trim(row);
    if (utils::StartsWith(tmp, "CPU architecture")) {
      auto split = utils::Split(tmp, ":");
      if (split.size() != 2) continue;
      return std::stoi(split[1]);
    }
  }
  return 0;
}

std::string ExtractArmCPUVariant(const std::vector<std::string> &cpu_data) {
  std::string variant;
  std::string implementer;
  for (const auto &row : cpu_data) {
    auto tmp = utils::Trim(row);
    if (utils::StartsWith(tmp, "CPU implementer")) {
      auto split = utils::Split(tmp, ":");
      if (split.size() != 2) continue;
      implementer = utils::Trim(split[1]);
    } else if (utils::StartsWith(tmp, "CPU variant")) {
      auto split = utils::Split(tmp, ":");
      if (split.size() != 2) continue;
      variant = utils::Trim(split[1]);
    }
  }
  if (implementer.empty()) {
    implementer = "Unknown Implementer";
  }
  if (variant.empty()) {
    variant = "Unknown Variant";
  }
  return fmt::format("{} {}", implementer, variant);
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
    // cpu_model is often empty on arm64 - try to extract cpu variant and implementer
    if (cpu_model.empty()) {
      cpu_model = ExtractArmCPUVariant(cpu_data);
    }
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

RuntimeEnv DetectRuntimeEnv() {
  // These are always added when the app is run in K8s
  if (std::getenv("KUBERNETES_SERVICE_HOST") || std::getenv("KUBERNETES_PORT")) {
    return RuntimeEnv::KUBERNETES;
  }
  return RuntimeEnv::NO_KUBERNETES;
}

unsigned GetSafeHardwareConcurrency(unsigned fallback) {
  auto hw = std::thread::hardware_concurrency();
  if (hw != 0) return hw;

  hw = static_cast<unsigned>(GetCPUInfo("").cpu_count);
  return hw != 0 ? hw : std::max(fallback, 1U);
}

namespace {

// Parses a base-10 integer out of a whitespace-trimmed token; nullopt on failure.
std::optional<long> ParseLong(std::string_view token) {
  token = utils::Trim(std::string{token});
  if (token.empty()) return std::nullopt;
  long value = 0;
  const auto *begin = token.data();
  const auto *end = token.data() + token.size();
  const auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc{} || ptr != end) return std::nullopt;
  return value;
}

// Reads the single-line contents of a cgroup file; nullopt if the file is
// absent or empty (which is how we distinguish cgroup v2 from v1).
std::optional<std::string> ReadCgroupLine(const std::filesystem::path &path) {
  const auto lines = utils::ReadLines(path);
  if (lines.empty()) return std::nullopt;
  return lines[0];
}

}  // namespace

std::optional<unsigned> CpuQuotaToCores(long quota_us, long period_us) {
  if (quota_us < 0) return std::nullopt;                     // unlimited (cgroup v1 uses -1)
  if (quota_us == 0 || period_us <= 0) return std::nullopt;  // malformed -> fall back to host
  // ceil(quota / period): a fractional quota still needs a whole worker thread.
  const long cores = (quota_us + period_us - 1) / period_us;
  return static_cast<unsigned>(cores);
}

std::optional<unsigned> ParseCgroupV2CpuMax(std::string_view cpu_max) {
  // Format: "<quota> <period>", where <quota> is the literal "max" when unlimited.
  auto rest = utils::Trim(std::string{cpu_max});
  if (rest.empty()) return std::nullopt;

  const auto sep = rest.find_first_of(" \t");
  const std::string_view quota_tok = std::string_view{rest}.substr(0, sep);
  if (quota_tok == "max") return std::nullopt;  // unlimited

  const auto quota = ParseLong(quota_tok);
  if (!quota) return std::nullopt;

  // The period field defaults to the cgroup v2 baseline of 100000us if absent.
  long period = 100000;
  if (sep != std::string::npos) {
    const auto period_opt = ParseLong(std::string_view{rest}.substr(sep + 1));
    if (!period_opt) return std::nullopt;
    period = *period_opt;
  }
  return CpuQuotaToCores(*quota, period);
}

unsigned UsableCoreCount(unsigned fallback) {
  const unsigned host = GetSafeHardwareConcurrency(fallback);

  std::optional<unsigned> cores;
  if (const auto v2 = ReadCgroupLine("/sys/fs/cgroup/cpu.max")) {
    // cgroup v2
    cores = ParseCgroupV2CpuMax(*v2);
  } else {
    // cgroup v1
    const auto quota = ReadCgroupLine("/sys/fs/cgroup/cpu/cpu.cfs_quota_us");
    const auto period = ReadCgroupLine("/sys/fs/cgroup/cpu/cpu.cfs_period_us");
    if (quota && period) {
      const auto quota_us = ParseLong(*quota);
      const auto period_us = ParseLong(*period);
      if (quota_us && period_us) {
        cores = CpuQuotaToCores(*quota_us, *period_us);
      }
    }
  }

  if (!cores) return host;  // unlimited, absent, or unparsable -> true hardware
  return std::clamp(*cores, 1U, host);
}

}  // namespace memgraph::utils
