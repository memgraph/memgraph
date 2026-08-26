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

#pragma once

#include <cstdint>
#include <format>
#include <nlohmann/json_fwd.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace memgraph::utils {

// This is a bit imprecise but it is important that we can be certain about whether we are running in K8s or not
// For Docker, .dockerenv file apparently doesn't exist always so it could lead us into wrong direction that people
// aren't actually using that much Docker while in fact they do
enum class RuntimeEnv : uint8_t { KUBERNETES, NO_KUBERNETES };

struct MemoryInfo {
  uint64_t memory;
  uint64_t swap;
};

struct CPUInfo {
  std::string cpu_model;
  uint64_t cpu_count;
  uint8_t microarch_level;
};

std::string GetMachineId();

MemoryInfo GetMemoryInfo();

CPUInfo GetCPUInfo(const std::string &machine);

uint8_t DetectX86LevelFromFlags(const std::unordered_set<std::string> &flags);

uint8_t DetectArmArchitectureLevel(const std::vector<std::string> &cpu_data);

bool HasCPUFlag(const std::unordered_set<std::string> &flags, const std::string &flag);

std::unordered_set<std::string> ExtractCPUFlags(const std::vector<std::string> &cpu_data);

std::string ExtractArmCPUVariant(const std::vector<std::string> &cpu_data);

RuntimeEnv DetectRuntimeEnv();

/**
 * Returns the number of hardware threads available, with a guaranteed
 * non-zero result. Tries std::thread::hardware_concurrency() first,
 * falls back to parsing /proc/cpuinfo, and finally uses the numeric
 * fallback if neither source succeeds.
 *
 * @param fallback Value to use as a last resort (default: 2).
 * @return Number of hardware threads, always > 0.
 */
unsigned GetSafeHardwareConcurrency(unsigned fallback = 2);

/**
 * Converts a raw CPU bandwidth quota/period pair (microseconds) into a whole
 * number of cores, rounding up. Pure arithmetic, no I/O, so it is unit-testable.
 *
 * @return ceil(quota_us / period_us) cores, or std::nullopt when the quota is
 *         unlimited (quota_us < 0, e.g. cgroup v1's -1) or the inputs are
 *         invalid (non-positive period, zero quota).
 */
std::optional<unsigned> CpuQuotaToCores(long quota_us, long period_us);

/**
 * Parses the contents of a cgroup v2 `cpu.max` file ("<quota> <period>", with
 * the quota being the literal "max" when unlimited). Pure, no I/O.
 *
 * @return the derived core count, or std::nullopt when the quota is "max"
 *         (unlimited) or the contents cannot be parsed.
 */
std::optional<unsigned> ParseCgroupV2CpuMax(std::string_view cpu_max);

/**
 * Returns the number of cores this process may usefully run on, honouring a
 * cgroup CPU quota (v2 `cpu.max`, falling back to v1 `cpu.cfs_quota_us` /
 * `cpu.cfs_period_us`). The result is clamped to
 * [1, GetSafeHardwareConcurrency()]. When no quota applies (unlimited, files
 * absent, or a parse error) the host core count is returned unchanged.
 *
 * Intended for sizing worker-pool defaults so a CPU-limited container is not
 * over-provisioned. Distinct from GetSafeHardwareConcurrency(), which keeps
 * reporting the true hardware for telemetry.
 *
 * @param fallback Last-resort value forwarded to GetSafeHardwareConcurrency().
 * @return Usable core count, always > 0.
 */
unsigned UsableCoreCount(unsigned fallback = 2);

/**
 * This function return a dictionary containing some basic system information
 * (eg. operating system name, cpu information, memory information, etc.).
 */
nlohmann::json GetSystemInfo();

}  // namespace memgraph::utils
