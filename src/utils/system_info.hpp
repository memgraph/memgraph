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

#pragma once

#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include <nlohmann/json_fwd.hpp>

namespace memgraph::utils {

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

/**
 * This function return a dictionary containing some basic system information
 * (eg. operating system name, cpu information, memory information, etc.).
 */
nlohmann::json GetSystemInfo();

}  // namespace memgraph::utils
