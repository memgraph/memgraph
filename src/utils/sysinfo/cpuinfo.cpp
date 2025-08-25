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

#include "utils/sysinfo/cpuinfo.hpp"

#include <fstream>
#include <string>
#include <thread>

namespace memgraph::utils::sysinfo {

std::optional<uint64_t> LogicalCPUCores() {
  const auto logical_cores = static_cast<uint64_t>(std::thread::hardware_concurrency());
  if (logical_cores != 0) {
    return logical_cores;
  }

  std::ifstream cpuinfo("/proc/cpuinfo");
  if (cpuinfo.is_open()) {
    uint64_t count = 0;
    std::string line;
    while (std::getline(cpuinfo, line)) {
      if (line.rfind("processor", 0) == 0) {  // starts with "processor"
        ++count;
      }
    }
    if (count > 0) {
      return count;
    }
  }
  return std::nullopt;
}

}  // namespace memgraph::utils::sysinfo
