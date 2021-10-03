// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/sysinfo/memory.hpp"

namespace utils::sysinfo {

namespace {
std::optional<uint64_t> ExtractAmountFromMemInfo(const std::string_view header_name) {
  std::string token;
  std::ifstream meminfo("/proc/meminfo");
  const auto meminfo_header = fmt::format("{}:", header_name);
  while (meminfo >> token) {
    if (token == meminfo_header) {
      uint64_t mem = 0;
      if (meminfo >> mem) {
        return mem;
      } else {
        return std::nullopt;
      }
    }
    meminfo.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
  }
  SPDLOG_WARN("Failed to read {} from /proc/meminfo", header_name);
  return std::nullopt;
}

}  // namespace

std::optional<uint64_t> AvailableMemory() { return ExtractAmountFromMemInfo("MemAvailable"); }

std::optional<uint64_t> TotalMemory() { return ExtractAmountFromMemInfo("MemTotal"); }

std::optional<uint64_t> SwapTotalMemory() { return ExtractAmountFromMemInfo("SwapTotal"); }

}  // namespace utils::sysinfo
