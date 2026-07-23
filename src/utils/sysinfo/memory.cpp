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

#include "utils/sysinfo/memory.hpp"

#include <sys/sysinfo.h>

#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <fstream>
#include <limits>
#include <string>
#include <string_view>

namespace memgraph::utils::sysinfo {

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

std::optional<struct ::sysinfo> GetSysinfo() {
  struct ::sysinfo info{};
  if (::sysinfo(&info) != 0) {
    SPDLOG_WARN("sysinfo() failed");
    return std::nullopt;
  }
  return info;
}

}  // namespace

// MemAvailable is a kernel estimate with no syscall equivalent, so it still comes from /proc/meminfo.
std::optional<uint64_t> AvailableMemory() { return ExtractAmountFromMemInfo("MemAvailable"); }

std::optional<uint64_t> TotalMemory() {
  const auto info = GetSysinfo();
  if (!info) return std::nullopt;
  return info->totalram * info->mem_unit / 1024;
}

std::optional<uint64_t> SwapTotalMemory() {
  const auto info = GetSysinfo();
  if (!info) return std::nullopt;
  return info->totalswap * info->mem_unit / 1024;
}

}  // namespace memgraph::utils::sysinfo
