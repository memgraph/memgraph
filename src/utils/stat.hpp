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

#include <cstddef>
#include <filesystem>
#include <optional>

#include <unistd.h>

#include "utils/file.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"

namespace memgraph::utils {

static constexpr int64_t VM_MAX_MAP_COUNT_DEFAULT{-1};

/// Returns the number of bytes a directory is using on disk. If the given path
/// isn't a directory, zero will be returned. If there are some files with
/// wrong permission, it will be skipped
template <bool IgnoreSymlink = true>
inline uint64_t GetDirDiskUsage(const std::filesystem::path &path) {
  if (!std::filesystem::is_directory(path)) return 0;

  if (!utils::HasReadAccess(path)) {
    spdlog::warn(
        "Skipping directory path on collecting directory disk usage '{}' because it is not readable, check file "
        "ownership and read permissions!",
        path);
    return 0;
  }
  uint64_t size = 0;
  for (const auto &dir_entry : std::filesystem::directory_iterator(path)) {
    if (IgnoreSymlink && std::filesystem::is_symlink(dir_entry)) continue;
    if (std::filesystem::is_directory(dir_entry)) {
      size += GetDirDiskUsage(dir_entry);
    } else if (std::filesystem::is_regular_file(dir_entry)) {
      if (!utils::HasReadAccess(dir_entry)) {
        spdlog::warn(
            "Skipping file path on collecting directory disk usage '{}' because it is not readable, check file "
            "ownership and read permissions!",
            dir_entry.path());
        continue;
      }
      size += std::filesystem::file_size(dir_entry);
    }
  }

  return size;
}

/// Returns the number of bytes the process is using in the memory.
inline uint64_t GetMemoryRES() {
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
inline std::optional<int64_t> GetVmMaxMapCount() {
  auto vm_max_map_count_data = utils::ReadLines("/proc/sys/vm/max_map_count");
  if (vm_max_map_count_data.empty()) {
    return std::nullopt;
  }
  if (vm_max_map_count_data.size() != 1) {
    return std::nullopt;
  }
  const auto parts{utils::Split(vm_max_map_count_data[0])};
  if (parts.size() != 1) {
    return std::nullopt;
  }
  return std::stoi(parts[0]);
}

}  // namespace memgraph::utils
