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

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <system_error>

#include <unistd.h>

#include "utils/file.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"

namespace memgraph::utils {

static constexpr int64_t VM_MAX_MAP_COUNT_DEFAULT{-1};

/// Returns the number of bytes a directory is using on disk. If the given path
/// isn't a directory, zero will be returned. If there are some files with
/// wrong permission, it will be skipped. Entries that vanish during the scan
/// (e.g. WAL rotation, snapshot retention deletes) are skipped silently so
/// that the function is non-throwing by construction even under filesystem
/// races.
template <bool IgnoreSymlink = true>
inline uint64_t GetDirDiskUsage(const std::filesystem::path &path) {
  std::error_code ec;

  // Guard: path must be an existing directory (non-throwing).
  const bool is_dir = std::filesystem::is_directory(path, ec);
  if (ec || !is_dir) {
    return 0;
  }

  if (!utils::HasReadAccess(path)) {
    spdlog::warn(
        "Skipping directory path on collecting directory disk usage '{}' because it is not readable, check file "
        "ownership and read permissions!",
        path);
    return 0;
  }

  // Construct iterator without throwing; warn and bail on error.
  std::filesystem::directory_iterator it(path, ec);
  if (ec) {
    spdlog::warn("Cannot open directory for disk usage scan '{}': {}", path, ec.message());
    return 0;
  }

  uint64_t size = 0;

  // Single increment site: std::filesystem::directory_iterator::increment(ec)
  // sets the iterator to end on error, so the loop condition handles termination
  // correctly.  We clear ec before every increment so the post-loop check is
  // authoritative for the last advance that actually failed.
  for (const std::filesystem::directory_iterator end{}; it != end; it.increment(ec)) {
    const auto &dir_entry = *it;

    // All per-entry status queries use the error_code overloads so that a
    // file disappearing mid-scan (e.g. WAL rename/delete) is silently skipped.
    if constexpr (IgnoreSymlink) {
      ec.clear();
      const bool is_symlink = dir_entry.is_symlink(ec);
      if (ec || is_symlink) {
        // Entry vanished, unreadable, or is a symlink we should skip.
        ec.clear();
        continue;
      }
    }

    ec.clear();
    const bool entry_is_dir = dir_entry.is_directory(ec);
    if (!ec && entry_is_dir) {
      size += GetDirDiskUsage<IgnoreSymlink>(dir_entry.path());
      ec.clear();
      continue;
    }

    ec.clear();
    const bool entry_is_file = dir_entry.is_regular_file(ec);
    if (!ec && entry_is_file) {
      if (!utils::HasReadAccess(dir_entry.path())) {
        spdlog::warn(
            "Skipping file path on collecting directory disk usage '{}' because it is not readable, check file "
            "ownership and read permissions!",
            dir_entry.path());
        ec.clear();
        continue;
      }
      ec.clear();
      const auto fsize = dir_entry.file_size(ec);
      if (!ec) {
        size += fsize;
      }
      // If ec is set the file vanished between is_regular_file and file_size
      // (e.g. WAL rotation) — skip silently.
    }
    // ec set by is_directory or is_regular_file: entry vanished — skip.
    ec.clear();
  }

  if (ec) {
    spdlog::warn("Error advancing directory iterator for '{}': {}", path, ec.message());
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
