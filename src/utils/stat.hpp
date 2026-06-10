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
  static_assert(sizeof(std::uintmax_t) >= sizeof(uint64_t), "file_size() result may be narrowed");
  std::error_code ec;

  // Guard: path must be an existing directory (non-throwing). A vanished or
  // unstattable path is expected under filesystem races (e.g. a directory
  // removed by snapshot retention), so trace rather than warn.
  const bool is_dir = std::filesystem::is_directory(path, ec);
  if (ec) {
    spdlog::trace("Skipping disk usage scan of '{}': cannot stat path: {}", path, ec.message());
    return 0;
  }
  if (!is_dir) {
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
  // correctly. increment(ec) is the last operation before each loop exit and the
  // error_code overloads clear ec on success ([fs.err.report]), so the post-loop
  // check below is authoritative for the advance that actually failed — no manual
  // reset of ec is needed inside the body.
  for (const std::filesystem::directory_iterator end{}; it != end; it.increment(ec)) {
    const auto &dir_entry = *it;

    // All per-entry status queries use the error_code overloads so that a
    // file disappearing mid-scan (e.g. WAL rename/delete) is silently skipped.
    if constexpr (IgnoreSymlink) {
      const bool is_symlink = dir_entry.is_symlink(ec);
      if (ec) {
        // Entry vanished or became unstattable mid-scan.
        spdlog::trace("Skipping entry '{}' during disk usage scan: {}", dir_entry.path(), ec.message());
        continue;
      }
      if (is_symlink) {
        continue;  // expected skip — do not log
      }
    }

    const bool entry_is_dir = dir_entry.is_directory(ec);
    if (!ec && entry_is_dir) {
      size += GetDirDiskUsage<IgnoreSymlink>(dir_entry.path());
      continue;
    }

    // Not a directory; only regular files contribute. ec set here means the
    // entry vanished between iteration and stat — skip it.
    const bool entry_is_file = dir_entry.is_regular_file(ec);
    if (ec || !entry_is_file) {
      continue;
    }

    if (!utils::HasReadAccess(dir_entry.path())) {
      spdlog::warn(
          "Skipping file path on collecting directory disk usage '{}' because it is not readable, check file "
          "ownership and read permissions!",
          dir_entry.path());
      continue;
    }

    const auto fsize = dir_entry.file_size(ec);
    if (ec) {
      // The file vanished between is_regular_file and file_size (e.g. WAL
      // rotation) — skip silently at trace level.
      spdlog::trace("Skipping file '{}' during disk usage scan: {}", dir_entry.path(), ec.message());
      continue;
    }
    size += static_cast<uint64_t>(fsize);
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
