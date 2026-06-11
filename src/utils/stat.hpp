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

// Forward declaration so the per-entry helper below can recurse into
// subdirectories.
template <bool IgnoreSymlink = true>
inline uint64_t GetDirDiskUsage(const std::filesystem::path &path);

namespace detail {

// Returns the on-disk byte size contributed by a single directory entry, or 0
// if the entry should be skipped (a symlink when IgnoreSymlink, a vanished or
// unreadable entry, a non-regular file). Uses its own error_code so a per-entry
// failure never leaks out; recurses into real subdirectories.
template <bool IgnoreSymlink>
inline uint64_t DirEntryDiskUsage(const std::filesystem::directory_entry &dir_entry) {
  static_assert(sizeof(std::uintmax_t) >= sizeof(uint64_t), "file_size() result may be narrowed");
  std::error_code ec;

  if constexpr (IgnoreSymlink) {
    const bool is_symlink = dir_entry.is_symlink(ec);
    if (ec) {
      // Entry vanished or became unstattable mid-scan.
      spdlog::trace("Skipping entry '{}' during disk usage scan: {}", dir_entry.path(), ec.message());
      return 0;
    }
    if (is_symlink) {
      return 0;  // expected skip — do not log
    }
  }

  const bool entry_is_dir = dir_entry.is_directory(ec);
  if (!ec && entry_is_dir) {
    return GetDirDiskUsage<IgnoreSymlink>(dir_entry.path());
  }

  // Not a directory; only regular files contribute. ec set here means the entry
  // vanished between iteration and stat — skip it.
  const bool entry_is_file = dir_entry.is_regular_file(ec);
  if (ec || !entry_is_file) {
    return 0;
  }

  if (!utils::HasReadAccess(dir_entry.path())) {
    spdlog::warn(
        "Skipping file path on collecting directory disk usage '{}' because it is not readable, check file "
        "ownership and read permissions!",
        dir_entry.path());
    return 0;
  }

  const auto fsize = dir_entry.file_size(ec);
  if (ec) {
    // The file vanished between is_regular_file and file_size (e.g. WAL
    // rotation) — skip silently at trace level.
    spdlog::trace("Skipping file '{}' during disk usage scan: {}", dir_entry.path(), ec.message());
    return 0;
  }
  return static_cast<uint64_t>(fsize);
}

}  // namespace detail

/// Returns the number of bytes a directory is using on disk. If the given path
/// isn't a directory, zero will be returned. If there are some files with
/// wrong permission, it will be skipped. Entries that vanish during the scan
/// (e.g. WAL rotation, snapshot retention deletes) are skipped silently so
/// that the function is non-throwing by construction even under filesystem
/// races.
template <bool IgnoreSymlink>
inline uint64_t GetDirDiskUsage(const std::filesystem::path &path) {
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

  // Construct iterator without throwing. A directory that vanished between the
  // is_directory check above and this open (e.g. snapshot retention) is an
  // expected race — trace it, like a vanished file; other failures (permissions,
  // I/O) warrant a warning.
  std::filesystem::directory_iterator it(path, ec);
  if (ec) {
    if (ec == std::errc::no_such_file_or_directory || ec == std::errc::not_a_directory) {
      spdlog::trace("Skipping disk usage scan of '{}': directory vanished mid-scan: {}", path, ec.message());
    } else {
      spdlog::warn("Cannot open directory for disk usage scan '{}': {}", path, ec.message());
    }
    return 0;
  }

  uint64_t size = 0;
  // *it is dereferenced only when valid: the constructor produced the first
  // entry (checked above), and after a successful increment the loop condition
  // re-validates against end{}. A failed increment is reported and breaks
  // before the iterator is touched again, so we never dereference past an error
  // regardless of how the failed iterator compares to end{}.
  for (const std::filesystem::directory_iterator end{}; it != end;) {
    size += detail::DirEntryDiskUsage<IgnoreSymlink>(*it);

    it.increment(ec);
    if (ec) {
      spdlog::warn("Error advancing directory iterator for '{}': {}", path, ec.message());
      break;
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
