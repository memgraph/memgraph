// Copyright 2023 Memgraph Ltd.
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

#include <filesystem>

#include <unistd.h>

#include "utils/file.hpp"
#include "utils/string.hpp"

namespace memgraph::utils {

/// Returns the number of bytes a directory is using on disk. If the given path
/// isn't a directory, zero will be returned.
template <bool IgnoreSymlink = true>
inline uint64_t GetDirDiskUsage(const std::filesystem::path &path) {
  if (!std::filesystem::is_directory(path)) return 0;

  uint64_t size = 0;
  for (auto &p : std::filesystem::directory_iterator(path)) {
    if (IgnoreSymlink && std::filesystem::is_symlink(p)) continue;
    if (std::filesystem::is_directory(p)) {
      size += GetDirDiskUsage(p);
    } else if (std::filesystem::is_regular_file(p)) {
      size += std::filesystem::file_size(p);
    }
  }

  return size;
}

/// Returns the number of bytes the process is using in the memory.
inline uint64_t GetMemoryUsage() {
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

}  // namespace memgraph::utils
