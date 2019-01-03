#pragma once

#include <experimental/filesystem>

#include <unistd.h>

#include "utils/file.hpp"
#include "utils/string.hpp"

namespace utils {

/// Returns the number of bytes a directory is using on disk. If the given path
/// isn't a directory, zero will be returned.
inline uint64_t GetDirDiskUsage(
    const std::experimental::filesystem::path &path) {
  if (!std::experimental::filesystem::is_directory(path)) return 0;

  uint64_t size = 0;
  for (auto &p : std::experimental::filesystem::directory_iterator(path)) {
    if (std::experimental::filesystem::is_directory(p)) {
      size += GetDirDiskUsage(p);
    } else if (std::experimental::filesystem::is_regular_file(p)) {
      size += std::experimental::filesystem::file_size(p);
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
  if (statm_data.size() >= 1) {
    auto split = utils::Split(statm_data[0]);
    if (split.size() >= 2) {
      memory = std::stoull(split[1]) * sysconf(_SC_PAGESIZE);
    }
  }
  return memory;
}

}  // namespace utils
