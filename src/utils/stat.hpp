#pragma once

#include <filesystem>

#include <unistd.h>

#include "utils/file.hpp"
#include "utils/string.hpp"

namespace utils {

/// Returns the number of bytes a directory is using on disk. If the given path
/// isn't a directory, zero will be returned.
inline uint64_t GetDirDiskUsage(const std::filesystem::path &path) {
  if (!std::filesystem::is_directory(path)) return 0;

  uint64_t size = 0;
  for (auto &p : std::filesystem::directory_iterator(path)) {
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
  if (statm_data.size() >= 1) {
    auto split = utils::Split(statm_data[0]);
    if (split.size() >= 2) {
      memory = std::stoull(split[1]) * sysconf(_SC_PAGESIZE);
    }
  }
  return memory;
}

}  // namespace utils
