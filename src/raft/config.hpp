/// @file

#pragma once

#include <chrono>
#include <experimental/filesystem>

namespace raft {

/// Configurable Raft parameters.
struct Config {
  std::experimental::filesystem::path disk_storage_path;
  std::chrono::milliseconds leader_timeout_min;
  std::chrono::milliseconds leader_timeout_max;
  std::chrono::milliseconds heartbeat_interval;
  std::chrono::milliseconds replicate_timeout;
};

}  // namespace raft
