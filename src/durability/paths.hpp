#pragma once

#include <experimental/filesystem>
#include <string>

#include "glog/logging.h"

namespace durability {
const std::string kSnapshotDir = "snapshots";
const std::string kWalDir = "wal";

/// Ensures the given durability directory exists and is ready for use. Creates
/// the directory if it doesn't exist.
inline void CheckDurabilityDir(const std::string &durability_dir) {
  namespace fs = std::experimental::filesystem;
  if (fs::exists(durability_dir)) {
    CHECK(fs::is_directory(durability_dir)) << "The durability directory path '"
                                            << durability_dir
                                            << "' is not a directory!";
  } else {
    bool success = fs::create_directory(durability_dir);
    CHECK(success) << "Failed to create durability directory '"
                   << durability_dir << "'.";
  }
}
}
