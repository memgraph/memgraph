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

#include <filesystem>

namespace memgraph::utils {
/**
 * @brief Creates a temporary directory with a unique name under /tmp/
 *
 * @return std::filesystem::path
 * @throws std::filesystem::filesystem_error as defined by create_directories
 */
inline std::filesystem::path TempDir() {
  const std::filesystem::path tmp_dir_path{std::filesystem::temp_directory_path() /= std::tmpnam(nullptr)};
  std::filesystem::create_directories(tmp_dir_path);
  return tmp_dir_path;
}

}  // namespace memgraph::utils
