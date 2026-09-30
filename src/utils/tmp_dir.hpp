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

#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <system_error>

namespace memgraph::utils {
/**
 * @brief Creates a temporary directory whose name the kernel makes unique, under the directory
 * TMPDIR names.
 *
 * The name is chosen and the directory created in one step, so no other process can take the name in
 * between.
 *
 * @return std::filesystem::path
 * @throws std::filesystem::filesystem_error if the directory cannot be created
 */
inline std::filesystem::path TempDir() {
  auto path = (std::filesystem::temp_directory_path() / "memgraph_XXXXXX").string();

  if (::mkdtemp(path.data()) == nullptr) {
    throw std::filesystem::filesystem_error("Couldn't create a temporary directory",
                                            std::error_code{errno, std::generic_category()});
  }

  return path;
}

}  // namespace memgraph::utils
