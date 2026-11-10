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

#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <iterator>
#include <map>
#include <string>
#include <string_view>

#include "storage/v2/durability/paths.hpp"

namespace memgraph::tests {

// Contents of every file under the recovery directories, keyed by path relative to `root`.
using DurableFileImage = std::map<std::string, std::string>;

inline DurableFileImage ImageDurableFiles(const std::filesystem::path &root) {
  DurableFileImage image;
  for (const std::string_view directory :
       std::initializer_list<std::string_view>{"branches",
                                               storage::durability::kWalDirectory,
                                               storage::durability::kSnapshotDirectory,
                                               storage::durability::kBackupDirectory}) {
    const auto base = root / directory;
    std::error_code ec;
    if (!std::filesystem::exists(base, ec)) continue;
    for (const auto &entry : std::filesystem::recursive_directory_iterator(
             base, std::filesystem::directory_options::skip_permission_denied)) {
      const auto key = std::filesystem::relative(entry.path(), root).string();
      if (!entry.is_regular_file()) {
        image[key] = "<directory>";
        continue;
      }
      std::ifstream in{entry.path(), std::ios::binary};
      image[key] = std::string{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
    }
  }
  return image;
}

}  // namespace memgraph::tests
