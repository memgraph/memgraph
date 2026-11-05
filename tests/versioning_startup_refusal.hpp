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

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <iterator>
#include <map>
#include <string>
#include <string_view>
#include <utility>

#include "dbms/database.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/durability/paths.hpp"
#include "utils/exceptions.hpp"

namespace memgraph::tests {

// Every file the recovery paths could touch (main WAL and snapshots, the backup directory, the branches directory),
// keyed by its path relative to `root`, with its contents.
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

// Starting a Database over `config` must throw a message containing every fragment, and leave every durable file under
// `root` byte-identical.
inline void ExpectStartupRefused(const std::filesystem::path &root, storage::Config config,
                                 std::initializer_list<std::string_view> fragments) {
  const auto before = ImageDurableFiles(root);
  ASSERT_FALSE(before.empty());
  try {
    dbms::Database db{std::move(config)};
    ADD_FAILURE() << "startup must be refused";
  } catch (const utils::BasicException &e) {
    const std::string_view what{e.what()};
    for (const auto fragment : fragments) {
      EXPECT_NE(what.find(fragment), std::string_view::npos) << "'" << fragment << "' missing from: " << what;
    }
    EXPECT_NE(what.find("Memgraph will not start, so no branch data is lost"), std::string_view::npos) << what;
  }
  EXPECT_EQ(ImageDurableFiles(root), before) << "a refused startup must leave every file untouched";
}

}  // namespace memgraph::tests
