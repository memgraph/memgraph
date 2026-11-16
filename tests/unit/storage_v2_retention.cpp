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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ranges>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <unistd.h>

#include "gtest/gtest.h"

#include "storage/v2/durability/snapshot.hpp"

using memgraph::storage::durability::DeleteOldSnapshotFiles;
using memgraph::storage::durability::OldSnapshotFiles;
using memgraph::utils::FileRetainer;

TEST(RetentionTest, DeleteOldSnapshotFilesTest) {
  FileRetainer file_retainer;
  OldSnapshotFiles old_snapshot_files{{{10, std::filesystem::path{"/tmp/file1"}},
                                       {5, std::filesystem::path{"/tmp/file2"}},
                                       {7, std::filesystem::path{"/tmp/file3"}},
                                       {2, std::filesystem::path{"/tmp/file4"}},
                                       {6, std::filesystem::path{"/tmp/file5"}}}};

  for (auto const &path : std::views::values(old_snapshot_files)) {
    std::ofstream{path};
  }

  DeleteOldSnapshotFiles(old_snapshot_files, 3, &file_retainer);
  ASSERT_EQ(old_snapshot_files.size(), 2);
  ASSERT_EQ(old_snapshot_files[0].first, 7);
  ASSERT_EQ(old_snapshot_files[1].first, 10);

  for (auto const &path : std::views::values(old_snapshot_files)) {
    if (std::filesystem::exists(path)) {
      file_retainer.DeleteFile(path);
    }
  }
}

// Without a floor the newest `n` files survive: the shapes cover ties and the timestamp 0 that corrupt or foreign
// snapshots get, which must sort first and go first.
TEST(RetentionTest, DeleteOldSnapshotFilesKeepsTheNewestWithoutAFloor) {
  struct Shape {
    char const *name;
    std::vector<uint64_t> timestamps;
  };

  std::vector<Shape> const shapes{
      {"ascending", {10, 20, 30, 40, 50, 60}},
      {"descending", {60, 50, 40, 30, 20, 10}},
      {"tie", {100, 100, 120, 130, 140, 150}},
      {"zeros", {0, 20, 30, 0, 50, 0}},
  };
  auto const root =
      std::filesystem::temp_directory_path() / ("MG_test_retention_nofloor_" + std::to_string(::getpid()));
  std::filesystem::remove_all(root);

  for (auto const &shape : shapes) {
    for (size_t const n : {1U, 2U, 3U, 5U}) {
      for (size_t const count : std::set<size_t>{0, 1, n, n + 1, shape.timestamps.size()}) {
        SCOPED_TRACE(std::string{shape.name} + " n=" + std::to_string(n) + " files=" + std::to_string(count));
        auto const dir = root / (std::string{shape.name} + std::to_string(n) + "_" + std::to_string(count));
        std::filesystem::create_directories(dir);
        OldSnapshotFiles files;
        for (size_t i = 0; i < count; ++i) {
          auto path = dir / ("snapshot_" + std::to_string(i));
          std::ofstream{path};
          files.emplace_back(shape.timestamps[i % shape.timestamps.size()], std::move(path));
        }
        auto expected = files;
        std::ranges::sort(expected);
        if (count >= n)
          expected.erase(expected.begin(), expected.begin() + static_cast<std::ptrdiff_t>(count - (n - 1)));

        FileRetainer file_retainer;
        DeleteOldSnapshotFiles(files, n, &file_retainer);

        EXPECT_EQ(files, expected);
        std::set<std::filesystem::path> on_disk;
        for (auto const &entry : std::filesystem::directory_iterator(dir)) on_disk.insert(entry.path());
        std::set<std::filesystem::path> expected_on_disk;
        for (auto const &file : expected) expected_on_disk.insert(file.second);
        EXPECT_EQ(on_disk, expected_on_disk);
      }
    }
  }
  std::filesystem::remove_all(root);
}
