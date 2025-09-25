// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <ranges>
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
  ASSERT_EQ(old_snapshot_files.size(), 3);
  ASSERT_EQ(old_snapshot_files[0].first, 6);
  ASSERT_EQ(old_snapshot_files[0].first, 7);
  ASSERT_EQ(old_snapshot_files[0].first, 10);

  for (auto const &path : std::views::values(old_snapshot_files)) {
    if (std::filesystem::exists(path)) {
      file_retainer.DeleteFile(path);
    }
  }
}
