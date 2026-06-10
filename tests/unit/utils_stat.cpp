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

#include <filesystem>
#include <fstream>
#include <string>

#include <unistd.h>

#include <gtest/gtest.h>

#include "utils/stat.hpp"

namespace fs = std::filesystem;

class GetDirDiskUsageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Per-process unique root so parallel test binaries (ctest -j) don't collide.
    root_ = fs::temp_directory_path() / ("MG_test_get_dir_disk_usage_" + std::to_string(::getpid()));
    fs::remove_all(root_);
    fs::create_directories(root_);
  }

  void TearDown() override { fs::remove_all(root_); }

  // Creates a file with `size` bytes of content and returns its path.
  fs::path MakeFile(const fs::path &path, size_t size) {
    fs::create_directories(path.parent_path());
    std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
    ofs << std::string(size, 'x');
    ofs.close();
    return path;
  }

  fs::path root_;
};

TEST_F(GetDirDiskUsageTest, NonExistentPathReturnsZero) {
  EXPECT_EQ(memgraph::utils::GetDirDiskUsage(root_ / "does_not_exist"), 0U);
}

TEST_F(GetDirDiskUsageTest, RegularFileAsPathReturnsZero) {
  // The function only operates on directories; a file path yields 0.
  const auto file = MakeFile(root_ / "a.bin", 42);
  EXPECT_EQ(memgraph::utils::GetDirDiskUsage(file), 0U);
}

TEST_F(GetDirDiskUsageTest, EmptyDirectoryReturnsZero) { EXPECT_EQ(memgraph::utils::GetDirDiskUsage(root_), 0U); }

TEST_F(GetDirDiskUsageTest, SumsRegularFiles) {
  MakeFile(root_ / "a.bin", 5);
  MakeFile(root_ / "b.bin", 10);
  EXPECT_EQ(memgraph::utils::GetDirDiskUsage(root_), 15U);
}

TEST_F(GetDirDiskUsageTest, RecursesIntoSubdirectories) {
  MakeFile(root_ / "top.bin", 5);
  MakeFile(root_ / "sub" / "nested.bin", 10);
  MakeFile(root_ / "sub" / "deeper" / "leaf.bin", 20);
  EXPECT_EQ(memgraph::utils::GetDirDiskUsage(root_), 35U);
}

TEST_F(GetDirDiskUsageTest, IgnoresSymlinksByDefault) {
  // Default template parameter IgnoreSymlink == true: a symlink to a file is
  // not counted, so the directory holding only the symlink measures 0.
  const auto target = MakeFile(root_ / "external" / "target.bin", 7);
  const auto scan_dir = root_ / "scan";
  fs::create_directories(scan_dir);
  fs::create_symlink(target, scan_dir / "link.bin");

  EXPECT_EQ(memgraph::utils::GetDirDiskUsage(scan_dir), 0U);
}
