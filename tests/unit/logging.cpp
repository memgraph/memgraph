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

#include "flags/logging.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>

DECLARE_string(log_file);
DECLARE_uint64(log_retention_days);

namespace {

namespace fs = std::filesystem;

class CleanLogsDirTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = fs::temp_directory_path() / ("mg_log_test_" + std::to_string(::getpid()));
    fs::create_directories(test_dir_);
    original_log_file_ = FLAGS_log_file;
    original_retention_days_ = FLAGS_log_retention_days;
  }

  void TearDown() override {
    FLAGS_log_file = original_log_file_;
    FLAGS_log_retention_days = original_retention_days_;
    std::error_code ec;
    fs::remove_all(test_dir_, ec);
  }

  void CreateFile(const std::string &name, std::chrono::days age) {
    auto path = test_dir_ / name;
    std::ofstream{path} << "log content";
    auto const old_time = fs::file_time_type::clock::now() - age;
    fs::last_write_time(path, old_time);
  }

  std::vector<std::string> RemainingFiles() {
    std::vector<std::string> result;
    for (auto const &entry : fs::directory_iterator(test_dir_)) {
      if (entry.is_regular_file()) {
        result.emplace_back(entry.path().filename().string());
      }
    }
    std::sort(result.begin(), result.end());
    return result;
  }

  fs::path test_dir_;
  std::string original_log_file_;
  uint64_t original_retention_days_{};
};

TEST_F(CleanLogsDirTest, EmptyLogFileFlag) {
  FLAGS_log_file = "";
  CreateFile("old.log", std::chrono::days{10});
  memgraph::flags::CleanLogsDir();
  EXPECT_EQ(RemainingFiles().size(), 1);
}

TEST_F(CleanLogsDirTest, AllFilesWithinRetention) {
  FLAGS_log_file = (test_dir_ / "memgraph.log").string();
  FLAGS_log_retention_days = 5;

  CreateFile("recent_1.log", std::chrono::days{2});
  CreateFile("recent_2.log", std::chrono::days{1});
  CreateFile("recent_3.log", std::chrono::days{0});

  memgraph::flags::CleanLogsDir();

  EXPECT_EQ(RemainingFiles().size(), 3);
}

TEST_F(CleanLogsDirTest, OldFilesDeleted) {
  FLAGS_log_file = (test_dir_ / "memgraph.log").string();
  FLAGS_log_retention_days = 2;

  CreateFile("old_1.log", std::chrono::days{7});
  CreateFile("old_2.log", std::chrono::days{6});
  CreateFile("old_3.log", std::chrono::days{5});
  CreateFile("old_4.log", std::chrono::days{3});
  CreateFile("recent_1.log", std::chrono::days{1});
  CreateFile("recent_2.log", std::chrono::days{0});

  memgraph::flags::CleanLogsDir();

  auto remaining = RemainingFiles();
  EXPECT_EQ(remaining.size(), 2);
  EXPECT_EQ(remaining[0], "recent_1.log");
  EXPECT_EQ(remaining[1], "recent_2.log");
}

TEST_F(CleanLogsDirTest, AllFilesOld) {
  FLAGS_log_file = (test_dir_ / "memgraph.log").string();
  FLAGS_log_retention_days = 1;

  CreateFile("old_1.log", std::chrono::days{10});
  CreateFile("old_2.log", std::chrono::days{9});
  CreateFile("old_3.log", std::chrono::days{8});

  memgraph::flags::CleanLogsDir();

  EXPECT_TRUE(RemainingFiles().empty());
}

TEST_F(CleanLogsDirTest, DirectoriesAreNotDeleted) {
  FLAGS_log_file = (test_dir_ / "memgraph.log").string();
  FLAGS_log_retention_days = 1;

  CreateFile("old.log", std::chrono::days{10});
  fs::create_directory(test_dir_ / "subdir");

  memgraph::flags::CleanLogsDir();

  auto remaining = RemainingFiles();
  EXPECT_TRUE(remaining.empty());
  // subdir should still exist
  EXPECT_TRUE(fs::exists(test_dir_ / "subdir"));
}

TEST_F(CleanLogsDirTest, NonExistentDirectory) {
  FLAGS_log_file = "/tmp/mg_nonexistent_dir_test/memgraph.log";
  FLAGS_log_retention_days = 2;
  EXPECT_NO_THROW(memgraph::flags::CleanLogsDir());
}

}  // namespace
