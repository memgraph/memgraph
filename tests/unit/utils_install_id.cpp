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

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "utils/file.hpp"
#include "utils/system_info.hpp"

namespace {
class InstallIdTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::remove_all(dir_);
    std::filesystem::create_directories(dir_);
  }

  void TearDown() override { std::filesystem::remove_all(dir_); }

  std::filesystem::path IdFile() const { return dir_ / memgraph::utils::kInstallIdFileName; }

  void WriteIdFile(const std::string &content) const {
    std::ofstream out(IdFile());
    out << content;
  }

  std::filesystem::path dir_{std::filesystem::temp_directory_path() / "MG_test_unit_utils_install_id"};
};
}  // namespace

TEST_F(InstallIdTest, CreatesFileOnFirstCall) {
  const auto id = memgraph::utils::GetOrCreateInstallId(dir_);
  EXPECT_EQ(id.size(), 36);
  ASSERT_TRUE(std::filesystem::exists(IdFile()));
  const auto lines = memgraph::utils::ReadLines(IdFile());
  ASSERT_EQ(lines.size(), 1);
  EXPECT_EQ(lines[0], id);
  EXPECT_FALSE(std::filesystem::exists(dir_ / (std::string{memgraph::utils::kInstallIdFileName} + ".tmp")));
}

TEST_F(InstallIdTest, ReusesExistingId) {
  const auto first = memgraph::utils::GetOrCreateInstallId(dir_);
  const auto second = memgraph::utils::GetOrCreateInstallId(dir_);
  EXPECT_EQ(first, second);
}

TEST_F(InstallIdTest, ReadsIdWrittenWithSurroundingWhitespace) {
  const std::string id{"0f8fad5b-d9cb-469f-a165-70867728950e"};
  WriteIdFile("  " + id + "  \n");
  EXPECT_EQ(memgraph::utils::GetOrCreateInstallId(dir_), id);
}

TEST_F(InstallIdTest, ReplacesInvalidContent) {
  for (const auto *bad : {"", "\n", "not-a-uuid", "0f8fad5b-d9cb-469f-a165-70867728950"}) {
    WriteIdFile(bad);
    const auto id = memgraph::utils::GetOrCreateInstallId(dir_);
    EXPECT_EQ(id.size(), 36) << "content: '" << bad << "'";
    EXPECT_NE(id, bad);
    EXPECT_EQ(memgraph::utils::GetOrCreateInstallId(dir_), id) << "replacement wasn't persisted";
  }
}

TEST_F(InstallIdTest, ReturnsIdEvenIfDirectoryIsMissing) {
  const auto missing = dir_ / "does_not_exist";
  const auto id = memgraph::utils::GetOrCreateInstallId(missing);
  EXPECT_EQ(id.size(), 36);
  EXPECT_FALSE(std::filesystem::exists(missing / memgraph::utils::kInstallIdFileName));
}
