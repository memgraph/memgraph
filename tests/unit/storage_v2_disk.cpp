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

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <string_view>

#include "disk_test_utils.hpp"
#include "kvstore/kvstore.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_store.hpp"  // FLAGS_storage_floating_point_resolution_bits
#include "utils/exceptions.hpp"
#include "utils/file.hpp"

class DiskStorageTest : public ::testing::TestWithParam<bool> {};

TEST_F(DiskStorageTest, CreateDiskStorageInDataDirectory) {
  const std::string testSuite = "storage_v2_disk";

  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  auto storage = disk_test_utils::CreateDiskStorage(config);
  ASSERT_TRUE(memgraph::utils::DirExists(config.disk.main_storage_directory));

  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

namespace {

struct RestoreFpResolutionGuard {
  ~RestoreFpResolutionGuard() { FLAGS_storage_floating_point_resolution_bits = saved; }

  uint64_t saved = FLAGS_storage_floating_point_resolution_bits;
};

// Must match the key `DurableMetadata` records the resolution under; a rename makes the
// pre-existing-database test below fail rather than pass vacuously.
constexpr std::string_view kFloatingPointResolutionKey = "floating_point_resolution_bits";

class DiskStorageFloatingPointResolutionTest : public ::testing::Test {
 protected:
  void SetUp() override { disk_test_utils::RemoveRocksDbDirs(testSuite); }

  void TearDown() override { disk_test_utils::RemoveRocksDbDirs(testSuite); }

  memgraph::storage::Config Config() const { return disk_test_utils::GenerateOnDiskConfig(testSuite); }

  static constexpr const char *testSuite = "storage_v2_disk_fp_resolution";
  RestoreFpResolutionGuard guard;
};

}  // namespace

TEST_F(DiskStorageFloatingPointResolutionTest, ReopeningWithTheSameResolutionSucceeds) {
  FLAGS_storage_floating_point_resolution_bits = 32;
  disk_test_utils::CreateDiskStorage(Config());

  FLAGS_storage_floating_point_resolution_bits = 32;
  EXPECT_NO_THROW(disk_test_utils::CreateDiskStorage(Config()));
}

TEST_F(DiskStorageFloatingPointResolutionTest, ReopeningWithADifferentResolutionIsRefused) {
  FLAGS_storage_floating_point_resolution_bits = 64;
  disk_test_utils::CreateDiskStorage(Config());

  FLAGS_storage_floating_point_resolution_bits = 16;
  try {
    disk_test_utils::CreateDiskStorage(Config());
    FAIL() << "opening the database with a different resolution should have thrown";
  } catch (const memgraph::utils::BasicException &e) {
    const std::string message = e.what();
    EXPECT_NE(message.find("64"), std::string::npos) << message;
    EXPECT_NE(message.find("16"), std::string::npos) << message;
    EXPECT_NE(message.find("--storage-floating-point-resolution-bits"), std::string::npos) << message;
  }
}

// A database created before this check exists carries no recorded resolution. It is adopted at the
// resolution now in force, with a warning, rather than refusing to start.
TEST_F(DiskStorageFloatingPointResolutionTest, DatabaseWithoutARecordedResolutionIsAdoptedAtTheCurrentSetting) {
  FLAGS_storage_floating_point_resolution_bits = 64;
  disk_test_utils::CreateDiskStorage(Config());

  {
    memgraph::kvstore::KVStore durability{Config().disk.durability_directory};
    ASSERT_TRUE(durability.Get(kFloatingPointResolutionKey).has_value());
    ASSERT_TRUE(durability.Delete(kFloatingPointResolutionKey));
  }

  FLAGS_storage_floating_point_resolution_bits = 16;
  EXPECT_NO_THROW(disk_test_utils::CreateDiskStorage(Config()));

  // The adopted value is recorded, so the check is armed from the next start onwards.
  {
    memgraph::kvstore::KVStore durability{Config().disk.durability_directory};
    EXPECT_EQ(durability.Get(kFloatingPointResolutionKey), std::optional<std::string>{"16"});
  }
  FLAGS_storage_floating_point_resolution_bits = 32;
  EXPECT_THROW(disk_test_utils::CreateDiskStorage(Config()), memgraph::utils::BasicException);
}
