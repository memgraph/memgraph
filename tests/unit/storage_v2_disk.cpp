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

#include <array>
#include <memory>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <string_view>

#include "disk_test_utils.hpp"
#include "kvstore/kvstore.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/edge_import_mode.hpp"
#include "storage/v2/indices/label_property_index.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_store.hpp"  // FLAGS_storage_floating_point_resolution_bits
#include "tests/test_commit_args_helper.hpp"
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

// A value predicate on a label-property range must be applied in edge import mode too, which reads
// from its own cache: once it aborted the process, and dropping it returns rows the filter would not.
TEST_F(DiskStorageTest, EdgeImportModeAppliesTheRangeValuePredicate) {
  using memgraph::storage::PropertyPath;
  using memgraph::storage::PropertyValue;
  using memgraph::storage::PropertyValueRange;
  const std::string test_suite = "storage_v2_disk_import_predicate";

  auto storage = disk_test_utils::CreateDiskStorage(disk_test_utils::GenerateOnDiskConfig(test_suite));
  auto label = memgraph::storage::LabelId{};
  auto property = memgraph::storage::PropertyId{};
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    label = acc->NameToLabel("L");
    property = acc->NameToProperty("p");
    for (auto const value : {1, 2, 2, 3, 4}) {
      auto vertex = acc->CreateVertex();
      ASSERT_TRUE(vertex.AddLabel(label).has_value());
      ASSERT_TRUE(vertex.SetProperty(property, PropertyValue(value)).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = storage->UniqueAccess();
    ASSERT_TRUE(acc->CreateIndex(label, {PropertyPath{property}}).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto const keeps_two = std::make_shared<PropertyValueRange::ValuePredicateFn const>(
      [](PropertyValue const &value) { return value == PropertyValue(2); });
  auto not_null = PropertyValueRange::IsNotNull();
  not_null.SetValuePredicate(keeps_two);
  auto bounded = PropertyValueRange::Bounded(memgraph::utils::MakeBoundInclusive(PropertyValue(1)),
                                             memgraph::utils::MakeBoundInclusive(PropertyValue(3)));
  bounded.SetValuePredicate(keeps_two);

  auto const props = std::array{PropertyPath{property}};
  auto const count = [&](auto &acc, PropertyValueRange const &range) {
    auto const ranges = std::array{range};
    auto found = 0;
    for (auto const &vertex : acc.Vertices(label, props, ranges, memgraph::storage::View::OLD)) {
      (void)vertex;
      ++found;
    }
    return found;
  };

  {
    auto acc = storage->Access(memgraph::storage::READ);
    EXPECT_EQ(count(*acc, not_null), 2);
    EXPECT_EQ(count(*acc, bounded), 2);
  }

  static_cast<memgraph::storage::DiskStorage *>(storage.get())
      ->SetEdgeImportMode(memgraph::storage::EdgeImportMode::ACTIVE);
  {
    // Both scans share one transaction. The edge import cache keeps the vertices the first scan
    // loads, but their deltas are allocated in the loading transaction, and only a transaction
    // that commits is handed to the cache to outlive its accessor. A scan from a later
    // transaction would read deltas the loading one has already released.
    auto acc = storage->Access(memgraph::storage::READ);
    EXPECT_EQ(count(*acc, not_null), 2) << "edge import mode ignored the predicate on an IS NOT NULL range";
    EXPECT_EQ(count(*acc, bounded), 2) << "edge import mode ignored the predicate on a bounded range";
  }
  static_cast<memgraph::storage::DiskStorage *>(storage.get())
      ->SetEdgeImportMode(memgraph::storage::EdgeImportMode::INACTIVE);

  storage.reset();
  disk_test_utils::RemoveRocksDbDirs(test_suite);
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
