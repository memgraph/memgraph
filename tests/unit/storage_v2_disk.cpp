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

#include <gtest/gtest.h>

#include "disk_test_utils.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/edge_import_mode.hpp"
#include "storage/v2/indices/label_property_index.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "tests/test_commit_args_helper.hpp"
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
