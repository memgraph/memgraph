// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <cassert>
#include <exception>
#include <string>
#include <unordered_set>

#include "disk_test_utils.hpp"
#include "query/common.hpp"
#include "query/db_accessor.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "utils/rocksdb_serialization.hpp"

/* Tests that serialization of vertices and edges to RocksDB works correctly.
 */
class RocksDBStorageTest : public ::testing::TestWithParam<bool> {
 public:
  const std::string testSuite = "storage_rocks";

  RocksDBStorageTest() {
    config_ = disk_test_utils::GenerateOnDiskConfig(testSuite);
    storage = std::make_unique<memgraph::storage::DiskStorage>(config_);
  }

  void TearDown() override {
    storage.reset(nullptr);
    disk_test_utils::RemoveRocksDbDirs(testSuite);
  }

  ~RocksDBStorageTest() override {}

 protected:
  std::unique_ptr<memgraph::storage::Storage> storage;
  memgraph::storage::Config config_;
};

TEST_F(RocksDBStorageTest, SerializeVertexGID) {
  auto acc = storage->Access();
  auto vertex = acc->CreateVertex();
  auto gid = vertex.Gid();
  ASSERT_EQ(memgraph::utils::SerializeVertex(*vertex.vertex_), "|" + memgraph::utils::SerializeIdType(gid));
}

TEST_F(RocksDBStorageTest, SerializeVertexGIDLabels) {
  auto acc = storage->Access();
  auto vertex = acc->CreateVertex();
  auto ser_player_label = acc->NameToLabel("Player");
  ASSERT_FALSE(vertex.AddLabel(ser_player_label).HasError());
  auto gid = vertex.Gid();
  ASSERT_EQ(memgraph::utils::SerializeVertex(*vertex.vertex_),
            std::to_string(ser_player_label.AsInt()) + "|" + memgraph::utils::SerializeIdType(gid));
}

TEST_F(RocksDBStorageTest, SerializePropertiesLocalBuffer) {
  memgraph::storage::PropertyStore props;
  auto id = memgraph::storage::PropertyId::FromInt(0);
  auto id_value = memgraph::storage::PropertyValue(1);
  auto completion_percentage = memgraph::storage::PropertyId::FromInt(1);
  auto completion_percentage_value = memgraph::storage::PropertyValue(14);
  auto gender = memgraph::storage::PropertyId::FromInt(2);
  auto gender_value = memgraph::storage::PropertyValue("man");
  auto age = memgraph::storage::PropertyId::FromInt(3);
  auto age_value = memgraph::storage::PropertyValue(26);
  ASSERT_TRUE(props.SetProperty(id, id_value));
  ASSERT_TRUE(props.SetProperty(age, age_value));
  ASSERT_TRUE(props.SetProperty(completion_percentage, completion_percentage_value));
  ASSERT_TRUE(props.SetProperty(gender, gender_value));
  std::string serialized_props = memgraph::utils::SerializeProperties(props);
  memgraph::storage::PropertyStore deserialized_props =
      memgraph::storage::PropertyStore::CreateFromBuffer(serialized_props);

  for (const auto &[prop_id, prop_value] : props.Properties()) {
    ASSERT_TRUE(deserialized_props.IsPropertyEqual(prop_id, prop_value));
  }
}

TEST_F(RocksDBStorageTest, SerializePropertiesExternalBuffer) {
  memgraph::storage::PropertyStore props;
  auto id = memgraph::storage::PropertyId::FromInt(0);
  auto id_value = memgraph::storage::PropertyValue(1);
  auto completion_percentage = memgraph::storage::PropertyId::FromInt(1);
  auto completion_percentage_value = memgraph::storage::PropertyValue(14);
  auto gender = memgraph::storage::PropertyId::FromInt(2);
  auto gender_value = memgraph::storage::PropertyValue("man167863816386826");
  auto age = memgraph::storage::PropertyId::FromInt(3);
  auto age_value = memgraph::storage::PropertyValue(26);
  ASSERT_TRUE(props.SetProperty(id, id_value));
  ASSERT_TRUE(props.SetProperty(age, age_value));
  ASSERT_TRUE(props.SetProperty(completion_percentage, completion_percentage_value));
  ASSERT_TRUE(props.SetProperty(gender, gender_value));
  std::string serialized_props = memgraph::utils::SerializeProperties(props);
  memgraph::storage::PropertyStore deserialized_props =
      memgraph::storage::PropertyStore::CreateFromBuffer(serialized_props);

  for (const auto &[prop_id, prop_value] : props.Properties()) {
    ASSERT_TRUE(deserialized_props.IsPropertyEqual(prop_id, prop_value));
  }
}
