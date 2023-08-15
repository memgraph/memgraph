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

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace memgraph::storage;

/* Tests that serialization of vertices and edges to RocksDB works correctly.
 */
class RocksDBStorageTest : public ::testing::TestWithParam<bool> {
 public:
  const std::string testSuite = "storage_rocks";

  RocksDBStorageTest() {
    config_ = disk_test_utils::GenerateOnDiskConfig(testSuite);
    storage = std::make_unique<DiskStorage>(config_);
  }

  void TearDown() override {
    storage.reset(nullptr);
    disk_test_utils::RemoveRocksDbDirs(testSuite);
  }

  ~RocksDBStorageTest() override {}

 protected:
  std::unique_ptr<Storage> storage;
  Config config_;
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
  auto ser_user_label = acc->NameToLabel("User");
  ASSERT_FALSE(vertex.AddLabel(ser_player_label).HasError());
  ASSERT_FALSE(vertex.AddLabel(ser_user_label).HasError());
  auto gid = vertex.Gid();
  ASSERT_EQ(memgraph::utils::SerializeVertex(*vertex.vertex_), std::to_string(ser_player_label.AsInt()) + "," +
                                                                   std::to_string(ser_user_label.AsInt()) + "|" +
                                                                   memgraph::utils::SerializeIdType(gid));
}

TEST_F(RocksDBStorageTest, SerializePropertiesLocalBuffer) {
  PropertyStore props;
  auto id = PropertyId::FromInt(0);
  auto id_value = PropertyValue(1);
  auto completion_percentage = PropertyId::FromInt(1);
  auto completion_percentage_value = PropertyValue(14);
  auto gender = PropertyId::FromInt(2);
  auto gender_value = PropertyValue("man");
  auto age = PropertyId::FromInt(3);
  auto age_value = PropertyValue(26);
  ASSERT_TRUE(props.SetProperty(id, id_value));
  ASSERT_TRUE(props.SetProperty(age, age_value));
  ASSERT_TRUE(props.SetProperty(completion_percentage, completion_percentage_value));
  ASSERT_TRUE(props.SetProperty(gender, gender_value));
  std::string serialized_props = memgraph::utils::SerializeProperties(props);
  PropertyStore deserialized_props = PropertyStore::CreateFromBuffer(serialized_props);

  for (const auto &[prop_id, prop_value] : props.Properties()) {
    ASSERT_TRUE(deserialized_props.IsPropertyEqual(prop_id, prop_value));
  }
}

TEST_F(RocksDBStorageTest, SerializePropertiesExternalBuffer) {
  PropertyStore props;
  auto id = PropertyId::FromInt(0);
  auto id_value = PropertyValue(1);
  auto completion_percentage = PropertyId::FromInt(1);
  auto completion_percentage_value = PropertyValue(14);
  auto gender = PropertyId::FromInt(2);
  // Use big value so that memory for properties is allocated on the heap not on the stack
  auto gender_value = PropertyValue("man167863816386826");
  auto age = PropertyId::FromInt(3);
  auto age_value = PropertyValue(26);
  ASSERT_TRUE(props.SetProperty(id, id_value));
  ASSERT_TRUE(props.SetProperty(age, age_value));
  ASSERT_TRUE(props.SetProperty(completion_percentage, completion_percentage_value));
  ASSERT_TRUE(props.SetProperty(gender, gender_value));
  std::string serialized_props = memgraph::utils::SerializeProperties(props);
  PropertyStore deserialized_props = PropertyStore::CreateFromBuffer(serialized_props);

  for (const auto &[prop_id, prop_value] : props.Properties()) {
    ASSERT_TRUE(deserialized_props.IsPropertyEqual(prop_id, prop_value));
  }
}

TEST(RocksDbSerDeSuite, ExtractVertexGidFromVertexKeyNoLabels) {
  auto gid = Gid::FromInt(1);
  Vertex vertex(gid, nullptr);
  std::string serializedVertex = memgraph::utils::SerializeVertex(vertex);

  ASSERT_EQ(memgraph::utils::ExtractGidFromKey(serializedVertex), "1");
}

TEST(RocksDbSerDeSuite, ExtractVertexGidFromVertexKeyWithOneLabel) {
  auto gid = Gid::FromInt(1);
  Vertex vertex(gid, nullptr);
  vertex.labels.push_back(LabelId::FromInt(2));
  std::string serializedVertex = memgraph::utils::SerializeVertex(vertex);

  ASSERT_EQ(memgraph::utils::ExtractGidFromKey(serializedVertex), "1");
}

TEST(RocksDbSerDeSuite, ExtractVertexGidFromVertexKeyWithMultipleLabels) {
  auto gid = Gid::FromInt(1);
  Vertex vertex(gid, nullptr);
  std::vector<unsigned> labels = {2, 3, 4};
  for (unsigned label : labels) {
    vertex.labels.push_back(LabelId::FromInt(label));
  }
  std::string serializedVertex = memgraph::utils::SerializeVertex(vertex);

  ASSERT_EQ(memgraph::utils::ExtractGidFromKey(serializedVertex), "1");
}

TEST(RocksDbSerDeSuite, ExtractLabelsFromMainDiskStorageWhenOnlyOneLabel) {
  auto gid = Gid::FromInt(1);
  Vertex vertex(gid, nullptr);
  std::vector<unsigned> labels = {2};
  std::vector<std::string> expectedLabelsStr = {"2"};
  for (unsigned label : labels) {
    vertex.labels.push_back(LabelId::FromInt(label));
  }
  std::string serializedVertex = memgraph::utils::SerializeVertex(vertex);

  ASSERT_EQ(memgraph::utils::ExtractLabelsFromMainDiskStorage(serializedVertex), expectedLabelsStr);
}

TEST(RocksDbSerDeSuite, ExtractLabelsFromMainDiskStorageWhenMultipleLabels) {
  auto gid = Gid::FromInt(1);
  Vertex vertex(gid, nullptr);
  std::vector<unsigned> labels = {2, 3, 4};
  std::vector<std::string> expectedLabelsStr = {"2", "3", "4"};
  for (unsigned label : labels) {
    vertex.labels.push_back(LabelId::FromInt(label));
  }
  std::string serializedVertex = memgraph::utils::SerializeVertex(vertex);

  ASSERT_EQ(memgraph::utils::ExtractLabelsFromMainDiskStorage(serializedVertex), expectedLabelsStr);
}

TEST(RocksDbSerDeSuite, ExtractVertexGidFromMainDiskStorageNoLabels) {
  auto gid = Gid::FromInt(1);
  Vertex vertex(gid, nullptr);
  std::string serializedVertex = memgraph::utils::SerializeVertex(vertex);

  ASSERT_EQ(memgraph::utils::ExtractGidFromMainDiskStorage(serializedVertex), "1");
}

TEST(RocksDbSerDeSuite, ExtractVertexGidFromMainDiskStorageWithOneLabel) {
  auto gid = Gid::FromInt(1);
  Vertex vertex(gid, nullptr);
  vertex.labels.push_back(LabelId::FromInt(2));
  std::string serializedVertex = memgraph::utils::SerializeVertex(vertex);

  ASSERT_EQ(memgraph::utils::ExtractGidFromMainDiskStorage(serializedVertex), "1");
}

TEST(RocksDbSerDeSuite, ExtractVertexGidFromMainDiskStorageWithMultipleLabels) {
  auto gid = Gid::FromInt(1);
  Vertex vertex(gid, nullptr);
  vertex.labels.push_back(LabelId::FromInt(2));
  vertex.labels.push_back(LabelId::FromInt(3));
  vertex.labels.push_back(LabelId::FromInt(4));
  std::string serializedVertex = memgraph::utils::SerializeVertex(vertex);

  ASSERT_EQ(memgraph::utils::ExtractGidFromLabelIndexStorage(serializedVertex), "1");
}

TEST(RocksDbSerDeSuite, ExtractGidFromLabelIndexStorageKey) {
  auto gid = Gid::FromInt(1);
  LabelId label = LabelId::FromInt(2);
  std::string serializedVertex = memgraph::utils::SerializeVertexAsKeyForLabelIndex(label, gid);

  ASSERT_EQ(memgraph::utils::ExtractGidFromLabelIndexStorage(serializedVertex), "1");
}

TEST(RocksDbSerDeSuite, DeserializePropertiesFromLabelIndexStorage) {
  std::string expectedGid = "1";
  LabelId indexingLabel = LabelId::FromInt(2);
  std::vector<LabelId> labels = {LabelId::FromInt(3), LabelId::FromInt(4)};
  std::map<PropertyId, PropertyValue> properties = {{PropertyId::FromInt(5), PropertyValue("5")},
                                                    {PropertyId::FromInt(6), PropertyValue("6")}};
  PropertyStore propertyStore;
  propertyStore.InitProperties(properties);
  std::string serializedVertex =
      memgraph::utils::SerializeVertexAsValueForLabelIndex(indexingLabel, labels, propertyStore);

  ASSERT_EQ(memgraph::utils::DeserializePropertiesFromLabelIndexStorage(serializedVertex).StringBuffer(),
            propertyStore.StringBuffer());
}

TEST(RocksDbSerDeSuite, DeserializePropertiesFromLabelPropertyIndexStorage) {
  std::string expectedGid = "1";
  LabelId indexingLabel = LabelId::FromInt(2);
  std::vector<LabelId> labels = {LabelId::FromInt(3), LabelId::FromInt(4)};
  std::map<PropertyId, PropertyValue> properties = {{PropertyId::FromInt(5), PropertyValue("5")},
                                                    {PropertyId::FromInt(6), PropertyValue("6")}};
  PropertyStore propertyStore;
  propertyStore.InitProperties(properties);
  std::string serializedVertex =
      memgraph::utils::SerializeVertexAsValueForLabelPropertyIndex(indexingLabel, labels, propertyStore);

  ASSERT_EQ(memgraph::utils::DeserializePropertiesFromLabelPropertyIndexStorage(serializedVertex).StringBuffer(),
            propertyStore.StringBuffer());
}

TEST(RocksDbSerDeSuite, ExtractGidFromLabelPropertyIndexStorageKey) {
  auto gid = Gid::FromInt(1);
  LabelId label = LabelId::FromInt(2);
  PropertyId property = PropertyId::FromInt(3);
  std::string serializedVertex = memgraph::utils::SerializeVertexAsKeyForLabelPropertyIndex(label, property, gid);

  ASSERT_EQ(memgraph::utils::ExtractGidFromLabelPropertyIndexStorage(serializedVertex), "1");
}

TEST(RocksDbSerDeSuite, ExtractGidFromUniqueConstraintStorageKey) {
  std::string expectedGid = "1";
  LabelId constraintLabel = LabelId::FromInt(2);
  std::set<PropertyId> properties = {PropertyId::FromInt(3), PropertyId::FromInt(4)};
  std::string serializedVertex =
      memgraph::utils::SerializeVertexAsKeyForUniqueConstraint(constraintLabel, properties, expectedGid);

  ASSERT_EQ(memgraph::utils::ExtractGidFromUniqueConstraintStorage(serializedVertex), expectedGid);
}

TEST(RocksDbSerDeSuite, DeserializeConstraintLabelFromUniqueConstraintStorage) {
  std::string expectedGid = "1";
  LabelId constraintLabel = LabelId::FromInt(2);
  std::set<PropertyId> properties = {PropertyId::FromInt(3), PropertyId::FromInt(4)};
  std::string serializedVertex =
      memgraph::utils::SerializeVertexAsKeyForUniqueConstraint(constraintLabel, properties, expectedGid);

  ASSERT_EQ(memgraph::utils::DeserializeConstraintLabelFromUniqueConstraintStorage(serializedVertex), constraintLabel);
}

TEST(RocksDbSerDeSuite, DeserializePropertiesFromUniqueConstraintStorage) {
  std::string expectedGid = "1";
  LabelId constraintLabel = LabelId::FromInt(2);
  std::vector<LabelId> labels = {LabelId::FromInt(3), LabelId::FromInt(4)};
  std::map<PropertyId, PropertyValue> properties = {{PropertyId::FromInt(5), PropertyValue("5")},
                                                    {PropertyId::FromInt(6), PropertyValue("6")}};
  PropertyStore propertyStore;
  propertyStore.InitProperties(properties);
  std::string serializedVertex =
      memgraph::utils::SerializeVertexAsValueForUniqueConstraint(constraintLabel, labels, propertyStore);

  ASSERT_EQ(memgraph::utils::DeserializePropertiesFromUniqueConstraintStorage(serializedVertex).StringBuffer(),
            propertyStore.StringBuffer());
}
