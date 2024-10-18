// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <string>

#include "gtest/gtest.h"
#include "query/db_accessor.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/transaction.hpp"
#include "tests/unit/disk_test_utils.hpp"

template <typename StorageType>
class VectorSearchTest : public testing::Test {
 public:
  const std::string testSuite = "vector_search";
  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  std::unique_ptr<memgraph::storage::Storage> db = std::make_unique<memgraph::storage::InMemoryStorage>(config);
};

TYPED_TEST_SUITE(VectorSearchTest, memgraph::storage::InMemoryStorage);

TYPED_TEST(VectorSearchTest, CreateIndexTest) {
  auto storage_dba = this->db->Access();
  memgraph::query::DbAccessor dba(storage_dba.get());
  const auto label = dba.NameToLabel("test_label");
  const auto property = dba.NameToProperty("test_property");
  memgraph::storage::VectorIndexSpec spec{
      .index_name = "test_index",
      .label = label,
      .property = property,
      .config = nlohmann::json::parse(R"({"size": 10, "limit": 10})"),
  };

  memgraph::storage::VectorIndex index;
  index.CreateIndex(spec);

  EXPECT_EQ(index.ListAllIndices().size(), 1);
}

TYPED_TEST(VectorSearchTest, AddNodeTest) {
  auto storage_dba = this->db->Access();
  memgraph::query::DbAccessor dba(storage_dba.get());
  const auto label = dba.NameToLabel("test_label");
  const auto property = dba.NameToProperty("test_property");
  memgraph::storage::VectorIndexSpec spec{
      .index_name = "test_index",
      .label = label,
      .property = property,
      .config = nlohmann::json::parse(R"({"size": 5, "limit": 10})"),
  };

  memgraph::storage::VectorIndex index;
  index.CreateIndex(spec);

  EXPECT_EQ(index.ListAllIndices().size(), 1);

  auto vertex_accessor = storage_dba->CreateVertex();
  vertex_accessor.AddLabel(label);
  std::vector<memgraph::storage::PropertyValue> vec = {
      memgraph::storage::PropertyValue(1.0), memgraph::storage::PropertyValue(2.0),
      memgraph::storage::PropertyValue(3.0), memgraph::storage::PropertyValue(4.0),
      memgraph::storage::PropertyValue(5.0)};
  vertex_accessor.SetProperty(property, memgraph::storage::PropertyValue(vec));
  const auto label_prop = memgraph::storage::LabelPropKey(label, property);
  index.AddNodeToIndex(vertex_accessor.vertex_, label_prop, 0);

  EXPECT_EQ(index.Size("test_index"), 1);
}

TYPED_TEST(VectorSearchTest, SimpleSearchTest) {
  auto storage_dba = this->db->Access();
  memgraph::query::DbAccessor dba(storage_dba.get());
  const auto label = dba.NameToLabel("test_label");
  const auto property = dba.NameToProperty("test_property");
  memgraph::storage::VectorIndexSpec spec{
      .index_name = "test_index",
      .label = label,
      .property = property,
      .config = nlohmann::json::parse(R"({"size": 5, "limit": 10})"),
  };

  memgraph::storage::VectorIndex index;
  index.CreateIndex(spec);

  EXPECT_EQ(index.ListAllIndices().size(), 1);

  auto vertex_accessor = storage_dba->CreateVertex();
  vertex_accessor.AddLabel(label);
  std::vector<memgraph::storage::PropertyValue> vec = {
      memgraph::storage::PropertyValue(1.0), memgraph::storage::PropertyValue(2.0),
      memgraph::storage::PropertyValue(3.0), memgraph::storage::PropertyValue(4.0),
      memgraph::storage::PropertyValue(5.0)};
  vertex_accessor.SetProperty(property, memgraph::storage::PropertyValue(vec));
  const auto label_prop = memgraph::storage::LabelPropKey(label, property);
  index.AddNodeToIndex(vertex_accessor.vertex_, label_prop, 0);

  EXPECT_EQ(index.Size("test_index"), 1);
  std::vector<float> query = {1.0, 2.0, 3.0, 4.0, 5.0};
  const auto &result = index.Search("test_index", 1, 1, query);
  EXPECT_EQ(result.size(), 1);

  const auto &vertex = result[0];
  EXPECT_EQ(vertex->gid, vertex_accessor.Gid());
}

TYPED_TEST(VectorSearchTest, SearchWithMultipleNodes) {
  auto storage_dba = this->db->Access();
  memgraph::query::DbAccessor dba(storage_dba.get());
  const auto label = dba.NameToLabel("test_label");
  const auto property = dba.NameToProperty("test_property");
  memgraph::storage::VectorIndexSpec spec{
      .index_name = "test_index",
      .label = label,
      .property = property,
      .config = nlohmann::json::parse(R"({"size": 5, "limit": 10})"),
  };

  memgraph::storage::VectorIndex index;
  index.CreateIndex(spec);

  EXPECT_EQ(index.ListAllIndices().size(), 1);

  auto vertex_accessor = storage_dba->CreateVertex();
  vertex_accessor.AddLabel(label);
  std::vector<memgraph::storage::PropertyValue> vec = {
      memgraph::storage::PropertyValue(1.0), memgraph::storage::PropertyValue(2.0),
      memgraph::storage::PropertyValue(3.0), memgraph::storage::PropertyValue(4.0),
      memgraph::storage::PropertyValue(5.0)};
  vertex_accessor.SetProperty(property, memgraph::storage::PropertyValue(vec));
  const auto label_prop = memgraph::storage::LabelPropKey(label, property);

  index.AddNodeToIndex(vertex_accessor.vertex_, label_prop, 0);

  auto vertex_accessor2 = storage_dba->CreateVertex();
  vertex_accessor2.AddLabel(label);
  std::vector<memgraph::storage::PropertyValue> vec2 = {
      memgraph::storage::PropertyValue(10.0), memgraph::storage::PropertyValue(11.0),
      memgraph::storage::PropertyValue(12.0), memgraph::storage::PropertyValue(13.0),
      memgraph::storage::PropertyValue(14.0)};
  vertex_accessor2.SetProperty(property, memgraph::storage::PropertyValue(vec2));
  const auto label_prop2 = memgraph::storage::LabelPropKey(label, property);

  index.AddNodeToIndex(vertex_accessor2.vertex_, label_prop2, 0);

  EXPECT_EQ(index.Size("test_index"), 2);
  std::vector<float> query = {10.0, 11.0, 12.0, 13.0, 14.0};
  const auto &result = index.Search("test_index", 1, 1, query);
  EXPECT_EQ(result.size(), 1);

  const auto &vertex = result[0];
  EXPECT_EQ(vertex->gid, vertex_accessor2.Gid());
}

TYPED_TEST(VectorSearchTest, TransactionTest) {
  auto storage_dba = this->db->Access();
  memgraph::query::DbAccessor dba(storage_dba.get());
  const auto label = dba.NameToLabel("test_label");
  const auto property = dba.NameToProperty("test_property");
  memgraph::storage::VectorIndexSpec spec{
      .index_name = "test_index",
      .label = label,
      .property = property,
      .config = nlohmann::json::parse(R"({"size": 5, "limit": 10})"),
  };

  memgraph::storage::VectorIndex index;
  index.CreateIndex(spec);

  EXPECT_EQ(index.ListAllIndices().size(), 1);

  auto vertex_accessor1 = storage_dba->CreateVertex();
  vertex_accessor1.AddLabel(label);
  std::vector<memgraph::storage::PropertyValue> vec = {
      memgraph::storage::PropertyValue(1.0), memgraph::storage::PropertyValue(2.0),
      memgraph::storage::PropertyValue(3.0), memgraph::storage::PropertyValue(4.0),
      memgraph::storage::PropertyValue(5.0)};
  vertex_accessor1.SetProperty(property, memgraph::storage::PropertyValue(vec));
  const auto label_prop = memgraph::storage::LabelPropKey(label, property);

  index.AddNodeToIndex(vertex_accessor1.vertex_, label_prop, 0);

  auto vertex_accessor2 = storage_dba->CreateVertex();
  vertex_accessor2.AddLabel(label);
  std::vector<memgraph::storage::PropertyValue> vec2 = {
      memgraph::storage::PropertyValue(10.0), memgraph::storage::PropertyValue(11.0),
      memgraph::storage::PropertyValue(12.0), memgraph::storage::PropertyValue(13.0),
      memgraph::storage::PropertyValue(14.0)};

  vertex_accessor2.SetProperty(property, memgraph::storage::PropertyValue(vec2));
  const auto label_prop2 = memgraph::storage::LabelPropKey(label, property);
  index.AddNodeToIndex(vertex_accessor2.vertex_, label_prop2, 5);

  // vertex_accessor1 is not visible
  std::vector<float> query = {10.0, 11.0, 12.0, 13.0, 14.0};
  const auto &result = index.Search("test_index", 1, 2, query);
  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(result[0]->gid, vertex_accessor1.Gid());

  // vertex_accessor2 is visible
  const auto &result2 = index.Search("test_index", 6, 2, query);
  EXPECT_EQ(result2.size(), 2);
}
