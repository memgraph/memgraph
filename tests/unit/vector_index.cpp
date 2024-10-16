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
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "tests/unit/disk_test_utils.hpp"

template <typename StorageType>
class VectorSearchTest : public testing::Test {
 public:
  const std::string testSuite = "vector_search";
  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  std::unique_ptr<memgraph::storage::Storage> db = std::make_unique<StorageType>(config);

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_SUITE(VectorSearchTest, StorageTypes);

TYPED_TEST(VectorSearchTest, CreateIndexTest) {
  auto storage_dba = this->db->Access();
  memgraph::query::DbAccessor dba(storage_dba.get());
  const auto label = dba.NameToLabel("test_label");
  const auto property = dba.NameToProperty("test_property");
  memgraph::storage::VectorIndexSpec spec{
      .index_name = "test_index",
      .label = label,
      .property = property,
      .config = nlohmann::json::parse(R"({"size": 10})"),
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
      .config = nlohmann::json::parse(R"({"size": 5})"),
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
  index.AddNode(vertex_accessor.vertex_, 0);

  EXPECT_EQ(index.Size("test_index"), 1);
}
