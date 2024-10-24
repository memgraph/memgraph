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

#include <cstdint>
#include <string>

#include <sys/types.h>
#include "gtest/gtest.h"
#include "query/db_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"

const std::string_view test_index = "test_index";
const std::string_view test_label = "test_label";
const std::string_view test_property = "test_property";

template <typename StorageType>
class VectorSearchTest : public testing::Test {
 public:
  const std::string testSuite = "vector_search";
  std::unique_ptr<memgraph::storage::Storage> db = std::make_unique<memgraph::storage::InMemoryStorage>();

  void CreateTestIndex(memgraph::storage::VectorIndex &index, std::size_t dimension = 5, std::size_t limit = 10) {
    auto storage_dba = db->Access();
    memgraph::query::DbAccessor dba(storage_dba.get());
    const auto label = dba.NameToLabel(test_label.data());
    const auto property = dba.NameToProperty(test_property.data());

    // Build JSON object dynamically
    nlohmann::json config;
    config["dimension"] = dimension;
    config["limit"] = limit;

    memgraph::storage::VectorIndexSpec spec{
        .index_name = test_index.data(),
        .label = label,
        .property = property,
        .config = config,  // Pass the dynamically created config
    };

    index.CreateIndex(spec);
  }

  std::vector<memgraph::storage::PropertyValue> ConvertToPropertyValueVector(const std::vector<float> &float_list) {
    std::vector<memgraph::storage::PropertyValue> property_values;
    property_values.reserve(float_list.size());  // Reserve space for efficiency

    std::ranges::transform(float_list, std::back_inserter(property_values),
                           [](float value) { return memgraph::storage::PropertyValue(value); });

    return property_values;
  }

  memgraph::storage::Gid AddNodeToIndex(memgraph::storage::VectorIndex &index,
                                        const std::vector<memgraph::storage::PropertyValue> &properties,
                                        uint64_t commit_timestamp) {
    auto storage_dba = db->Access();
    memgraph::query::DbAccessor dba(storage_dba.get());

    auto vertex_accessor = storage_dba->CreateVertex();
    const auto label_id = dba.NameToLabel(test_label);
    const auto property_id = dba.NameToProperty(test_property);

    vertex_accessor.AddLabel(label_id);
    vertex_accessor.SetProperty(property_id, memgraph::storage::PropertyValue(properties));
    const auto label_prop =
        memgraph::storage::LabelPropKey(dba.NameToLabel(test_label), dba.NameToProperty(test_property));
    index.AddNodeToIndex(vertex_accessor.vertex_, label_prop, commit_timestamp);

    return vertex_accessor.Gid();
  }
};

TYPED_TEST_SUITE(VectorSearchTest, memgraph::storage::InMemoryStorage);

TYPED_TEST(VectorSearchTest, CreateIndexTest) {
  memgraph::storage::VectorIndex index;
  this->CreateTestIndex(index);

  EXPECT_EQ(index.ListAllIndices().size(), 1);
}

TYPED_TEST(VectorSearchTest, AddNodeTest) {
  memgraph::storage::VectorIndex index;
  this->CreateTestIndex(index);

  const auto properties = this->ConvertToPropertyValueVector({1.0, 2.0, 3.0, 4.0, 5.0});
  this->AddNodeToIndex(index, properties, 0);

  EXPECT_EQ(index.Size(test_index), 1);
}

TYPED_TEST(VectorSearchTest, SimpleSearchTest) {
  memgraph::storage::VectorIndex index;
  this->CreateTestIndex(index);

  const auto properties = this->ConvertToPropertyValueVector({1.0, 2.0, 3.0, 4.0, 5.0});
  const auto vertex_gid = this->AddNodeToIndex(index, properties, 0);

  std::vector<float> query = {1.0, 2.0, 3.0, 4.0, 5.0};
  const auto &result = index.Search(test_index, 1, 1, query);
  EXPECT_EQ(result.size(), 1);

  const auto &[gid, score] = result[0];
  EXPECT_EQ(gid, vertex_gid);
  EXPECT_EQ(score, 0.0);
}

TYPED_TEST(VectorSearchTest, SearchWithMultipleNodes) {
  memgraph::storage::VectorIndex index;
  this->CreateTestIndex(index);

  const auto properties1 = this->ConvertToPropertyValueVector({1.0, 2.0, 3.0, 4.0, 5.0});
  const auto vertex_gid1 = this->AddNodeToIndex(index, properties1, 0);

  const auto properties2 = this->ConvertToPropertyValueVector({10.0, 11.0, 12.0, 13.0, 14.0});
  const auto vertex_gid2 = this->AddNodeToIndex(index, properties2, 0);

  EXPECT_EQ(index.Size(test_index), 2);
  std::vector<float> query = {10.0, 11.0, 12.0, 13.0, 14.0};

  // Search for one node
  const auto &result = index.Search(test_index, 1, 1, query);
  EXPECT_EQ(result.size(), 1);

  const auto &[gid, score] = result[0];
  EXPECT_EQ(gid, vertex_gid2);

  // Search for two nodes
  const auto &result2 = index.Search(test_index, 1, 2, query);
  EXPECT_EQ(result2.size(), 2);
}

TYPED_TEST(VectorSearchTest, TransactionTest) {
  memgraph::storage::VectorIndex index;
  this->CreateTestIndex(index);

  const auto properties1 = this->ConvertToPropertyValueVector({1.0, 2.0, 3.0, 4.0, 5.0});
  const auto vertex_gid1 = this->AddNodeToIndex(index, properties1, 0);

  const auto properties2 = this->ConvertToPropertyValueVector({10.0, 11.0, 12.0, 13.0, 14.0});
  const auto vertex_gid2 = this->AddNodeToIndex(index, properties2, 5);

  // vertex_accessor2 is not visible even though it would be the closest match
  std::vector<float> query = {10.0, 11.0, 12.0, 13.0, 14.0};
  const auto &result = index.Search(test_index, 1, 1, query);
  EXPECT_EQ(result.size(), 1);

  const auto &[gid, score] = result[0];
  EXPECT_EQ(gid, vertex_gid1);

  // vertex_accessor2 is visible
  const auto &result2 = index.Search(test_index, 6, 1, query);
  EXPECT_EQ(result2.size(), 1);

  const auto &[gid2, score2] = result2[0];
  EXPECT_EQ(gid2, vertex_gid2);
}

TYPED_TEST(VectorSearchTest, ConcurrencyTest) {
  memgraph::storage::VectorIndex index;
  const auto index_size = 10;
  this->CreateTestIndex(index, 5, index_size);

  // create 1k threads and add 1k nodes
  std::vector<std::thread> threads;
  threads.reserve(index_size);
  for (int i = 0; i < index_size; i++) {
    threads.emplace_back(std::thread([this, &index, i]() {
      // properties start from i and end at i + 5
      const auto properties =
          this->ConvertToPropertyValueVector({(float)i, (float)i + 1, (float)i + 2, (float)i + 3, (float)i + 4});
      this->AddNodeToIndex(index, properties, i);
    }));
  }

  for (auto &thread : threads) {
    thread.join();
  }

  EXPECT_EQ(index.Size(test_index), index_size);
}
