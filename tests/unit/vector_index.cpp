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
#include "storage/v2/indices/vector_index.hpp"
#include <sys/types.h>
#include "gtest/gtest.h"
#include "query/db_accessor.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace memgraph::storage;

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define ASSERT_NO_ERROR(result) ASSERT_FALSE((result).HasError())

constexpr std::string_view test_index = "test_index";
constexpr std::string_view test_label = "test_label";
constexpr std::string_view test_property = "test_property";

template <typename StorageType>
class VectorSearchTest : public testing::Test {
 public:
  static constexpr std::string_view testSuite = "vector_search";
  std::unique_ptr<Storage> storage = std::make_unique<InMemoryStorage>();

  void CreateIndex(const auto dimension = 2, const auto limit = 10) {
    // only requirement is that this is not empty at the moment since it is not used in the test but flag is checked
    // through the code
    FLAGS_experimental_vector_indexes = R"(test_index__test_label__test_property__{"dimension": 2, "limit": 10})";
    auto storage_dba = storage->Access();
    memgraph::query::DbAccessor dba(storage_dba.get());
    const auto label = dba.NameToLabel(test_label.data());
    const auto property = dba.NameToProperty(test_property.data());

    // Build JSON object dynamically
    nlohmann::json config;
    config["dimension"] = dimension;
    config["limit"] = limit;

    VectorIndexSpec spec{
        .index_name = test_index.data(),
        .label = label,
        .property = property,
        .config = config,  // Pass the dynamically created config
    };

    storage_dba->CreateVectorIndex(spec);
  }

  VertexAccessor CreateVertex(Storage::Accessor *accessor, const PropertyValue &property, const LabelId &label) {
    VertexAccessor vertex = accessor->CreateVertex();
    // NOLINTBEGIN
    MG_ASSERT(!vertex.AddLabel(label).HasError());
    MG_ASSERT(!vertex.SetProperty(accessor->NameToProperty(test_property), property).HasError());
    // NOLINTEND

    return vertex;
  }
};

TYPED_TEST_SUITE(VectorSearchTest, InMemoryStorage);

TYPED_TEST(VectorSearchTest, SimpleAddNodeTest) {
  this->CreateIndex();
  auto acc = this->storage->Access();

  PropertyValue property_value(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  this->CreateVertex(acc.get(), property_value, acc->NameToLabel(test_label));
  ASSERT_NO_ERROR(acc->Commit());
  const auto vector_index_info = acc->ListAllVectorIndices();
  EXPECT_EQ(vector_index_info[0].size, 1);
}

TYPED_TEST(VectorSearchTest, SimpleSearchTest) {
  this->CreateIndex();
  auto acc = this->storage->Access();

  PropertyValue property_value(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  const auto vertex = this->CreateVertex(acc.get(), property_value, acc->NameToLabel(test_label));
  ASSERT_NO_ERROR(acc->Commit());

  const auto result = acc->VectorIndexSearch(test_index.data(), 1, std::vector<float>{1.0, 1.0});
  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].first, vertex.Gid());
}

TYPED_TEST(VectorSearchTest, SearchWithMultipleNodes) {
  this->CreateIndex();
  auto acc = this->storage->Access();

  PropertyValue properties1(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  [[maybe_unused]] const auto vertex1 = this->CreateVertex(acc.get(), properties1, acc->NameToLabel(test_label));

  PropertyValue properties2(std::vector<PropertyValue>{PropertyValue(10.0), PropertyValue(10.0)});
  const auto vertex2 = this->CreateVertex(acc.get(), properties2, acc->NameToLabel(test_label));
  ASSERT_NO_ERROR(acc->Commit());

  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 2);

  std::vector<float> query = {10.0, 10.0};

  // Perform search for one closest node
  const auto result = acc->VectorIndexSearch(test_index.data(), 1, query);
  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].first, vertex2.Gid());  // Expect the closest match to be vertex2

  // Perform search for two closest nodes
  const auto result2 = acc->VectorIndexSearch(test_index.data(), 2, query);
  EXPECT_EQ(result2.size(), 2);
}

TYPED_TEST(VectorSearchTest, ConcurrencyTest) {
  this->CreateIndex();
  auto acc = this->storage->Access();

  constexpr auto index_size = 10;

  // Create 1k threads to add 1k nodes
  std::vector<std::thread> threads;
  threads.reserve(index_size);
  for (int i = 0; i < index_size; i++) {
    threads.emplace_back(std::thread([this, &acc, i]() {
      // Properties start from i and end at i + 1 (2-dimensional vector)
      PropertyValue properties(
          std::vector<PropertyValue>{PropertyValue(static_cast<double>(i)), PropertyValue(static_cast<double>(i + 1))});

      // Each thread adds a node to the index
      [[maybe_unused]] const auto vertex = this->CreateVertex(acc.get(), properties, acc->NameToLabel(test_label));
    }));
  }

  // Join all threads to ensure all nodes are added
  for (auto &thread : threads) {
    thread.join();
  }

  // Commit the changes made by each thread
  ASSERT_NO_ERROR(acc->Commit());

  // Check that the index has the expected number of entries
  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, index_size);
}

TYPED_TEST(VectorSearchTest, SimpleAbortTest) {
  this->CreateIndex();
  auto acc = this->storage->Access();
  constexpr auto index_size = 10;

  // Create multiple nodes within a transaction that will be aborted
  for (int i = 0; i < index_size; i++) {
    PropertyValue properties(
        std::vector<PropertyValue>{PropertyValue(static_cast<double>(i)), PropertyValue(static_cast<double>(i + 1))});

    // Add each node to the index
    [[maybe_unused]] const auto vertex = this->CreateVertex(acc.get(), properties, acc->NameToLabel(test_label));
  }
  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, index_size);

  // Abort the transaction
  acc->Abort();

  // Expect the index to have 0 entries, as the transaction was aborted
  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);
}
