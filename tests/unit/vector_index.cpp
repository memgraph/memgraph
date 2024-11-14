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
#include <sys/types.h>
#include <stdexcept>
#include <string_view>
#include <thread>

#include "flags/experimental.hpp"
#include "gtest/gtest.h"
#include "query/db_accessor.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"

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
  std::unique_ptr<Storage> storage;

  void SetUp() override { storage = std::make_unique<InMemoryStorage>(); }

  void TearDown() override {
    storage.reset();
    memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);
  }

  void CreateIndex(std::size_t dimension, std::size_t limit) {
    memgraph::flags::SetExperimental(memgraph::flags::Experiments::VECTOR_SEARCH);

    auto storage_dba = storage->Access();
    memgraph::query::DbAccessor dba(storage_dba.get());
    const auto label = dba.NameToLabel(test_label.data());
    const auto property = dba.NameToProperty(test_property.data());

    // Create a specification for the index
    const auto spec = VectorIndexSpec{test_index.data(), label, property, "l2sq", "f32", dimension, limit};
    storage_dba->CreateVectorIndex(spec);
  }

  VertexAccessor CreateVertex(Storage::Accessor *accessor, std::string_view property,
                              const PropertyValue &property_value, std::string_view label) {
    VertexAccessor vertex = accessor->CreateVertex();
    // NOLINTBEGIN
    MG_ASSERT(!vertex.AddLabel(accessor->NameToLabel(label)).HasError());
    MG_ASSERT(!vertex.SetProperty(accessor->NameToProperty(property), property_value).HasError());
    // NOLINTEND

    return vertex;
  }
};

TYPED_TEST_SUITE(VectorSearchTest, InMemoryStorage);

TYPED_TEST(VectorSearchTest, SimpleAddNodeTest) {
  this->CreateIndex(2, 10);
  auto acc = this->storage->Access();

  PropertyValue property_value(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});

  // should end up in the index
  this->CreateVertex(acc.get(), test_property, property_value, test_label);

  // should not end up in the index
  this->CreateVertex(acc.get(), "wrong_property", property_value, test_label);

  // should not end up in the index
  this->CreateVertex(acc.get(), test_property, property_value, "wrong_label");

  ASSERT_NO_ERROR(acc->Commit());
  const auto vector_index_info = acc->ListAllVectorIndices();
  EXPECT_EQ(vector_index_info[0].size, 1);
}

TYPED_TEST(VectorSearchTest, SimpleSearchTest) {
  this->CreateIndex(2, 10);
  auto acc = this->storage->Access();

  PropertyValue property_value(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  const auto vertex = this->CreateVertex(acc.get(), test_property, property_value, test_label);
  ASSERT_NO_ERROR(acc->Commit());

  const auto result = acc->VectorIndexSearch(test_index.data(), 1, std::vector<float>{1.0, 1.0});
  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(std::get<0>(result[0]).vertex_->gid, vertex.Gid());
}

TYPED_TEST(VectorSearchTest, HighDimensionalSearchTest) {
  // Create index with high dimension
  this->CreateIndex(1000, 2);
  auto acc = this->storage->Access();

  std::vector<PropertyValue> high_dim_vector(1000, PropertyValue(1.0));
  PropertyValue property_value(high_dim_vector);
  const auto vertex = this->CreateVertex(acc.get(), test_property, property_value, test_label);
  ASSERT_NO_ERROR(acc->Commit());

  std::vector<float> query_vector(1000, 1.0);
  const auto result = acc->VectorIndexSearch(test_index.data(), 1, query_vector);
  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(std::get<0>(result[0]).vertex_->gid, vertex.Gid());
}

TYPED_TEST(VectorSearchTest, InvalidDimensionTest) {
  this->CreateIndex(2, 10);
  auto acc = this->storage->Access();

  // Create a node with a property that has a different dimension than the index
  std::vector<PropertyValue> properties(3, PropertyValue(1.0));
  PropertyValue property_value(properties);

  EXPECT_THROW(this->CreateVertex(acc.get(), test_property, property_value, test_label), std::invalid_argument);
}

TYPED_TEST(VectorSearchTest, SearchWithMultipleNodes) {
  this->CreateIndex(2, 10);
  auto acc = this->storage->Access();

  PropertyValue properties1(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  [[maybe_unused]] const auto vertex1 = this->CreateVertex(acc.get(), test_property, properties1, test_label);

  PropertyValue properties2(std::vector<PropertyValue>{PropertyValue(10.0), PropertyValue(10.0)});
  const auto vertex2 = this->CreateVertex(acc.get(), test_property, properties2, test_label);
  ASSERT_NO_ERROR(acc->Commit());

  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 2);

  std::vector<float> query = {10.0, 10.0};

  // Perform search for one closest node
  const auto result = acc->VectorIndexSearch(test_index.data(), 1, query);
  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(std::get<0>(result[0]).vertex_->gid, vertex2.Gid());  // Expect the second vertex to be the closest

  // Perform search for two closest nodes
  const auto result2 = acc->VectorIndexSearch(test_index.data(), 2, query);
  EXPECT_EQ(result2.size(), 2);
}

TYPED_TEST(VectorSearchTest, ConcurrencyTest) {
  this->CreateIndex(2, 10);

  const auto index_size = std::thread::hardware_concurrency();  // default value for the number of threads in the pool

  std::vector<std::thread> threads;
  threads.reserve(index_size);
  for (int i = 0; i < index_size; i++) {
    threads.emplace_back(std::thread([this, i]() {
      auto acc = this->storage->Access();
      PropertyValue properties(
          std::vector<PropertyValue>{PropertyValue(static_cast<double>(i)), PropertyValue(static_cast<double>(i + 1))});

      // Each thread adds a node to the index
      [[maybe_unused]] const auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
      ASSERT_NO_ERROR(acc->Commit());
    }));
  }

  for (auto &thread : threads) {
    thread.join();
  }

  auto acc = this->storage->Access();
  // Check that the index has the expected number of entries
  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, index_size);
}

TYPED_TEST(VectorSearchTest, UpdatePropertyValueTest) {
  this->CreateIndex(2, 10);
  auto acc = this->storage->Access();

  // Create initial vertex
  PropertyValue initial_value(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  auto vertex = this->CreateVertex(acc.get(), test_property, initial_value, test_label);
  ASSERT_NO_ERROR(acc->Commit());

  // Update property value
  acc = this->storage->Access();
  PropertyValue updated_value(std::vector<PropertyValue>{PropertyValue(2.0), PropertyValue(2.0)});
  MG_ASSERT(!vertex.SetProperty(acc->NameToProperty(test_property), updated_value).HasError());  // NOLINT
  ASSERT_NO_ERROR(acc->Commit());

  // Verify update with search
  const auto search_result = acc->VectorIndexSearch(test_index.data(), 1, std::vector<float>{2.0, 2.0});
  EXPECT_EQ(search_result.size(), 1);
  EXPECT_EQ(std::get<0>(search_result[0]).vertex_->properties.GetProperty(acc->NameToProperty(test_property)),
            updated_value);
}

TYPED_TEST(VectorSearchTest, DeleteVertexTest) {
  this->CreateIndex(2, 10);
  auto acc = this->storage->Access();

  PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);

  // Delete the vertex
  auto maybe_deleted_vertex = acc->DeleteVertex(&vertex);
  EXPECT_EQ(maybe_deleted_vertex.HasValue(), true);
  ASSERT_NO_ERROR(acc->Commit());

  // Verify that the vertex was deleted
  std::vector<float> query = {1.0, 1.0};
  const auto result = acc->VectorIndexSearch(test_index.data(), 1, query);
  EXPECT_EQ(result.size(), 0);
}

TYPED_TEST(VectorSearchTest, SimpleAbortTest) {
  this->CreateIndex(2, 10);
  auto acc = this->storage->Access();
  static constexpr auto index_size = 10;  // has to be equal or less than the limit of the index

  // Create multiple nodes within a transaction that will be aborted
  for (int i = 0; i < index_size; i++) {
    PropertyValue properties(
        std::vector<PropertyValue>{PropertyValue(static_cast<double>(i)), PropertyValue(static_cast<double>(i + 1))});
    // Add each node to the index
    [[maybe_unused]] const auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
  }

  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, index_size);
  acc->Abort();

  // Expect the index to have 0 entries, as the transaction was aborted
  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);
}

TYPED_TEST(VectorSearchTest, MultipleAbortsAndUpdatesTest) {
  this->CreateIndex(2, 10);
  auto acc = this->storage->Access();

  PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
  ASSERT_NO_ERROR(acc->Commit());

  // Remove label and then abort
  acc = this->storage->Access();
  vertex = acc->FindVertex(vertex.Gid(), View::OLD).value();
  MG_ASSERT(!vertex.RemoveLabel(acc->NameToLabel(test_label)).HasError());  // NOLINT
  acc->Abort();

  // Expect the index to have 1 entry, as the transaction was aborted
  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 1);

  // Remove property and then abort
  acc = this->storage->Access();
  vertex = acc->FindVertex(vertex.Gid(), View::OLD).value();
  PropertyValue null_value;
  MG_ASSERT(!vertex.SetProperty(acc->NameToProperty(test_property), null_value).HasError());  // NOLINT
  acc->Abort();

  // Expect the index to have 1 entry, as the transaction was aborted
  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 1);

  // Remove label and then commit
  acc = this->storage->Access();
  vertex = acc->FindVertex(vertex.Gid(), View::OLD).value();
  MG_ASSERT(!vertex.RemoveLabel(acc->NameToLabel(test_label)).HasError());  // NOLINT
  ASSERT_NO_ERROR(acc->Commit());

  // Expect the index to have 0 entries, as the transaction was committed
  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);

  // At this point, the vertex has no label but has a property

  // Add label and then abort
  acc = this->storage->Access();
  vertex = acc->FindVertex(vertex.Gid(), View::OLD).value();
  MG_ASSERT(!vertex.AddLabel(acc->NameToLabel(test_label)).HasError());  // NOLINT
  acc->Abort();

  // Expect the index to have 0 entries, as the transaction was aborted
  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);

  // Add label and then commit
  acc = this->storage->Access();
  vertex = acc->FindVertex(vertex.Gid(), View::OLD).value();
  MG_ASSERT(!vertex.AddLabel(acc->NameToLabel(test_label)).HasError());  // NOLINT
  ASSERT_NO_ERROR(acc->Commit());

  // Expect the index to have 1 entry, as the transaction was committed
  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 1);

  // At this point, the vertex has a label and a property

  // Remove property and then commit
  acc = this->storage->Access();
  vertex = acc->FindVertex(vertex.Gid(), View::OLD).value();
  MG_ASSERT(!vertex.SetProperty(acc->NameToProperty(test_property), null_value).HasError());  // NOLINT
  ASSERT_NO_ERROR(acc->Commit());

  // Expect the index to have 0 entries, as the transaction was committed
  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);

  // At this point, the vertex has a label but no property

  // Add property and then abort
  acc = this->storage->Access();
  vertex = acc->FindVertex(vertex.Gid(), View::OLD).value();
  MG_ASSERT(!vertex.SetProperty(acc->NameToProperty(test_property), properties).HasError());  // NOLINT
  acc->Abort();

  // Expect the index to have 0 entries, as the transaction was aborted
  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);
}
