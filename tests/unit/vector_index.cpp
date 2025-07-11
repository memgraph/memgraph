// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
#include <gtest/gtest.h>
#include <sys/types.h>
#include <string_view>
#include <thread>
#include <usearch/index_plugins.hpp>

#include "query/exceptions.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace memgraph::storage;

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define ASSERT_NO_ERROR(result) ASSERT_FALSE((result).HasError())

static constexpr std::string_view test_index = "test_index";
static constexpr std::string_view test_label = "test_label";
static constexpr std::string_view test_property = "test_property";
static constexpr unum::usearch::metric_kind_t metric = unum::usearch::metric_kind_t::l2sq_k;
static constexpr std::size_t resize_coefficient = 2;
static constexpr unum::usearch::scalar_kind_t scalar_kind = unum::usearch::scalar_kind_t::f32_k;

class VectorIndexTest : public testing::Test {
 public:
  static constexpr std::string_view testSuite = "vector_search";
  std::unique_ptr<Storage> storage;

  void SetUp() override { storage = std::make_unique<InMemoryStorage>(); }

  void TearDown() override { storage.reset(); }

  void CreateIndex(std::uint16_t dimension, std::size_t capacity) {
    auto unique_acc = this->storage->UniqueAccess();
    const auto label = unique_acc->NameToLabel(test_label.data());
    const auto property = unique_acc->NameToProperty(test_property.data());

    // Create a specification for the index
    const auto spec = VectorIndexSpec{test_index.data(),  label,    property,   metric, dimension,
                                      resize_coefficient, capacity, scalar_kind};

    EXPECT_FALSE(unique_acc->CreateVectorIndex(spec).HasError());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase());
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

TEST_F(VectorIndexTest, SimpleAddNodeTest) {
  this->CreateIndex(2, 10);
  auto acc = this->storage->Access();

  PropertyValue property_value(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});

  // should end up in the index
  this->CreateVertex(acc.get(), test_property, property_value, test_label);

  // should not end up in the index
  this->CreateVertex(acc.get(), "wrong_property", property_value, test_label);

  // should not end up in the index
  this->CreateVertex(acc.get(), test_property, property_value, "wrong_label");

  ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  const auto vector_index_info = acc->ListAllVectorIndices();
  EXPECT_EQ(vector_index_info[0].size, 1);
}

TEST_F(VectorIndexTest, SimpleSearchTest) {
  this->CreateIndex(2, 10);
  auto acc = this->storage->Access();

  PropertyValue property_value(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  const auto vertex = this->CreateVertex(acc.get(), test_property, property_value, test_label);
  ASSERT_NO_ERROR(acc->PrepareForCommitPhase());

  const auto result = acc->VectorIndexSearchOnNodes(test_index.data(), 1, std::vector<float>{1.0, 1.0});
  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(std::get<0>(result[0]).vertex_->gid, vertex.Gid());
}

TEST_F(VectorIndexTest, HighDimensionalSearchTest) {
  // Create index with high dimension
  this->CreateIndex(1000, 2);
  auto acc = this->storage->Access();

  std::vector<PropertyValue> high_dim_vector(1000, PropertyValue(1.0));
  PropertyValue property_value(high_dim_vector);
  const auto vertex = this->CreateVertex(acc.get(), test_property, property_value, test_label);
  ASSERT_NO_ERROR(acc->PrepareForCommitPhase());

  std::vector<float> query_vector(1000, 1.0);
  const auto result = acc->VectorIndexSearchOnNodes(test_index.data(), 1, query_vector);
  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(std::get<0>(result[0]).vertex_->gid, vertex.Gid());
}

TEST_F(VectorIndexTest, InvalidDimensionTest) {
  this->CreateIndex(2, 10);
  auto acc = this->storage->Access();

  // Create a node with a property that has a different dimension than the index
  std::vector<PropertyValue> properties(3, PropertyValue(1.0));
  PropertyValue property_value(properties);

  EXPECT_THROW(this->CreateVertex(acc.get(), test_property, property_value, test_label),
               memgraph::query::VectorSearchException);
}

TEST_F(VectorIndexTest, SearchWithMultipleNodes) {
  this->CreateIndex(2, 10);
  auto acc = this->storage->Access();

  PropertyValue properties1(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  [[maybe_unused]] const auto vertex1 = this->CreateVertex(acc.get(), test_property, properties1, test_label);

  PropertyValue properties2(std::vector<PropertyValue>{PropertyValue(10.0), PropertyValue(10.0)});
  const auto vertex2 = this->CreateVertex(acc.get(), test_property, properties2, test_label);
  ASSERT_NO_ERROR(acc->PrepareForCommitPhase());

  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 2);

  std::vector<float> query = {10.0, 10.0};

  // Perform search for one closest node
  const auto result = acc->VectorIndexSearchOnNodes(test_index.data(), 1, query);
  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(std::get<0>(result[0]).vertex_->gid, vertex2.Gid());  // Expect the second vertex to be the closest

  // Perform search for two closest nodes
  const auto result2 = acc->VectorIndexSearchOnNodes(test_index.data(), 2, query);
  EXPECT_EQ(result2.size(), 2);
}

TEST_F(VectorIndexTest, ConcurrencyTest) {
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
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    }));
  }

  for (auto &thread : threads) {
    thread.join();
  }

  auto acc = this->storage->Access();
  // Check that the index has the expected number of entries
  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, index_size);
}

TEST_F(VectorIndexTest, UpdatePropertyValueTest) {
  this->CreateIndex(2, 10);
  Gid vertex_gid;
  {
    auto acc = this->storage->Access();

    // Create initial vertex
    PropertyValue initial_value(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
    auto vertex = this->CreateVertex(acc.get(), test_property, initial_value, test_label);
    vertex_gid = vertex.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  // Update property value
  {
    auto acc = this->storage->Access();
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    PropertyValue updated_value(std::vector<PropertyValue>{PropertyValue(2.0), PropertyValue(2.0)});
    MG_ASSERT(!vertex.SetProperty(acc->NameToProperty(test_property), updated_value).HasError());  // NOLINT
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());

    // Verify update with search
    const auto search_result = acc->VectorIndexSearchOnNodes(test_index.data(), 1, std::vector<float>{2.0, 2.0});
    EXPECT_EQ(search_result.size(), 1);
    EXPECT_EQ(std::get<0>(search_result[0]).vertex_->properties.GetProperty(acc->NameToProperty(test_property)),
              updated_value);
  }
}

TEST_F(VectorIndexTest, DeleteVertexTest) {
  this->CreateIndex(2, 10);
  auto acc = this->storage->Access();

  PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);

  // Delete the vertex
  auto maybe_deleted_vertex = acc->DeleteVertex(&vertex);
  EXPECT_EQ(maybe_deleted_vertex.HasValue(), true);
  ASSERT_NO_ERROR(acc->PrepareForCommitPhase());

  // Verify that the vertex was deleted
  std::vector<float> query = {1.0, 1.0};
  const auto result = acc->VectorIndexSearchOnNodes(test_index.data(), 1, query);
  EXPECT_EQ(result.size(), 0);
}

TEST_F(VectorIndexTest, SimpleAbortTest) {
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

TEST_F(VectorIndexTest, MultipleAbortsAndUpdatesTest) {
  this->CreateIndex(2, 10);
  Gid vertex_gid;
  PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  PropertyValue null_value;
  {
    auto acc = this->storage->Access();

    auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());

    // Remove label and then abort
    acc = this->storage->Access();
    vertex = acc->FindVertex(vertex.Gid(), View::OLD).value();
    vertex_gid = vertex.Gid();
    MG_ASSERT(!vertex.RemoveLabel(acc->NameToLabel(test_label)).HasError());  // NOLINT
    acc->Abort();

    // Expect the index to have 1 entry, as the transaction was aborted
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 1);
  }

  // Remove property and then abort
  {
    auto acc = this->storage->Access();
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    PropertyValue null_value;
    MG_ASSERT(!vertex.SetProperty(acc->NameToProperty(test_property), null_value).HasError());  // NOLINT
    acc->Abort();

    // Expect the index to have 1 entry, as the transaction was aborted
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 1);
  }

  // Remove label and then commit
  {
    auto acc = this->storage->Access();
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    MG_ASSERT(!vertex.RemoveLabel(acc->NameToLabel(test_label)).HasError());  // NOLINT
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());

    // Expect the index to have 0 entries, as the transaction was committed
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);
  }

  // At this point, the vertex has no label but has a property

  // Add label and then abort
  {
    auto acc = this->storage->Access();
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    MG_ASSERT(!vertex.AddLabel(acc->NameToLabel(test_label)).HasError());  // NOLINT
    acc->Abort();

    // Expect the index to have 0 entries, as the transaction was aborted
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);
  }

  // Add label and then commit
  {
    auto acc = this->storage->Access();
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    MG_ASSERT(!vertex.AddLabel(acc->NameToLabel(test_label)).HasError());  // NOLINT
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());

    // Expect the index to have 1 entry, as the transaction was committed
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 1);
  }

  // At this point, the vertex has a label and a property

  // Remove property and then commit
  {
    auto acc = this->storage->Access();
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    MG_ASSERT(!vertex.SetProperty(acc->NameToProperty(test_property), null_value).HasError());  // NOLINT
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());

    // Expect the index to have 0 entries, as the transaction was committed
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);
  }

  // At this point, the vertex has a label but no property

  // Add property and then abort
  {
    auto acc = this->storage->Access();
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    MG_ASSERT(!vertex.SetProperty(acc->NameToProperty(test_property), properties).HasError());  // NOLINT
    acc->Abort();

    // Expect the index to have 0 entries, as the transaction was aborted
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);
  }
}

TEST_F(VectorIndexTest, RemoveObsoleteEntriesTest) {
  this->CreateIndex(2, 10);
  Gid vertex_gid;
  {
    auto acc = this->storage->Access();

    PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
    auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
    vertex_gid = vertex.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  // Delete the vertex
  {
    auto acc = this->storage->Access();
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    auto maybe_deleted_vertex = acc->DeleteVertex(&vertex);
    EXPECT_EQ(maybe_deleted_vertex.HasValue(), true);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  // Expect the index to have 1 entry since gc has not been run
  {
    auto acc = this->storage->Access();
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 1);
  }

  // Expect the index to have 0 entries, as the vertex was deleted
  {
    auto acc = this->storage->Access();
    auto *mem_storage = static_cast<InMemoryStorage *>(this->storage.get());
    mem_storage->indices_.vector_index_.RemoveObsoleteEntries(std::stop_token());
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);
  }
}

TEST_F(VectorIndexTest, IndexResizeTest) {
  this->CreateIndex(2, 1);
  auto size = 0;
  auto capacity = 1;

  PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  while (size <= capacity) {
    auto acc = this->storage->Access();
    [[maybe_unused]] const auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
    size++;
  }

  // Expect the index to have increased its capacity
  auto acc = this->storage->Access();
  const auto vector_index_info = acc->ListAllVectorIndices();
  size = vector_index_info[0].size;
  capacity = vector_index_info[0].capacity;
  EXPECT_GT(capacity, size);
}

TEST_F(VectorIndexTest, DropIndexTest) {
  this->CreateIndex(2, 10);
  {
    auto acc = this->storage->Access();

    PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
    [[maybe_unused]] const auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  // Drop the index
  {
    auto unique_acc = this->storage->UniqueAccess();
    EXPECT_FALSE(unique_acc->DropVectorIndex(test_index.data()).HasError());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase());
  }

  // Expect the index to have been dropped
  {
    auto acc = this->storage->Access();
    EXPECT_EQ(acc->ListAllVectorIndices().size(), 0);
  }
}

TEST_F(VectorIndexTest, ClearTest) {
  this->CreateIndex(2, 10);
  {
    auto acc = this->storage->Access();

    PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
    [[maybe_unused]] const auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());

    // Clear the index
    auto *mem_storage = static_cast<InMemoryStorage *>(this->storage.get());
    mem_storage->indices_.DropGraphClearIndices();
  }

  // Expect the index to have been cleared
  {
    auto acc = this->storage->Access();
    EXPECT_EQ(acc->ListAllVectorIndices().size(), 0);
  }
}

TEST_F(VectorIndexTest, CreateIndexWhenNodesExistsAlreadyTest) {
  {
    auto acc = this->storage->Access();

    PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
    static constexpr std::string_view test_label_2 = "test_label2";
    [[maybe_unused]] const auto vertex1 = this->CreateVertex(acc.get(), test_property, properties, test_label);
    [[maybe_unused]] const auto vertex2 = this->CreateVertex(acc.get(), test_property, properties, test_label_2);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }
  // Index created with test_label. The vertex vertex2 shouldn't be seen
  this->CreateIndex(2, 10);

  // Expect the index to have 1 entry
  {
    auto acc = this->storage->Access();
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 1);
  }
}
