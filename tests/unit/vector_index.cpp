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
#include "tests/test_commit_args_helper.hpp"

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

  PropertyValue MakeVectorIndexProperty(Storage::Accessor *accessor, const std::vector<float> &vector) {
    const auto index_id = accessor->GetNameIdMapper()->NameToId(test_index.data());
    return PropertyValue(std::vector<uint64_t>{index_id}, vector);
  }

  PropertyValue MakeVectorIndexProperty(Storage::Accessor *accessor, std::initializer_list<float> vector) {
    return MakeVectorIndexProperty(accessor, std::vector<float>(vector));
  }

  PropertyValue MakeEmptyVectorIndexProperty(Storage::Accessor *accessor) {
    const auto index_id = accessor->GetNameIdMapper()->NameToId(test_index.data());
    return PropertyValue(std::vector<uint64_t>{index_id}, std::vector<float>{});
  }

  void CreateIndex(std::uint16_t dimension, std::size_t capacity) {
    auto unique_acc = this->storage->UniqueAccess();
    const auto label = unique_acc->NameToLabel(test_label.data());
    const auto property = unique_acc->NameToProperty(test_property.data());

    // Create a specification for the index
    const auto spec = VectorIndexSpec{.index_name = test_index.data(),
                                      .label_id = label,
                                      .property = property,
                                      .metric_kind = metric,
                                      .dimension = dimension,
                                      .resize_coefficient = resize_coefficient,
                                      .capacity = capacity,
                                      .scalar_kind = scalar_kind};

    EXPECT_FALSE(unique_acc->CreateVectorIndex(spec).HasError());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
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

TEST_F(VectorIndexTest, HighDimensionalSearchTest) {
  // Create index with high dimension
  this->CreateIndex(1000, 2);
  auto acc = this->storage->Access();

  std::vector<float> high_dim_vector(1000, 1.0F);
  auto property_value = MakeVectorIndexProperty(acc.get(), high_dim_vector);
  const auto vertex = this->CreateVertex(acc.get(), test_property, property_value, test_label);
  ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

  std::vector<float> query_vector(1000, 1.0);
  const auto result = acc->VectorIndexSearchOnNodes(test_index.data(), 1, query_vector);
  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(std::get<0>(result[0]).vertex_->gid, vertex.Gid());
}

TEST_F(VectorIndexTest, ConcurrencyTest) {
  this->CreateIndex(2, 10);

  const auto index_size = std::thread::hardware_concurrency();  // default value for the number of threads in the pool

  std::vector<std::thread> threads;
  threads.reserve(index_size);
  for (int i = 0; i < index_size; i++) {
    threads.emplace_back([this, i]() {
      auto acc = this->storage->Access();
      auto properties =
          MakeVectorIndexProperty(acc.get(), std::vector<float>{static_cast<float>(i), static_cast<float>(i + 1)});

      // Each thread adds a node to the index
      [[maybe_unused]] const auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  auto acc = this->storage->Access();
  // Check that the index has the expected number of entries
  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, index_size);
}

TEST_F(VectorIndexTest, SimpleAbortTest) {
  this->CreateIndex(2, 10);
  auto acc = this->storage->Access();
  static constexpr auto index_size = 10;  // has to be equal or less than the limit of the index

  // Create multiple nodes within a transaction that will be aborted
  for (int i = 0; i < index_size; i++) {
    auto properties =
        MakeVectorIndexProperty(acc.get(), std::vector<float>{static_cast<float>(i), static_cast<float>(i + 1)});
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
  {
    auto acc = this->storage->Access();

    auto properties = MakeVectorIndexProperty(acc.get(), {1.0F, 1.0F});
    auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

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
    auto empty_vector_value = MakeEmptyVectorIndexProperty(acc.get());
    MG_ASSERT(!vertex.SetProperty(acc->NameToProperty(test_property), empty_vector_value).HasError());  // NOLINT
    acc->Abort();

    // Expect the index to have 1 entry, as the transaction was aborted
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 1);
  }

  // Remove label and then commit
  {
    auto acc = this->storage->Access();
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    MG_ASSERT(!vertex.RemoveLabel(acc->NameToLabel(test_label)).HasError());  // NOLINT
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

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
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

    // Expect the index to have 1 entry, as the transaction was committed
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 1);
  }

  // At this point, the vertex has a label and a property

  // Remove property and then commit
  {
    auto acc = this->storage->Access();
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    auto empty_vector_value = MakeEmptyVectorIndexProperty(acc.get());
    MG_ASSERT(!vertex.SetProperty(acc->NameToProperty(test_property), empty_vector_value).HasError());  // NOLINT
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

    // Expect the index to have 0 entries, as the transaction was committed
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);
  }

  // At this point, the vertex has a label but no property

  // Add property and then abort
  {
    auto acc = this->storage->Access();
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    auto properties = MakeVectorIndexProperty(acc.get(), {1.0F, 1.0F});
    MG_ASSERT(!vertex.SetProperty(acc->NameToProperty(test_property), properties).HasError());  // NOLINT
    acc->Abort();

    // Expect the index to have 0 entries, as the transaction was aborted
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);
  }
}

TEST_F(VectorIndexTest, IndexResizeTest) {
  this->CreateIndex(2, 1);
  auto size = 0;
  auto capacity = 1;

  while (size <= capacity) {
    auto acc = this->storage->Access();
    auto properties = MakeVectorIndexProperty(acc.get(), {1.0F, 1.0F});
    [[maybe_unused]] const auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
    size++;
  }

  // Expect the index to have increased its capacity
  auto acc = this->storage->Access();
  const auto vector_index_info = acc->ListAllVectorIndices();
  size = vector_index_info[0].size;
  capacity = vector_index_info[0].capacity;
  EXPECT_GT(capacity, size);
}

TEST_F(VectorIndexTest, CreateIndexWhenNodesExistsAlreadyTest) {
  {
    auto acc = this->storage->Access();

    PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
    static constexpr std::string_view test_label_2 = "test_label2";
    [[maybe_unused]] const auto vertex1 = this->CreateVertex(acc.get(), test_property, properties, test_label);
    [[maybe_unused]] const auto vertex2 = this->CreateVertex(acc.get(), test_property, properties, test_label_2);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  // Index created with test_label. The vertex vertex2 shouldn't be seen
  this->CreateIndex(2, 10);

  // Expect the index to have 1 entry
  {
    auto acc = this->storage->Access();
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 1);
  }
}
