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
#include <gtest/gtest.h>
#include <sys/types.h>
#include <algorithm>
#include <string_view>
#include <thread>

#include "flags/general.hpp"
#include "storage/v2/indices/indices.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace memgraph::storage;

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define ASSERT_NO_ERROR(result) ASSERT_TRUE((result).has_value())

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

  PropertyValue MakeVectorIndexProperty(Storage::Accessor *accessor,
                                        const memgraph::utils::small_vector<float> &vector) {
    const auto index_id = accessor->GetNameIdMapper()->NameToId(test_index.data());
    return PropertyValue(
        PropertyValue::VectorIndexIdData{.ids = memgraph::utils::small_vector<uint64_t>{index_id}, .vector = vector});
  }

  PropertyValue MakeEmptyVectorIndexProperty(Storage::Accessor *accessor) {
    const auto index_id = accessor->GetNameIdMapper()->NameToId(test_index.data());
    return PropertyValue(PropertyValue::VectorIndexIdData{.ids = memgraph::utils::small_vector<uint64_t>{index_id},
                                                          .vector = memgraph::utils::small_vector<float>{}});
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

    EXPECT_FALSE(!unique_acc->CreateVectorIndex(spec).has_value());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  VertexAccessor CreateVertex(Storage::Accessor *accessor, std::string_view property,
                              const PropertyValue &property_value, std::string_view label) {
    VertexAccessor vertex = accessor->CreateVertex();
    // NOLINTBEGIN
    MG_ASSERT(vertex.AddLabel(accessor->NameToLabel(label)).has_value());
    MG_ASSERT(vertex.SetProperty(accessor->NameToProperty(property), property_value).has_value());
    // NOLINTEND

    return vertex;
  }
};

TEST_F(VectorIndexTest, HighDimensionalSearchTest) {
  // Create index with high dimension
  this->CreateIndex(1000, 2);
  auto acc = this->storage->Access(memgraph::storage::WRITE);

  memgraph::utils::small_vector<float> high_dim_vector;
  high_dim_vector.reserve(1000);
  for (int i = 0; i < 1000; i++) {
    high_dim_vector.push_back(1.0F);
  }
  auto property_value = MakeVectorIndexProperty(acc.get(), high_dim_vector);
  const auto vertex = this->CreateVertex(acc.get(), test_property, property_value, test_label);
  ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

  memgraph::utils::small_vector<float> query_vector;
  query_vector.reserve(1000);
  for (int i = 0; i < 1000; i++) {
    query_vector.push_back(1.0F);
  }
  const auto result =
      acc->VectorIndexSearchOnNodes(test_index.data(), 1, std::vector<float>(query_vector.begin(), query_vector.end()));
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
      auto acc = this->storage->Access(memgraph::storage::WRITE);
      auto properties = MakeVectorIndexProperty(
          acc.get(), memgraph::utils::small_vector<float>{static_cast<float>(i), static_cast<float>(i + 1)});

      // Each thread adds a node to the index
      [[maybe_unused]] const auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  auto acc = this->storage->Access(memgraph::storage::WRITE);
  // Check that the index has the expected number of entries
  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, index_size);
}

TEST_F(VectorIndexTest, DeleteVertexTest) {
  this->CreateIndex(2, 10);
  auto acc = this->storage->Access(memgraph::storage::WRITE);

  auto properties = MakeVectorIndexProperty(acc.get(), memgraph::utils::small_vector<float>{1.0F, 1.0F});
  auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);

  // Delete the vertex
  auto maybe_deleted_vertex = acc->DeleteVertex(&vertex);
  EXPECT_EQ(maybe_deleted_vertex.has_value(), true);
  ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

  // Vertex isn't deleted but can't be found
  std::vector<float> query = {1.0F, 1.0F};
  const auto result = acc->VectorIndexSearchOnNodes(test_index.data(), 1, query);
  EXPECT_EQ(result.size(), 0);
  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 1);

  // Vertex is deleted after gc runs
  auto *mem_storage = static_cast<InMemoryStorage *>(this->storage.get());
  mem_storage->indices_.vector_index_.RemoveObsoleteEntries(std::stop_token());
  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);
}

TEST_F(VectorIndexTest, SimpleAbortTest) {
  this->CreateIndex(2, 10);
  auto acc = this->storage->Access(memgraph::storage::WRITE);
  static constexpr auto index_size = 10;  // has to be equal or less than the limit of the index

  // Create multiple nodes within a transaction that will be aborted
  for (int i = 0; i < index_size; i++) {
    auto properties = MakeVectorIndexProperty(
        acc.get(), memgraph::utils::small_vector<float>{static_cast<float>(i), static_cast<float>(i + 1)});
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
    auto acc = this->storage->Access(memgraph::storage::WRITE);

    auto properties = MakeVectorIndexProperty(acc.get(), memgraph::utils::small_vector<float>{1.0F, 1.0F});
    auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

    // Remove label and then abort
    acc = this->storage->Access(memgraph::storage::WRITE);
    vertex = acc->FindVertex(vertex.Gid(), View::OLD).value();
    vertex_gid = vertex.Gid();
    MG_ASSERT(vertex.RemoveLabel(acc->NameToLabel(test_label)).has_value());  // NOLINT
    acc->Abort();

    // Expect the index to have 1 entry, as the transaction was aborted
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 1);
  }

  // Remove property and then abort
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    auto empty_vector_value = MakeEmptyVectorIndexProperty(acc.get());
    MG_ASSERT(vertex.SetProperty(acc->NameToProperty(test_property), empty_vector_value).has_value());  // NOLINT
    acc->Abort();

    // Expect the index to have 1 entry, as the transaction was aborted
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 1);
  }

  // Remove label and then commit
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    MG_ASSERT(vertex.RemoveLabel(acc->NameToLabel(test_label)).has_value());  // NOLINT
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

    // Expect the index to have 0 entries, as the transaction was committed
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);
  }

  // At this point, the vertex has no label but has a property

  // Add label and then abort
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    MG_ASSERT(vertex.AddLabel(acc->NameToLabel(test_label)).has_value());  // NOLINT
    acc->Abort();

    // Expect the index to have 0 entries, as the transaction was aborted
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);
  }

  // Add label and then commit
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    MG_ASSERT(vertex.AddLabel(acc->NameToLabel(test_label)).has_value());  // NOLINT
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

    // Expect the index to have 1 entry, as the transaction was committed
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 1);
  }

  // At this point, the vertex has a label and a property

  // Remove property and then commit
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    auto empty_vector_value = MakeEmptyVectorIndexProperty(acc.get());
    MG_ASSERT(vertex.SetProperty(acc->NameToProperty(test_property), empty_vector_value).has_value());  // NOLINT
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

    // Expect the index to have 0 entries, as the transaction was committed
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);
  }

  // At this point, the vertex has a label but no property

  // Add property and then abort
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    auto empty_vector_value = MakeEmptyVectorIndexProperty(acc.get());
    MG_ASSERT(vertex.SetProperty(acc->NameToProperty(test_property), empty_vector_value).has_value());  // NOLINT
    acc->Abort();

    // Expect the index to have 0 entries, as the transaction was aborted
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);
  }
}

TEST_F(VectorIndexTest, RemoveObsoleteEntriesTest) {
  this->CreateIndex(2, 10);
  Gid vertex_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
    auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
    vertex_gid = vertex.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Delete the vertex
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    auto maybe_deleted_vertex = acc->DeleteVertex(&vertex);
    EXPECT_EQ(maybe_deleted_vertex.has_value(), true);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Expect the index to have 1 entry, as gc hasn't run yet
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 1);
  }

  // Expect the index to have 0 entries, as the vertex was deleted
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    auto *mem_storage = static_cast<InMemoryStorage *>(this->storage.get());
    mem_storage->indices_.vector_index_.RemoveObsoleteEntries(std::stop_token());
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);
  }
}

TEST_F(VectorIndexTest, RemoveObsoleteEntriesConcurrentAddRaceTest) {
  // Regression: RemoveObsoleteEntries calls size() then export_keys() without
  // holding the same internal lock. A concurrent add() increments
  // nodes_count_ (used by size()) before inserting into slot_lookup_
  // (iterated by export_keys()). This leaves nullptr entries in the exported
  // buffer; without a null guard the filter dereferences nullptr.
  static constexpr int kVerticesPerThread = 100;
  static constexpr int kNumWriterThreads = 4;
  static constexpr int kCapacity = kVerticesPerThread * kNumWriterThreads * 2;

  this->CreateIndex(2, kCapacity);
  auto *mem_storage = static_cast<InMemoryStorage *>(this->storage.get());

  std::atomic<bool> stop{false};

  std::thread gc_thread([&]() {
    while (!stop.load(std::memory_order_relaxed)) {
      mem_storage->indices_.vector_index_.RemoveObsoleteEntries(std::stop_token());
    }
  });

  std::vector<std::thread> writer_threads;
  writer_threads.reserve(kNumWriterThreads);
  for (int t = 0; t < kNumWriterThreads; t++) {
    writer_threads.emplace_back([this, t]() {
      for (int i = 0; i < kVerticesPerThread; i++) {
        auto acc = this->storage->Access(memgraph::storage::WRITE);
        auto val = static_cast<float>((t * kVerticesPerThread) + i);
        auto properties = MakeVectorIndexProperty(acc.get(), memgraph::utils::small_vector<float>{val, val + 1.0F});
        [[maybe_unused]] const auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
        ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
      }
    });
  }

  for (auto &thread : writer_threads) {
    thread.join();
  }

  stop.store(true, std::memory_order_relaxed);
  gc_thread.join();
}

TEST_F(VectorIndexTest, IndexResizeTest) {
  this->CreateIndex(2, 1);
  auto size = 0;
  auto capacity = 1;

  while (size <= capacity) {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto properties = MakeVectorIndexProperty(acc.get(), memgraph::utils::small_vector<float>{1.0F, 1.0F});
    [[maybe_unused]] const auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
    size++;
  }

  // Expect the index to have increased its capacity
  auto acc = this->storage->Access(memgraph::storage::WRITE);
  const auto vector_index_info = acc->ListAllVectorIndices();
  size = vector_index_info[0].size;
  capacity = vector_index_info[0].capacity;
  EXPECT_GT(capacity, size);
}

TEST_F(VectorIndexTest, DropIndexTest) {
  this->CreateIndex(2, 10);
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);

    PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
    [[maybe_unused]] const auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Drop the index
  {
    auto unique_acc = this->storage->UniqueAccess();
    EXPECT_FALSE(!unique_acc->DropVectorIndex(test_index.data()).has_value());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Expect the index to have been dropped
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllVectorIndices().size(), 0);
  }
}

TEST_F(VectorIndexTest, ClearTest) {
  this->CreateIndex(2, 10);
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);

    PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
    [[maybe_unused]] const auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

    // Clear the index
    auto *mem_storage = static_cast<InMemoryStorage *>(this->storage.get());
    mem_storage->indices_.DropGraphClearIndices();
  }

  // Expect the index to have been cleared
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllVectorIndices().size(), 0);
  }
}

TEST_F(VectorIndexTest, CreateIndexWhenNodesExistsAlreadyTest) {
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);

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
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 1);
  }
}

TEST_F(VectorIndexTest, IndexCreationFailsWhenNodeHasNonVectorPropertyAndDatabaseRemainsUnchanged) {
  static constexpr std::string_view label = "L1";
  static constexpr std::string_view prop_name = "prop1";
  static constexpr std::string_view id_prop = "id";

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    const auto label_id = acc->NameToLabel(label);
    const auto prop_id = acc->NameToProperty(prop_name);
    const auto id_prop_id = acc->NameToProperty(id_prop);

    auto create_vertex = [&](int64_t id, PropertyValue prop1_val) {
      auto vertex = acc->CreateVertex();
      MG_ASSERT(vertex.AddLabel(label_id).has_value());
      MG_ASSERT(vertex.SetProperty(id_prop_id, PropertyValue(id)).has_value());
      MG_ASSERT(vertex.SetProperty(prop_id, prop1_val).has_value());
    };

    create_vertex(1, PropertyValue(PropertyValue::double_list_t{1.5, 1.5}));
    create_vertex(2, PropertyValue("not_a_vector"));
    create_vertex(3, PropertyValue(PropertyValue::double_list_t{5.5, 5.5}));

    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto unique_acc = this->storage->UniqueAccess();
    const auto label_id = unique_acc->NameToLabel(label);
    const auto property_id = unique_acc->NameToProperty(prop_name);
    const auto spec = VectorIndexSpec{.index_name = test_index.data(),
                                      .label_id = label_id,
                                      .property = property_id,
                                      .metric_kind = metric,
                                      .dimension = 2,
                                      .resize_coefficient = resize_coefficient,
                                      .capacity = 10,
                                      .scalar_kind = scalar_kind};

    EXPECT_THROW(static_cast<void>(unique_acc->CreateVectorIndex(spec)), std::exception);
  }

  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    const auto prop_id = acc->NameToProperty(prop_name);
    const auto id_prop_id = acc->NameToProperty(id_prop);

    std::vector<std::pair<int64_t, PropertyValue>> rows;
    for (auto vertex : acc->Vertices(View::NEW)) {
      auto id_val = vertex.GetProperty(id_prop_id, View::NEW);
      auto prop1_val = vertex.GetProperty(prop_id, View::NEW);
      ASSERT_TRUE(id_val.has_value() && !id_val->IsNull());
      ASSERT_TRUE(prop1_val.has_value());
      rows.emplace_back(static_cast<int64_t>(id_val->ValueInt()), *prop1_val);
    }
    std::ranges::sort(rows, [](const auto &lhs, const auto &rhs) { return lhs.first < rhs.first; });

    // Abort happened -> properties remain double list / string (no VectorIndexId, since index was never created)
    EXPECT_EQ(rows.size(), 3);
    EXPECT_EQ(rows[0].first, 1);
    EXPECT_TRUE(rows[0].second.IsDoubleList());
    EXPECT_EQ(rows[0].second.ValueDoubleList(), (std::vector<double>{1.5, 1.5}));
    EXPECT_EQ(rows[1].first, 2);
    EXPECT_TRUE(rows[1].second.IsString());
    EXPECT_EQ(rows[1].second.ValueString(), "not_a_vector");
    EXPECT_EQ(rows[2].first, 3);
    EXPECT_TRUE(rows[2].second.IsDoubleList());
    EXPECT_EQ(rows[2].second.ValueDoubleList(), (std::vector<double>{5.5, 5.5}));
  }

  // No vector index should exist after failed creation
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    EXPECT_EQ(acc->ListAllVectorIndices().size(), 0);
  }
}

class VectorIndexRecoveryTest : public testing::Test {
 public:
  static constexpr std::uint16_t kDimension = 2;
  static constexpr std::size_t kNumNodes = 100;

  void SetUp() override {
    storage_ = std::make_unique<InMemoryStorage>();
    auto acc = vertices_.access();
    for (std::size_t i = 0; i < kNumNodes; i++) {
      auto [vertex_iter, inserted] = acc.insert(Vertex{Gid::FromUint(i), nullptr});
      ASSERT_TRUE(inserted);
      vertex_iter->labels.push_back(LabelId::FromUint(1));
      PropertyValue property_value(
          DoubleListTag{},
          std::vector<PropertyValue>{PropertyValue(static_cast<double>(i)), PropertyValue(static_cast<double>(i + 1))});
      vertex_iter->properties.SetProperty(PropertyId::FromUint(1), property_value);
    }
  }

  static VectorIndexRecoveryInfo CreateRecoveryInfo(const std::string &name = "test_index",
                                                    std::size_t capacity = kNumNodes) {
    return VectorIndexRecoveryInfo{.spec = VectorIndexSpec{.index_name = name,
                                                           .label_id = LabelId::FromUint(1),
                                                           .property = PropertyId::FromUint(1),
                                                           .metric_kind = unum::usearch::metric_kind_t::l2sq_k,
                                                           .dimension = kDimension,
                                                           .resize_coefficient = 2,
                                                           .capacity = capacity,
                                                           .scalar_kind = unum::usearch::scalar_kind_t::f32_k},
                                   .index_entries = {}};
  }

  std::unique_ptr<InMemoryStorage> storage_;
  memgraph::utils::SkipList<Vertex> vertices_;
  VectorIndex vector_index_;
};

TEST_F(VectorIndexRecoveryTest, RecoverIndexSingleThreadTest) {
  FLAGS_storage_parallel_schema_recovery = false;

  auto vertices_acc = vertices_.access();
  auto recovery_info = CreateRecoveryInfo();

  EXPECT_NO_THROW(
      vector_index_.RecoverIndex(recovery_info, vertices_acc, &storage_->indices_, storage_->name_id_mapper_.get()));

  const auto vector_index_info = vector_index_.ListVectorIndicesInfo();
  EXPECT_EQ(vector_index_info.size(), 1);
  EXPECT_EQ(vector_index_info[0].size, kNumNodes);
}

TEST_F(VectorIndexRecoveryTest, RecoverIndexParallelTest) {
  FLAGS_storage_parallel_schema_recovery = true;
  FLAGS_storage_recovery_thread_count =
      (std::thread::hardware_concurrency() > 0) ? std::thread::hardware_concurrency() : 1;

  auto vertices_acc = vertices_.access();
  auto recovery_info = CreateRecoveryInfo();

  EXPECT_NO_THROW(
      vector_index_.RecoverIndex(recovery_info, vertices_acc, &storage_->indices_, storage_->name_id_mapper_.get()));

  const auto vector_index_info = vector_index_.ListVectorIndicesInfo();
  EXPECT_EQ(vector_index_info.size(), 1);
  EXPECT_EQ(vector_index_info[0].size, kNumNodes);
}

TEST_F(VectorIndexRecoveryTest, ConcurrentAddWithResizeTest) {
  FLAGS_storage_parallel_schema_recovery = true;
  FLAGS_storage_recovery_thread_count =
      (std::thread::hardware_concurrency() > 0) ? std::thread::hardware_concurrency() : 4;

  auto vertices_acc = vertices_.access();
  auto recovery_info = CreateRecoveryInfo("resize_test_index", 10);  // Small capacity to force resize

  EXPECT_NO_THROW(
      vector_index_.RecoverIndex(recovery_info, vertices_acc, &storage_->indices_, storage_->name_id_mapper_.get()));

  const auto vector_index_info = vector_index_.ListVectorIndicesInfo();
  EXPECT_EQ(vector_index_info.size(), 1);
  EXPECT_EQ(vector_index_info[0].size, kNumNodes);
  EXPECT_GE(vector_index_info[0].capacity, kNumNodes);
}

TEST_F(VectorIndexRecoveryTest, RecoverIndexWithPrecomputedEntries) {
  FLAGS_storage_parallel_schema_recovery = true;
  FLAGS_storage_recovery_thread_count =
      (std::thread::hardware_concurrency() > 0) ? std::thread::hardware_concurrency() : 4;

  auto vertices_acc = vertices_.access();

  // Create recovery info with pre-computed index entries (simulating snapshot recovery)
  absl::flat_hash_map<Gid, memgraph::utils::small_vector<float>> index_entries;
  for (auto i = 0; i < kNumNodes; i++) {
    index_entries.emplace(Gid::FromUint(i),
                          memgraph::utils::small_vector<float>{static_cast<float>(i), static_cast<float>(i + 1)});
  }

  VectorIndexRecoveryInfo recovery_info{.spec = VectorIndexSpec{.index_name = "precomputed_index",
                                                                .label_id = LabelId::FromUint(1),
                                                                .property = PropertyId::FromUint(1),
                                                                .metric_kind = unum::usearch::metric_kind_t::l2sq_k,
                                                                .dimension = kDimension,
                                                                .resize_coefficient = 2,
                                                                .capacity = kNumNodes,
                                                                .scalar_kind = unum::usearch::scalar_kind_t::f32_k},
                                        .index_entries = std::move(index_entries)};

  EXPECT_NO_THROW(
      vector_index_.RecoverIndex(recovery_info, vertices_acc, &storage_->indices_, storage_->name_id_mapper_.get()));

  const auto vector_index_info = vector_index_.ListVectorIndicesInfo();
  EXPECT_EQ(vector_index_info.size(), 1);
  EXPECT_EQ(vector_index_info[0].size, kNumNodes);
}
