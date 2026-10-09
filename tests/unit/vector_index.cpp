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
#include <array>
#include <filesystem>
#include <limits>
#include <optional>
#include <string_view>
#include <thread>

#include "flags/general.hpp"
#include "flags/run_time_configurable.hpp"
#include "glue/communication.hpp"
#include "storage/v2/indices/active_indices_updater.hpp"
#include "storage/v2/indices/indices.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "tests/unit/ddl_abort_helpers.hpp"
#include "utils/atomic_memory_block.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/settings.hpp"

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
    const auto spec =
        VectorIndexSpec{.index_name = test_index.data(),
                        .label_filter = VectorLabelFilter{.mode = VectorMatchMode::SINGLE, .ids = {label}},
                        .property = property,
                        .metric_kind = metric,
                        .dimension = dimension,
                        .resize_coefficient = resize_coefficient,
                        .capacity = capacity,
                        .scalar_kind = scalar_kind};

    EXPECT_FALSE(!unique_acc->CreateVectorIndex(spec).has_value());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Asserts test_property on `gid` is a plain empty list, not a tag; also checks the index size if given.
  void ExpectPlainEmptyList(Gid gid, std::optional<std::size_t> expected_index_size) {
    auto acc = this->storage->Access(memgraph::storage::READ);
    if (expected_index_size) {
      EXPECT_EQ(acc->ListAllVectorIndices()[0].size, *expected_index_size);
    }
    auto vertex = acc->FindVertex(gid, View::OLD).value();
    const auto stored = vertex.GetProperty(acc->NameToProperty(test_property), View::OLD);
    ASSERT_TRUE(stored.has_value());
    EXPECT_TRUE(stored->IsAnyList());
    EXPECT_EQ(stored->ListSize(), 0u);
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

TEST_F(VectorIndexTest, VectorIndexedPropertiesRespectsLabelFilter) {
  this->CreateIndex(2, 10);
  auto acc = this->storage->Access(memgraph::storage::WRITE);
  const PropertyValue vec(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(2.0)});
  const auto indexed = this->CreateVertex(acc.get(), test_property, vec, test_label);
  const auto other = this->CreateVertex(acc.get(), test_property, vec, "other_label");
  ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

  auto indexed_labels = indexed.Labels(View::NEW);
  ASSERT_NO_ERROR(indexed_labels);
  EXPECT_EQ(indexed.VectorIndexedProperties(*indexed_labels),
            (std::vector<PropertyId>{acc->NameToProperty(test_property.data())}));

  auto other_labels = other.Labels(View::NEW);
  ASSERT_NO_ERROR(other_labels);
  EXPECT_TRUE(other.VectorIndexedProperties(*other_labels).empty());
}

TEST_F(VectorIndexTest, VectorIndexedPropertiesMatchesOneOfManyLabels) {
  this->CreateIndex(2, 10);
  auto acc = this->storage->Access(memgraph::storage::WRITE);
  auto vertex = acc->CreateVertex();
  ASSERT_TRUE(vertex.AddLabel(acc->NameToLabel("extra")).has_value());
  ASSERT_TRUE(vertex.AddLabel(acc->NameToLabel(test_label.data())).has_value());
  ASSERT_TRUE(vertex
                  .SetProperty(acc->NameToProperty(test_property.data()),
                               PropertyValue(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(2.0)}))
                  .has_value());
  ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

  auto labels = vertex.Labels(View::NEW);
  ASSERT_NO_ERROR(labels);
  EXPECT_EQ(vertex.VectorIndexedProperties(*labels),
            (std::vector<PropertyId>{acc->NameToProperty(test_property.data())}));
}

TEST_F(VectorIndexTest, VectorIndexedPropertiesWildcardMatchesAnyLabel) {
  {
    auto unique_acc = this->storage->UniqueAccess();
    const auto spec = VectorIndexSpec{.index_name = "wildcard_index",
                                      .label_filter = VectorLabelFilter{.mode = VectorMatchMode::WILDCARD, .ids = {}},
                                      .property = unique_acc->NameToProperty(test_property.data()),
                                      .metric_kind = metric,
                                      .dimension = 2,
                                      .resize_coefficient = resize_coefficient,
                                      .capacity = 10,
                                      .scalar_kind = scalar_kind};
    ASSERT_TRUE(unique_acc->CreateVectorIndex(spec).has_value());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  auto acc = this->storage->Access(memgraph::storage::WRITE);
  auto vertex = acc->CreateVertex();
  ASSERT_TRUE(vertex.AddLabel(acc->NameToLabel("anything")).has_value());
  ASSERT_TRUE(vertex
                  .SetProperty(acc->NameToProperty(test_property.data()),
                               PropertyValue(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(2.0)}))
                  .has_value());
  ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

  auto labels = vertex.Labels(View::NEW);
  ASSERT_NO_ERROR(labels);
  EXPECT_EQ(vertex.VectorIndexedProperties(*labels),
            (std::vector<PropertyId>{acc->NameToProperty(test_property.data())}));
}

TEST_F(VectorIndexTest, ToBoltVertexOmitsVectorIndexedPropertyWhenFlagOn) {
  const auto settings_dir = std::filesystem::temp_directory_path() / "MG_tests_unit_vector_index_omit";
  std::filesystem::remove_all(settings_dir);
  memgraph::utils::Settings settings(settings_dir);
  memgraph::flags::run_time::Initialize(settings);
  const auto set_omit = [&](bool enabled) {
    settings.SetValue("storage.omit_vector_index_properties_on_return", enabled ? "true" : "false");
  };
  // restore the process-global flag even if an assertion aborts the test early
  memgraph::utils::OnScopeExit reset_flag{[&] { set_omit(false); }};
  set_omit(false);

  this->CreateIndex(2, 10);
  auto acc = this->storage->Access(memgraph::storage::WRITE);
  auto vertex = this->CreateVertex(acc.get(),
                                   test_property,
                                   PropertyValue(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(2.0)}),
                                   test_label);
  ASSERT_TRUE(vertex.SetProperty(acc->NameToProperty("title"), PropertyValue("t")).has_value());
  // a non-indexed list property is never hidden — hiding is by vector-index membership, not by type
  ASSERT_TRUE(vertex
                  .SetProperty(acc->NameToProperty("tags"),
                               PropertyValue(std::vector<PropertyValue>{PropertyValue(int64_t{1})}))
                  .has_value());
  ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

  auto with_prop = memgraph::glue::ToBoltVertex(vertex, *this->storage, View::NEW, nullptr);
  ASSERT_TRUE(with_prop.has_value());
  EXPECT_TRUE(with_prop->properties.contains(test_property.data()));

  set_omit(true);
  auto without_prop = memgraph::glue::ToBoltVertex(vertex, *this->storage, View::NEW, nullptr);
  ASSERT_TRUE(without_prop.has_value());
  EXPECT_FALSE(without_prop->properties.contains(test_property.data()));
  EXPECT_TRUE(without_prop->properties.contains("title"));
  EXPECT_TRUE(without_prop->properties.contains("tags"));
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
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto properties = MakeVectorIndexProperty(acc.get(), memgraph::utils::small_vector<float>{1.0F, 1.0F});
    auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
    auto maybe_deleted_vertex = acc->DeleteVertex(&vertex);
    EXPECT_EQ(maybe_deleted_vertex.has_value(), true);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  this->storage->FreeMemory();
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    std::vector<float> query = {1.0F, 1.0F};
    const auto result = acc->VectorIndexSearchOnNodes(test_index.data(), 1, query);
    EXPECT_EQ(result.size(), 0);
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);
  }
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

TEST_F(VectorIndexTest, RemoveVertexTest) {
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
    auto *mem_storage = static_cast<InMemoryStorage *>(this->storage.get());
    mem_storage->indices_.vector_index_.RemoveVertices({vertex.vertex_});
  }

  // Expect the index to have 1 entry, as gc hasn't run yet
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);
  }
}

TEST_F(VectorIndexTest, SerializeAllVectorIndicesConcurrentAddRemoveTest) {
  static constexpr auto kVerticesPerThread = 50;
  static constexpr auto kNumWriterThreads = 4;
  static constexpr auto kCapacity = kVerticesPerThread * kNumWriterThreads * 2;

  this->CreateIndex(2, kCapacity);
  auto *mem_storage = static_cast<InMemoryStorage *>(this->storage.get());

  auto tmp_dir = std::filesystem::temp_directory_path() / "mg_serialize_vector_index_test";
  std::filesystem::create_directories(tmp_dir);

  std::jthread serialize_thread([&](std::stop_token stoken) {
    while (!stoken.stop_requested()) {
      auto path = tmp_dir / "snapshot_test.bin";
      durability::Encoder<memgraph::utils::NonConcurrentOutputFile> encoder;
      encoder.Initialize(path);
      auto mapped_ids = std::unordered_set<uint64_t>{};
      mem_storage->indices_.vector_index_.SerializeAllVectorIndices(&encoder, mapped_ids);
      encoder.Close();
    }
  });

  std::vector<std::jthread> writer_threads;
  writer_threads.reserve(kNumWriterThreads);
  for (auto t = 0; t < kNumWriterThreads; t++) {
    writer_threads.emplace_back([this, t]() {
      for (auto i = 0; i < kVerticesPerThread; i++) {
        auto acc = this->storage->Access(memgraph::storage::WRITE);
        auto val = static_cast<float>((t * kVerticesPerThread) + i);
        auto properties = MakeVectorIndexProperty(acc.get(), memgraph::utils::small_vector<float>{val, val + 1.0F});
        [[maybe_unused]] const auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
        ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
      }
    });
  }
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
    const auto spec =
        VectorIndexSpec{.index_name = test_index.data(),
                        .label_filter = VectorLabelFilter{.mode = VectorMatchMode::SINGLE, .ids = {label_id}},
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

TEST_F(VectorIndexTest, CreateIndexWithWrongDimensionRollsBack) {
  PropertyValue good_vec(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(2.0)});
  PropertyValue bad_vec(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(2.0), PropertyValue(3.0)});
  Gid good_vertex_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto v1 = this->CreateVertex(acc.get(), test_property, good_vec, test_label);
    good_vertex_gid = v1.Gid();
    [[maybe_unused]] auto v2 = this->CreateVertex(acc.get(), test_property, bad_vec, test_label);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  EXPECT_THROW(this->CreateIndex(2, 10), std::exception);
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    EXPECT_EQ(acc->ListAllVectorIndices().size(), 0);
    auto v1 = acc->FindVertex(good_vertex_gid, View::OLD).value();
    auto prop = v1.GetProperty(acc->NameToProperty(test_property), View::OLD);
    EXPECT_TRUE(prop->IsDoubleList());
    EXPECT_EQ(prop->ValueDoubleList().size(), 2);
  }
}

TEST_F(VectorIndexTest, CreateIndexConvertsPropertiesToVectorIndexId) {
  Gid vertex_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(2.0)});
    auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
    vertex_gid = vertex.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    auto prop = vertex.GetProperty(acc->NameToProperty(test_property), View::OLD);
    EXPECT_TRUE(prop->IsList());
  }
  this->CreateIndex(2, 10);
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    EXPECT_EQ(acc->ListAllVectorIndices().size(), 1);
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 1);
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    auto prop = vertex.GetProperty(acc->NameToProperty(test_property), View::OLD);
    EXPECT_TRUE(prop->IsVectorIndexId());
    EXPECT_EQ(prop->ValueVectorIndexList().size(), 2);
    EXPECT_FLOAT_EQ(prop->ValueVectorIndexList()[0], 1.0f);
    EXPECT_FLOAT_EQ(prop->ValueVectorIndexList()[1], 2.0f);
  }
}

TEST_F(VectorIndexTest, DropIndexRestoresPropertiesToLists) {
  Gid vertex_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(2.0)});
    auto vertex = this->CreateVertex(acc.get(), test_property, properties, test_label);
    vertex_gid = vertex.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  this->CreateIndex(2, 10);
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    EXPECT_TRUE(vertex.GetProperty(acc->NameToProperty(test_property), View::OLD)->IsVectorIndexId());
  }
  {
    auto unique_acc = this->storage->UniqueAccess();
    EXPECT_FALSE(!unique_acc->DropVectorIndex(test_index.data()).has_value());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    EXPECT_EQ(acc->ListAllVectorIndices().size(), 0);
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    auto prop = vertex.GetProperty(acc->NameToProperty(test_property), View::OLD);
    EXPECT_TRUE(prop->IsDoubleList());
    auto list = prop->ValueDoubleList();
    EXPECT_EQ(list.size(), 2);
    EXPECT_DOUBLE_EQ(list[0], 1.0);
    EXPECT_DOUBLE_EQ(list[1], 2.0);
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
    return VectorIndexRecoveryInfo{
        .spec = VectorIndexSpec{
            .index_name = name,
            .label_filter = VectorLabelFilter{.mode = VectorMatchMode::SINGLE, .ids = {LabelId::FromUint(1)}},
            .property = PropertyId::FromUint(1),
            .metric_kind = unum::usearch::metric_kind_t::l2sq_k,
            .dimension = kDimension,
            .resize_coefficient = 2,
            .capacity = capacity,
            .scalar_kind = unum::usearch::scalar_kind_t::f32_k}};
  }

  // Stand-in for the map LoadPartialVertices builds from tags (snapshot loading never captures plain lists).
  VectorIndexRecovery::VertexVectors BuildVertexVectors(PropertyId prop) {
    VectorIndexRecovery::VertexVectors vv;
    auto &prop_map = vv[prop];
    auto acc = vertices_.access();
    for (auto it = acc.begin(); it != acc.end(); ++it) {
      auto maybe_vec = TryListToVector(it->properties.GetProperty(prop));
      if (maybe_vec) {
        prop_map.emplace(it->gid, std::move(*maybe_vec));
      }
    }
    return vv;
  }

  std::unique_ptr<InMemoryStorage> storage_;
  memgraph::utils::SkipListDb<Vertex> vertices_;
  VectorIndex vector_index_;
};

struct RecoverAllParam {
  bool parallel;
  std::size_t capacity;
};

class VectorIndexRecoveryPlainListTest : public VectorIndexRecoveryTest,
                                         public testing::WithParamInterface<RecoverAllParam> {};

// A capacity below kNumNodes forces usearch to resize during population.
TEST_P(VectorIndexRecoveryPlainListTest, RecoverAllVectorIndices) {
  const auto [parallel, capacity] = GetParam();
  FLAGS_storage_parallel_schema_recovery = parallel;
  FLAGS_storage_recovery_thread_count =
      (std::thread::hardware_concurrency() > 0) ? std::thread::hardware_concurrency() : 4;

  auto vertices_acc = vertices_.access();
  std::vector<VectorIndexRecoveryInfo> infos{CreateRecoveryInfo("test_index", capacity)};
  VectorIndexRecovery::VertexVectors vv;
  EXPECT_NO_THROW(vector_index_.RecoverAllVectorIndices(infos,
                                                        vv,
                                                        vertices_acc,
                                                        storage_->name_id_mapper_.get(),
                                                        ActiveIndicesUpdater{storage_->indices_.active_indices_}));

  const auto info = vector_index_.ListVectorIndicesInfo();
  ASSERT_EQ(info.size(), 1);
  EXPECT_EQ(info[0].size, kNumNodes);
  EXPECT_GE(info[0].capacity, static_cast<std::size_t>(kNumNodes));
}

INSTANTIATE_TEST_SUITE_P(Modes, VectorIndexRecoveryPlainListTest,
                         testing::Values(RecoverAllParam{.parallel = false, .capacity = 100},
                                         RecoverAllParam{.parallel = true, .capacity = 100},
                                         RecoverAllParam{.parallel = true, .capacity = 10}),
                         [](const testing::TestParamInfo<RecoverAllParam> &p) {
                           return std::string(p.param.parallel ? "Parallel" : "SingleThread") + "Capacity" +
                                  std::to_string(p.param.capacity);
                         });

TEST_F(VectorIndexTest, DropVectorIndexAbortRestoresIndex) {
  this->CreateIndex(2, 16);
  // Add a couple of vertices so DropIndex actually walks property values.
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto property_value = MakeVectorIndexProperty(acc.get(), memgraph::utils::small_vector<float>{1.0F, 2.0F});
    [[maybe_unused]] auto v = this->CreateVertex(acc.get(), test_property, property_value, test_label);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Aborted DROP must leave the index live (and the vertex property reachable
  // through the index, not rewritten to plain Vector).
  {
    auto acc = this->storage->UniqueAccess();
    ASSERT_TRUE(acc->DropVectorIndex(test_index).has_value());
    acc->Abort();
  }

  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    auto info = acc->ListAllIndices().vector_indices_spec;
    EXPECT_EQ(info.size(), 1u);
  }

  // A subsequent search must still find the vertex via the restored index.
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    const auto result = acc->VectorIndexSearchOnNodes(test_index.data(), 1, std::vector<float>{1.0F, 2.0F});
    EXPECT_EQ(result.size(), 1u);
  }

  // A second DROP must succeed (i.e. the restored entry is reachable, not a ghost).
  {
    auto acc = this->storage->UniqueAccess();
    ASSERT_TRUE(acc->DropVectorIndex(test_index).has_value());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

TEST_F(VectorIndexTest, CreateVectorIndexAbortLeavesNoGhostEntry) {
  VectorIndexSpec spec{};
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    spec = VectorIndexSpec{.index_name = test_index.data(),
                           .label_filter = VectorLabelFilter{.mode = VectorMatchMode::SINGLE,
                                                             .ids = {acc->NameToLabel(test_label.data())}},
                           .property = acc->NameToProperty(test_property.data()),
                           .metric_kind = metric,
                           .dimension = 2,
                           .resize_coefficient = resize_coefficient,
                           .capacity = 16,
                           .scalar_kind = scalar_kind};
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  memgraph::tests::ExpectCreateAbortLeavesNoGhostEntry(
      this, memgraph::tests::UniqueAcc, [&](auto *acc) { return acc->CreateVectorIndex(spec); });
}

// Two specs on one property; the fixture vertices use PropId 1, not kProp, so only GIDs 200-203 take part.
TEST_F(VectorIndexRecoveryTest, RecoverAllVectorIndicesResolvesEachVertexState) {
  FLAGS_storage_parallel_schema_recovery = false;

  static constexpr LabelId kLabelA = LabelId::FromUint(10);
  static constexpr LabelId kLabelB = LabelId::FromUint(11);
  static constexpr LabelId kLabelC = LabelId::FromUint(12);

  // Names must be registered: the missing-vector error path (case d) calls IdToName(property).
  const uint64_t idx_a_id = storage_->name_id_mapper_->NameToId("idx_a");
  const uint64_t idx_b_id = storage_->name_id_mapper_->NameToId("idx_b");
  const PropertyId kProp = PropertyId::FromUint(storage_->name_id_mapper_->NameToId("test_prop"));

  std::vector<VectorIndexRecoveryInfo> infos{
      VectorIndexRecoveryInfo{
          .spec = VectorIndexSpec{.index_name = "idx_a",
                                  .label_filter = VectorLabelFilter{.mode = VectorMatchMode::SINGLE, .ids = {kLabelA}},
                                  .property = kProp,
                                  .metric_kind = unum::usearch::metric_kind_t::l2sq_k,
                                  .dimension = kDimension,
                                  .resize_coefficient = 2,
                                  .capacity = 10,
                                  .scalar_kind = unum::usearch::scalar_kind_t::f32_k}},
      VectorIndexRecoveryInfo{
          .spec = VectorIndexSpec{.index_name = "idx_b",
                                  .label_filter = VectorLabelFilter{.mode = VectorMatchMode::SINGLE, .ids = {kLabelB}},
                                  .property = kProp,
                                  .metric_kind = unum::usearch::metric_kind_t::l2sq_k,
                                  .dimension = kDimension,
                                  .resize_coefficient = 2,
                                  .capacity = 10,
                                  .scalar_kind = unum::usearch::scalar_kind_t::f32_k}}};

  {
    auto acc = vertices_.access();

    // (a) Stored plain list [3.0, 4.0]; stale map entry {9.0, 10.0} that must NOT be used; label A.
    auto [it_a, ok_a] = acc.insert(Vertex{Gid::FromUint(200), nullptr});
    ASSERT_TRUE(ok_a);
    it_a->labels.push_back(kLabelA);
    it_a->properties.SetProperty(
        kProp, PropertyValue(DoubleListTag{}, std::vector<PropertyValue>{PropertyValue(3.0), PropertyValue(4.0)}));

    // (b) Stored tag (stale id 999, no embedded vector); map entry {5.0, 6.0}; label C — matches neither.
    auto [it_b, ok_b] = acc.insert(Vertex{Gid::FromUint(201), nullptr});
    ASSERT_TRUE(ok_b);
    it_b->labels.push_back(kLabelC);
    it_b->properties.SetProperty(kProp,
                                 PropertyValue(PropertyValue::VectorIndexIdData{
                                     .ids = memgraph::utils::small_vector<uint64_t>{999u}, .vector = {}}));

    // (c) Stored tag (stale id 999); map entry {7.0, 8.0}; labels A+B — matches both.
    auto [it_c, ok_c] = acc.insert(Vertex{Gid::FromUint(202), nullptr});
    ASSERT_TRUE(ok_c);
    it_c->labels.push_back(kLabelA);
    it_c->labels.push_back(kLabelB);
    it_c->properties.SetProperty(kProp,
                                 PropertyValue(PropertyValue::VectorIndexIdData{
                                     .ids = memgraph::utils::small_vector<uint64_t>{999u}, .vector = {}}));

    // (d) Stored tag (stale id 999); NO map entry; label A — exercises the missing-vector null path.
    auto [it_d, ok_d] = acc.insert(Vertex{Gid::FromUint(203), nullptr});
    ASSERT_TRUE(ok_d);
    it_d->labels.push_back(kLabelA);
    it_d->properties.SetProperty(kProp,
                                 PropertyValue(PropertyValue::VectorIndexIdData{
                                     .ids = memgraph::utils::small_vector<uint64_t>{999u}, .vector = {}}));
  }

  VectorIndexRecovery::VertexVectors vv;
  vv[kProp].emplace(Gid::FromUint(200), memgraph::utils::small_vector<float>{9.0F, 10.0F});
  vv[kProp].emplace(Gid::FromUint(201), memgraph::utils::small_vector<float>{5.0F, 6.0F});
  vv[kProp].emplace(Gid::FromUint(202), memgraph::utils::small_vector<float>{7.0F, 8.0F});

  auto vertices_acc = vertices_.access();
  EXPECT_NO_THROW(vector_index_.RecoverAllVectorIndices(infos,
                                                        vv,
                                                        vertices_acc,
                                                        storage_->name_id_mapper_.get(),
                                                        ActiveIndicesUpdater{storage_->indices_.active_indices_}));

  const auto index_info = vector_index_.ListVectorIndicesInfo();
  ASSERT_EQ(index_info.size(), 2u);
  std::unordered_map<std::string, std::size_t> sizes;
  for (const auto &info : index_info) sizes[info.index_name] = info.size;
  EXPECT_EQ(sizes["idx_a"], 2u);
  EXPECT_EQ(sizes["idx_b"], 1u);

  {
    auto it = vertices_acc.find(Gid::FromUint(200));
    ASSERT_NE(it, vertices_acc.end());
    const auto prop = it->properties.GetProperty(kProp);
    ASSERT_TRUE(prop.IsVectorIndexId());
    EXPECT_EQ(prop.ValueVectorIndexIds(), (memgraph::utils::small_vector<uint64_t>{idx_a_id}));
    const auto vec_from_idx = vector_index_.GetVectorPropertyFromIndex(&*it, "idx_a", storage_->name_id_mapper_.get());
    EXPECT_EQ(vec_from_idx, (memgraph::utils::small_vector<float>{3.0F, 4.0F}));
  }

  {
    auto it = vertices_acc.find(Gid::FromUint(201));
    ASSERT_NE(it, vertices_acc.end());
    const auto prop = it->properties.GetProperty(kProp);
    EXPECT_TRUE(prop.IsDoubleList());
    const auto dl = prop.ValueDoubleList();
    ASSERT_EQ(dl.size(), 2u);
    EXPECT_DOUBLE_EQ(dl[0], 5.0);
    EXPECT_DOUBLE_EQ(dl[1], 6.0);
  }

  {
    auto it = vertices_acc.find(Gid::FromUint(202));
    ASSERT_NE(it, vertices_acc.end());
    const auto prop = it->properties.GetProperty(kProp);
    ASSERT_TRUE(prop.IsVectorIndexId());
    const auto &ids = prop.ValueVectorIndexIds();
    ASSERT_EQ(ids.size(), 2u);
    EXPECT_TRUE(std::ranges::is_permutation(ids, memgraph::utils::small_vector<uint64_t>{idx_a_id, idx_b_id}));
    const memgraph::utils::small_vector<float> expected{7.0F, 8.0F};
    EXPECT_EQ(vector_index_.GetVectorPropertyFromIndex(&*it, "idx_a", storage_->name_id_mapper_.get()), expected);
    EXPECT_EQ(vector_index_.GetVectorPropertyFromIndex(&*it, "idx_b", storage_->name_id_mapper_.get()), expected);
  }

  {
    auto it = vertices_acc.find(Gid::FromUint(203));
    ASSERT_NE(it, vertices_acc.end());
    const auto prop = it->properties.GetProperty(kProp);
    EXPECT_TRUE(prop.IsNull());
  }
}

// Tagged vertices take their vector from vertex_vectors; each tag is rewritten to the created index id.
TEST_F(VectorIndexRecoveryTest, RecoverAllVectorIndicesFromVertexVectors) {
  FLAGS_storage_parallel_schema_recovery = true;
  FLAGS_storage_recovery_thread_count =
      (std::thread::hardware_concurrency() > 0) ? std::thread::hardware_concurrency() : 4;

  VectorIndexRecovery::VertexVectors vv = BuildVertexVectors(PropertyId::FromUint(1));
  const auto expected_vec = vv[PropertyId::FromUint(1)].at(Gid::FromUint(7));

  // Bare tags, as property_store persists them (ids only, no vector).
  {
    auto acc = vertices_.access();
    for (auto it = acc.begin(); it != acc.end(); ++it) {
      it->properties.SetProperty(PropertyId::FromUint(1),
                                 PropertyValue(PropertyValue::VectorIndexIdData{
                                     .ids = memgraph::utils::small_vector<uint64_t>{999}, .vector = {}}));
    }
  }

  auto vertices_acc = vertices_.access();
  std::vector<VectorIndexRecoveryInfo> infos{CreateRecoveryInfo("precomputed_index")};
  EXPECT_NO_THROW(vector_index_.RecoverAllVectorIndices(infos,
                                                        vv,
                                                        vertices_acc,
                                                        storage_->name_id_mapper_.get(),
                                                        ActiveIndicesUpdater{storage_->indices_.active_indices_}));

  const auto info = vector_index_.ListVectorIndicesInfo();
  EXPECT_EQ(info.size(), 1);
  EXPECT_EQ(info[0].size, kNumNodes);

  const uint64_t index_id = storage_->name_id_mapper_->NameToId("precomputed_index");
  auto it = vertices_acc.find(Gid::FromUint(7));
  ASSERT_NE(it, vertices_acc.end());
  const auto prop = it->properties.GetProperty(PropertyId::FromUint(1));
  ASSERT_TRUE(prop.IsVectorIndexId());
  EXPECT_EQ(prop.ValueVectorIndexIds(), (memgraph::utils::small_vector<uint64_t>{index_id}));
  EXPECT_EQ(vector_index_.GetVectorPropertyFromIndex(&*it, "precomputed_index", storage_->name_id_mapper_.get()),
            expected_vec);
}

// UpdateOnSetProperty with a spec: a tag's embedded vector is captured in vertex_vectors[property][gid].
TEST_F(VectorIndexRecoveryTest, UpdateOnSetPropertyCapturesVectorWhenSpecExists) {
  static constexpr PropertyId kProp = PropertyId::FromUint(1);

  std::vector<VectorIndexRecoveryInfo> infos{CreateRecoveryInfo()};
  VectorIndexRecovery::VertexVectors vv;

  memgraph::utils::small_vector<float> raw_vec{1.0F, 2.0F};
  PropertyValue tag_value(
      PropertyValue::VectorIndexIdData{.ids = memgraph::utils::small_vector<uint64_t>{42}, .vector = raw_vec});

  Vertex vertex(Gid::FromUint(77), nullptr);
  VectorIndexRecovery::UpdateOnSetProperty(kProp, tag_value, &vertex, infos, vv);

  ASSERT_TRUE(vv.contains(kProp));
  ASSERT_TRUE(vv[kProp].contains(vertex.gid));
  EXPECT_EQ(vv[kProp][vertex.gid], raw_vec);
}

// UpdateOnSetProperty without a spec: a stale tag becomes a plain list in place.
TEST_F(VectorIndexRecoveryTest, UpdateOnSetPropertyOrphanTagConvertedToList) {
  static constexpr PropertyId kProp = PropertyId::FromUint(1);

  std::vector<VectorIndexRecoveryInfo> infos;
  VectorIndexRecovery::VertexVectors vv;

  memgraph::utils::small_vector<float> raw_vec{3.0F, 4.0F};
  PropertyValue tag_value(
      PropertyValue::VectorIndexIdData{.ids = memgraph::utils::small_vector<uint64_t>{99}, .vector = raw_vec});

  Vertex vertex(Gid::FromUint(5), nullptr);
  VectorIndexRecovery::UpdateOnSetProperty(kProp, tag_value, &vertex, infos, vv);

  ASSERT_TRUE(tag_value.IsDoubleList());
  ASSERT_EQ(tag_value.ValueDoubleList().size(), 2u);
  EXPECT_DOUBLE_EQ(tag_value.ValueDoubleList()[0], 3.0);
  EXPECT_DOUBLE_EQ(tag_value.ValueDoubleList()[1], 4.0);
  EXPECT_TRUE(vv.empty());
}

// UpdateOnIndexDrop of the last spec on a property: tags become plain lists (null if no vector) and the map entry goes.
TEST_F(VectorIndexRecoveryTest, UpdateOnIndexDropRestoresTagsToPlainLists) {
  static constexpr PropertyId kProp = PropertyId::FromUint(1);

  std::vector<VectorIndexRecoveryInfo> infos{CreateRecoveryInfo()};

  VectorIndexRecovery::VertexVectors vv;
  vv[kProp].emplace(Gid::FromUint(0), memgraph::utils::small_vector<float>{7.0F, 8.0F});

  // Tag without embedded vector, as persisted by property_store.
  auto acc = vertices_.access();
  auto v0 = acc.find(Gid::FromUint(0));
  ASSERT_NE(v0, acc.end());
  v0->properties.SetProperty(
      kProp,
      PropertyValue(PropertyValue::VectorIndexIdData{.ids = memgraph::utils::small_vector<uint64_t>{1}, .vector = {}}));

  // Tagged but no map entry: becomes null.
  auto v1 = acc.find(Gid::FromUint(1));
  ASSERT_NE(v1, acc.end());
  v1->properties.SetProperty(
      kProp,
      PropertyValue(PropertyValue::VectorIndexIdData{.ids = memgraph::utils::small_vector<uint64_t>{1}, .vector = {}}));

  VectorIndexRecovery::UpdateOnIndexDrop(infos[0].spec.index_name, infos, vv, acc);

  EXPECT_TRUE(infos.empty());
  EXPECT_FALSE(vv.contains(kProp));
  const auto restored = v0->properties.GetProperty(kProp);
  ASSERT_TRUE(restored.IsDoubleList());
  ASSERT_EQ(restored.ValueDoubleList().size(), 2u);
  EXPECT_DOUBLE_EQ(restored.ValueDoubleList()[0], 7.0);
  EXPECT_DOUBLE_EQ(restored.ValueDoubleList()[1], 8.0);
  EXPECT_TRUE(v1->properties.GetProperty(kProp).IsNull());
}

// UpdateOnIndexDrop with a surviving spec on the property keeps vertex_vectors for the final build.
TEST_F(VectorIndexRecoveryTest, UpdateOnIndexDropPreservesVertexVectorsForSurvivingSpec) {
  static constexpr PropertyId kProp = PropertyId::FromUint(1);

  std::vector<VectorIndexRecoveryInfo> infos{CreateRecoveryInfo("idx_a"), CreateRecoveryInfo("idx_b")};
  VectorIndexRecovery::VertexVectors vv;
  vv[kProp].emplace(Gid::FromUint(0), memgraph::utils::small_vector<float>{1.0F, 2.0F});

  auto acc = vertices_.access();
  VectorIndexRecovery::UpdateOnIndexDrop("idx_a", infos, vv, acc);

  ASSERT_EQ(infos.size(), 1u);
  EXPECT_EQ(infos[0].spec.index_name, "idx_b");
  EXPECT_TRUE(vv.contains(kProp));
  EXPECT_TRUE(vv[kProp].contains(Gid::FromUint(0)));
}

TEST_F(VectorIndexTest, OverlappingLabelIndicesBothUpdatedOnAddLabel) {
  const std::string_view label_a_name = "A";
  const std::string_view label_b_name = "B";
  const std::string_view idx_a = "idx_a";
  const std::string_view idx_ab = "idx_ab";
  {
    auto unique_acc = this->storage->UniqueAccess();
    const auto a = unique_acc->NameToLabel(label_a_name);
    const auto b = unique_acc->NameToLabel(label_b_name);
    const auto property = unique_acc->NameToProperty(test_property.data());
    EXPECT_TRUE(unique_acc
                    ->CreateVectorIndex({.index_name = std::string{idx_a},
                                         .label_filter = {.mode = VectorMatchMode::SINGLE, .ids = {a}},
                                         .property = property,
                                         .metric_kind = metric,
                                         .dimension = 2,
                                         .resize_coefficient = resize_coefficient,
                                         .capacity = 10,
                                         .scalar_kind = scalar_kind})
                    .has_value());
    EXPECT_TRUE(unique_acc
                    ->CreateVectorIndex({.index_name = std::string{idx_ab},
                                         .label_filter = {.mode = VectorMatchMode::ANY_OF, .ids = {a, b}},
                                         .property = property,
                                         .metric_kind = metric,
                                         .dimension = 2,
                                         .resize_coefficient = resize_coefficient,
                                         .capacity = 10,
                                         .scalar_kind = scalar_kind})
                    .has_value());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    PropertyValue prop(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(2.0)});
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty(test_property.data()), prop));
    ASSERT_NO_ERROR(vertex.AddLabel(acc->NameToLabel(label_a_name)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  auto acc = this->storage->Access(memgraph::storage::WRITE);
  std::unordered_map<std::string, std::size_t> sizes_by_name;
  for (const auto &info : acc->ListAllVectorIndices()) sizes_by_name[info.index_name] = info.size;
  EXPECT_EQ(sizes_by_name[std::string{idx_a}], 1) << "single-label index :A(prop) missing vertex after adding :A";
  EXPECT_EQ(sizes_by_name[std::string{idx_ab}], 1)
      << "overlapping ANY_OF index :A|B(prop) missing vertex after adding :A";
}

TEST_F(VectorIndexTest, MultiLabelFilterEqualityIsOrderInsensitive) {
  auto unique_acc = this->storage->UniqueAccess();
  const auto a = unique_acc->NameToLabel("A");
  const auto b = unique_acc->NameToLabel("B");
  const auto property = unique_acc->NameToProperty(test_property.data());
  EXPECT_TRUE(unique_acc
                  ->CreateVectorIndex({.index_name = "ab",
                                       .label_filter = {.mode = VectorMatchMode::ANY_OF, .ids = {a, b}},
                                       .property = property,
                                       .metric_kind = metric,
                                       .dimension = 2,
                                       .resize_coefficient = resize_coefficient,
                                       .capacity = 10,
                                       .scalar_kind = scalar_kind})
                  .has_value());
  EXPECT_FALSE(unique_acc
                   ->CreateVectorIndex({.index_name = "ba",
                                        .label_filter = {.mode = VectorMatchMode::ANY_OF, .ids = {b, a}},
                                        .property = property,
                                        .metric_kind = metric,
                                        .dimension = 2,
                                        .resize_coefficient = resize_coefficient,
                                        .capacity = 10,
                                        .scalar_kind = scalar_kind})
                   .has_value());
}

TEST_F(VectorIndexTest, AddLabelToVertexAlreadyInIndexDoesNotDuplicateId) {
  const std::string_view idx_ab = "idx_ab";
  {
    auto unique_acc = this->storage->UniqueAccess();
    const auto a = unique_acc->NameToLabel("A");
    const auto b = unique_acc->NameToLabel("B");
    const auto property = unique_acc->NameToProperty(test_property.data());
    EXPECT_TRUE(unique_acc
                    ->CreateVectorIndex({.index_name = std::string{idx_ab},
                                         .label_filter = {.mode = VectorMatchMode::ANY_OF, .ids = {a, b}},
                                         .property = property,
                                         .metric_kind = metric,
                                         .dimension = 2,
                                         .resize_coefficient = resize_coefficient,
                                         .capacity = 10,
                                         .scalar_kind = scalar_kind})
                    .has_value());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  Gid vertex_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    vertex_gid = vertex.Gid();
    PropertyValue prop(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(2.0)});
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty(test_property.data()), prop));
    ASSERT_NO_ERROR(vertex.AddLabel(acc->NameToLabel("A")));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    ASSERT_NO_ERROR(vertex.AddLabel(acc->NameToLabel("B")));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  auto acc = this->storage->Access(memgraph::storage::READ);
  auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
  auto prop = vertex.GetProperty(acc->NameToProperty(test_property.data()), View::OLD).value();
  ASSERT_TRUE(prop.IsVectorIndexId());
  EXPECT_EQ(prop.ValueVectorIndexIds().size(), 1);
  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 1);
}

TEST_F(VectorIndexTest, RemoveLabelFromNonMemberOfAllOfIndexIsNoOp) {
  {
    auto unique_acc = this->storage->UniqueAccess();
    const auto a = unique_acc->NameToLabel("A");
    const auto b = unique_acc->NameToLabel("B");
    const auto property = unique_acc->NameToProperty(test_property.data());
    EXPECT_TRUE(unique_acc
                    ->CreateVectorIndex({.index_name = "and_idx",
                                         .label_filter = {.mode = VectorMatchMode::ALL_OF, .ids = {a, b}},
                                         .property = property,
                                         .metric_kind = metric,
                                         .dimension = 2,
                                         .resize_coefficient = resize_coefficient,
                                         .capacity = 10,
                                         .scalar_kind = scalar_kind})
                    .has_value());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  Gid vertex_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    vertex_gid = vertex.Gid();
    PropertyValue prop(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(2.0)});
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty(test_property.data()), prop));
    ASSERT_NO_ERROR(vertex.AddLabel(acc->NameToLabel("A")));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    EXPECT_NO_THROW(ASSERT_NO_ERROR(vertex.RemoveLabel(acc->NameToLabel("A"))));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  auto acc = this->storage->Access(memgraph::storage::READ);
  auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
  auto prop = vertex.GetProperty(acc->NameToProperty(test_property.data()), View::OLD).value();
  EXPECT_FALSE(prop.IsVectorIndexId());
  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);
}

TEST_F(VectorIndexTest, SetPropertyToScalarRemovesIndexedVertex) {
  this->CreateIndex(2, 10);

  Gid vertex_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto property_value = MakeVectorIndexProperty(acc.get(), memgraph::utils::small_vector<float>{1.0F, 2.0F});
    auto vertex = this->CreateVertex(acc.get(), test_property, property_value, test_label);
    vertex_gid = vertex.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 1);
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty(test_property), PropertyValue("not a vector")));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  auto acc = this->storage->Access(memgraph::storage::READ);
  EXPECT_EQ(acc->ListAllVectorIndices()[0].size, 0);
}

TEST_F(VectorIndexTest, SetEmptyListKeepsPlainListAndLeavesIndex) {
  this->CreateIndex(2, 10);
  Gid vertex_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto property_value = MakeVectorIndexProperty(acc.get(), memgraph::utils::small_vector<float>{1.0F, 1.0F});
    auto vertex = this->CreateVertex(acc.get(), test_property, property_value, test_label);
    vertex_gid = vertex.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    ASSERT_NO_ERROR(
        vertex.SetProperty(acc->NameToProperty(test_property), PropertyValue(std::vector<PropertyValue>{})));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  this->ExpectPlainEmptyList(vertex_gid, 0);
}

TEST_F(VectorIndexTest, CreateIndexOverExistingEmptyListLeavesEmptyList) {
  Gid vertex_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = this->CreateVertex(acc.get(), test_property, PropertyValue(std::vector<PropertyValue>{}), test_label);
    vertex_gid = vertex.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  this->CreateIndex(2, 10);
  this->ExpectPlainEmptyList(vertex_gid, 0);
}

TEST_F(VectorIndexTest, AddLabelToVertexWithEmptyListStaysPlainList) {
  this->CreateIndex(2, 10);
  Gid vertex_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    vertex_gid = vertex.Gid();
    ASSERT_NO_ERROR(
        vertex.SetProperty(acc->NameToProperty(test_property), PropertyValue(std::vector<PropertyValue>{})));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    ASSERT_NO_ERROR(vertex.AddLabel(acc->NameToLabel(test_label)));
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  this->ExpectPlainEmptyList(vertex_gid, 0);
}

// Aborting a write over an indexed or empty-list vertex restores the prior value and index membership.
TEST_F(VectorIndexTest, AbortRestoresPriorValueAndIndexMembership) {
  struct Row {
    bool initially_indexed;
    PropertyValue overwrite;
    bool overwrite_with_tag = false;
  };

  const std::vector<PropertyValue> two_doubles{PropertyValue(1.0), PropertyValue(2.0)};
  const std::array rows{Row{true, PropertyValue(std::vector<PropertyValue>{})},
                        Row{false, PropertyValue(two_doubles)},
                        Row{true, PropertyValue("str")},
                        Row{false, PropertyValue("str")},
                        Row{false, PropertyValue(), true}};

  for (const auto &row : rows) {
    SCOPED_TRACE(testing::Message() << "initially_indexed=" << row.initially_indexed
                                    << " overwrite_with_tag=" << row.overwrite_with_tag);
    storage = std::make_unique<InMemoryStorage>();
    this->CreateIndex(2, 10);
    Gid vertex_gid;
    {
      auto acc = this->storage->Access(memgraph::storage::WRITE);
      const auto initial = row.initially_indexed
                               ? MakeVectorIndexProperty(acc.get(), memgraph::utils::small_vector<float>{1.0F, 2.0F})
                               : PropertyValue(std::vector<PropertyValue>{});
      vertex_gid = this->CreateVertex(acc.get(), test_property, initial, test_label).Gid();
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
    }
    const std::size_t expected_size = row.initially_indexed ? 1 : 0;
    {
      auto acc = this->storage->Access(memgraph::storage::WRITE);
      auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
      const auto overwrite = row.overwrite_with_tag
                                 ? MakeVectorIndexProperty(acc.get(), memgraph::utils::small_vector<float>{1.0F, 1.0F})
                                 : row.overwrite;
      ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty(test_property), overwrite));
      acc->Abort();
    }
    if (row.initially_indexed) {
      auto acc = this->storage->Access(memgraph::storage::READ);
      EXPECT_EQ(acc->ListAllVectorIndices()[0].size, expected_size);
      auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
      const auto stored = vertex.GetProperty(acc->NameToProperty(test_property), View::OLD);
      ASSERT_TRUE(stored.has_value());
      EXPECT_TRUE(stored->IsVectorIndexId());
      EXPECT_EQ(stored->ValueVectorIndexList(), (memgraph::utils::small_vector<float>{1.0F, 2.0F}));
    } else {
      this->ExpectPlainEmptyList(vertex_gid, expected_size);
    }
  }
}

// An older main ships [] as a tag with no vector; SetProperty must store it as a plain empty list.
TEST_F(VectorIndexTest, SetEmptyVectorIndexTagStoresPlainEmptyList) {
  this->CreateIndex(2, 10);
  Gid vertex_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto empty_tag = MakeEmptyVectorIndexProperty(acc.get());
    ASSERT_TRUE(empty_tag.IsVectorIndexId());
    vertex_gid = this->CreateVertex(acc.get(), test_property, empty_tag, test_label).Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  this->ExpectPlainEmptyList(vertex_gid, 0);
  {
    auto unique_acc = this->storage->UniqueAccess();
    ASSERT_TRUE(unique_acc->DropVectorIndex(test_index.data()).has_value());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  this->ExpectPlainEmptyList(vertex_gid, std::nullopt);
}

// UpdateOnSetProperty: a tag with no embedded vector is the legacy on-disk form of [] and must become a
// plain empty list, dropping any earlier captured vector for that GID.
TEST_F(VectorIndexRecoveryTest, UpdateOnSetPropertyEmptyTagBecomesEmptyList) {
  static constexpr PropertyId kProp = PropertyId::FromUint(1);

  std::vector<VectorIndexRecoveryInfo> infos{CreateRecoveryInfo()};
  VectorIndexRecovery::VertexVectors vv;
  Vertex vertex(Gid::FromUint(77), nullptr);
  vv[kProp].emplace(vertex.gid, memgraph::utils::small_vector<float>{1.0F, 2.0F});

  PropertyValue empty_tag(
      PropertyValue::VectorIndexIdData{.ids = memgraph::utils::small_vector<uint64_t>{42}, .vector = {}});
  VectorIndexRecovery::UpdateOnSetProperty(kProp, empty_tag, &vertex, infos, vv);

  EXPECT_FALSE(empty_tag.IsVectorIndexId());
  EXPECT_TRUE(empty_tag.IsAnyList());
  EXPECT_EQ(empty_tag.ListSize(), 0u);
  EXPECT_FALSE(vv[kProp].contains(vertex.gid));
}

TEST_F(VectorIndexRecoveryTest, RecoverAllVectorIndicesLeavesEmptyListUntouched) {
  FLAGS_storage_parallel_schema_recovery = false;
  static constexpr PropertyId kProp = PropertyId::FromUint(1);

  {
    auto acc = vertices_.access();
    auto v0 = acc.find(Gid::FromUint(0));
    ASSERT_NE(v0, acc.end());
    v0->properties.SetProperty(kProp, PropertyValue(std::vector<double>{}));
  }

  std::vector<VectorIndexRecoveryInfo> infos{CreateRecoveryInfo()};
  VectorIndexRecovery::VertexVectors vv;
  auto vertices_acc = vertices_.access();
  EXPECT_NO_THROW(vector_index_.RecoverAllVectorIndices(infos,
                                                        vv,
                                                        vertices_acc,
                                                        storage_->name_id_mapper_.get(),
                                                        ActiveIndicesUpdater{storage_->indices_.active_indices_}));

  const auto info = vector_index_.ListVectorIndicesInfo();
  ASSERT_EQ(info.size(), 1);
  EXPECT_EQ(info[0].size, kNumNodes - 1);

  auto v0 = vertices_acc.find(Gid::FromUint(0));
  ASSERT_NE(v0, vertices_acc.end());
  const auto stored = v0->properties.GetProperty(kProp);
  EXPECT_FALSE(stored.IsVectorIndexId());
  EXPECT_TRUE(stored.IsAnyList());
  EXPECT_EQ(stored.ListSize(), 0u);
}

namespace {
using memgraph::utils::MemoryTracker;

mg_vector_index_t MakeUsearchIndex(MemoryTracker *tracker) {
  auto made = mg_vector_index_t::make(unum::usearch::metric_punned_t(2, metric, scalar_kind),
                                      {},
                                      {},
                                      TrackedVectorAllocator<64>{tracker},
                                      TrackedVectorAllocator<8>{tracker});
  MG_ASSERT(made);
  return std::move(made.index);
}

Vertex *FakeKey(std::size_t i) { return reinterpret_cast<Vertex *>((i + 1) * 8); }

const memgraph::utils::small_vector<float> kVector{1.0F, 2.0F};

VectorIndexSpec MakeSpec(const synchronized_mg_vector_index_t &sync_index, std::uint16_t coefficient) {
  return VectorIndexSpec{
      .index_name = "test_index",
      .label_filter = VectorLabelFilter{.mode = VectorMatchMode::SINGLE, .ids = {LabelId::FromUint(1)}},
      .property = PropertyId::FromUint(1),
      .metric_kind = metric,
      .dimension = 2,
      .resize_coefficient = coefficient,
      .capacity = sync_index.index.capacity(),
      .scalar_kind = scalar_kind};
}

void FillToCapacity(synchronized_mg_vector_index_t &sync_index, VectorIndexSpec &spec) {
  const auto capacity = sync_index.index.capacity();
  for (std::size_t i = 0; i < capacity; ++i) {
    UpdateVectorIndex(sync_index, spec, FakeKey(i), kVector);
  }
  ASSERT_EQ(sync_index.index.size(), capacity);
  ASSERT_EQ(sync_index.index.capacity(), capacity);
}
}  // namespace

TEST(VectorIndexReserve, ReserveYieldsKeyLookupSlotCount) {
  const std::pair<std::size_t, std::size_t> kCases[] = {{1, 64},
                                                        {10, 64},
                                                        {43, 64},
                                                        {64, 128},
                                                        {171, 256},
                                                        {683, 1024},
                                                        {1000, 2048},
                                                        {100'000, 262'144},
                                                        {1'048'577, 2'097'152}};
  for (const auto &[n, expected] : kCases) {
    MemoryTracker tape;
    auto index = MakeUsearchIndex(&tape);
    ReserveOrThrow(index, "test_index", n);
    const auto capacity = index.capacity();
    EXPECT_EQ(capacity, expected) << n;
    EXPECT_EQ(capacity, ReservedSlots(n)) << n;
    if (n > 100'000) continue;

    for (std::size_t i = 0; i < capacity; ++i) {
      auto result = index.add(FakeKey(i), kVector.data());
      ASSERT_FALSE(result.error) << n << " " << i;
    }
    auto overflow = index.add(FakeKey(capacity), kVector.data());
    EXPECT_TRUE(overflow.error) << n;
    overflow.error.release();
    EXPECT_EQ(index.capacity(), capacity) << n;
  }
}

TEST(VectorIndexReserve, AddRefusedByTrackerDoesNotTerminate) {
  MemoryTracker tape;
  tape.SetHardLimit(tape.Amount() + 64 * 1024);
  synchronized_mg_vector_index_t sync_index{MakeUsearchIndex(&tape)};
  sync_index.memory_tracker = &tape;
  ReserveOrThrow(sync_index.index, "test_index", 10'000);

  VectorIndexSpec spec{
      .index_name = "test_index",
      .label_filter = VectorLabelFilter{.mode = VectorMatchMode::SINGLE, .ids = {LabelId::FromUint(1)}},
      .property = PropertyId::FromUint(1),
      .metric_kind = metric,
      .dimension = 2,
      .resize_coefficient = 2,
      .capacity = sync_index.index.capacity(),
      .scalar_kind = scalar_kind};

  constexpr std::size_t kLoopBound = 100'000;
  std::size_t last = 0;
  bool refused = false;
  {
    const MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
    try {
      for (; last < kLoopBound; ++last) {
        UpdateVectorIndex(sync_index, spec, FakeKey(last), kVector);
      }
    } catch (const memgraph::utils::OutOfMemoryException &) {
      refused = true;
    }
  }
  ASSERT_TRUE(refused);

  tape.SetHardLimit(0);
  EXPECT_FALSE(sync_index.index.contains(FakeKey(last)));
  UpdateVectorIndex(sync_index, spec, FakeKey(last), kVector);
  EXPECT_TRUE(sync_index.index.contains(FakeKey(last)));
  UpdateVectorIndex(sync_index, spec, FakeKey(last), memgraph::utils::small_vector<float>{});
  EXPECT_FALSE(sync_index.index.contains(FakeKey(last)));
  EXPECT_FALSE(sync_index.index.search(kVector.data(), 1).error);
}

TEST(VectorIndexReserve, ResizeCoefficientOneGrows) {
  for (const std::uint16_t coefficient : {std::uint16_t{1}, std::uint16_t{2}}) {
    MemoryTracker tape;
    synchronized_mg_vector_index_t sync_index{MakeUsearchIndex(&tape)};
    sync_index.memory_tracker = &tape;
    ReserveOrThrow(sync_index.index, "test_index", 10);

    VectorIndexSpec spec{
        .index_name = "test_index",
        .label_filter = VectorLabelFilter{.mode = VectorMatchMode::SINGLE, .ids = {LabelId::FromUint(1)}},
        .property = PropertyId::FromUint(1),
        .metric_kind = metric,
        .dimension = 2,
        .resize_coefficient = coefficient,
        .capacity = sync_index.index.capacity(),
        .scalar_kind = scalar_kind};

    constexpr std::size_t kKeys = 5000;
    for (std::size_t i = 0; i < kKeys; ++i) {
      ASSERT_NO_THROW(UpdateVectorIndex(sync_index, spec, FakeKey(i), kVector)) << coefficient << " " << i;
    }
    EXPECT_EQ(sync_index.index.size(), kKeys) << coefficient;
  }
}

TEST(VectorIndexReserve, GrowthInsideAtomicMemoryBlockIsCapped) {
  MemoryTracker tape;
  synchronized_mg_vector_index_t sync_index{MakeUsearchIndex(&tape)};
  sync_index.memory_tracker = &tape;
  ReserveOrThrow(sync_index.index, "test_index", 1000);
  auto spec = MakeSpec(sync_index, std::numeric_limits<std::uint16_t>::max());
  FillToCapacity(sync_index, spec);
  const auto capacity = sync_index.index.capacity();

  ASSERT_NO_THROW(
      memgraph::utils::AtomicMemoryBlock([&] { UpdateVectorIndex(sync_index, spec, FakeKey(capacity), kVector); }));
  EXPECT_TRUE(sync_index.index.contains(FakeKey(capacity)));
  EXPECT_EQ(sync_index.index.capacity(), ReservedSlots(capacity + capacity / 8));
  EXPECT_EQ(spec.capacity, sync_index.index.capacity());
}

TEST(VectorIndexReserve, UpdatingExistingKeyOnFullIndexDoesNotGrow) {
  MemoryTracker tape;
  synchronized_mg_vector_index_t sync_index{MakeUsearchIndex(&tape)};
  sync_index.memory_tracker = &tape;
  ReserveOrThrow(sync_index.index, "test_index", 1000);
  auto spec = MakeSpec(sync_index, std::numeric_limits<std::uint16_t>::max());
  FillToCapacity(sync_index, spec);
  const auto capacity = sync_index.index.capacity();

  const memgraph::utils::small_vector<float> other{3.0F, 4.0F};
  UpdateVectorIndex(sync_index, spec, FakeKey(7), other);
  EXPECT_EQ(sync_index.index.capacity(), capacity);
  EXPECT_EQ(sync_index.index.size(), capacity);

  EnsureVectorIndexHeadroom(sync_index, spec, FakeKey(7));
  EXPECT_EQ(sync_index.index.capacity(), capacity);
}

TEST(VectorIndexReserve, EnsureHeadroomGrowsFullIndexForNewKey) {
  MemoryTracker tape;
  synchronized_mg_vector_index_t sync_index{MakeUsearchIndex(&tape)};
  sync_index.memory_tracker = &tape;
  ReserveOrThrow(sync_index.index, "test_index", 1000);
  auto spec = MakeSpec(sync_index, 2);
  FillToCapacity(sync_index, spec);
  const auto capacity = sync_index.index.capacity();

  EnsureVectorIndexHeadroom(sync_index, spec, FakeKey(capacity));
  EXPECT_EQ(sync_index.index.capacity(), ReservedSlots(2 * capacity));
  EXPECT_EQ(spec.capacity, sync_index.index.capacity());
}

TEST_F(VectorIndexRecoveryTest, ReserveBeyondSlotRangeIsRejected) {
  for (const std::size_t capacity : {kMaxVectorIndexCapacity + 1, std::size_t{1} << 62U}) {
    auto spec = CreateRecoveryInfo("test_index", capacity).spec;
    auto vertices_acc = vertices_.access();
    EXPECT_THROW(vector_index_.CreateIndex(spec, vertices_acc, &storage_->indices_, storage_->name_id_mapper_.get()),
                 VectorSearchException);
    EXPECT_TRUE(vector_index_.ListVectorIndicesInfo().empty());
  }
}
