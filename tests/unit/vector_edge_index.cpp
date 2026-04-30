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
#include <chrono>
#include <string_view>
#include <thread>

#include "flags/general.hpp"
#include "query/exceptions.hpp"
#include "storage/v2/indices/active_indices_updater.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace memgraph::storage;

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define ASSERT_NO_ERROR(result) ASSERT_TRUE((result).has_value())

static constexpr std::string_view test_index = "test_edge_index";
static constexpr std::string_view test_edge_type = "test_edge_type";
static constexpr std::string_view test_property = "test_property";
static constexpr unum::usearch::metric_kind_t metric = unum::usearch::metric_kind_t::l2sq_k;
static constexpr std::size_t resize_coefficient = 2;
static constexpr unum::usearch::scalar_kind_t scalar_kind = unum::usearch::scalar_kind_t::f32_k;

class VectorEdgeIndexTest : public testing::Test {
 public:
  std::unique_ptr<Storage> storage;

  void SetUp() override { storage = std::make_unique<InMemoryStorage>(config_); }

  void TearDown() override { storage.reset(); }

  void CreateEdgeIndex(std::uint16_t dimension, std::size_t capacity) {
    auto unique_acc = this->storage->UniqueAccess();
    const auto edge_type = unique_acc->NameToEdgeType(test_edge_type.data());
    const auto property = unique_acc->NameToProperty(test_property.data());
    auto spec = VectorEdgeIndexSpec{
        .index_name = test_index.data(),
        .edge_type_filter = VectorEdgeTypeFilter{.mode = VectorEdgeTypeMode::SINGLE, .edge_types = {edge_type}},
        .property = property,
        .metric_kind = metric,
        .dimension = dimension,
        .resize_coefficient = resize_coefficient,
        .capacity = capacity,
        .scalar_kind = scalar_kind};
    EXPECT_FALSE(!unique_acc->CreateVectorEdgeIndex(spec).has_value());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  std::tuple<VertexAccessor, VertexAccessor, EdgeAccessor> CreateEdge(Storage::Accessor *accessor,
                                                                      std::string_view property,
                                                                      const PropertyValue &property_value,
                                                                      std::string_view edge_type) {
    VertexAccessor from_vertex = accessor->CreateVertex();
    VertexAccessor to_vertex = accessor->CreateVertex();
    const auto etype = accessor->NameToEdgeType(edge_type);
    auto edge_result = accessor->CreateEdge(&from_vertex, &to_vertex, etype);
    MG_ASSERT(edge_result.has_value());
    auto edge = edge_result.value();
    MG_ASSERT(edge.SetProperty(accessor->NameToProperty(property), property_value).has_value());
    return {from_vertex, to_vertex, edge};
  }

 private:
  memgraph::storage::Config config_;
};

TEST_F(VectorEdgeIndexTest, SimpleAddEdgeTest) {
  this->CreateEdgeIndex(2, 10);
  auto acc = this->storage->Access(memgraph::storage::WRITE);
  PropertyValue property_value(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  this->CreateEdge(acc.get(), test_property, property_value, test_edge_type);
  this->CreateEdge(acc.get(), "wrong_property", property_value, test_edge_type);
  this->CreateEdge(acc.get(), test_property, property_value, "wrong_edge_type");
  ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  const auto all_vector_indices = acc->ListAllVectorEdgeIndices();
  EXPECT_EQ(all_vector_indices.size(), 1);
}

TEST_F(VectorEdgeIndexTest, SimpleSearchTest) {
  this->CreateEdgeIndex(2, 10);
  auto acc = this->storage->Access(memgraph::storage::WRITE);
  PropertyValue property_value(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  auto [from_vertex, to_vertex, edge] = this->CreateEdge(acc.get(), test_property, property_value, test_edge_type);
  ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  const auto result = acc->VectorIndexSearchOnEdges(test_index.data(), 1, std::vector<float>{1.0, 1.0});
  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(std::get<0>(result[0]).Gid(), edge.Gid());
}

TEST_F(VectorEdgeIndexTest, InvalidDimensionTest) {
  this->CreateEdgeIndex(2, 10);
  auto acc = this->storage->Access(memgraph::storage::WRITE);
  std::vector<PropertyValue> properties(3, PropertyValue(1.0));
  PropertyValue property_value(properties);
  EXPECT_THROW(this->CreateEdge(acc.get(), test_property, property_value, test_edge_type),
               memgraph::query::VectorSearchException);
}

TEST_F(VectorEdgeIndexTest, SearchWithMultipleEdges) {
  this->CreateEdgeIndex(2, 10);
  auto acc = this->storage->Access(memgraph::storage::WRITE);
  PropertyValue properties1(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  [[maybe_unused]] auto [from_vertex1, to_vertex1, edge1] =
      this->CreateEdge(acc.get(), test_property, properties1, test_edge_type);
  PropertyValue properties2(std::vector<PropertyValue>{PropertyValue(10.0), PropertyValue(10.0)});
  auto [from_vertex2, to_vertex2, edge2] = this->CreateEdge(acc.get(), test_property, properties2, test_edge_type);
  ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  EXPECT_EQ(acc->ListAllVectorEdgeIndices().size(), 1);
  std::vector<float> query = {10.0, 10.0};
  const auto result = acc->VectorIndexSearchOnEdges(test_index.data(), 1, query);
  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(std::get<0>(result[0]).Gid(), edge2.Gid());
  const auto result2 = acc->VectorIndexSearchOnEdges(test_index.data(), 2, query);
  EXPECT_EQ(result2.size(), 2);
}

TEST_F(VectorEdgeIndexTest, ConcurrencyTest) {
  this->CreateEdgeIndex(2, 10);
  const auto hardware_concurrency = std::thread::hardware_concurrency();
  const auto index_size = hardware_concurrency > 0 ? hardware_concurrency : 1;
  std::vector<std::thread> threads;
  threads.reserve(index_size);
  for (int i = 0; i < index_size; i++) {
    threads.emplace_back(std::thread([this, i]() {
      auto acc = this->storage->Access(memgraph::storage::WRITE);
      PropertyValue properties(
          std::vector<PropertyValue>{PropertyValue(static_cast<double>(i)), PropertyValue(static_cast<double>(i + 1))});
      [[maybe_unused]] auto [from_vertex, to_vertex, edge] =
          this->CreateEdge(acc.get(), test_property, properties, test_edge_type);
      ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
    }));
  }
  for (auto &thread : threads) {
    thread.join();
  }
  auto acc = this->storage->Access(memgraph::storage::WRITE);
  EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, index_size);
}

TEST_F(VectorEdgeIndexTest, UpdatePropertyValueTest) {
  this->CreateEdgeIndex(2, 10);
  Gid edge_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    PropertyValue initial_value(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
    auto [from_vertex, to_vertex, edge] = this->CreateEdge(acc.get(), test_property, initial_value, test_edge_type);
    edge_gid = edge.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    PropertyValue updated_value(std::vector<PropertyValue>{PropertyValue(2.0), PropertyValue(2.0)});
    MG_ASSERT(edge.SetProperty(acc->NameToProperty(test_property), updated_value).has_value());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
    const auto search_result = acc->VectorIndexSearchOnEdges(test_index.data(), 1, std::vector<float>{2.0, 2.0});
    EXPECT_EQ(search_result.size(), 1);
    EXPECT_EQ(std::get<0>(search_result[0]).Gid(), edge_gid);
  }
}

TEST_F(VectorEdgeIndexTest, DeleteEdgeTest) {
  this->CreateEdgeIndex(2, 10);
  Gid edge_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
    auto [from_vertex, to_vertex, edge] = this->CreateEdge(acc.get(), test_property, properties, test_edge_type);
    edge_gid = edge.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    auto maybe_deleted_edge = acc->DeleteEdge(&edge);
    EXPECT_EQ(maybe_deleted_edge.has_value(), true);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  this->storage->FreeMemory();
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    std::vector<float> query = {1.0, 1.0};
    const auto result = acc->VectorIndexSearchOnEdges(test_index.data(), 1, query);
    EXPECT_EQ(result.size(), 0);
  }
}

TEST_F(VectorEdgeIndexTest, MultipleAbortsAndUpdatesTest) {
  this->CreateEdgeIndex(2, 10);
  Gid edge_gid;
  PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  PropertyValue null_value;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto [from_vertex, to_vertex, edge] = this->CreateEdge(acc.get(), test_property, properties, test_edge_type);
    edge_gid = edge.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  // Verify index has 1 entry after first commit
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, 1);
    // Verify the property is stored as VectorIndexId
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    auto prop = edge.GetProperty(acc->NameToProperty(test_property), View::OLD);
    ASSERT_TRUE(prop.has_value());
    EXPECT_TRUE(prop->IsVectorIndexId());
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    MG_ASSERT(edge.SetProperty(acc->NameToProperty(test_property), null_value).has_value());
    acc->Abort();
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, 1);
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    MG_ASSERT(edge.SetProperty(acc->NameToProperty(test_property), null_value).has_value());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, 0);
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    MG_ASSERT(edge.SetProperty(acc->NameToProperty(test_property), properties).has_value());
    acc->Abort();
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, 0);
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    // add new edge to the index
    [[maybe_unused]] auto [from_vertex, to_vertex, edge] =
        this->CreateEdge(acc.get(), test_property, properties, test_edge_type);
    acc->Abort();
    // check that the index is still empty
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, 0);
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    // add new edge to the index
    [[maybe_unused]] auto [from_vertex, to_vertex, edge] =
        this->CreateEdge(acc.get(), test_property, properties, test_edge_type);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
    edge_gid = edge.Gid();
    // check that the index is not empty
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, 1);
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    // delete the edge
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    EXPECT_EQ(acc->DeleteEdge(&edge).has_value(), true);
    acc->Abort();
    // check that the index is still not empty
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, 1);
  }
}

TEST_F(VectorEdgeIndexTest, RemoveEntriesTest) {
  this->CreateEdgeIndex(2, 10);
  Gid edge_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
    auto [from_vertex, to_vertex, edge] = this->CreateEdge(acc.get(), test_property, properties, test_edge_type);
    edge_gid = edge.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    auto maybe_deleted_edge = acc->DeleteEdge(&edge);
    EXPECT_EQ(maybe_deleted_edge.has_value(), true);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
    auto *mem_storage = static_cast<InMemoryStorage *>(this->storage.get());
    mem_storage->indices_.vector_edge_index_.RemoveEdges(std::array<Edge *, 1>{edge.edge_.ptr});
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, 0);
  }
}

TEST_F(VectorEdgeIndexTest, IndexResizeTest) {
  this->CreateEdgeIndex(2, 1);
  auto size = 0;
  auto capacity = 1;
  PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  while (size <= capacity) {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    [[maybe_unused]] auto [from_vertex, to_vertex, edge] =
        this->CreateEdge(acc.get(), test_property, properties, test_edge_type);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
    size++;
  }
  auto acc = this->storage->Access(memgraph::storage::WRITE);
  const auto all_vector_indices = acc->ListAllVectorEdgeIndices();
  size = all_vector_indices[0].size;
  capacity = all_vector_indices[0].capacity;
  EXPECT_GT(capacity, size);
}

TEST_F(VectorEdgeIndexTest, DropIndexTest) {
  this->CreateEdgeIndex(2, 10);
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
    [[maybe_unused]] auto [from_vertex, to_vertex, edge] =
        this->CreateEdge(acc.get(), test_property, properties, test_edge_type);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto unique_acc = this->storage->UniqueAccess();
    EXPECT_FALSE(!unique_acc->DropVectorIndex(test_index).has_value());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllVectorEdgeIndices().size(), 0);
  }
}

TEST_F(VectorEdgeIndexTest, ClearTest) {
  this->CreateEdgeIndex(2, 10);
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
    [[maybe_unused]] auto [from_vertex, to_vertex, edge] =
        this->CreateEdge(acc.get(), test_property, properties, test_edge_type);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
    auto *mem_storage = static_cast<InMemoryStorage *>(this->storage.get());
    mem_storage->indices_.DropGraphClearIndices();
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllVectorEdgeIndices().size(), 0);
  }
}

TEST_F(VectorEdgeIndexTest, CreateIndexWhenEdgesExistsAlreadyTest) {
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
    static constexpr std::string_view test_edge_type_2 = "test_edge_type2";
    [[maybe_unused]] auto [from_vertex1, to_vertex1, edge1] =
        this->CreateEdge(acc.get(), test_property, properties, test_edge_type);
    [[maybe_unused]] auto [from_vertex2, to_vertex2, edge2] =
        this->CreateEdge(acc.get(), test_property, properties, test_edge_type_2);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  this->CreateEdgeIndex(2, 10);
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllVectorEdgeIndices().size(), 1);
  }
}

TEST_F(VectorEdgeIndexTest, CreateIndexWithWrongDimensionRollsBack) {
  PropertyValue good_vec(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(2.0)});
  PropertyValue bad_vec(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(2.0), PropertyValue(3.0)});
  Gid good_edge_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto [fv1, tv1, e1] = this->CreateEdge(acc.get(), test_property, good_vec, test_edge_type);
    good_edge_gid = e1.Gid();
    [[maybe_unused]] auto [fv2, tv2, e2] = this->CreateEdge(acc.get(), test_property, bad_vec, test_edge_type);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  EXPECT_THROW(this->CreateEdgeIndex(2, 10), memgraph::query::VectorSearchException);
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    EXPECT_EQ(acc->ListAllVectorEdgeIndices().size(), 0);
    auto e1 = acc->FindEdge(good_edge_gid, View::OLD).value();
    auto prop = e1.GetProperty(acc->NameToProperty(test_property), View::OLD);
    EXPECT_TRUE(prop->IsDoubleList());
    EXPECT_EQ(prop->ValueDoubleList().size(), 2);
  }
}

TEST_F(VectorEdgeIndexTest, CreateIndexConvertsPropertiesToVectorIndexId) {
  Gid edge_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(2.0)});
    auto [fv, tv, edge] = this->CreateEdge(acc.get(), test_property, properties, test_edge_type);
    edge_gid = edge.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    auto prop = edge.GetProperty(acc->NameToProperty(test_property), View::OLD);
    EXPECT_TRUE(prop->IsList());
  }
  this->CreateEdgeIndex(2, 10);
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    EXPECT_EQ(acc->ListAllVectorEdgeIndices().size(), 1);
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, 1);
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    auto prop = edge.GetProperty(acc->NameToProperty(test_property), View::OLD);
    EXPECT_TRUE(prop->IsVectorIndexId());
    EXPECT_EQ(prop->ValueVectorIndexList().size(), 2);
    EXPECT_FLOAT_EQ(prop->ValueVectorIndexList()[0], 1.0f);
    EXPECT_FLOAT_EQ(prop->ValueVectorIndexList()[1], 2.0f);
  }
}

TEST_F(VectorEdgeIndexTest, IndexedPropertyDecoderDecodesVectorIndexId) {
  this->CreateEdgeIndex(2, 10);
  Gid edge_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    PropertyValue properties(std::vector<PropertyValue>{PropertyValue(3.0), PropertyValue(4.0)});
    auto [fv, tv, edge] = this->CreateEdge(acc.get(), test_property, properties, test_edge_type);
    edge_gid = edge.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    // GetProperty goes through IndexedPropertyDecoder<Edge> which fetches the vector from uSearch.
    auto prop = edge.GetProperty(acc->NameToProperty(test_property), View::OLD);
    ASSERT_TRUE(prop.has_value());
    EXPECT_TRUE(prop->IsVectorIndexId());
    ASSERT_EQ(prop->ValueVectorIndexList().size(), 2);
    EXPECT_FLOAT_EQ(prop->ValueVectorIndexList()[0], 3.0f);
    EXPECT_FLOAT_EQ(prop->ValueVectorIndexList()[1], 4.0f);
    // Properties() also goes through the decoder.
    auto all_props = edge.Properties(View::OLD);
    ASSERT_TRUE(all_props.has_value());
    auto it = all_props->find(acc->NameToProperty(test_property));
    ASSERT_NE(it, all_props->end());
    EXPECT_TRUE(it->second.IsVectorIndexId());
    ASSERT_EQ(it->second.ValueVectorIndexList().size(), 2);
    EXPECT_FLOAT_EQ(it->second.ValueVectorIndexList()[0], 3.0f);
    EXPECT_FLOAT_EQ(it->second.ValueVectorIndexList()[1], 4.0f);
  }
}

TEST_F(VectorEdgeIndexTest, DropIndexRestoresPropertiesToLists) {
  Gid edge_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(2.0)});
    auto [fv, tv, edge] = this->CreateEdge(acc.get(), test_property, properties, test_edge_type);
    edge_gid = edge.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  this->CreateEdgeIndex(2, 10);
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    EXPECT_TRUE(edge.GetProperty(acc->NameToProperty(test_property), View::OLD)->IsVectorIndexId());
  }
  {
    auto unique_acc = this->storage->UniqueAccess();
    EXPECT_FALSE(!unique_acc->DropVectorIndex(test_index).has_value());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    EXPECT_EQ(acc->ListAllVectorEdgeIndices().size(), 0);
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    auto prop = edge.GetProperty(acc->NameToProperty(test_property), View::OLD);
    EXPECT_TRUE(prop->IsDoubleList());
    auto list = prop->ValueDoubleList();
    EXPECT_EQ(list.size(), 2);
    EXPECT_DOUBLE_EQ(list[0], 1.0);
    EXPECT_DOUBLE_EQ(list[1], 2.0);
  }
}

class VectorEdgeIndexRecoveryTest : public testing::Test {
 public:
  static constexpr std::uint16_t kDimension = 2;
  static constexpr std::size_t kNumEdges = 100;

  void SetUp() override {
    // Initialize the active indices store with a valid ActiveIndices object
    // so that ActiveIndicesUpdater assertions pass during recovery.
    active_indices_store_.WithLock([&](ActiveIndicesPtr &ai) {
      ai = std::make_shared<ActiveIndices>(nullptr,
                                           nullptr,
                                           nullptr,
                                           nullptr,
                                           nullptr,
                                           nullptr,
                                           nullptr,
                                           nullptr,
                                           nullptr,
                                           vector_edge_index_.GetActiveIndices());
    });

    auto vertices_acc = vertices_.access();
    auto edges_acc = edges_.access();

    // Create pairs of vertices and edges between them
    for (std::size_t i = 0; i < kNumEdges; i++) {
      // Create from and to vertices
      auto from_gid = Gid::FromUint(i * 2);
      auto to_gid = Gid::FromUint((i * 2) + 1);
      auto [from_vertex_iter, from_inserted] = vertices_acc.insert(Vertex{from_gid, nullptr});
      ASSERT_TRUE(from_inserted);
      auto [to_vertex_iter, to_inserted] = vertices_acc.insert(Vertex{to_gid, nullptr});
      ASSERT_TRUE(to_inserted);

      // Create edge
      auto edge_gid = Gid::FromUint(i);
      auto [edge_iter, edge_inserted] = edges_acc.insert(Edge{edge_gid, nullptr});
      ASSERT_TRUE(edge_inserted);

      // Set edge property (vector)
      PropertyValue property_value(
          std::vector<PropertyValue>{PropertyValue(static_cast<double>(i)), PropertyValue(static_cast<double>(i + 1))});
      edge_iter->properties.SetProperty(PropertyId::FromUint(1), property_value);

      // Connect edge to vertices via out_edges
      EdgeRef edge_ref(&(*edge_iter));
      from_vertex_iter->out_edges.emplace_back(EdgeTypeId::FromUint(1), &(*to_vertex_iter), edge_ref);
    }
  }

  static VectorEdgeIndexSpec CreateSpec(const std::string &name = "test_edge_index") {
    return VectorEdgeIndexSpec{.index_name = name,
                               .edge_type_filter = VectorEdgeTypeFilter{.mode = VectorEdgeTypeMode::SINGLE, .edge_types = {EdgeTypeId::FromUint(1)}},
                               .property = PropertyId::FromUint(1),
                               .metric_kind = unum::usearch::metric_kind_t::l2sq_k,
                               .dimension = kDimension,
                               .resize_coefficient = 2,
                               .capacity = kNumEdges,
                               .scalar_kind = unum::usearch::scalar_kind_t::f32_k};
  }

  memgraph::utils::SkipListDb<Vertex> vertices_;
  memgraph::utils::SkipListDb<Edge> edges_;
  VectorEdgeIndex vector_edge_index_;
  NameIdMapper name_id_mapper_;
  ActiveIndicesStore active_indices_store_;
};

TEST_F(VectorEdgeIndexRecoveryTest, RecoverIndexSingleThreadTest) {
  // Ensure single-threaded recovery
  FLAGS_storage_parallel_schema_recovery = false;

  auto vertices_acc = vertices_.access();
  auto spec = CreateSpec();
  VectorEdgeIndexRecoveryInfo recovery_info{.spec = spec, .index_entries = {}};

  EXPECT_NO_THROW(vector_edge_index_.RecoverIndex(
      recovery_info, vertices_acc, &name_id_mapper_, ActiveIndicesUpdater{active_indices_store_}));

  // Verify all edges are in the index
  const auto vector_index_info = vector_edge_index_.ListVectorIndicesInfo();
  EXPECT_EQ(vector_index_info.size(), 1);
  EXPECT_EQ(vector_index_info[0].size, kNumEdges);

  // Search for each edge and verify it's found
  auto edges_acc = edges_.access();
  for (auto &edge : edges_acc) {
    Vertex *from_vertex = nullptr;
    Vertex *to_vertex = nullptr;
    for (auto &vertex : vertices_acc) {
      for (auto &edge_tuple : vertex.out_edges) {
        if (std::get<kEdgeRefPos>(edge_tuple).ptr == &edge) {
          from_vertex = &vertex;
          to_vertex = std::get<kVertexPos>(edge_tuple);
          break;
        }
      }
      if (from_vertex) break;
    }
    ASSERT_NE(from_vertex, nullptr);
    ASSERT_NE(to_vertex, nullptr);

    const auto vector = vector_edge_index_.GetVectorPropertyFromEdgeIndex(&edge, "test_edge_index", &name_id_mapper_);
    EXPECT_EQ(vector.size(), kDimension);
    EXPECT_EQ(vector[0], static_cast<float>(edge.gid.AsUint()));
    EXPECT_EQ(vector[1], static_cast<float>(edge.gid.AsUint() + 1));
  }
}

TEST_F(VectorEdgeIndexRecoveryTest, RecoverIndexParallelTest) {
  // Enable parallel recovery with multiple threads
  FLAGS_storage_parallel_schema_recovery = true;
  FLAGS_storage_recovery_thread_count =
      (std::thread::hardware_concurrency() > 0) ? std::thread::hardware_concurrency() : 1;

  auto vertices_acc = vertices_.access();
  auto spec = CreateSpec();
  VectorEdgeIndexRecoveryInfo recovery_info{.spec = spec, .index_entries = {}};

  EXPECT_NO_THROW(vector_edge_index_.RecoverIndex(
      recovery_info, vertices_acc, &name_id_mapper_, ActiveIndicesUpdater{active_indices_store_}));

  // Verify all edges are in the index
  const auto vector_index_info = vector_edge_index_.ListVectorIndicesInfo();
  EXPECT_EQ(vector_index_info.size(), 1);
  EXPECT_EQ(vector_index_info[0].size, kNumEdges);

  // Verify all edges are in the index
  auto edges_acc = edges_.access();
  for (auto &edge : edges_acc) {
    Vertex *from_vertex = nullptr;
    Vertex *to_vertex = nullptr;
    for (auto &vertex : vertices_acc) {
      for (auto &edge_tuple : vertex.out_edges) {
        if (std::get<kEdgeRefPos>(edge_tuple).ptr == &edge) {
          from_vertex = &vertex;
          to_vertex = std::get<kVertexPos>(edge_tuple);
          break;
        }
      }
      if (from_vertex) break;
    }
    ASSERT_NE(from_vertex, nullptr);
    ASSERT_NE(to_vertex, nullptr);

    const auto vector = vector_edge_index_.GetVectorPropertyFromEdgeIndex(&edge, "test_edge_index", &name_id_mapper_);
    EXPECT_EQ(vector.size(), kDimension);
    EXPECT_EQ(vector[0], static_cast<float>(edge.gid.AsUint()));
    EXPECT_EQ(vector[1], static_cast<float>(edge.gid.AsUint() + 1));
  }
}

TEST_F(VectorEdgeIndexRecoveryTest, ConcurrentAddWithResizeTest) {
  FLAGS_storage_parallel_schema_recovery = true;
  FLAGS_storage_recovery_thread_count =
      (std::thread::hardware_concurrency() > 0) ? std::thread::hardware_concurrency() : 4;

  auto vertices_acc = vertices_.access();

  auto spec = VectorEdgeIndexSpec{.index_name = "resize_test_edge_index",
                                  .edge_type_filter = VectorEdgeTypeFilter{.mode = VectorEdgeTypeMode::SINGLE, .edge_types = {EdgeTypeId::FromUint(1)}},
                                  .property = PropertyId::FromUint(1),
                                  .metric_kind = unum::usearch::metric_kind_t::l2sq_k,
                                  .dimension = kDimension,
                                  .resize_coefficient = 2,
                                  .capacity = 10,
                                  .scalar_kind = unum::usearch::scalar_kind_t::f32_k};
  VectorEdgeIndexRecoveryInfo recovery_info{.spec = spec, .index_entries = {}};

  EXPECT_NO_THROW(vector_edge_index_.RecoverIndex(
      recovery_info, vertices_acc, &name_id_mapper_, ActiveIndicesUpdater{active_indices_store_}));

  const auto vector_index_info = vector_edge_index_.ListVectorIndicesInfo();
  EXPECT_EQ(vector_index_info.size(), 1);
  EXPECT_EQ(vector_index_info[0].size, kNumEdges);
  EXPECT_GE(vector_index_info[0].capacity, kNumEdges);

  auto edges_acc = edges_.access();
  for (auto &edge : edges_acc) {
    Vertex *from_vertex = nullptr;
    Vertex *to_vertex = nullptr;
    for (auto &vertex : vertices_acc) {
      for (auto &edge_tuple : vertex.out_edges) {
        if (std::get<kEdgeRefPos>(edge_tuple).ptr == &edge) {
          from_vertex = &vertex;
          to_vertex = std::get<kVertexPos>(edge_tuple);
          break;
        }
      }
      if (from_vertex) break;
    }
    ASSERT_NE(from_vertex, nullptr);
    ASSERT_NE(to_vertex, nullptr);

    const auto vector =
        vector_edge_index_.GetVectorPropertyFromEdgeIndex(&edge, "resize_test_edge_index", &name_id_mapper_);
    EXPECT_EQ(vector.size(), kDimension);
    EXPECT_EQ(vector[0], static_cast<float>(edge.gid.AsUint()));
    EXPECT_EQ(vector[1], static_cast<float>((edge.gid.AsUint()) + 1));
  }
}

// Test fixture for GC-related vector edge index tests.
// Uses periodic GC with a short interval to trigger automatic garbage collection.
class VectorEdgeIndexGCTest : public testing::Test {
 public:
  std::unique_ptr<Storage> storage;

  void SetUp() override {
    memgraph::storage::Config config;
    config.gc.type = memgraph::storage::Config::Gc::Type::PERIODIC;
    config.gc.interval = std::chrono::milliseconds(100);
    storage = std::make_unique<InMemoryStorage>(config);
  }

  void TearDown() override { storage.reset(); }

  void CreateEdgeIndex(std::uint16_t dimension, std::size_t capacity) {
    auto unique_acc = this->storage->UniqueAccess();
    const auto edge_type = unique_acc->NameToEdgeType(test_edge_type.data());
    const auto property = unique_acc->NameToProperty(test_property.data());
    auto spec = VectorEdgeIndexSpec{
        .index_name = test_index.data(),
        .edge_type_filter = VectorEdgeTypeFilter{.mode = VectorEdgeTypeMode::SINGLE, .edge_types = {edge_type}},
        .property = property,
        .metric_kind = metric,
        .dimension = dimension,
        .resize_coefficient = resize_coefficient,
        .capacity = capacity,
        .scalar_kind = scalar_kind};
    EXPECT_FALSE(!unique_acc->CreateVectorEdgeIndex(spec).has_value());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  std::tuple<VertexAccessor, VertexAccessor, EdgeAccessor> CreateEdge(Storage::Accessor *accessor,
                                                                      std::string_view property,
                                                                      const PropertyValue &property_value,
                                                                      std::string_view edge_type) {
    VertexAccessor from_vertex = accessor->CreateVertex();
    VertexAccessor to_vertex = accessor->CreateVertex();
    const auto etype = accessor->NameToEdgeType(edge_type);
    auto edge_result = accessor->CreateEdge(&from_vertex, &to_vertex, etype);
    MG_ASSERT(edge_result.has_value());
    auto edge = edge_result.value();
    MG_ASSERT(edge.SetProperty(accessor->NameToProperty(property), property_value).has_value());
    return {from_vertex, to_vertex, edge};
  }
};

TEST_F(VectorEdgeIndexGCTest, AnalyticalModeDeleteEdgeGCCleansVectorIndex) {
  this->CreateEdgeIndex(2, 10);
  PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  Gid edge_gid;

  // Create an edge with a vector property in transactional mode
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto [from_vertex, to_vertex, edge] = this->CreateEdge(acc.get(), test_property, properties, test_edge_type);
    edge_gid = edge.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Verify the edge is in the vector index
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, 1);
  }

  // Switch to analytical mode
  static_cast<InMemoryStorage *>(this->storage.get())->SetStorageMode(StorageMode::IN_MEMORY_ANALYTICAL);

  // Delete the edge in analytical mode
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    auto maybe_deleted_edge = acc->DeleteEdge(&edge);
    EXPECT_TRUE(maybe_deleted_edge.has_value());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Switch back to transactional mode — this triggers GC which processes the full-scan path
  static_cast<InMemoryStorage *>(this->storage.get())->SetStorageMode(StorageMode::IN_MEMORY_TRANSACTIONAL);

  // Verify the edge is no longer in the vector index
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, 0);
  }
}

TEST_F(VectorEdgeIndexGCTest, AnalyticalModeDeleteMultipleEdgesGCCleansVectorIndex) {
  this->CreateEdgeIndex(2, 10);
  PropertyValue properties1(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  PropertyValue properties2(std::vector<PropertyValue>{PropertyValue(2.0), PropertyValue(2.0)});
  PropertyValue properties3(std::vector<PropertyValue>{PropertyValue(3.0), PropertyValue(3.0)});
  Gid edge_gid1;
  Gid edge_gid2;

  // Create three edges
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto [fv1, tv1, e1] = this->CreateEdge(acc.get(), test_property, properties1, test_edge_type);
    auto [fv2, tv2, e2] = this->CreateEdge(acc.get(), test_property, properties2, test_edge_type);
    [[maybe_unused]] auto [fv3, tv3, e3] = this->CreateEdge(acc.get(), test_property, properties3, test_edge_type);
    edge_gid1 = e1.Gid();
    edge_gid2 = e2.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, 3);
  }

  // Switch to analytical mode and delete two of the three edges
  static_cast<InMemoryStorage *>(this->storage.get())->SetStorageMode(StorageMode::IN_MEMORY_ANALYTICAL);

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto edge1 = acc->FindEdge(edge_gid1, View::OLD).value();
    EXPECT_TRUE(acc->DeleteEdge(&edge1).has_value());
    auto edge2 = acc->FindEdge(edge_gid2, View::OLD).value();
    EXPECT_TRUE(acc->DeleteEdge(&edge2).has_value());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Switch back — triggers GC
  static_cast<InMemoryStorage *>(this->storage.get())->SetStorageMode(StorageMode::IN_MEMORY_TRANSACTIONAL);

  // Only the third edge should remain in the index
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, 1);
    const auto result = acc->VectorIndexSearchOnEdges(test_index.data(), 1, std::vector<float>{3.0, 3.0});
    EXPECT_EQ(result.size(), 1);
  }
}

TEST_F(VectorEdgeIndexGCTest, TransactionalModeDeleteEdgeGCCleansVectorIndex) {
  this->CreateEdgeIndex(2, 10);
  PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  Gid edge_gid;

  // Create an edge
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto [from_vertex, to_vertex, edge] = this->CreateEdge(acc.get(), test_property, properties, test_edge_type);
    edge_gid = edge.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, 1);
  }

  // Delete the edge in transactional mode
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    EXPECT_TRUE(acc->DeleteEdge(&edge).has_value());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Wait for periodic GC to run
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  // Verify the edge is cleaned from the vector index
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, 0);
  }
}
