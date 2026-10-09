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
#include <chrono>
#include <filesystem>
#include <optional>
#include <string_view>
#include <thread>
#include <tuple>
#include <unordered_map>

#include "flags/general.hpp"
#include "flags/run_time_configurable.hpp"
#include "glue/communication.hpp"
#include "query/exceptions.hpp"
#include "storage/v2/indices/active_indices_updater.hpp"
#include "storage/v2/indices/point_index.hpp"
#include "storage/v2/indices/text_edge_index.hpp"
#include "storage/v2/indices/text_index.hpp"
#include "storage/v2/indices/vector_edge_index.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/edge_property_index.hpp"
#include "storage/v2/inmemory/edge_type_index.hpp"
#include "storage/v2/inmemory/edge_type_property_index.hpp"
#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/inmemory/vertex_property_index.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "tests/unit/ddl_abort_helpers.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/settings.hpp"

#include "storage/v2/exceptions.hpp"
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
        .edge_type_filter = VectorEdgeTypeFilter{.mode = VectorMatchMode::SINGLE, .ids = {edge_type}},
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

  // The form an older main replicates for SET e.prop = [] on an indexed edge.
  PropertyValue MakeEmptyVectorEdgeIndexProperty(Storage::Accessor *accessor) {
    const auto index_id = accessor->GetNameIdMapper()->NameToId(test_index.data());
    return PropertyValue(PropertyValue::VectorIndexIdData{.ids = memgraph::utils::small_vector<uint64_t>{index_id},
                                                          .vector = memgraph::utils::small_vector<float>{}});
  }

  void ExpectPlainEmptyList(Gid edge_gid, std::optional<std::size_t> expected_index_size) {
    auto acc = this->storage->Access(memgraph::storage::READ);
    if (expected_index_size) {
      EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, *expected_index_size);
    }
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    const auto stored = edge.GetProperty(acc->NameToProperty(test_property), View::OLD);
    ASSERT_TRUE(stored.has_value());
    EXPECT_FALSE(stored->IsVectorIndexId());
    EXPECT_TRUE(stored->IsAnyList());
    EXPECT_EQ(stored->ListSize(), 0u);
  }

  void CreateEdgeIndexNamed(std::string_view name, VectorMatchMode mode, std::uint16_t dimension,
                            std::size_t capacity) {
    auto unique_acc = this->storage->UniqueAccess();
    const auto edge_type = unique_acc->NameToEdgeType(test_edge_type.data());
    const auto property = unique_acc->NameToProperty(test_property.data());
    auto ids = mode == VectorMatchMode::WILDCARD ? std::vector<EdgeTypeId>{} : std::vector<EdgeTypeId>{edge_type};
    auto spec = VectorEdgeIndexSpec{.index_name = std::string{name},
                                    .edge_type_filter = VectorEdgeTypeFilter{.mode = mode, .ids = std::move(ids)},
                                    .property = property,
                                    .metric_kind = metric,
                                    .dimension = dimension,
                                    .resize_coefficient = resize_coefficient,
                                    .capacity = capacity,
                                    .scalar_kind = scalar_kind};
    EXPECT_FALSE(!unique_acc->CreateVectorEdgeIndex(spec).has_value());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
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

TEST_F(VectorEdgeIndexTest, VectorIndexedPropertiesRespectsEdgeTypeFilter) {
  this->CreateEdgeIndex(2, 10);
  auto acc = this->storage->Access(memgraph::storage::WRITE);
  PropertyValue property_value(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  auto [f1, t1, indexed] = this->CreateEdge(acc.get(), test_property, property_value, test_edge_type);
  auto [f2, t2, other] = this->CreateEdge(acc.get(), test_property, property_value, "other_edge_type");
  ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

  EXPECT_EQ(indexed.VectorIndexedProperties(), (std::vector<PropertyId>{acc->NameToProperty(test_property.data())}));
  EXPECT_TRUE(other.VectorIndexedProperties().empty());
}

TEST_F(VectorEdgeIndexTest, ToBoltEdgeOmitsVectorIndexedPropertyWhenFlagOn) {
  const auto settings_dir = std::filesystem::temp_directory_path() / "MG_tests_unit_vector_edge_index_omit";
  std::filesystem::remove_all(settings_dir);
  memgraph::utils::Settings settings(settings_dir);
  memgraph::flags::run_time::Initialize(settings);
  const auto set_omit = [&](bool enabled) {
    settings.SetValue("storage.omit_vector_index_properties_on_return", enabled ? "true" : "false");
  };
  // restore the process-global flag even if an assertion aborts the test early
  memgraph::utils::OnScopeExit reset_flag{[&] { set_omit(false); }};

  this->CreateEdgeIndex(2, 10);
  auto acc = this->storage->Access(memgraph::storage::WRITE);
  PropertyValue property_value(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  auto [from, to, edge] = this->CreateEdge(acc.get(), test_property, property_value, test_edge_type);
  ASSERT_TRUE(edge.SetProperty(acc->NameToProperty("weight"), PropertyValue(0.5)).has_value());
  ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));

  set_omit(false);
  auto with_prop = memgraph::glue::ToBoltEdge(edge, *this->storage, View::NEW, nullptr);
  ASSERT_TRUE(with_prop.has_value());
  EXPECT_TRUE(with_prop->properties.contains(test_property.data()));

  set_omit(true);
  auto without_prop = memgraph::glue::ToBoltEdge(edge, *this->storage, View::NEW, nullptr);
  ASSERT_TRUE(without_prop.has_value());
  EXPECT_FALSE(without_prop->properties.contains(test_property.data()));
  EXPECT_TRUE(without_prop->properties.contains("weight"));
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

TEST_F(VectorEdgeIndexTest, SecondIndexBackfillsAlreadyIndexedEdge) {
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    PropertyValue property_value(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(0.0)});
    this->CreateEdge(acc.get(), test_property, property_value, test_edge_type);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  this->CreateEdgeIndexNamed("idx_typed", VectorMatchMode::SINGLE, 2, 10);
  this->CreateEdgeIndexNamed("idx_wild", VectorMatchMode::WILDCARD, 2, 10);

  auto acc = this->storage->Access(memgraph::storage::WRITE);
  const auto typed = acc->VectorIndexSearchOnEdges("idx_typed", 1, std::vector<float>{1.0, 0.0});
  const auto wild = acc->VectorIndexSearchOnEdges("idx_wild", 1, std::vector<float>{1.0, 0.0});
  EXPECT_EQ(typed.size(), 1);
  EXPECT_EQ(wild.size(), 1);
}

TEST_F(VectorEdgeIndexTest, InvalidDimensionTest) {
  this->CreateEdgeIndex(2, 10);
  auto acc = this->storage->Access(memgraph::storage::WRITE);
  std::vector<PropertyValue> properties(3, PropertyValue(1.0));
  PropertyValue property_value(properties);
  EXPECT_THROW(this->CreateEdge(acc.get(), test_property, property_value, test_edge_type),
               memgraph::storage::VectorSearchException);
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

TEST_F(VectorEdgeIndexTest, SetEmptyListKeepsPlainListAndLeavesIndex) {
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
    const auto property = acc->NameToProperty(test_property);
    MG_ASSERT(edge.SetProperty(property, PropertyValue(std::vector<PropertyValue>{})).has_value());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, 0);
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    const auto stored = edge.GetProperty(acc->NameToProperty(test_property), View::OLD);
    ASSERT_TRUE(stored.has_value());
    EXPECT_FALSE(stored->IsVectorIndexId());
    EXPECT_TRUE(stored->IsAnyList());
    EXPECT_EQ(stored->ListSize(), 0u);
  }
}

TEST_F(VectorEdgeIndexTest, AbortOverwriteOfEmptyListLeavesEdgeOutOfIndex) {
  this->CreateEdgeIndex(2, 10);
  Gid edge_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto [from_vertex, to_vertex, edge] =
        this->CreateEdge(acc.get(), test_property, PropertyValue(std::vector<PropertyValue>{}), test_edge_type);
    edge_gid = edge.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    PropertyValue new_value(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
    MG_ASSERT(edge.SetProperty(acc->NameToProperty(test_property), new_value).has_value());
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, 1);
    acc->Abort();
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, 0);
  }
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    const auto stored = edge.GetProperty(acc->NameToProperty(test_property), View::OLD);
    ASSERT_TRUE(stored.has_value());
    EXPECT_FALSE(stored->IsVectorIndexId());
    EXPECT_TRUE(stored->IsAnyList());
    EXPECT_EQ(stored->ListSize(), 0u);
  }
}

TEST_F(VectorEdgeIndexTest, CreateIndexOverEmptyListThenDropKeepsPlainList) {
  Gid edge_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto [from_vertex, to_vertex, edge] =
        this->CreateEdge(acc.get(), test_property, PropertyValue(std::vector<PropertyValue>{}), test_edge_type);
    edge_gid = edge.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  this->CreateEdgeIndex(2, 10);
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, 0);
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    const auto stored = edge.GetProperty(acc->NameToProperty(test_property), View::OLD);
    ASSERT_TRUE(stored.has_value());
    EXPECT_FALSE(stored->IsVectorIndexId());
    EXPECT_TRUE(stored->IsAnyList());
    EXPECT_EQ(stored->ListSize(), 0u);
  }
  {
    auto unique_acc = this->storage->UniqueAccess();
    EXPECT_FALSE(!unique_acc->DropVectorIndex(test_index).has_value());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    const auto stored = edge.GetProperty(acc->NameToProperty(test_property), View::OLD);
    ASSERT_TRUE(stored.has_value());
    EXPECT_TRUE(stored->IsAnyList());
    EXPECT_EQ(stored->ListSize(), 0u);
  }
}

TEST_F(VectorEdgeIndexTest, SetEmptyVectorIndexTagStoresPlainEmptyList) {
  this->CreateEdgeIndex(2, 10);
  Gid edge_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto empty_tag = MakeEmptyVectorEdgeIndexProperty(acc.get());
    ASSERT_TRUE(empty_tag.IsVectorIndexId());
    auto [from_vertex, to_vertex, edge] = this->CreateEdge(acc.get(), test_property, empty_tag, test_edge_type);
    edge_gid = edge.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  this->ExpectPlainEmptyList(edge_gid, 0);
  {
    auto unique_acc = this->storage->UniqueAccess();
    EXPECT_FALSE(!unique_acc->DropVectorIndex(test_index).has_value());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  this->ExpectPlainEmptyList(edge_gid, std::nullopt);
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

TEST_F(VectorEdgeIndexTest, CreateVectorEdgeIndexAbortLeavesNoGhostEntry) {
  VectorEdgeIndexSpec spec{};
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    spec = VectorEdgeIndexSpec{
        .index_name = test_index.data(),
        .edge_type_filter =
            VectorEdgeTypeFilter{.mode = VectorMatchMode::SINGLE, .ids = {acc->NameToEdgeType(test_edge_type.data())}},
        .property = acc->NameToProperty(test_property.data()),
        .metric_kind = metric,
        .dimension = 2,
        .resize_coefficient = resize_coefficient,
        .capacity = 16,
        .scalar_kind = scalar_kind};
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  memgraph::tests::ExpectCreateAbortLeavesNoGhostEntry(
      this, memgraph::tests::UniqueAcc, [&](auto *acc) { return acc->CreateVectorEdgeIndex(spec); });
}

TEST_F(VectorEdgeIndexTest, DropVectorEdgeIndexAbortRestoresIndex) {
  this->CreateEdgeIndex(2, 16);
  PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(1.0)});
  Gid edge_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto [from_vertex, to_vertex, edge] = this->CreateEdge(acc.get(), test_property, properties, test_edge_type);
    edge_gid = edge.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto unique_acc = this->storage->UniqueAccess();
    ASSERT_TRUE(unique_acc->DropVectorIndex(test_index).has_value());
    unique_acc->Abort();
  }
  // Index entry must still exist and the edge property must remain stored as
  // VectorIndexId (not rewritten to plain Vector).
  {
    auto acc = this->storage->Access(memgraph::storage::READ);
    ASSERT_EQ(acc->ListAllVectorEdgeIndices().size(), 1u);
    auto edge = acc->FindEdge(edge_gid, View::OLD).value();
    auto prop = edge.GetProperty(acc->NameToProperty(test_property), View::OLD);
    ASSERT_TRUE(prop.has_value());
    EXPECT_TRUE(prop->IsVectorIndexId()) << "Aborted DROP must not leave the edge property rewritten to plain Vector.";
  }
  // A second DROP must succeed (i.e. restored entry is reachable).
  {
    auto unique_acc = this->storage->UniqueAccess();
    ASSERT_TRUE(unique_acc->DropVectorIndex(test_index).has_value());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
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
  EXPECT_THROW(this->CreateEdgeIndex(2, 10), memgraph::storage::VectorSearchException);
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

// Regression: a wildcard '(prop)' edge index and a specific ':E1(prop)' edge index both cover
// edges of type :E1, so a property write must register the edge with BOTH index ids — otherwise
// later updates/deletes drift only one of them.
TEST_F(VectorEdgeIndexTest, OverlappingEdgeIndicesBothTrackEdge) {
  const std::string_view idx_wild = "idx_wild";
  const std::string_view idx_e1 = "idx_e1";
  {
    auto unique_acc = this->storage->UniqueAccess();
    const auto e1 = unique_acc->NameToEdgeType(test_edge_type.data());
    const auto property = unique_acc->NameToProperty(test_property.data());
    EXPECT_TRUE(unique_acc
                    ->CreateVectorEdgeIndex({.index_name = std::string{idx_wild},
                                             .edge_type_filter = {.mode = VectorMatchMode::WILDCARD, .ids = {}},
                                             .property = property,
                                             .metric_kind = metric,
                                             .dimension = 2,
                                             .resize_coefficient = resize_coefficient,
                                             .capacity = 10,
                                             .scalar_kind = scalar_kind})
                    .has_value());
    EXPECT_TRUE(unique_acc
                    ->CreateVectorEdgeIndex({.index_name = std::string{idx_e1},
                                             .edge_type_filter = {.mode = VectorMatchMode::SINGLE, .ids = {e1}},
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
    PropertyValue properties(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(2.0)});
    this->CreateEdge(acc.get(), test_property, properties, test_edge_type);
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  auto acc = this->storage->Access(memgraph::storage::WRITE);
  std::unordered_map<std::string, std::size_t> sizes_by_name;
  for (const auto &info : acc->ListAllVectorEdgeIndices()) sizes_by_name[info.index_name] = info.size;
  EXPECT_EQ(sizes_by_name[std::string{idx_wild}], 1) << "wildcard edge index missing the edge";
  EXPECT_EQ(sizes_by_name[std::string{idx_e1}], 1) << "specific :E1 edge index missing the edge";
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
      ai = std::make_shared<ActiveIndices>(std::make_shared<InMemoryLabelIndex::ActiveIndices>(),
                                           std::make_shared<InMemoryLabelPropertyIndex::ActiveIndices>(),
                                           std::make_shared<InMemoryEdgeTypeIndex::ActiveIndices>(),
                                           std::make_shared<InMemoryEdgeTypePropertyIndex::ActiveIndices>(),
                                           std::make_shared<InMemoryEdgePropertyIndex::ActiveIndices>(),
                                           std::make_shared<InMemoryVertexPropertyIndex::ActiveIndices>(),
                                           std::make_shared<memgraph::storage::TextIndex::ActiveIndices>(),
                                           std::make_shared<memgraph::storage::TextEdgeIndex::ActiveIndices>(),
                                           std::make_shared<memgraph::storage::PointIndexStorage::ActiveIndices>(),
                                           std::make_shared<memgraph::storage::VectorIndex::ActiveIndices>(),
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
    return VectorEdgeIndexSpec{
        .index_name = name,
        .edge_type_filter = VectorEdgeTypeFilter{.mode = VectorMatchMode::SINGLE, .ids = {EdgeTypeId::FromUint(1)}},
        .property = PropertyId::FromUint(1),
        .metric_kind = unum::usearch::metric_kind_t::l2sq_k,
        .dimension = kDimension,
        .resize_coefficient = 2,
        .capacity = kNumEdges,
        .scalar_kind = unum::usearch::scalar_kind_t::f32_k};
  }

  void RecoverAll(std::vector<VectorEdgeIndexRecoveryInfo> &infos, VectorEdgeIndexRecovery::EdgeVectors &edge_vectors) {
    auto vertices_acc = vertices_.access();
    vector_edge_index_.RecoverAllVectorEdgeIndices(
        infos, edge_vectors, vertices_acc, &name_id_mapper_, ActiveIndicesUpdater{active_indices_store_});
  }

  void ExpectAllFixtureEdgesIndexed(std::string_view index_name) {
    const auto info = vector_edge_index_.ListVectorIndicesInfo();
    ASSERT_EQ(info.size(), 1);
    EXPECT_EQ(info[0].size, kNumEdges);
    auto edges_acc = edges_.access();
    for (auto &edge : edges_acc) {
      const auto vector = vector_edge_index_.GetVectorPropertyFromEdgeIndex(&edge, index_name, &name_id_mapper_);
      ASSERT_EQ(vector.size(), kDimension);
      EXPECT_EQ(vector[0], static_cast<float>(edge.gid.AsUint()));
      EXPECT_EQ(vector[1], static_cast<float>(edge.gid.AsUint() + 1));
    }
  }

  // Mirrors what LoadPartialEdges captures for an edge whose snapshot value is a tag with its vector.
  VectorEdgeIndexRecovery::EdgeVectors BuildEdgeVectors(PropertyId prop) {
    VectorEdgeIndexRecovery::EdgeVectors ev;
    auto &prop_map = ev[prop];
    auto acc = edges_.access();
    for (auto &edge : acc) {
      if (auto maybe_vec = TryListToVector(edge.properties.GetProperty(prop))) {
        prop_map.emplace(edge.gid, std::move(*maybe_vec));
      }
    }
    return ev;
  }

  // Replace every fixture edge property with a bare tag, as the property store persists it.
  void TagAllFixtureEdges(PropertyId prop, uint64_t index_id) {
    auto acc = edges_.access();
    for (auto &edge : acc) {
      edge.properties.SetProperty(prop,
                                  PropertyValue(PropertyValue::VectorIndexIdData{
                                      .ids = memgraph::utils::small_vector<uint64_t>{index_id}, .vector = {}}));
    }
  }

  memgraph::utils::SkipListDb<Vertex> vertices_;
  memgraph::utils::SkipListDb<Edge> edges_;
  VectorEdgeIndex vector_edge_index_;
  NameIdMapper name_id_mapper_;
  ActiveIndicesStore active_indices_store_;
};

// Fixture edges carry plain lists, so edge_vectors is empty and the build reads the lists directly.
TEST_F(VectorEdgeIndexRecoveryTest, RecoverAllVectorEdgeIndicesSingleThread) {
  FLAGS_storage_parallel_schema_recovery = false;

  std::vector<VectorEdgeIndexRecoveryInfo> infos{VectorEdgeIndexRecoveryInfo{.spec = CreateSpec()}};
  VectorEdgeIndexRecovery::EdgeVectors ev;
  EXPECT_NO_THROW(RecoverAll(infos, ev));
  ExpectAllFixtureEdgesIndexed("test_edge_index");
}

TEST_F(VectorEdgeIndexRecoveryTest, RecoverAllVectorEdgeIndicesParallel) {
  FLAGS_storage_parallel_schema_recovery = true;
  FLAGS_storage_recovery_thread_count =
      (std::thread::hardware_concurrency() > 0) ? std::thread::hardware_concurrency() : 1;

  std::vector<VectorEdgeIndexRecoveryInfo> infos{VectorEdgeIndexRecoveryInfo{.spec = CreateSpec()}};
  VectorEdgeIndexRecovery::EdgeVectors ev;
  EXPECT_NO_THROW(RecoverAll(infos, ev));
  ExpectAllFixtureEdgesIndexed("test_edge_index");
}

TEST_F(VectorEdgeIndexRecoveryTest, RecoverAllVectorEdgeIndicesConcurrentAddWithResize) {
  FLAGS_storage_parallel_schema_recovery = true;
  FLAGS_storage_recovery_thread_count =
      (std::thread::hardware_concurrency() > 0) ? std::thread::hardware_concurrency() : 4;

  // Small capacity forces usearch to resize during parallel population.
  auto spec = CreateSpec("resize_test_edge_index");
  spec.capacity = 10;
  std::vector<VectorEdgeIndexRecoveryInfo> infos{VectorEdgeIndexRecoveryInfo{.spec = std::move(spec)}};
  VectorEdgeIndexRecovery::EdgeVectors ev;
  EXPECT_NO_THROW(RecoverAll(infos, ev));
  ExpectAllFixtureEdgesIndexed("resize_test_edge_index");
  EXPECT_GE(vector_edge_index_.ListVectorIndicesInfo()[0].capacity, kNumEdges);
}

// Tag recovery: every edge is stored as a bare tag and its vector is only available from edge_vectors,
// which is what the snapshot edge section provides when the index section lacks the gid.
TEST_F(VectorEdgeIndexRecoveryTest, RecoverAllVectorEdgeIndicesFromEdgeVectors) {
  FLAGS_storage_parallel_schema_recovery = true;
  FLAGS_storage_recovery_thread_count =
      (std::thread::hardware_concurrency() > 0) ? std::thread::hardware_concurrency() : 4;

  static constexpr PropertyId kProp = PropertyId::FromUint(1);
  auto ev = BuildEdgeVectors(kProp);
  TagAllFixtureEdges(kProp, 999);

  std::vector<VectorEdgeIndexRecoveryInfo> infos{VectorEdgeIndexRecoveryInfo{.spec = CreateSpec("captured_index")}};
  EXPECT_NO_THROW(RecoverAll(infos, ev));
  ExpectAllFixtureEdgesIndexed("captured_index");
  EXPECT_TRUE(ev.empty());

  // Tags are rewritten from the stale id to the real index id.
  const auto index_id = name_id_mapper_.NameToId("captured_index");
  auto acc = edges_.access();
  for (auto &edge : acc) {
    const auto prop = edge.properties.GetProperty(kProp);
    ASSERT_TRUE(prop.IsVectorIndexId());
    EXPECT_EQ(prop.ValueVectorIndexIds(), (memgraph::utils::small_vector<uint64_t>{index_id}));
  }
}

// Two specs share the property: idx_a covers edge type 1 only, idx_b covers types 1 and 2.
// Fixture edges (type 1, PropId 1) carry no kProp entry and are skipped.
TEST_F(VectorEdgeIndexRecoveryTest, RecoverAllVectorEdgeIndicesResolvesEachEdgeState) {
  FLAGS_storage_parallel_schema_recovery = false;

  static constexpr EdgeTypeId kType1 = EdgeTypeId::FromUint(1);
  static constexpr EdgeTypeId kType2 = EdgeTypeId::FromUint(2);
  static constexpr EdgeTypeId kType3 = EdgeTypeId::FromUint(3);

  const uint64_t idx_a_id = name_id_mapper_.NameToId("idx_a");
  const uint64_t idx_b_id = name_id_mapper_.NameToId("idx_b");
  const PropertyId kProp = PropertyId::FromUint(name_id_mapper_.NameToId("test_prop"));

  auto make_spec = [&](const std::string &name, VectorMatchMode mode, std::vector<EdgeTypeId> ids) {
    return VectorEdgeIndexRecoveryInfo{
        .spec = VectorEdgeIndexSpec{.index_name = name,
                                    .edge_type_filter = {.mode = mode, .ids = std::move(ids)},
                                    .property = kProp,
                                    .metric_kind = unum::usearch::metric_kind_t::l2sq_k,
                                    .dimension = kDimension,
                                    .resize_coefficient = 2,
                                    .capacity = 10,
                                    .scalar_kind = unum::usearch::scalar_kind_t::f32_k}};
  };
  std::vector<VectorEdgeIndexRecoveryInfo> infos{make_spec("idx_a", VectorMatchMode::SINGLE, {kType1}),
                                                 make_spec("idx_b", VectorMatchMode::ANY_OF, {kType1, kType2})};

  // Edge gids 200-203 and vertex gids 400-407 are above the fixture's ranges.
  auto add_edge = [&](uint64_t gid, EdgeTypeId type, PropertyValue value) {
    auto vertices_acc = vertices_.access();
    auto edges_acc = edges_.access();
    auto [from, from_ok] = vertices_acc.insert(Vertex{Gid::FromUint(gid * 2), nullptr});
    auto [to, to_ok] = vertices_acc.insert(Vertex{Gid::FromUint(gid * 2 + 1), nullptr});
    auto [edge, edge_ok] = edges_acc.insert(Edge{Gid::FromUint(gid), nullptr});
    EXPECT_TRUE(from_ok && to_ok && edge_ok);
    edge->properties.SetProperty(kProp, std::move(value));
    from->out_edges.emplace_back(type, &*to, EdgeRef(&*edge));
  };
  const auto stale_tag = [] {
    return PropertyValue(
        PropertyValue::VectorIndexIdData{.ids = memgraph::utils::small_vector<uint64_t>{999u}, .vector = {}});
  };

  // (a) plain list [3.0, 4.0] and a stale map entry that must be ignored; type 1 matches both specs.
  add_edge(200, kType1, PropertyValue(std::vector<double>{3.0, 4.0}));
  // (b) stale tag; map entry {5.0, 6.0}; type 3 matches neither spec.
  add_edge(201, kType3, stale_tag());
  // (c) stale tag; map entry {7.0, 8.0}; type 2 matches idx_b only.
  add_edge(202, kType2, stale_tag());
  // (d) stale tag; no map entry; type 1 exercises the missing-vector null path.
  add_edge(203, kType1, stale_tag());

  VectorEdgeIndexRecovery::EdgeVectors ev;
  ev[kProp].emplace(Gid::FromUint(200), memgraph::utils::small_vector<float>{9.0F, 10.0F});
  ev[kProp].emplace(Gid::FromUint(201), memgraph::utils::small_vector<float>{5.0F, 6.0F});
  ev[kProp].emplace(Gid::FromUint(202), memgraph::utils::small_vector<float>{7.0F, 8.0F});

  EXPECT_NO_THROW(RecoverAll(infos, ev));

  std::unordered_map<std::string, std::size_t> sizes;
  for (const auto &info : vector_edge_index_.ListVectorIndicesInfo()) sizes[info.index_name] = info.size;
  EXPECT_EQ(sizes["idx_a"], 1u);
  EXPECT_EQ(sizes["idx_b"], 2u);

  auto edges_acc = edges_.access();
  {
    auto it = edges_acc.find(Gid::FromUint(200));
    ASSERT_NE(it, edges_acc.end());
    const auto prop = it->properties.GetProperty(kProp);
    ASSERT_TRUE(prop.IsVectorIndexId());
    EXPECT_TRUE(std::ranges::is_permutation(prop.ValueVectorIndexIds(),
                                            memgraph::utils::small_vector<uint64_t>{idx_a_id, idx_b_id}));
    EXPECT_EQ(vector_edge_index_.GetVectorPropertyFromEdgeIndex(&*it, "idx_a", &name_id_mapper_),
              (memgraph::utils::small_vector<float>{3.0F, 4.0F}));
  }
  {
    auto it = edges_acc.find(Gid::FromUint(201));
    ASSERT_NE(it, edges_acc.end());
    const auto prop = it->properties.GetProperty(kProp);
    ASSERT_TRUE(prop.IsDoubleList());
    const auto dl = prop.ValueDoubleList();
    ASSERT_EQ(dl.size(), 2u);
    EXPECT_DOUBLE_EQ(dl[0], 5.0);
    EXPECT_DOUBLE_EQ(dl[1], 6.0);
  }
  {
    auto it = edges_acc.find(Gid::FromUint(202));
    ASSERT_NE(it, edges_acc.end());
    const auto prop = it->properties.GetProperty(kProp);
    ASSERT_TRUE(prop.IsVectorIndexId());
    EXPECT_EQ(prop.ValueVectorIndexIds(), (memgraph::utils::small_vector<uint64_t>{idx_b_id}));
    EXPECT_EQ(vector_edge_index_.GetVectorPropertyFromEdgeIndex(&*it, "idx_b", &name_id_mapper_),
              (memgraph::utils::small_vector<float>{7.0F, 8.0F}));
  }
  {
    auto it = edges_acc.find(Gid::FromUint(203));
    ASSERT_NE(it, edges_acc.end());
    EXPECT_TRUE(it->properties.GetProperty(kProp).IsNull());
  }
}

// A tagged edge whose index was dropped before a new index was created on the same property. The
// vector comes from edge_vectors, the drop demotes the tag, and the new index picks the edge up.
TEST_F(VectorEdgeIndexRecoveryTest, DropThenRecreateOnSamePropertyRecoversFromEdgeVectors) {
  FLAGS_storage_parallel_schema_recovery = false;
  static constexpr PropertyId kProp = PropertyId::FromUint(1);

  auto ev = BuildEdgeVectors(kProp);
  TagAllFixtureEdges(kProp, 999);

  std::vector<VectorEdgeIndexRecoveryInfo> infos{VectorEdgeIndexRecoveryInfo{.spec = CreateSpec("idx_old")}};
  {
    auto vertices_acc = vertices_.access();
    VectorEdgeIndexRecovery::UpdateOnIndexDrop("idx_old", infos, ev, vertices_acc, &name_id_mapper_);
  }
  EXPECT_TRUE(infos.empty());
  EXPECT_TRUE(ev.empty());
  {
    auto acc = edges_.access();
    for (auto &edge : acc) {
      EXPECT_TRUE(edge.properties.GetProperty(kProp).IsAnyList());
    }
  }

  infos.push_back(VectorEdgeIndexRecoveryInfo{.spec = CreateSpec("idx_new")});
  EXPECT_NO_THROW(RecoverAll(infos, ev));
  ExpectAllFixtureEdgesIndexed("idx_new");

  const auto new_id = name_id_mapper_.NameToId("idx_new");
  auto acc = edges_.access();
  for (auto &edge : acc) {
    const auto prop = edge.properties.GetProperty(kProp);
    ASSERT_TRUE(prop.IsVectorIndexId());
    EXPECT_EQ(prop.ValueVectorIndexIds(), (memgraph::utils::small_vector<uint64_t>{new_id}));
  }
}

// A plain [] under a matching spec has nothing to index. It must stay a plain list rather than be
// promoted to a tag, since a tag with no usearch entry would read as lost data on the next recovery.
TEST_F(VectorEdgeIndexRecoveryTest, RecoverAllVectorEdgeIndicesLeavesEmptyListUntouched) {
  FLAGS_storage_parallel_schema_recovery = false;
  static constexpr PropertyId kProp = PropertyId::FromUint(1);

  {
    auto acc = edges_.access();
    auto e0 = acc.find(Gid::FromUint(0));
    ASSERT_NE(e0, acc.end());
    e0->properties.SetProperty(kProp, PropertyValue(std::vector<double>{}));
  }

  std::vector<VectorEdgeIndexRecoveryInfo> infos{VectorEdgeIndexRecoveryInfo{.spec = CreateSpec()}};
  VectorEdgeIndexRecovery::EdgeVectors ev;
  EXPECT_NO_THROW(RecoverAll(infos, ev));

  const auto info = vector_edge_index_.ListVectorIndicesInfo();
  ASSERT_EQ(info.size(), 1);
  EXPECT_EQ(info[0].size, kNumEdges - 1);

  auto acc = edges_.access();
  auto e0 = acc.find(Gid::FromUint(0));
  ASSERT_NE(e0, acc.end());
  const auto stored = e0->properties.GetProperty(kProp);
  EXPECT_FALSE(stored.IsVectorIndexId());
  EXPECT_TRUE(stored.IsAnyList());
  EXPECT_EQ(stored.ListSize(), 0u);
}

// UpdateOnSetEdgeProperty: a tag with no vector is the legacy on-disk form of [] and becomes a plain
// empty list, dropping any earlier captured vector for the edge.
TEST_F(VectorEdgeIndexRecoveryTest, UpdateOnSetEdgePropertyEmptyTagBecomesEmptyList) {
  static constexpr PropertyId kProp = PropertyId::FromUint(1);

  std::vector<VectorEdgeIndexRecoveryInfo> infos{VectorEdgeIndexRecoveryInfo{.spec = CreateSpec()}};
  VectorEdgeIndexRecovery::EdgeVectors ev;
  Edge edge(Gid::FromUint(77), nullptr);
  ev[kProp].emplace(edge.gid, memgraph::utils::small_vector<float>{1.0F, 2.0F});

  PropertyValue empty_tag(
      PropertyValue::VectorIndexIdData{.ids = memgraph::utils::small_vector<uint64_t>{42}, .vector = {}});
  VectorEdgeIndexRecovery::UpdateOnSetEdgeProperty(kProp, empty_tag, &edge, infos, ev);

  EXPECT_FALSE(empty_tag.IsVectorIndexId());
  EXPECT_TRUE(empty_tag.IsAnyList());
  EXPECT_EQ(empty_tag.ListSize(), 0u);
  EXPECT_FALSE(ev[kProp].contains(edge.gid));
}

// UpdateOnSetEdgeProperty: with a spec on the property, a tag carrying its vector is captured into
// edge_vectors and the value is left unchanged for the subsequent SetProperty.
TEST_F(VectorEdgeIndexRecoveryTest, UpdateOnSetEdgePropertyCapturesVectorWhenSpecExists) {
  static constexpr PropertyId kProp = PropertyId::FromUint(1);

  std::vector<VectorEdgeIndexRecoveryInfo> infos{VectorEdgeIndexRecoveryInfo{.spec = CreateSpec()}};
  VectorEdgeIndexRecovery::EdgeVectors ev;

  memgraph::utils::small_vector<float> raw_vec{1.0F, 2.0F};
  PropertyValue tag_value(
      PropertyValue::VectorIndexIdData{.ids = memgraph::utils::small_vector<uint64_t>{42}, .vector = raw_vec});

  Edge edge(Gid::FromUint(77), nullptr);
  VectorEdgeIndexRecovery::UpdateOnSetEdgeProperty(kProp, tag_value, &edge, infos, ev);

  ASSERT_TRUE(tag_value.IsVectorIndexId());
  ASSERT_TRUE(ev.contains(kProp));
  ASSERT_TRUE(ev[kProp].contains(edge.gid));
  EXPECT_EQ(ev[kProp][edge.gid], raw_vec);

  // A later plain-list value on the same edge drops the captured vector.
  PropertyValue list_value(std::vector<double>{3.0, 4.0});
  VectorEdgeIndexRecovery::UpdateOnSetEdgeProperty(kProp, list_value, &edge, infos, ev);
  EXPECT_FALSE(ev[kProp].contains(edge.gid));
}

// UpdateOnSetEdgeProperty: with no spec on the property, a stale tag is converted in place to a list.
TEST_F(VectorEdgeIndexRecoveryTest, UpdateOnSetEdgePropertyOrphanTagConvertedToList) {
  static constexpr PropertyId kProp = PropertyId::FromUint(1);

  std::vector<VectorEdgeIndexRecoveryInfo> infos;
  VectorEdgeIndexRecovery::EdgeVectors ev;

  memgraph::utils::small_vector<float> raw_vec{3.0F, 4.0F};
  PropertyValue tag_value(
      PropertyValue::VectorIndexIdData{.ids = memgraph::utils::small_vector<uint64_t>{99}, .vector = raw_vec});

  Edge edge(Gid::FromUint(5), nullptr);
  VectorEdgeIndexRecovery::UpdateOnSetEdgeProperty(kProp, tag_value, &edge, infos, ev);

  EXPECT_TRUE(tag_value.IsAnyList());
  EXPECT_EQ(tag_value.ListSize(), 2u);
  EXPECT_TRUE(ev.empty());
}

// UpdateOnIndexDrop: dropping the only spec on a property demotes every stored tag, whether or not the
// dropped index's own entries knew about the edge, and clears the property's entry in edge_vectors.
TEST_F(VectorEdgeIndexRecoveryTest, UpdateOnIndexDropRestoresTagsToPlainLists) {
  // The null path logs the property name, so the id must be registered with the mapper.
  const PropertyId kProp = PropertyId::FromUint(name_id_mapper_.NameToId("test_prop"));

  auto spec = CreateSpec();
  spec.property = kProp;
  std::vector<VectorEdgeIndexRecoveryInfo> infos{VectorEdgeIndexRecoveryInfo{.spec = std::move(spec)}};

  VectorEdgeIndexRecovery::EdgeVectors ev;
  ev[kProp].emplace(Gid::FromUint(0), memgraph::utils::small_vector<float>{7.0F, 8.0F});

  auto acc = edges_.access();
  auto e0 = acc.find(Gid::FromUint(0));
  ASSERT_NE(e0, acc.end());
  e0->properties.SetProperty(
      kProp,
      PropertyValue(PropertyValue::VectorIndexIdData{.ids = memgraph::utils::small_vector<uint64_t>{1}, .vector = {}}));

  // Edge 1 carries a tag but has no map entry, exercising the null-assignment path.
  auto e1 = acc.find(Gid::FromUint(1));
  ASSERT_NE(e1, acc.end());
  e1->properties.SetProperty(
      kProp,
      PropertyValue(PropertyValue::VectorIndexIdData{.ids = memgraph::utils::small_vector<uint64_t>{1}, .vector = {}}));

  auto vertices_acc = vertices_.access();
  VectorEdgeIndexRecovery::UpdateOnIndexDrop(infos[0].spec.index_name, infos, ev, vertices_acc, &name_id_mapper_);

  EXPECT_TRUE(infos.empty());
  EXPECT_FALSE(ev.contains(kProp));
  const auto restored = e0->properties.GetProperty(kProp);
  ASSERT_TRUE(restored.IsDoubleList());
  const auto dl = restored.ValueDoubleList();
  ASSERT_EQ(dl.size(), 2u);
  EXPECT_DOUBLE_EQ(dl[0], 7.0);
  EXPECT_DOUBLE_EQ(dl[1], 8.0);
  EXPECT_TRUE(e1->properties.GetProperty(kProp).IsNull());
}

// UpdateOnIndexDrop: when another spec still covers the property, edge_vectors and the stored tags are
// kept for the final build.
TEST_F(VectorEdgeIndexRecoveryTest, UpdateOnIndexDropPreservesEdgeVectorsForSurvivingSpec) {
  static constexpr PropertyId kProp = PropertyId::FromUint(1);

  auto spec_b = CreateSpec("idx_b");
  spec_b.edge_type_filter = VectorEdgeTypeFilter{.mode = VectorMatchMode::WILDCARD, .ids = {}};
  std::vector<VectorEdgeIndexRecoveryInfo> infos{VectorEdgeIndexRecoveryInfo{.spec = CreateSpec("idx_a")},
                                                 VectorEdgeIndexRecoveryInfo{.spec = std::move(spec_b)}};
  VectorEdgeIndexRecovery::EdgeVectors ev;
  ev[kProp].emplace(Gid::FromUint(0), memgraph::utils::small_vector<float>{1.0F, 2.0F});

  auto acc = edges_.access();
  auto e0 = acc.find(Gid::FromUint(0));
  ASSERT_NE(e0, acc.end());
  e0->properties.SetProperty(
      kProp,
      PropertyValue(PropertyValue::VectorIndexIdData{.ids = memgraph::utils::small_vector<uint64_t>{1}, .vector = {}}));

  auto vertices_acc = vertices_.access();
  VectorEdgeIndexRecovery::UpdateOnIndexDrop("idx_a", infos, ev, vertices_acc, &name_id_mapper_);

  ASSERT_EQ(infos.size(), 1u);
  EXPECT_EQ(infos[0].spec.index_name, "idx_b");
  EXPECT_TRUE(ev.contains(kProp));
  EXPECT_TRUE(ev[kProp].contains(Gid::FromUint(0)));
  EXPECT_TRUE(e0->properties.GetProperty(kProp).IsVectorIndexId());
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
        .edge_type_filter = VectorEdgeTypeFilter{.mode = VectorMatchMode::SINGLE, .ids = {edge_type}},
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

TEST_F(VectorEdgeIndexTest, MultiTypeFilterEqualityIsOrderInsensitive) {
  auto unique_acc = this->storage->UniqueAccess();
  const auto e1 = unique_acc->NameToEdgeType("E1");
  const auto e2 = unique_acc->NameToEdgeType("E2");
  const auto property = unique_acc->NameToProperty(test_property.data());
  EXPECT_TRUE(unique_acc
                  ->CreateVectorEdgeIndex({.index_name = "e1e2",
                                           .edge_type_filter = {.mode = VectorMatchMode::ANY_OF, .ids = {e1, e2}},
                                           .property = property,
                                           .metric_kind = metric,
                                           .dimension = 2,
                                           .resize_coefficient = resize_coefficient,
                                           .capacity = 10,
                                           .scalar_kind = scalar_kind})
                  .has_value());
  EXPECT_FALSE(unique_acc
                   ->CreateVectorEdgeIndex({.index_name = "e2e1",
                                            .edge_type_filter = {.mode = VectorMatchMode::ANY_OF, .ids = {e2, e1}},
                                            .property = property,
                                            .metric_kind = metric,
                                            .dimension = 2,
                                            .resize_coefficient = resize_coefficient,
                                            .capacity = 10,
                                            .scalar_kind = scalar_kind})
                   .has_value());
}

TEST_F(VectorEdgeIndexTest, AbortMapStyleWriteRestoresEmbedding) {
  // ClearProperties / UpdateProperties back SET r = {...}, SET r = {} and SET r += {...}; their undo before-image
  // must carry the floats, which live only in usearch.
  this->CreateEdgeIndex(3, 10);
  const std::vector<float> original{1.0F, 2.0F, 3.0F};
  Gid edge_gid;
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    PropertyValue property_value(
        std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(2.0), PropertyValue(3.0)});
    auto [from_vertex, to_vertex, edge] = this->CreateEdge(acc.get(), test_property, property_value, test_edge_type);
    edge_gid = edge.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  auto const expect_original = [&](Storage::Accessor *acc) {
    auto edge = acc->FindEdge(edge_gid, View::OLD);
    ASSERT_TRUE(edge.has_value());
    auto value = edge->GetProperty(acc->NameToProperty(test_property), View::OLD);
    ASSERT_TRUE(value.has_value());
    ASSERT_TRUE(value->IsVectorIndexId());
    EXPECT_TRUE(std::ranges::equal(value->ValueVectorIndexList(), original));
  };

  enum class Write : uint8_t { REPLACE, CLEAR, UPDATE };
  for (auto const write : {Write::REPLACE, Write::CLEAR, Write::UPDATE}) {
    SCOPED_TRACE(static_cast<int>(write));
    {
      auto acc = this->storage->Access(memgraph::storage::WRITE);
      auto edge = acc->FindEdge(edge_gid, View::OLD).value();
      std::map<PropertyId, PropertyValue> new_properties{
          {acc->NameToProperty(test_property),
           PropertyValue(std::vector<PropertyValue>{PropertyValue(7.0), PropertyValue(7.0), PropertyValue(7.0)})}};
      if (write != Write::UPDATE) ASSERT_NO_ERROR(edge.ClearProperties());
      if (write != Write::CLEAR) ASSERT_NO_ERROR(edge.UpdateProperties(new_properties));
      {
        auto reader = this->storage->Access(memgraph::storage::READ);
        expect_original(reader.get());
      }
      acc->Abort();
    }
    auto acc = this->storage->Access(memgraph::storage::READ);
    expect_original(acc.get());
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, 1);
    const auto result = acc->VectorIndexSearchOnEdges(test_index.data(), 1, original);
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(std::get<0>(result[0]).Gid(), edge_gid);
  }
}

namespace {
PropertyValue FloatList(std::initializer_list<double> values) {
  std::vector<PropertyValue> list;
  for (const auto value : values) list.emplace_back(value);
  return PropertyValue(std::move(list));
}
}  // namespace

// Abort undoes vector edge index changes per delta under the edge lock; these pin the stored value (tag vs plain
// list) and the usearch membership/value after it.
class VectorEdgeIndexAbortTest : public VectorEdgeIndexTest {
 protected:
  using Floats = memgraph::utils::small_vector<float>;

  Gid CommitEdge(std::string_view property, const PropertyValue &value) {
    auto acc = storage->Access(WRITE);
    auto [from_vertex, to_vertex, edge] = CreateEdge(acc.get(), property, value, test_edge_type);
    const auto gid = edge.Gid();
    MG_ASSERT(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());  // NOLINT
    return gid;
  }

  void ExpectIndexed(Gid gid, const Floats &expected, std::size_t expected_size = 1) {
    auto acc = storage->Access(READ);
    auto edge = acc->FindEdge(gid, View::OLD).value();
    const auto value = edge.GetProperty(acc->NameToProperty(test_property), View::OLD).value();
    ASSERT_TRUE(value.IsVectorIndexId());
    EXPECT_EQ(value.ValueVectorIndexList(), expected);
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, expected_size);
    const auto hits = acc->VectorIndexSearchOnEdges(
        test_index.data(), expected_size, std::vector<float>(expected.begin(), expected.end()));
    const auto hit = std::ranges::find_if(hits, [&](const auto &h) { return std::get<0>(h).Gid() == gid; });
    ASSERT_NE(hit, hits.end());
    EXPECT_FLOAT_EQ(std::get<1>(*hit), 0.0);
  }

  void ExpectPlain(Gid gid, const PropertyValue &expected, std::size_t expected_size = 0) {
    auto acc = storage->Access(READ);
    auto edge = acc->FindEdge(gid, View::OLD).value();
    const auto value = edge.GetProperty(acc->NameToProperty(test_property), View::OLD).value();
    EXPECT_FALSE(value.IsVectorIndexId());
    EXPECT_EQ(value, expected);
    EXPECT_EQ(acc->ListAllVectorEdgeIndices()[0].size, expected_size);
  }
};

TEST_F(VectorEdgeIndexAbortTest, SetOverIndexedVectorThenAbortRestoresIt) {
  CreateEdgeIndex(2, 10);
  const auto e = CommitEdge(test_property, FloatList({1, 2}));
  const std::vector<std::pair<std::string_view, PropertyValue>> writes{
      {"empty list", PropertyValue(std::vector<PropertyValue>{})},
      {"null", PropertyValue()},
      {"string", PropertyValue("abc")},
      {"other vector", FloatList({3, 4})},
  };
  for (const auto &[name, value] : writes) {
    SCOPED_TRACE(name);
    auto acc = storage->Access(WRITE);
    auto edge = acc->FindEdge(e, View::OLD).value();
    ASSERT_NO_ERROR(edge.SetProperty(acc->NameToProperty(test_property), value));
    acc->Abort();
    ExpectIndexed(e, Floats{1.0F, 2.0F});
  }
}

TEST_F(VectorEdgeIndexAbortTest, WrongDimensionSetThenAbortRestoresIndexedVector) {
  CreateEdgeIndex(2, 10);
  const auto e = CommitEdge(test_property, FloatList({1, 2}));
  auto acc = storage->Access(WRITE);
  auto edge = acc->FindEdge(e, View::OLD).value();
  EXPECT_ANY_THROW(std::ignore = edge.SetProperty(acc->NameToProperty(test_property), FloatList({1, 2, 3})));
  acc->Abort();
  ExpectIndexed(e, Floats{1.0F, 2.0F});
}

TEST_F(VectorEdgeIndexAbortTest, SetOverNonVectorStartThenAbortLeavesItUnindexed) {
  CreateEdgeIndex(2, 10);
  const std::vector<std::pair<std::string_view, PropertyValue>> starts{
      {"string", PropertyValue("abc")},
      {"empty list", PropertyValue(std::vector<PropertyValue>{})},
  };
  for (const auto &[name, start] : starts) {
    SCOPED_TRACE(name);
    const auto e = CommitEdge(test_property, start);
    auto acc = storage->Access(WRITE);
    auto edge = acc->FindEdge(e, View::OLD).value();
    ASSERT_NO_ERROR(edge.SetProperty(acc->NameToProperty(test_property), FloatList({3, 4})));
    acc->Abort();
    ExpectPlain(e, start);
  }
}

TEST_F(VectorEdgeIndexAbortTest, SetThenDeleteThenAbortRestoresIndexedVector) {
  // Deleting the edge takes its link off the source vertex, so the undo has to find the edge type in the deltas.
  CreateEdgeIndex(2, 10);
  const auto e = CommitEdge(test_property, FloatList({1, 2}));
  {
    auto acc = storage->Access(WRITE);
    auto edge = acc->FindEdge(e, View::OLD).value();
    ASSERT_NO_ERROR(edge.SetProperty(acc->NameToProperty(test_property), FloatList({3, 4})));
    ASSERT_NO_ERROR(acc->DeleteEdge(&edge));
    acc->Abort();
  }
  ExpectIndexed(e, Floats{1.0F, 2.0F});
}

TEST_F(VectorEdgeIndexAbortTest, ConcurrentWriterBetweenEdgeUndosKeepsItsEmbedding) {
  CreateEdgeIndex(2, 10);
  const auto e = CommitEdge(test_property, FloatList({1, 2}));
  const auto other = CommitEdge("unrelated", PropertyValue(0));
  auto acc = storage->Access(WRITE);
  auto edge = acc->FindEdge(e, View::OLD).value();
  ASSERT_NO_ERROR(edge.SetProperty(acc->NameToProperty(test_property), FloatList({3, 4})));
  auto other_edge = acc->FindEdge(other, View::OLD).value();
  ASSERT_NO_ERROR(other_edge.SetProperty(acc->NameToProperty("unrelated"), PropertyValue(1)));

  // The edges pass calls back before each delta, in delta order: the second call is after e's undo, before the
  // other edge's. The vertices pass then calls back once per delta without touching edges.
  std::size_t calls = 0;
  static_cast<InMemoryStorage::InMemoryAccessor *>(acc.get())->Abort([&] {
    if (++calls != 2) return;
    auto writer = storage->Access(WRITE);
    auto written = writer->FindEdge(e, View::OLD).value();
    ASSERT_NO_ERROR(written.SetProperty(writer->NameToProperty(test_property), FloatList({5, 6})));
    ASSERT_NO_ERROR(writer->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  });
  // Two deltas, each seen once by the edges pass and once by the vertices pass.
  ASSERT_EQ(calls, 4U);

  ExpectIndexed(e, Floats{5.0F, 6.0F});
}
