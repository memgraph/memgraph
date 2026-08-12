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

#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "flags/run_time_configurable.hpp"
#include "glue/communication.hpp"
#include "storage/v2/indices/vector_edge_index.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/settings.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace memgraph::storage;

namespace {

constexpr std::string_view kOmitSetting = "storage.omit_vector_index_properties_on_return";
constexpr auto kMetric = unum::usearch::metric_kind_t::l2sq_k;
constexpr auto kScalar = unum::usearch::scalar_kind_t::f32_k;

PropertyValue Embedding() { return PropertyValue(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(2.0)}); }

}  // namespace

class VectorIndexReturnOmissionTest : public ::testing::Test {
 public:
  std::filesystem::path data_directory{std::filesystem::temp_directory_path() /
                                       "MG_tests_unit_vector_index_return_omission"};
  std::unique_ptr<Storage> storage{std::make_unique<InMemoryStorage>()};
  std::optional<memgraph::utils::Settings> settings;

  void SetUp() override {
    std::filesystem::create_directories(data_directory);
    settings.emplace(data_directory / "settings");
    memgraph::flags::run_time::Initialize(*settings);
  }

  void TearDown() override {
    SetOmit(false);
    settings.reset();
    storage.reset();
    std::filesystem::remove_all(data_directory);
  }

  void SetOmit(bool enabled) { settings->SetValue(std::string{kOmitSetting}, enabled ? "true" : "false"); }

  void CreateNodeIndex(std::string_view label, std::string_view property) {
    auto acc = storage->UniqueAccess();
    VectorIndexSpec spec{
        .index_name = "node_idx",
        .label_filter = VectorLabelFilter{.mode = VectorMatchMode::SINGLE, .ids = {acc->NameToLabel(label)}},
        .property = acc->NameToProperty(property),
        .metric_kind = kMetric,
        .dimension = 2,
        .resize_coefficient = 2,
        .capacity = 10,
        .scalar_kind = kScalar};
    ASSERT_TRUE(acc->CreateVectorIndex(spec).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  void CreateEdgeIndex(std::string_view edge_type, std::string_view property) {
    auto acc = storage->UniqueAccess();
    VectorEdgeIndexSpec spec{.index_name = "edge_idx",
                             .edge_type_filter = VectorEdgeTypeFilter{.mode = VectorMatchMode::SINGLE,
                                                                      .ids = {acc->NameToEdgeType(edge_type)}},
                             .property = acc->NameToProperty(property),
                             .metric_kind = kMetric,
                             .dimension = 2,
                             .resize_coefficient = 2,
                             .capacity = 10,
                             .scalar_kind = kScalar};
    ASSERT_TRUE(acc->CreateVectorEdgeIndex(spec).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
};

TEST_F(VectorIndexReturnOmissionTest, NodeEmbeddingOmittedOnlyWhenFlagOn) {
  CreateNodeIndex("Doc", "embedding");
  auto acc = storage->Access(memgraph::storage::WRITE);
  auto vertex = acc->CreateVertex();
  ASSERT_TRUE(vertex.AddLabel(acc->NameToLabel("Doc")).has_value());
  ASSERT_TRUE(vertex.SetProperty(acc->NameToProperty("embedding"), Embedding()).has_value());
  ASSERT_TRUE(vertex.SetProperty(acc->NameToProperty("title"), PropertyValue("hello")).has_value());
  // a non-indexed list property must never be hidden — hiding is by vector-index membership, not by type
  ASSERT_TRUE(
      vertex
          .SetProperty(acc->NameToProperty("tags"),
                       PropertyValue(std::vector<PropertyValue>{PropertyValue(int64_t{1}), PropertyValue(int64_t{2})}))
          .has_value());
  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

  SetOmit(false);
  auto with_embedding = memgraph::glue::ToBoltVertex(vertex, *storage, View::NEW, nullptr);
  ASSERT_TRUE(with_embedding.has_value());
  EXPECT_TRUE(with_embedding->properties.contains("embedding"));
  EXPECT_TRUE(with_embedding->properties.contains("title"));

  SetOmit(true);
  auto without_embedding = memgraph::glue::ToBoltVertex(vertex, *storage, View::NEW, nullptr);
  ASSERT_TRUE(without_embedding.has_value());
  EXPECT_FALSE(without_embedding->properties.contains("embedding"));
  EXPECT_TRUE(without_embedding->properties.contains("title"));
  EXPECT_TRUE(without_embedding->properties.contains("tags"));
}

TEST_F(VectorIndexReturnOmissionTest, NodeEmbeddingKeptWhenLabelDoesNotMatchIndex) {
  CreateNodeIndex("Doc", "embedding");
  auto acc = storage->Access(memgraph::storage::WRITE);
  auto vertex = acc->CreateVertex();
  ASSERT_TRUE(vertex.AddLabel(acc->NameToLabel("Note")).has_value());
  ASSERT_TRUE(vertex.SetProperty(acc->NameToProperty("embedding"), Embedding()).has_value());
  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

  SetOmit(true);
  auto result = memgraph::glue::ToBoltVertex(vertex, *storage, View::NEW, nullptr);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->properties.contains("embedding"));
}

TEST_F(VectorIndexReturnOmissionTest, EdgeEmbeddingOmittedWhenFlagOn) {
  CreateEdgeIndex("SIMILAR", "embedding");
  auto acc = storage->Access(memgraph::storage::WRITE);
  auto from = acc->CreateVertex();
  auto to = acc->CreateVertex();
  auto edge = acc->CreateEdge(&from, &to, acc->NameToEdgeType("SIMILAR"));
  ASSERT_TRUE(edge.has_value());
  ASSERT_TRUE(edge->SetProperty(acc->NameToProperty("embedding"), Embedding()).has_value());
  ASSERT_TRUE(edge->SetProperty(acc->NameToProperty("weight"), PropertyValue(0.5)).has_value());
  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

  SetOmit(true);
  auto result = memgraph::glue::ToBoltEdge(*edge, *storage, View::NEW, nullptr);
  ASSERT_TRUE(result.has_value());
  EXPECT_FALSE(result->properties.contains("embedding"));
  EXPECT_TRUE(result->properties.contains("weight"));
}
