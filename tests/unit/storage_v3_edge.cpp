// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <limits>

#include "storage/v3/config.hpp"
#include "storage/v3/name_id_mapper.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/shard.hpp"

namespace memgraph::storage::v3::tests {
using testing::UnorderedElementsAre;

class StorageEdgeTest : public ::testing::TestWithParam<bool> {
 protected:
  void SetUp() override {
    store.StoreMapping({{1, "label"}, {2, "property"}, {3, "et5"}, {4, "other"}, {5, "different_label"}});
  }

  [[nodiscard]] LabelId NameToLabelId(std::string_view label_name) { return store.NameToLabel(label_name); }

  [[nodiscard]] PropertyId NameToPropertyId(std::string_view property_name) {
    return store.NameToProperty(property_name);
  }

  [[nodiscard]] EdgeTypeId NameToEdgeTypeId(std::string_view edge_type_name) {
    return store.NameToEdgeType(edge_type_name);
  }

  static ShardResult<VertexAccessor> CreateVertex(Shard::Accessor &acc, const PropertyValue &key) {
    return acc.CreateVertexAndValidate({}, {key}, {});
  }

  coordinator::Hlc GetNextHlc() {
    ++last_hlc.logical_id;
    last_hlc.coordinator_wall_clock += std::chrono::seconds(10);
    return last_hlc;
  }

  const std::vector<PropertyValue> min_pk{PropertyValue{0}};
  const std::vector<PropertyValue> max_pk{PropertyValue{10000}};
  const LabelId primary_label{LabelId::FromUint(1)};
  const PropertyId primary_property{PropertyId::FromUint(2)};
  std::vector<storage::v3::SchemaProperty> schema_property_vector = {
      storage::v3::SchemaProperty{primary_property, common::SchemaType::INT}};
  coordinator::Hlc last_hlc{0, io::Time{}};
  Shard store{primary_label,          min_pk,   max_pk,
              schema_property_vector, last_hlc, Config{.items = {.properties_on_edges = GetParam()}}};
};

INSTANTIATE_TEST_SUITE_P(EdgesWithProperties, StorageEdgeTest, ::testing::Values(true));
INSTANTIATE_TEST_SUITE_P(EdgesWithoutProperties, StorageEdgeTest, ::testing::Values(false));

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeCreateFromSmallerCommit) {
  // Create vertices
  const PropertyValue from_key{0};
  const PropertyValue to_key{1};
  const PropertyValue non_existing_key{2};
  const auto et = NameToEdgeTypeId("et5");
  const auto edge_id = Gid::FromUint(0U);
  auto acc = store.Access(GetNextHlc());
  const auto [from_id, to_id] = std::invoke([&from_key, &to_key, &acc]() mutable -> std::pair<VertexId, VertexId> {
    auto from_id = CreateVertex(acc, from_key)->Id(View::NEW).GetValue();
    auto to_id = CreateVertex(acc, to_key)->Id(View::NEW).GetValue();
    return std::make_pair(std::move(from_id), std::move(to_id));
  });
  const auto other_et = NameToEdgeTypeId("other");
  const VertexId from_id_with_different_label{NameToLabelId("different_label"), from_id.primary_key};
  const VertexId to_id_with_different_label{NameToLabelId("different_label"), to_id.primary_key};
  const VertexId non_existing_id{primary_label, {non_existing_key}};

  acc.Commit(GetNextHlc());

  // Create edge
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex_from = acc.FindVertex(from_id.primary_key, View::NEW);
    auto vertex_to = acc.FindVertex({to_id.primary_key}, View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto res = acc.CreateEdge(from_id, to_id, et, edge_id);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.Gid(), edge_id);
    ASSERT_EQ(edge.From(), from_id);
    ASSERT_EQ(edge.To(), to_id);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    ASSERT_EQ(vertex_to->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(View::OLD), 0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &to_id)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &to_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &from_id_with_different_label)->size(), 0);

    acc.Commit(GetNextHlc());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex_from = acc.FindVertex(from_id.primary_key, View::NEW);
    auto vertex_to = acc.FindVertex(to_id.primary_key, View::NEW);
    ASSERT_TRUE(vertex_from.has_value());
    ASSERT_TRUE(vertex_to.has_value());

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    {
      auto ret = vertex_from->OutEdges(View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    {
      auto ret = vertex_to->InEdges(View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    {
      auto ret = vertex_to->InEdges(View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &to_id)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD, {other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD, {et, other_et}, &to_id)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &to_id)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD, {et, other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &to_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD, {et, other_et}, &to_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &to_id_with_different_label)->size(), 0);

    ASSERT_EQ(vertex_to->InEdges(View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &from_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et}, &from_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &from_id_with_different_label)->size(), 0);

    acc.Commit(GetNextHlc());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeCreateFromLargerAbort) {
  // Create vertices
  const PropertyValue from_key{0};
  const PropertyValue to_key{1};
  const PropertyValue non_existing_key{2};
  auto acc = store.Access(GetNextHlc());
  const auto [from_id, to_id] = std::invoke([&from_key, &to_key, &acc]() mutable -> std::pair<VertexId, VertexId> {
    auto from_id = CreateVertex(acc, from_key)->Id(View::NEW).GetValue();
    auto to_id = CreateVertex(acc, to_key)->Id(View::NEW).GetValue();
    return std::make_pair(std::move(from_id), std::move(to_id));
  });

  const auto et = NameToEdgeTypeId("et5");
  const auto other_et = NameToEdgeTypeId("other");
  const VertexId from_id_with_different_label{NameToLabelId("different_label"), from_id.primary_key};
  const VertexId to_id_with_different_label{NameToLabelId("different_label"), to_id.primary_key};
  const VertexId non_existing_id{primary_label, {non_existing_key}};

  acc.Commit(GetNextHlc());

  // Create edge but abort
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex_from = acc.FindVertex(from_id.primary_key, View::NEW);
    auto vertex_to = acc.FindVertex({to_id.primary_key}, View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    const auto edge_id = Gid::FromUint(0U);

    auto res = acc.CreateEdge(from_id, to_id, et, edge_id);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.Gid(), edge_id);
    ASSERT_EQ(edge.From(), from_id);
    ASSERT_EQ(edge.To(), to_id);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    ASSERT_EQ(vertex_to->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(View::OLD), 0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &to_id)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &to_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW, {}, &from_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW, {et, other_et}, &from_id_with_different_label)->size(), 0);

    acc.Abort();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex_from = acc.FindVertex(from_id.primary_key, View::NEW);
    auto vertex_to = acc.FindVertex(to_id.primary_key, View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(View::OLD), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

    acc.Commit(GetNextHlc());
  }

  // Create edge
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex_from = acc.FindVertex(from_id.primary_key, View::NEW);
    auto vertex_to = acc.FindVertex(to_id.primary_key, View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    const auto edge_id = Gid::FromUint(1U);

    auto res = acc.CreateEdge(from_id, to_id, et, edge_id);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.Gid(), edge_id);
    ASSERT_EQ(edge.From(), from_id);
    ASSERT_EQ(edge.To(), to_id);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    ASSERT_EQ(vertex_to->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(View::OLD), 0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &to_id)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &to_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW, {}, &from_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW, {et, other_et}, &from_id_with_different_label)->size(), 0);

    acc.Commit(GetNextHlc());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex_from = acc.FindVertex(from_id.primary_key, View::NEW);
    auto vertex_to = acc.FindVertex(to_id.primary_key, View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    const auto edge_id = Gid::FromUint(1U);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    {
      auto ret = vertex_from->OutEdges(View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    {
      auto ret = vertex_to->InEdges(View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    {
      auto ret = vertex_to->InEdges(View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &to_id)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &to_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW, {}, &from_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW, {et, other_et}, &from_id_with_different_label)->size(), 0);

    acc.Commit(GetNextHlc());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeDeleteFromSmallerCommit) {
  // Create vertex
  const PropertyValue from_key{0};
  const PropertyValue to_key{max_pk};
  const PropertyValue non_existing_key{2};
  auto acc = store.Access(GetNextHlc());
  const auto from_id = std::invoke(
      [&from_key, &acc]() mutable -> VertexId { return CreateVertex(acc, from_key)->Id(View::NEW).GetValue(); });
  const VertexId to_id{primary_label, {to_key}};
  const auto et = NameToEdgeTypeId("et5");
  const auto edge_id = Gid::FromUint(1U);
  const auto other_et = NameToEdgeTypeId("other");
  const VertexId to_id_with_different_label{NameToLabelId("different_label"), to_id.primary_key};
  const VertexId non_existing_id{primary_label, {non_existing_key}};

  acc.Commit(GetNextHlc());

  // Create edge
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex_from = acc.FindVertex(from_id.primary_key, View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_FALSE(acc.FindVertex(to_id.primary_key, View::NEW).has_value());

    const auto et = NameToEdgeTypeId("et5");
    const auto edge_id = Gid::FromUint(1U);

    auto res = acc.CreateEdge(from_id, to_id, et, edge_id);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.Gid(), edge_id);
    ASSERT_EQ(edge.From(), from_id);
    ASSERT_EQ(edge.To(), to_id);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &to_id)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &to_id_with_different_label)->size(), 0);

    acc.Commit(GetNextHlc());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex_from = acc.FindVertex(from_id.primary_key, View::NEW);
    ASSERT_TRUE(vertex_from);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    {
      auto ret = vertex_from->OutEdges(View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }

    // Check edges with filters

    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &to_id)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et}, &to_id_with_different_label)->size(), 0);

    acc.Commit(GetNextHlc());
  }

  // Delete edge
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex_from = acc.FindVertex(from_id.primary_key, View::NEW);
    ASSERT_TRUE(vertex_from);

    const auto edge = vertex_from->OutEdges(View::NEW).GetValue()[0];

    const auto res = acc.DeleteEdge(edge.From(), edge.To(), edge.Gid());
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    ASSERT_EQ(vertex_from->OutEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 0);

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &to_id)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD, {other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD, {et, other_et}, &to_id)->size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD, {et, other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &to_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD, {et, other_et}, &to_id_with_different_label)->size(), 0);

    acc.Commit(GetNextHlc());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex_from = acc.FindVertex(from_id.primary_key, View::NEW);
    ASSERT_TRUE(vertex_from);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 0);
    ASSERT_EQ(vertex_from->OutEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 0);

    acc.Commit(GetNextHlc());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeDeleteFromLargerAbort) {
  // Create vertex
  const PropertyValue from_key{max_pk};
  const PropertyValue to_key{0};
  const PropertyValue non_existing_key{2};
  auto acc = store.Access(GetNextHlc());
  const auto to_id = std::invoke(
      [&to_key, &acc]() mutable -> VertexId { return CreateVertex(acc, to_key)->Id(View::NEW).GetValue(); });
  const VertexId from_id{primary_label, {from_key}};
  const auto et = NameToEdgeTypeId("et5");
  const auto edge_id = Gid::FromUint(1U);
  const auto other_et = NameToEdgeTypeId("other");
  const VertexId from_id_with_different_label{NameToLabelId("different_label"), from_id.primary_key};
  const VertexId non_existing_id{primary_label, {non_existing_key}};

  acc.Commit(GetNextHlc());
  // Create edge
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex_to = acc.FindVertex(to_id.primary_key, View::NEW);
    ASSERT_TRUE(vertex_to);

    auto res = acc.CreateEdge(from_id, to_id, et, edge_id);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.Gid(), edge_id);
    ASSERT_EQ(edge.From(), from_id);
    ASSERT_EQ(edge.To(), to_id);

    // Check edges without filters
    ASSERT_EQ(vertex_to->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

    // Check edges with filters
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW, {}, &from_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW, {et, other_et}, &from_id_with_different_label)->size(), 0);

    acc.Commit(GetNextHlc());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex_to = acc.FindVertex(to_id.primary_key, View::NEW);

    // Check edges without filters
    {
      auto ret = vertex_to->InEdges(View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    {
      auto ret = vertex_to->InEdges(View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

    // Check edges with filters
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &from_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et}, &from_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &from_id_with_different_label)->size(), 0);

    acc.Commit(GetNextHlc());
  }

  // Delete the edge, but abort the transaction
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex_to = acc.FindVertex(to_id.primary_key, View::NEW);
    ASSERT_TRUE(vertex_to);

    const auto edge = vertex_to->InEdges(View::NEW).GetValue()[0];

    auto res = acc.DeleteEdge(edge.From(), edge.To(), edge.Gid());
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters

    {
      auto ret = vertex_to->InEdges(View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    ASSERT_EQ(vertex_to->InEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

    // Check edges with filters
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &to_id)->size(), 0);

    acc.Abort();
  }

  // Check whether the edge exists
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex_to = acc.FindVertex(to_id.primary_key, View::NEW);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    {
      auto ret = vertex_to->InEdges(View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    {
      auto ret = vertex_to->InEdges(View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

    // Check edges with filters
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &from_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et}, &from_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &from_id_with_different_label)->size(), 0);

    acc.Commit(GetNextHlc());
  }

  // Delete the edge
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex_to = acc.FindVertex(to_id.primary_key, View::NEW);
    ASSERT_TRUE(vertex_to);

    const auto edge = vertex_to->InEdges(View::NEW).GetValue()[0];

    auto res = acc.DeleteEdge(edge.From(), edge.To(), edge.Gid());
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters
    {
      auto ret = vertex_to->InEdges(View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    ASSERT_EQ(vertex_to->InEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

    // Check edges with filters
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et})->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et})->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et}, &from_id)->size(), 1);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &from_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &to_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &from_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &non_existing_id)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et}, &from_id_with_different_label)->size(), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et}, &from_id_with_different_label)->size(), 0);

    acc.Commit(GetNextHlc());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex_to = acc.FindVertex(to_id.primary_key, View::NEW);
    ASSERT_TRUE(vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_to->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(View::OLD), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

    acc.Commit(GetNextHlc());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, VertexDetachDeleteSingleCommit) {
  // Create vertices
  const PropertyValue from_key{0};
  const PropertyValue to_key{1};
  const PropertyValue non_existing_key{2};
  const auto et = NameToEdgeTypeId("et5");
  const auto edge_id = Gid::FromUint(0U);
  const VertexId from_id{primary_label, {from_key}};
  const VertexId to_id{primary_label, {to_key}};
  const VertexId non_existing_id{primary_label, {non_existing_key}};

  // Create dataset
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex_from = CreateVertex(acc, from_key).GetValue();
    auto vertex_to = CreateVertex(acc, to_key).GetValue();

    auto res = acc.CreateEdge(from_id, to_id, et, edge_id);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.Gid(), edge_id);
    ASSERT_EQ(edge.From(), from_id);
    ASSERT_EQ(edge.To(), to_id);

    // Check edges
    ASSERT_EQ(vertex_from.InEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_from.InDegree(View::NEW), 0);
    {
      auto ret = vertex_from.OutEdges(View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from.OutDegree(View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    {
      auto ret = vertex_to.InEdges(View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to.InDegree(View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    ASSERT_EQ(vertex_to.OutEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to.OutDegree(View::NEW), 0);

    acc.Commit(GetNextHlc());
  }

  // Detach delete vertex
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex_from = acc.FindVertex(from_id.primary_key, View::NEW);
    auto vertex_to = acc.FindVertex(to_id.primary_key, View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Delete must fail
    {
      auto ret = acc.DeleteVertex(&vertex_from.value());
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), SHARD_ERROR(ErrorCode::VERTEX_HAS_EDGES));
    }

    // Detach delete vertex
    {
      auto ret = acc.DetachDeleteVertex(&vertex_from.value());
      ASSERT_TRUE(ret.HasValue());
      ASSERT_TRUE(*ret);
    }

    // Check edges
    ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(View::NEW).GetError(), SHARD_ERROR(ErrorCode::DELETED_OBJECT));
    ASSERT_EQ(vertex_from->InDegree(View::NEW).GetError(), SHARD_ERROR(ErrorCode::DELETED_OBJECT));
    {
      auto ret = vertex_from->OutEdges(View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    ASSERT_EQ(vertex_from->OutEdges(View::NEW).GetError(), SHARD_ERROR(ErrorCode::DELETED_OBJECT));
    ASSERT_EQ(vertex_from->OutDegree(View::NEW).GetError(), SHARD_ERROR(ErrorCode::DELETED_OBJECT));
    {
      auto ret = vertex_to->InEdges(View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.From(), from_id);
      ASSERT_EQ(e.To(), to_id);
    }
    ASSERT_EQ(vertex_to->InEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

    acc.Commit(GetNextHlc());
  }

  // Check dataset
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex_from = acc.FindVertex(from_id.primary_key, View::NEW);
    auto vertex_to = acc.FindVertex(to_id.primary_key, View::NEW);
    ASSERT_FALSE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Check edges
    ASSERT_EQ(vertex_to->InEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(View::OLD), 0);
    ASSERT_EQ(vertex_to->InEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);
  }
}
}  // namespace memgraph::storage::v3::tests
