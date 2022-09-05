// Copyright 2022 Memgraph Ltd.
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

namespace memgraph::storage::v3 {
using testing::UnorderedElementsAre;

class StorageEdgeTest : public ::testing::TestWithParam<bool> {
 protected:
  void SetUp() override {
    ASSERT_TRUE(
        store.CreateSchema(primary_label, {storage::v3::SchemaProperty{primary_property, common::SchemaType::INT}}));
  }

  [[nodiscard]] LabelId NameToLabelId(std::string_view label_name) {
    return LabelId::FromUint(id_mapper.NameToId(label_name));
  }

  [[nodiscard]] PropertyId NameToPropertyId(std::string_view property_name) {
    return PropertyId::FromUint(id_mapper.NameToId(property_name));
  }

  [[nodiscard]] EdgeTypeId NameToEdgeTypeId(std::string_view edge_type_name) {
    return EdgeTypeId::FromUint(id_mapper.NameToId(edge_type_name));
  }

  ResultSchema<VertexAccessor> CreateVertex(Shard::Accessor &acc, const PropertyValue &key) {
    return acc.CreateVertexAndValidate(primary_label, {}, {{primary_property, key}});
  }

  NameIdMapper id_mapper;
  static constexpr int64_t min_primary_key_value{0};
  static constexpr int64_t max_primary_key_value{10000};
  const LabelId primary_label{NameToLabelId("label")};
  Shard store{primary_label,
              {PropertyValue{min_primary_key_value}},
              std::vector{PropertyValue{max_primary_key_value}},
              Config{.items = {.properties_on_edges = GetParam()}}};
  const PropertyId primary_property{NameToPropertyId("property")};
};

INSTANTIATE_TEST_CASE_P(EdgesWithProperties, StorageEdgeTest, ::testing::Values(true));
INSTANTIATE_TEST_CASE_P(EdgesWithoutProperties, StorageEdgeTest, ::testing::Values(false));

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeCreateFromSmallerCommit) {
  // Create vertices
  const PropertyValue from_key{0};
  const PropertyValue to_key{1};
  const PropertyValue non_existing_key{2};
  const auto et = NameToEdgeTypeId("et5");
  const auto edge_id = Gid::FromUint(0U);
  auto acc = store.Access();
  const auto [from_id, to_id] =
      std::invoke([this, &from_key, &to_key, &acc]() mutable -> std::pair<VertexId, VertexId> {
        auto from_id = CreateVertex(acc, from_key)->Id(View::NEW).GetValue();
        auto to_id = CreateVertex(acc, to_key)->Id(View::NEW).GetValue();
        return std::make_pair(std::move(from_id), std::move(to_id));
      });
  const auto other_et = NameToEdgeTypeId("other");
  const VertexId from_id_with_different_label{NameToLabelId("different_label"), from_id.primary_key};
  const VertexId to_id_with_different_label{NameToLabelId("different_label"), to_id.primary_key};
  const VertexId non_existing_id{primary_label, {non_existing_key}};

  ASSERT_FALSE(acc.Commit().HasError());

  // Create edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(from_id.primary_key, View::NEW);
    auto vertex_to = acc.FindVertex({to_id.primary_key}, View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto res = acc.CreateEdge(from_id, to_id, et, edge_id);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.Gid(), edge_id);
    ASSERT_EQ(edge.FromVertex(), from_id);
    ASSERT_EQ(edge.ToVertex(), to_id);

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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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

    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// // NOLINTNEXTLINE(hicpp-special-member-functions)
// TEST_P(StorageEdgeTest, EdgeCreateFromLargerCommit) {
//   memgraph::storage::Storage store({.items = {.properties_on_edges = GetParam()}});
//   memgraph::storage::Gid gid_from = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
//   memgraph::storage::Gid gid_to = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

//   // Create vertices
//   {
//     auto acc = store.Access();
//     auto vertex_to = acc.CreateVertex();
//     auto vertex_from = acc.CreateVertex();
//     gid_to = vertex_to.Gid();
//     gid_from = vertex_from.Gid();
//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Create edge
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.FindVertex(gid_from, View::NEW);
//     auto vertex_to = acc.FindVertex(gid_to, View::NEW);
//     ASSERT_TRUE(vertex_from);
//     ASSERT_TRUE(vertex_to);

//     auto et = acc.NameToEdgeType("et5");

//     auto res = acc.CreateEdge(&from_id, &to_id, et);
//     ASSERT_TRUE(res.HasValue());
//     auto edge = res.GetValue();
//     ASSERT_EQ(edge.EdgeType(), et);
//     ASSERT_EQ(edge.FromVertex(), *vertex_from);
//     ASSERT_EQ(edge.ToVertex(), *vertex_to);

//     // Check edges without filters
//     ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 0);
//     {
//       auto ret = vertex_from->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->InDegree(View::OLD), 0);
//     {
//       auto ret = vertex_to->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id)->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &from_id)->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id)->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &to_id)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Check whether the edge exists
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.FindVertex(gid_from, View::NEW);
//     auto vertex_to = acc.FindVertex(gid_to, View::NEW);
//     ASSERT_TRUE(vertex_from);
//     ASSERT_TRUE(vertex_to);

//     auto et = acc.NameToEdgeType("et5");

//     // Check edges without filters
//     ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
//     {
//       auto ret = vertex_from->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     {
//       auto ret = vertex_from->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     {
//       auto ret = vertex_to->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     {
//       auto ret = vertex_to->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &to_id)->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id)->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &from_id)->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &from_id)->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &from_id)->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id)->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &to_id)->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &to_id)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }
// }

// // NOLINTNEXTLINE(hicpp-special-member-functions)
// TEST_P(StorageEdgeTest, EdgeCreateFromSameCommit) {
//   memgraph::storage::Storage store({.items = {.properties_on_edges = GetParam()}});
//   memgraph::storage::Gid gid_vertex = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

//   // Create vertex
//   {
//     auto acc = store.Access();
//     auto vertex = acc.CreateVertex();
//     gid_vertex = vertex.Gid();
//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Create edge
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid_vertex, View::NEW);
//     ASSERT_TRUE(vertex);

//     auto et = acc.NameToEdgeType("et5");

//     auto res = acc.CreateEdge(&*vertex, &*vertex, et);
//     ASSERT_TRUE(res.HasValue());
//     auto edge = res.GetValue();
//     ASSERT_EQ(edge.EdgeType(), et);
//     ASSERT_EQ(edge.FromVertex(), *vertex);
//     ASSERT_EQ(edge.ToVertex(), *vertex);

//     // Check edges without filters
//     ASSERT_EQ(vertex->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex->InDegree(View::OLD), 0);
//     {
//       auto ret = vertex->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     ASSERT_EQ(vertex->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex->OutDegree(View::OLD), 0);
//     {
//       auto ret = vertex->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {other_et}, &*vertex)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Check whether the edge exists
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid_vertex, View::NEW);
//     ASSERT_TRUE(vertex);

//     auto et = acc.NameToEdgeType("et5");

//     // Check edges without filters
//     {
//       auto ret = vertex->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->InDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     {
//       auto ret = vertex->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     {
//       auto ret = vertex->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->OutDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     {
//       auto ret = vertex->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex->InEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {other_et}, &*vertex)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }
// }

// // NOLINTNEXTLINE(hicpp-special-member-functions)
// TEST_P(StorageEdgeTest, EdgeCreateFromSmallerAbort) {
//   memgraph::storage::Storage store({.items = {.properties_on_edges = GetParam()}});
//   memgraph::storage::Gid gid_from = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
//   memgraph::storage::Gid gid_to = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

//   // Create vertices
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.CreateVertex();
//     auto vertex_to = acc.CreateVertex();
//     gid_from = vertex_from.Gid();
//     gid_to = vertex_to.Gid();
//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Create edge, but abort the transaction
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.FindVertex(gid_from, View::NEW);
//     auto vertex_to = acc.FindVertex(gid_to, View::NEW);
//     ASSERT_TRUE(vertex_from);
//     ASSERT_TRUE(vertex_to);

//     auto et = acc.NameToEdgeType("et5");

//     auto res = acc.CreateEdge(&from_id, &to_id, et);
//     ASSERT_TRUE(res.HasValue());
//     auto edge = res.GetValue();
//     ASSERT_EQ(edge.EdgeType(), et);
//     ASSERT_EQ(edge.FromVertex(), *vertex_from);
//     ASSERT_EQ(edge.ToVertex(), *vertex_to);

//     // Check edges without filters
//     ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 0);
//     {
//       auto ret = vertex_from->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->InDegree(View::OLD), 0);
//     {
//       auto ret = vertex_to->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id)->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &from_id)->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id)->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &to_id)->size(), 0);

//     acc.Abort();
//   }

//   // Check whether the edge exists
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.FindVertex(gid_from, View::NEW);
//     auto vertex_to = acc.FindVertex(gid_to, View::NEW);
//     ASSERT_TRUE(vertex_from);
//     ASSERT_TRUE(vertex_to);

//     // Check edges without filters
//     ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->InDegree(View::NEW), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Create edge
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.FindVertex(gid_from, View::NEW);
//     auto vertex_to = acc.FindVertex(gid_to, View::NEW);
//     ASSERT_TRUE(vertex_from);
//     ASSERT_TRUE(vertex_to);

//     auto et = acc.NameToEdgeType("et5");

//     auto res = acc.CreateEdge(&from_id, &to_id, et);
//     ASSERT_TRUE(res.HasValue());
//     auto edge = res.GetValue();
//     ASSERT_EQ(edge.EdgeType(), et);
//     ASSERT_EQ(edge.FromVertex(), *vertex_from);
//     ASSERT_EQ(edge.ToVertex(), *vertex_to);

//     // Check edges without filters
//     ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 0);
//     {
//       auto ret = vertex_from->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->InDegree(View::OLD), 0);
//     {
//       auto ret = vertex_to->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id)->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &from_id)->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id)->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &to_id)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Check whether the edge exists
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.FindVertex(gid_from, View::NEW);
//     auto vertex_to = acc.FindVertex(gid_to, View::NEW);
//     ASSERT_TRUE(vertex_from);
//     ASSERT_TRUE(vertex_to);

//     auto et = acc.NameToEdgeType("et5");

//     // Check edges without filters
//     ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
//     {
//       auto ret = vertex_from->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     {
//       auto ret = vertex_from->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     {
//       auto ret = vertex_to->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     {
//       auto ret = vertex_to->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &to_id)->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id)->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &from_id)->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &from_id)->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &from_id)->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id)->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &to_id)->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &to_id)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }
// }

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeCreateFromLargerAbort) {
  // Create vertices
  const PropertyValue from_key{0};
  const PropertyValue to_key{1};
  const PropertyValue non_existing_key{2};
  auto acc = store.Access();
  const auto [from_id, to_id] =
      std::invoke([this, &from_key, &to_key, &acc]() mutable -> std::pair<VertexId, VertexId> {
        auto from_id = CreateVertex(acc, from_key)->Id(View::NEW).GetValue();
        auto to_id = CreateVertex(acc, to_key)->Id(View::NEW).GetValue();
        return std::make_pair(std::move(from_id), std::move(to_id));
      });

  const auto et = NameToEdgeTypeId("et5");
  const auto other_et = NameToEdgeTypeId("other");
  const VertexId from_id_with_different_label{NameToLabelId("different_label"), from_id.primary_key};
  const VertexId to_id_with_different_label{NameToLabelId("different_label"), to_id.primary_key};
  const VertexId non_existing_id{primary_label, {non_existing_key}};

  ASSERT_FALSE(acc.Commit().HasError());

  // Create edge but abort
  {
    auto acc = store.Access();
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
    ASSERT_EQ(edge.FromVertex(), from_id);
    ASSERT_EQ(edge.ToVertex(), to_id);

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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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
    auto acc = store.Access();
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

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Create edge
  {
    auto acc = store.Access();
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
    ASSERT_EQ(edge.FromVertex(), from_id);
    ASSERT_EQ(edge.ToVertex(), to_id);

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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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

    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// // NOLINTNEXTLINE(hicpp-special-member-functions)
// TEST_P(StorageEdgeTest, EdgeCreateFromSameAbort) {
//   memgraph::storage::Storage store({.items = {.properties_on_edges = GetParam()}});
//   memgraph::storage::Gid gid_vertex = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

//   // Create vertex
//   {
//     auto acc = store.Access();
//     auto vertex = acc.CreateVertex();
//     gid_vertex = vertex.Gid();
//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Create edge, but abort the transaction
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid_vertex, View::NEW);
//     ASSERT_TRUE(vertex);

//     auto et = acc.NameToEdgeType("et5");

//     auto res = acc.CreateEdge(&*vertex, &*vertex, et);
//     ASSERT_TRUE(res.HasValue());
//     auto edge = res.GetValue();
//     ASSERT_EQ(edge.EdgeType(), et);
//     ASSERT_EQ(edge.FromVertex(), *vertex);
//     ASSERT_EQ(edge.ToVertex(), *vertex);

//     // Check edges without filters
//     ASSERT_EQ(vertex->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex->InDegree(View::OLD), 0);
//     {
//       auto ret = vertex->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     ASSERT_EQ(vertex->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex->OutDegree(View::OLD), 0);
//     {
//       auto ret = vertex->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {other_et}, &*vertex)->size(), 0);

//     acc.Abort();
//   }

//   // Check whether the edge exists
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid_vertex, View::NEW);
//     ASSERT_TRUE(vertex);

//     // Check edges without filters
//     ASSERT_EQ(vertex->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex->InDegree(View::NEW), 0);
//     ASSERT_EQ(vertex->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex->OutDegree(View::NEW), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Create edge
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid_vertex, View::NEW);
//     ASSERT_TRUE(vertex);

//     auto et = acc.NameToEdgeType("et5");

//     auto res = acc.CreateEdge(&*vertex, &*vertex, et);
//     ASSERT_TRUE(res.HasValue());
//     auto edge = res.GetValue();
//     ASSERT_EQ(edge.EdgeType(), et);
//     ASSERT_EQ(edge.FromVertex(), *vertex);
//     ASSERT_EQ(edge.ToVertex(), *vertex);

//     // Check edges without filters
//     ASSERT_EQ(vertex->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex->InDegree(View::OLD), 0);
//     {
//       auto ret = vertex->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     ASSERT_EQ(vertex->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex->OutDegree(View::OLD), 0);
//     {
//       auto ret = vertex->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {other_et}, &*vertex)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Check whether the edge exists
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid_vertex, View::NEW);
//     ASSERT_TRUE(vertex);

//     auto et = acc.NameToEdgeType("et5");

//     // Check edges without filters
//     {
//       auto ret = vertex->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->InDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     {
//       auto ret = vertex->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     {
//       auto ret = vertex->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->OutDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     {
//       auto ret = vertex->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex->InEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {other_et}, &*vertex)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }
// }

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeDeleteFromSmallerCommit) {
  // Create vertex
  const PropertyValue from_key{0};
  const PropertyValue to_key{max_primary_key_value};
  const PropertyValue non_existing_key{2};
  auto acc = store.Access();
  const auto from_id = std::invoke(
      [this, &from_key, &acc]() mutable -> VertexId { return CreateVertex(acc, from_key)->Id(View::NEW).GetValue(); });
  const VertexId to_id{primary_label, {to_key}};
  const auto et = NameToEdgeTypeId("et5");
  const auto edge_id = Gid::FromUint(1U);
  const auto other_et = NameToEdgeTypeId("other");
  const VertexId to_id_with_different_label{NameToLabelId("different_label"), to_id.primary_key};
  const VertexId non_existing_id{primary_label, {non_existing_key}};

  ASSERT_FALSE(acc.Commit().HasError());

  // Create edge
  {
    auto acc = store.Access();
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
    ASSERT_EQ(edge.FromVertex(), from_id);
    ASSERT_EQ(edge.ToVertex(), to_id);

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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Delete edge
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(from_id.primary_key, View::NEW);
    ASSERT_TRUE(vertex_from);

    auto edge = vertex_from->OutEdges(View::NEW).GetValue()[0];

    const auto res = acc.DeleteEdge(&edge);
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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
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

    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// // NOLINTNEXTLINE(hicpp-special-member-functions)
// TEST_P(StorageEdgeTest, EdgeDeleteFromLargerCommit) {
//   memgraph::storage::Storage store({.items = {.properties_on_edges = GetParam()}});
//   memgraph::storage::Gid gid_from = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
//   memgraph::storage::Gid gid_to = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

//   // Create vertices
//   {
//     auto acc = store.Access();
//     auto vertex_to = acc.CreateVertex();
//     auto vertex_from = acc.CreateVertex();
//     gid_from = vertex_from.Gid();
//     gid_to = vertex_to.Gid();
//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Create edge
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.FindVertex(gid_from, View::NEW);
//     auto vertex_to = acc.FindVertex(gid_to, View::NEW);
//     ASSERT_TRUE(vertex_from);
//     ASSERT_TRUE(vertex_to);

//     auto et = acc.NameToEdgeType("et5");

//     auto res = acc.CreateEdge(&from_id, &to_id, et);
//     ASSERT_TRUE(res.HasValue());
//     auto edge = res.GetValue();
//     ASSERT_EQ(edge.EdgeType(), et);
//     ASSERT_EQ(edge.FromVertex(), *vertex_from);
//     ASSERT_EQ(edge.ToVertex(), *vertex_to);

//     // Check edges without filters
//     ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 0);
//     {
//       auto ret = vertex_from->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->InDegree(View::OLD), 0);
//     {
//       auto ret = vertex_to->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id)->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &from_id)->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id)->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &to_id)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Check whether the edge exists
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.FindVertex(gid_from, View::NEW);
//     auto vertex_to = acc.FindVertex(gid_to, View::NEW);
//     ASSERT_TRUE(vertex_from);
//     ASSERT_TRUE(vertex_to);

//     auto et = acc.NameToEdgeType("et5");

//     // Check edges without filters
//     ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
//     {
//       auto ret = vertex_from->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     {
//       auto ret = vertex_from->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     {
//       auto ret = vertex_to->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     {
//       auto ret = vertex_to->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &to_id)->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id)->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &from_id)->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &from_id)->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &from_id)->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id)->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &to_id)->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &to_id)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Delete edge
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.FindVertex(gid_from, View::NEW);
//     auto vertex_to = acc.FindVertex(gid_to, View::NEW);
//     ASSERT_TRUE(vertex_from);
//     ASSERT_TRUE(vertex_to);

//     auto et = acc.NameToEdgeType("et5");

//     auto edge = vertex_from->OutEdges(View::NEW).GetValue()[0];

//     auto res = acc.DeleteEdge(&edge);
//     ASSERT_TRUE(res.HasValue());
//     ASSERT_TRUE(res.GetValue());

//     // Check edges without filters
//     ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
//     {
//       auto ret = vertex_from->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 0);
//     {
//       auto ret = vertex_to->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->InDegree(View::NEW), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &to_id)->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &from_id)->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &from_id)->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &to_id)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Check whether the edge exists
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.FindVertex(gid_from, View::NEW);
//     auto vertex_to = acc.FindVertex(gid_to, View::NEW);
//     ASSERT_TRUE(vertex_from);
//     ASSERT_TRUE(vertex_to);

//     // Check edges without filters
//     ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->InDegree(View::NEW), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }
// }

// // NOLINTNEXTLINE(hicpp-special-member-functions)
// TEST_P(StorageEdgeTest, EdgeDeleteFromSameCommit) {
//   memgraph::storage::Storage store({.items = {.properties_on_edges = GetParam()}});
//   memgraph::storage::Gid gid_vertex = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

//   // Create vertex
//   {
//     auto acc = store.Access();
//     auto vertex = acc.CreateVertex();
//     gid_vertex = vertex.Gid();
//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Create edge
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid_vertex, View::NEW);
//     ASSERT_TRUE(vertex);

//     auto et = acc.NameToEdgeType("et5");

//     auto res = acc.CreateEdge(&*vertex, &*vertex, et);
//     ASSERT_TRUE(res.HasValue());
//     auto edge = res.GetValue();
//     ASSERT_EQ(edge.EdgeType(), et);
//     ASSERT_EQ(edge.FromVertex(), *vertex);
//     ASSERT_EQ(edge.ToVertex(), *vertex);

//     // Check edges without filters
//     ASSERT_EQ(vertex->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex->InDegree(View::OLD), 0);
//     {
//       auto ret = vertex->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     ASSERT_EQ(vertex->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex->OutDegree(View::OLD), 0);
//     {
//       auto ret = vertex->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {other_et}, &*vertex)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Check whether the edge exists
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid_vertex, View::NEW);
//     ASSERT_TRUE(vertex);

//     auto et = acc.NameToEdgeType("et5");

//     // Check edges without filters
//     {
//       auto ret = vertex->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->InDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     {
//       auto ret = vertex->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     {
//       auto ret = vertex->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->OutDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     {
//       auto ret = vertex->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex->InEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {other_et}, &*vertex)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Delete edge
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid_vertex, View::NEW);
//     ASSERT_TRUE(vertex);

//     auto et = acc.NameToEdgeType("et5");

//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     auto res = acc.DeleteEdge(&edge);
//     ASSERT_TRUE(res.HasValue());
//     ASSERT_TRUE(res.GetValue());

//     // Check edges without filters
//     {
//       auto ret = vertex->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->InDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     ASSERT_EQ(vertex->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex->InDegree(View::NEW), 0);
//     {
//       auto ret = vertex->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->OutDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     ASSERT_EQ(vertex->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex->OutDegree(View::NEW), 0);

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex->InEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {other_et}, &*vertex)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Check whether the edge exists
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid_vertex, View::NEW);
//     ASSERT_TRUE(vertex);

//     // Check edges without filters
//     ASSERT_EQ(vertex->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex->InDegree(View::NEW), 0);
//     ASSERT_EQ(vertex->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex->OutDegree(View::NEW), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }
// }

// // NOLINTNEXTLINE(hicpp-special-member-functions)
// TEST_P(StorageEdgeTest, EdgeDeleteFromSmallerAbort) {
//   memgraph::storage::Storage store({.items = {.properties_on_edges = GetParam()}});
//   memgraph::storage::Gid gid_from = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
//   memgraph::storage::Gid gid_to = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

//   // Create vertices
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.CreateVertex();
//     auto vertex_to = acc.CreateVertex();
//     gid_from = vertex_from.Gid();
//     gid_to = vertex_to.Gid();
//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Create edge
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.FindVertex(gid_from, View::NEW);
//     auto vertex_to = acc.FindVertex(gid_to, View::NEW);
//     ASSERT_TRUE(vertex_from);
//     ASSERT_TRUE(vertex_to);

//     auto et = acc.NameToEdgeType("et5");

//     auto res = acc.CreateEdge(&from_id, &to_id, et);
//     ASSERT_TRUE(res.HasValue());
//     auto edge = res.GetValue();
//     ASSERT_EQ(edge.EdgeType(), et);
//     ASSERT_EQ(edge.FromVertex(), *vertex_from);
//     ASSERT_EQ(edge.ToVertex(), *vertex_to);

//     // Check edges without filters
//     ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 0);
//     {
//       auto ret = vertex_from->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->InDegree(View::OLD), 0);
//     {
//       auto ret = vertex_to->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id)->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &from_id)->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id)->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &to_id)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Check whether the edge exists
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.FindVertex(gid_from, View::NEW);
//     auto vertex_to = acc.FindVertex(gid_to, View::NEW);
//     ASSERT_TRUE(vertex_from);
//     ASSERT_TRUE(vertex_to);

//     auto et = acc.NameToEdgeType("et5");

//     // Check edges without filters
//     ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
//     {
//       auto ret = vertex_from->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     {
//       auto ret = vertex_from->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     {
//       auto ret = vertex_to->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     {
//       auto ret = vertex_to->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &to_id)->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id)->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &from_id)->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &from_id)->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &from_id)->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id)->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &to_id)->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &to_id)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Delete the edge, but abort the transaction
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.FindVertex(gid_from, View::NEW);
//     auto vertex_to = acc.FindVertex(gid_to, View::NEW);
//     ASSERT_TRUE(vertex_from);
//     ASSERT_TRUE(vertex_to);

//     auto et = acc.NameToEdgeType("et5");

//     auto edge = vertex_from->OutEdges(View::NEW).GetValue()[0];

//     auto res = acc.DeleteEdge(&edge);
//     ASSERT_TRUE(res.HasValue());
//     ASSERT_TRUE(res.GetValue());

//     // Check edges without filters
//     ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
//     {
//       auto ret = vertex_from->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 0);
//     {
//       auto ret = vertex_to->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->InDegree(View::NEW), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &to_id)->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &from_id)->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &from_id)->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &to_id)->size(), 0);

//     acc.Abort();
//   }

//   // Check whether the edge exists
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.FindVertex(gid_from, View::NEW);
//     auto vertex_to = acc.FindVertex(gid_to, View::NEW);
//     ASSERT_TRUE(vertex_from);
//     ASSERT_TRUE(vertex_to);

//     auto et = acc.NameToEdgeType("et5");

//     // Check edges without filters
//     ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
//     {
//       auto ret = vertex_from->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     {
//       auto ret = vertex_from->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     {
//       auto ret = vertex_to->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     {
//       auto ret = vertex_to->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &to_id)->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &to_id)->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &from_id)->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW, {}, &from_id)->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &from_id)->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &from_id)->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &to_id)->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW, {}, &to_id)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Delete the edge
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.FindVertex(gid_from, View::NEW);
//     auto vertex_to = acc.FindVertex(gid_to, View::NEW);
//     ASSERT_TRUE(vertex_from);
//     ASSERT_TRUE(vertex_to);

//     auto et = acc.NameToEdgeType("et5");

//     auto edge = vertex_from->OutEdges(View::NEW).GetValue()[0];

//     auto res = acc.DeleteEdge(&edge);
//     ASSERT_TRUE(res.HasValue());
//     ASSERT_TRUE(res.GetValue());

//     // Check edges without filters
//     ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
//     {
//       auto ret = vertex_from->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 0);
//     {
//       auto ret = vertex_to->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->InDegree(View::NEW), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &to_id)->size(), 1);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD, {}, &from_id)->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &from_id)->size(), 1);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD, {}, &to_id)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Check whether the edge exists
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.FindVertex(gid_from, View::NEW);
//     auto vertex_to = acc.FindVertex(gid_to, View::NEW);
//     ASSERT_TRUE(vertex_from);
//     ASSERT_TRUE(vertex_to);

//     // Check edges without filters
//     ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->InDegree(View::NEW), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }
// }

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeDeleteFromLargerAbort) {
  // Create vertex
  const PropertyValue from_key{max_primary_key_value};
  const PropertyValue to_key{0};
  const PropertyValue non_existing_key{2};
  auto acc = store.Access();
  const auto to_id = std::invoke(
      [this, &to_key, &acc]() mutable -> VertexId { return CreateVertex(acc, to_key)->Id(View::NEW).GetValue(); });
  const VertexId from_id{primary_label, {from_key}};
  const auto et = NameToEdgeTypeId("et5");
  const auto edge_id = Gid::FromUint(1U);
  const auto other_et = NameToEdgeTypeId("other");
  const VertexId from_id_with_different_label{NameToLabelId("different_label"), from_id.primary_key};
  const VertexId non_existing_id{primary_label, {non_existing_key}};

  ASSERT_FALSE(acc.Commit().HasError());
  // Create edge
  {
    auto acc = store.Access();
    auto vertex_to = acc.FindVertex(to_id.primary_key, View::NEW);
    ASSERT_TRUE(vertex_to);

    auto res = acc.CreateEdge(from_id, to_id, et, edge_id);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.Gid(), edge_id);
    ASSERT_EQ(edge.FromVertex(), from_id);
    ASSERT_EQ(edge.ToVertex(), to_id);

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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Delete the edge, but abort the transaction
  {
    auto acc = store.Access();
    auto vertex_to = acc.FindVertex(to_id.primary_key, View::NEW);
    ASSERT_TRUE(vertex_to);

    auto edge = vertex_to->InEdges(View::NEW).GetValue()[0];

    auto res = acc.DeleteEdge(&edge);
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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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
    auto acc = store.Access();
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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Delete the edge
  {
    auto acc = store.Access();
    auto vertex_to = acc.FindVertex(to_id.primary_key, View::NEW);
    ASSERT_TRUE(vertex_to);

    auto edge = vertex_to->InEdges(View::NEW).GetValue()[0];

    const auto res = acc.DeleteEdge(&edge);
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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
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

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store.Access();
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

    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// // NOLINTNEXTLINE(hicpp-special-member-functions)
// TEST_P(StorageEdgeTest, EdgeDeleteFromSameAbort) {
//   memgraph::storage::Storage store({.items = {.properties_on_edges = GetParam()}});
//   memgraph::storage::Gid gid_vertex = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

//   // Create vertex
//   {
//     auto acc = store.Access();
//     auto vertex = acc.CreateVertex();
//     gid_vertex = vertex.Gid();
//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Create edge
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid_vertex, View::NEW);
//     ASSERT_TRUE(vertex);

//     auto et = acc.NameToEdgeType("et5");

//     auto res = acc.CreateEdge(&*vertex, &*vertex, et);
//     ASSERT_TRUE(res.HasValue());
//     auto edge = res.GetValue();
//     ASSERT_EQ(edge.EdgeType(), et);
//     ASSERT_EQ(edge.FromVertex(), *vertex);
//     ASSERT_EQ(edge.ToVertex(), *vertex);

//     // Check edges without filters
//     ASSERT_EQ(vertex->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex->InDegree(View::OLD), 0);
//     {
//       auto ret = vertex->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     ASSERT_EQ(vertex->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex->OutDegree(View::OLD), 0);
//     {
//       auto ret = vertex->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {other_et}, &*vertex)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Check whether the edge exists
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid_vertex, View::NEW);
//     ASSERT_TRUE(vertex);

//     auto et = acc.NameToEdgeType("et5");

//     // Check edges without filters
//     {
//       auto ret = vertex->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->InDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     {
//       auto ret = vertex->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     {
//       auto ret = vertex->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->OutDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     {
//       auto ret = vertex->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex->InEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {other_et}, &*vertex)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Delete the edge, but abort the transaction
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid_vertex, View::NEW);
//     ASSERT_TRUE(vertex);

//     auto et = acc.NameToEdgeType("et5");

//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     auto res = acc.DeleteEdge(&edge);
//     ASSERT_TRUE(res.HasValue());
//     ASSERT_TRUE(res.GetValue());

//     // Check edges without filters
//     {
//       auto ret = vertex->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->InDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     ASSERT_EQ(vertex->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex->InDegree(View::NEW), 0);
//     {
//       auto ret = vertex->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->OutDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     ASSERT_EQ(vertex->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex->OutDegree(View::NEW), 0);

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex->InEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {other_et}, &*vertex)->size(), 0);

//     acc.Abort();
//   }

//   // Check whether the edge exists
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid_vertex, View::NEW);
//     ASSERT_TRUE(vertex);

//     auto et = acc.NameToEdgeType("et5");

//     // Check edges without filters
//     {
//       auto ret = vertex->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->InDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     {
//       auto ret = vertex->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     {
//       auto ret = vertex->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->OutDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     {
//       auto ret = vertex->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex->InEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::NEW, {other_et}, &*vertex)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Delete the edge
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid_vertex, View::NEW);
//     ASSERT_TRUE(vertex);

//     auto et = acc.NameToEdgeType("et5");

//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     auto res = acc.DeleteEdge(&edge);
//     ASSERT_TRUE(res.HasValue());
//     ASSERT_TRUE(res.GetValue());

//     // Check edges without filters
//     {
//       auto ret = vertex->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->InDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     ASSERT_EQ(vertex->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex->InDegree(View::NEW), 0);
//     {
//       auto ret = vertex->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex->OutDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex);
//       ASSERT_EQ(e.ToVertex(), *vertex);
//     }
//     ASSERT_EQ(vertex->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex->OutDegree(View::NEW), 0);

//     auto other_et = acc.NameToEdgeType("other");

//     // Check edges with filters
//     ASSERT_EQ(vertex->InEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->InEdges(View::OLD, {other_et}, &*vertex)->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {other_et})->size(), 0);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {et, other_et})->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {}, &*vertex)->size(), 1);
//     ASSERT_EQ(vertex->OutEdges(View::OLD, {other_et}, &*vertex)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Check whether the edge exists
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid_vertex, View::NEW);
//     ASSERT_TRUE(vertex);

//     // Check edges without filters
//     ASSERT_EQ(vertex->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex->InDegree(View::NEW), 0);
//     ASSERT_EQ(vertex->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex->OutDegree(View::NEW), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }
// }

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, VertexDetachDeleteSingleCommit) {
  // Create vertices
  const PropertyValue from_key{0};
  const PropertyValue to_key{1};
  const PropertyValue non_existing_key{2};
  const auto et = NameToEdgeTypeId("et5");
  const auto edge_id = Gid::FromUint(0U);
  auto acc = store.Access();
  VertexId from_id{};
  VertexId to_id{};
  const auto other_et = NameToEdgeTypeId("other");
  const VertexId non_existing_id{primary_label, {non_existing_key}};

  // Create dataset
  {
    auto acc = store.Access();
    auto vertex_from = CreateVertex(acc, from_key).GetValue();
    auto vertex_to = CreateVertex(acc, to_key).GetValue();
    from_id = vertex_from.Id(View::NEW).GetValue();
    to_id = vertex_to.Id(View::NEW).GetValue();

    auto res = acc.CreateEdge(from_id, to_id, et, edge_id);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.Gid(), edge_id);
    ASSERT_EQ(edge.FromVertex(), from_id);
    ASSERT_EQ(edge.ToVertex(), to_id);

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
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
    }
    {
      auto ret = vertex_to.InEdges(View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to.InDegree(View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
    }
    ASSERT_EQ(vertex_to.OutEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to.OutDegree(View::NEW), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Detach delete vertex
  {
    auto acc = store.Access();
    auto vertex_from = acc.FindVertex(from_id.primary_key, View::NEW);
    auto vertex_to = acc.FindVertex(to_id.primary_key, View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    // Delete must fail
    {
      auto ret = acc.DeleteVertex(&vertex_from.value());
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), Error::VERTEX_HAS_EDGES);
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
    ASSERT_EQ(vertex_from->InEdges(View::NEW).GetError(), Error::DELETED_OBJECT);
    ASSERT_EQ(vertex_from->InDegree(View::NEW).GetError(), Error::DELETED_OBJECT);
    {
      auto ret = vertex_from->OutEdges(View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
    }
    ASSERT_EQ(vertex_from->OutEdges(View::NEW).GetError(), Error::DELETED_OBJECT);
    ASSERT_EQ(vertex_from->OutDegree(View::NEW).GetError(), Error::DELETED_OBJECT);
    {
      auto ret = vertex_to->InEdges(View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue();
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.Gid(), edge_id);
      ASSERT_EQ(e.FromVertex(), from_id);
      ASSERT_EQ(e.ToVertex(), to_id);
    }
    ASSERT_EQ(vertex_to->InEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

    ASSERT_FALSE(acc.Commit().HasError());
  }

  // Check dataset
  {
    auto acc = store.Access();
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

// // NOLINTNEXTLINE(hicpp-special-member-functions)
// TEST_P(StorageEdgeTest, VertexDetachDeleteMultipleCommit) {
//   memgraph::storage::Storage store({.items = {.properties_on_edges = GetParam()}});
//   memgraph::storage::Gid gid_vertex1 = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
//   memgraph::storage::Gid gid_vertex2 = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

//   // Create dataset
//   {
//     auto acc = store.Access();
//     auto vertex1 = acc.CreateVertex();
//     auto vertex2 = acc.CreateVertex();

//     gid_vertex1 = vertex1.Gid();
//     gid_vertex2 = vertex2.Gid();

//     auto et1 = acc.NameToEdgeType("et1");
//     auto et2 = acc.NameToEdgeType("et2");
//     auto et3 = acc.NameToEdgeType("et3");
//     auto et4 = acc.NameToEdgeType("et4");

//     auto res1 = acc.CreateEdge(&vertex1, &vertex2, et1);
//     ASSERT_TRUE(res1.HasValue());
//     auto edge1 = res1.GetValue();
//     ASSERT_EQ(edge1.EdgeType(), et1);
//     ASSERT_EQ(edge1.FromVertex(), vertex1);
//     ASSERT_EQ(edge1.ToVertex(), vertex2);

//     auto res2 = acc.CreateEdge(&vertex2, &vertex1, et2);
//     ASSERT_TRUE(res2.HasValue());
//     auto edge2 = res2.GetValue();
//     ASSERT_EQ(edge2.EdgeType(), et2);
//     ASSERT_EQ(edge2.FromVertex(), vertex2);
//     ASSERT_EQ(edge2.ToVertex(), vertex1);

//     auto res3 = acc.CreateEdge(&vertex1, &vertex1, et3);
//     ASSERT_TRUE(res3.HasValue());
//     auto edge3 = res3.GetValue();
//     ASSERT_EQ(edge3.EdgeType(), et3);
//     ASSERT_EQ(edge3.FromVertex(), vertex1);
//     ASSERT_EQ(edge3.ToVertex(), vertex1);

//     auto res4 = acc.CreateEdge(&vertex2, &vertex2, et4);
//     ASSERT_TRUE(res4.HasValue());
//     auto edge4 = res4.GetValue();
//     ASSERT_EQ(edge4.EdgeType(), et4);
//     ASSERT_EQ(edge4.FromVertex(), vertex2);
//     ASSERT_EQ(edge4.ToVertex(), vertex2);

//     // Check edges
//     {
//       auto ret = vertex1.InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex1.InDegree(View::NEW), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et2);
//         ASSERT_EQ(e.FromVertex(), vertex2);
//         ASSERT_EQ(e.ToVertex(), vertex1);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et3);
//         ASSERT_EQ(e.FromVertex(), vertex1);
//         ASSERT_EQ(e.ToVertex(), vertex1);
//       }
//     }
//     {
//       auto ret = vertex1.OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex1.OutDegree(View::NEW), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et1);
//         ASSERT_EQ(e.FromVertex(), vertex1);
//         ASSERT_EQ(e.ToVertex(), vertex2);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et3);
//         ASSERT_EQ(e.FromVertex(), vertex1);
//         ASSERT_EQ(e.ToVertex(), vertex1);
//       }
//     }
//     {
//       auto ret = vertex2.InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex2.InDegree(View::NEW), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et1);
//         ASSERT_EQ(e.FromVertex(), vertex1);
//         ASSERT_EQ(e.ToVertex(), vertex2);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et4);
//         ASSERT_EQ(e.FromVertex(), vertex2);
//         ASSERT_EQ(e.ToVertex(), vertex2);
//       }
//     }
//     {
//       auto ret = vertex2.OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex2.OutDegree(View::NEW), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et2);
//         ASSERT_EQ(e.FromVertex(), vertex2);
//         ASSERT_EQ(e.ToVertex(), vertex1);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et4);
//         ASSERT_EQ(e.FromVertex(), vertex2);
//         ASSERT_EQ(e.ToVertex(), vertex2);
//       }
//     }

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Detach delete vertex
//   {
//     auto acc = store.Access();
//     auto vertex1 = acc.FindVertex(gid_vertex1, View::NEW);
//     auto vertex2 = acc.FindVertex(gid_vertex2, View::NEW);
//     ASSERT_TRUE(vertex1);
//     ASSERT_TRUE(vertex2);

//     auto et1 = acc.NameToEdgeType("et1");
//     auto et2 = acc.NameToEdgeType("et2");
//     auto et3 = acc.NameToEdgeType("et3");
//     auto et4 = acc.NameToEdgeType("et4");

//     // Delete must fail
//     {
//       auto ret = acc.DeleteVertex(&*vertex1);
//       ASSERT_TRUE(ret.HasError());
//       ASSERT_EQ(ret.GetError(), memgraph::storage::Error::VERTEX_HAS_EDGES);
//     }

//     // Detach delete vertex
//     {
//       auto ret = acc.DetachDeleteVertex(&*vertex1);
//       ASSERT_TRUE(ret.HasValue());
//       ASSERT_TRUE(*ret);
//     }

//     // Check edges
//     {
//       auto ret = vertex1->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex1->InDegree(View::OLD), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et2);
//         ASSERT_EQ(e.FromVertex(), *vertex2);
//         ASSERT_EQ(e.ToVertex(), *vertex1);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et3);
//         ASSERT_EQ(e.FromVertex(), *vertex1);
//         ASSERT_EQ(e.ToVertex(), *vertex1);
//       }
//     }
//     ASSERT_EQ(vertex1->InEdges(View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
//     ASSERT_EQ(vertex1->InDegree(View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
//     {
//       auto ret = vertex1->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex1->OutDegree(View::OLD), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et1);
//         ASSERT_EQ(e.FromVertex(), *vertex1);
//         ASSERT_EQ(e.ToVertex(), *vertex2);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et3);
//         ASSERT_EQ(e.FromVertex(), *vertex1);
//         ASSERT_EQ(e.ToVertex(), *vertex1);
//       }
//     }
//     ASSERT_EQ(vertex1->OutEdges(View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
//     ASSERT_EQ(vertex1->OutDegree(View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
//     {
//       auto ret = vertex2->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex2->InDegree(View::OLD), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et1);
//         ASSERT_EQ(e.FromVertex(), *vertex1);
//         ASSERT_EQ(e.ToVertex(), *vertex2);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et4);
//         ASSERT_EQ(e.FromVertex(), *vertex2);
//         ASSERT_EQ(e.ToVertex(), *vertex2);
//       }
//     }
//     {
//       auto ret = vertex2->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex2->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et4);
//       ASSERT_EQ(e.FromVertex(), *vertex2);
//       ASSERT_EQ(e.ToVertex(), *vertex2);
//     }
//     {
//       auto ret = vertex2->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex2->OutDegree(View::OLD), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et2);
//         ASSERT_EQ(e.FromVertex(), *vertex2);
//         ASSERT_EQ(e.ToVertex(), *vertex1);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et4);
//         ASSERT_EQ(e.FromVertex(), *vertex2);
//         ASSERT_EQ(e.ToVertex(), *vertex2);
//       }
//     }
//     {
//       auto ret = vertex2->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex2->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et4);
//       ASSERT_EQ(e.FromVertex(), *vertex2);
//       ASSERT_EQ(e.ToVertex(), *vertex2);
//     }

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Check dataset
//   {
//     auto acc = store.Access();
//     auto vertex1 = acc.FindVertex(gid_vertex1, View::NEW);
//     auto vertex2 = acc.FindVertex(gid_vertex2, View::NEW);
//     ASSERT_FALSE(vertex1);
//     ASSERT_TRUE(vertex2);

//     auto et4 = acc.NameToEdgeType("et4");

//     // Check edges
//     {
//       auto ret = vertex2->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex2->InDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et4);
//       ASSERT_EQ(e.FromVertex(), *vertex2);
//       ASSERT_EQ(e.ToVertex(), *vertex2);
//     }
//     {
//       auto ret = vertex2->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex2->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et4);
//       ASSERT_EQ(e.FromVertex(), *vertex2);
//       ASSERT_EQ(e.ToVertex(), *vertex2);
//     }
//     {
//       auto ret = vertex2->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex2->OutDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et4);
//       ASSERT_EQ(e.FromVertex(), *vertex2);
//       ASSERT_EQ(e.ToVertex(), *vertex2);
//     }
//     {
//       auto ret = vertex2->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex2->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et4);
//       ASSERT_EQ(e.FromVertex(), *vertex2);
//       ASSERT_EQ(e.ToVertex(), *vertex2);
//     }
//   }
// }

// // NOLINTNEXTLINE(hicpp-special-member-functions)
// TEST_P(StorageEdgeTest, VertexDetachDeleteSingleAbort) {
//   memgraph::storage::Storage store({.items = {.properties_on_edges = GetParam()}});
//   memgraph::storage::Gid gid_from = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
//   memgraph::storage::Gid gid_to = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

//   // Create dataset
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.CreateVertex();
//     auto vertex_to = acc.CreateVertex();

//     auto et = acc.NameToEdgeType("et5");

//     auto res = acc.CreateEdge(&vertex_from, &vertex_to, et);
//     ASSERT_TRUE(res.HasValue());
//     auto edge = res.GetValue();
//     ASSERT_EQ(edge.EdgeType(), et);
//     ASSERT_EQ(edge.FromVertex(), vertex_from);
//     ASSERT_EQ(edge.ToVertex(), vertex_to);

//     gid_from = vertex_from.Gid();
//     gid_to = vertex_to.Gid();

//     // Check edges
//     ASSERT_EQ(vertex_from.InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from.InDegree(View::NEW), 0);
//     {
//       auto ret = vertex_from.OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from.OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), vertex_from);
//       ASSERT_EQ(e.ToVertex(), vertex_to);
//     }
//     {
//       auto ret = vertex_to.InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to.InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), vertex_from);
//       ASSERT_EQ(e.ToVertex(), vertex_to);
//     }
//     ASSERT_EQ(vertex_to.OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to.OutDegree(View::NEW), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Detach delete vertex, but abort the transaction
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.FindVertex(gid_from, View::NEW);
//     auto vertex_to = acc.FindVertex(gid_to, View::NEW);
//     ASSERT_TRUE(vertex_from);
//     ASSERT_TRUE(vertex_to);

//     auto et = acc.NameToEdgeType("et5");

//     // Delete must fail
//     {
//       auto ret = acc.DeleteVertex(&from_id);
//       ASSERT_TRUE(ret.HasError());
//       ASSERT_EQ(ret.GetError(), memgraph::storage::Error::VERTEX_HAS_EDGES);
//     }

//     // Detach delete vertex
//     {
//       auto ret = acc.DetachDeleteVertex(&from_id);
//       ASSERT_TRUE(ret.HasValue());
//       ASSERT_TRUE(*ret);
//     }

//     // Check edges
//     ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->InEdges(View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
//     ASSERT_EQ(vertex_from->InDegree(View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
//     {
//       auto ret = vertex_from->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
//     ASSERT_EQ(vertex_from->OutDegree(View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
//     {
//       auto ret = vertex_to->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->InDegree(View::NEW), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

//     acc.Abort();
//   }

//   // Check dataset
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.FindVertex(gid_from, View::NEW);
//     auto vertex_to = acc.FindVertex(gid_to, View::NEW);
//     ASSERT_TRUE(vertex_from);
//     ASSERT_TRUE(vertex_to);

//     auto et = acc.NameToEdgeType("et5");

//     // Check edges
//     ASSERT_EQ(vertex_from->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::NEW), 0);
//     {
//       auto ret = vertex_from->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     {
//       auto ret = vertex_to->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Detach delete vertex
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.FindVertex(gid_from, View::NEW);
//     auto vertex_to = acc.FindVertex(gid_to, View::NEW);
//     ASSERT_TRUE(vertex_from);
//     ASSERT_TRUE(vertex_to);

//     auto et = acc.NameToEdgeType("et5");

//     // Delete must fail
//     {
//       auto ret = acc.DeleteVertex(&from_id);
//       ASSERT_TRUE(ret.HasError());
//       ASSERT_EQ(ret.GetError(), memgraph::storage::Error::VERTEX_HAS_EDGES);
//     }

//     // Detach delete vertex
//     {
//       auto ret = acc.DetachDeleteVertex(&from_id);
//       ASSERT_TRUE(ret.HasValue());
//       ASSERT_TRUE(*ret);
//     }

//     // Check edges
//     ASSERT_EQ(vertex_from->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_from->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_from->InEdges(View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
//     ASSERT_EQ(vertex_from->InDegree(View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
//     {
//       auto ret = vertex_from->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_from->OutDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_from->OutEdges(View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
//     ASSERT_EQ(vertex_from->OutDegree(View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
//     {
//       auto ret = vertex_to->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex_to->InDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et);
//       ASSERT_EQ(e.FromVertex(), *vertex_from);
//       ASSERT_EQ(e.ToVertex(), *vertex_to);
//     }
//     ASSERT_EQ(vertex_to->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->InDegree(View::NEW), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Check dataset
//   {
//     auto acc = store.Access();
//     auto vertex_from = acc.FindVertex(gid_from, View::NEW);
//     auto vertex_to = acc.FindVertex(gid_to, View::NEW);
//     ASSERT_FALSE(vertex_from);
//     ASSERT_TRUE(vertex_to);

//     // Check edges
//     ASSERT_EQ(vertex_to->InEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->InDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->InEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->InDegree(View::NEW), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::OLD)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::OLD), 0);
//     ASSERT_EQ(vertex_to->OutEdges(View::NEW)->size(), 0);
//     ASSERT_EQ(*vertex_to->OutDegree(View::NEW), 0);
//   }
// }

// // NOLINTNEXTLINE(hicpp-special-member-functions)
// TEST_P(StorageEdgeTest, VertexDetachDeleteMultipleAbort) {
//   memgraph::storage::Storage store({.items = {.properties_on_edges = GetParam()}});
//   memgraph::storage::Gid gid_vertex1 = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
//   memgraph::storage::Gid gid_vertex2 = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

//   // Create dataset
//   {
//     auto acc = store.Access();
//     auto vertex1 = acc.CreateVertex();
//     auto vertex2 = acc.CreateVertex();

//     gid_vertex1 = vertex1.Gid();
//     gid_vertex2 = vertex2.Gid();

//     auto et1 = acc.NameToEdgeType("et1");
//     auto et2 = acc.NameToEdgeType("et2");
//     auto et3 = acc.NameToEdgeType("et3");
//     auto et4 = acc.NameToEdgeType("et4");

//     auto res1 = acc.CreateEdge(&vertex1, &vertex2, et1);
//     ASSERT_TRUE(res1.HasValue());
//     auto edge1 = res1.GetValue();
//     ASSERT_EQ(edge1.EdgeType(), et1);
//     ASSERT_EQ(edge1.FromVertex(), vertex1);
//     ASSERT_EQ(edge1.ToVertex(), vertex2);

//     auto res2 = acc.CreateEdge(&vertex2, &vertex1, et2);
//     ASSERT_TRUE(res2.HasValue());
//     auto edge2 = res2.GetValue();
//     ASSERT_EQ(edge2.EdgeType(), et2);
//     ASSERT_EQ(edge2.FromVertex(), vertex2);
//     ASSERT_EQ(edge2.ToVertex(), vertex1);

//     auto res3 = acc.CreateEdge(&vertex1, &vertex1, et3);
//     ASSERT_TRUE(res3.HasValue());
//     auto edge3 = res3.GetValue();
//     ASSERT_EQ(edge3.EdgeType(), et3);
//     ASSERT_EQ(edge3.FromVertex(), vertex1);
//     ASSERT_EQ(edge3.ToVertex(), vertex1);

//     auto res4 = acc.CreateEdge(&vertex2, &vertex2, et4);
//     ASSERT_TRUE(res4.HasValue());
//     auto edge4 = res4.GetValue();
//     ASSERT_EQ(edge4.EdgeType(), et4);
//     ASSERT_EQ(edge4.FromVertex(), vertex2);
//     ASSERT_EQ(edge4.ToVertex(), vertex2);

//     // Check edges
//     {
//       auto ret = vertex1.InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex1.InDegree(View::NEW), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et2);
//         ASSERT_EQ(e.FromVertex(), vertex2);
//         ASSERT_EQ(e.ToVertex(), vertex1);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et3);
//         ASSERT_EQ(e.FromVertex(), vertex1);
//         ASSERT_EQ(e.ToVertex(), vertex1);
//       }
//     }
//     {
//       auto ret = vertex1.OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex1.OutDegree(View::NEW), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et1);
//         ASSERT_EQ(e.FromVertex(), vertex1);
//         ASSERT_EQ(e.ToVertex(), vertex2);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et3);
//         ASSERT_EQ(e.FromVertex(), vertex1);
//         ASSERT_EQ(e.ToVertex(), vertex1);
//       }
//     }
//     {
//       auto ret = vertex2.InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex2.InDegree(View::NEW), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et1);
//         ASSERT_EQ(e.FromVertex(), vertex1);
//         ASSERT_EQ(e.ToVertex(), vertex2);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et4);
//         ASSERT_EQ(e.FromVertex(), vertex2);
//         ASSERT_EQ(e.ToVertex(), vertex2);
//       }
//     }
//     {
//       auto ret = vertex2.OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex2.OutDegree(View::NEW), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et2);
//         ASSERT_EQ(e.FromVertex(), vertex2);
//         ASSERT_EQ(e.ToVertex(), vertex1);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et4);
//         ASSERT_EQ(e.FromVertex(), vertex2);
//         ASSERT_EQ(e.ToVertex(), vertex2);
//       }
//     }

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Detach delete vertex, but abort the transaction
//   {
//     auto acc = store.Access();
//     auto vertex1 = acc.FindVertex(gid_vertex1, View::NEW);
//     auto vertex2 = acc.FindVertex(gid_vertex2, View::NEW);
//     ASSERT_TRUE(vertex1);
//     ASSERT_TRUE(vertex2);

//     auto et1 = acc.NameToEdgeType("et1");
//     auto et2 = acc.NameToEdgeType("et2");
//     auto et3 = acc.NameToEdgeType("et3");
//     auto et4 = acc.NameToEdgeType("et4");

//     // Delete must fail
//     {
//       auto ret = acc.DeleteVertex(&*vertex1);
//       ASSERT_TRUE(ret.HasError());
//       ASSERT_EQ(ret.GetError(), memgraph::storage::Error::VERTEX_HAS_EDGES);
//     }

//     // Detach delete vertex
//     {
//       auto ret = acc.DetachDeleteVertex(&*vertex1);
//       ASSERT_TRUE(ret.HasValue());
//       ASSERT_TRUE(*ret);
//     }

//     // Check edges
//     {
//       auto ret = vertex1->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex1->InDegree(View::OLD), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et2);
//         ASSERT_EQ(e.FromVertex(), *vertex2);
//         ASSERT_EQ(e.ToVertex(), *vertex1);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et3);
//         ASSERT_EQ(e.FromVertex(), *vertex1);
//         ASSERT_EQ(e.ToVertex(), *vertex1);
//       }
//     }
//     ASSERT_EQ(vertex1->InEdges(View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
//     ASSERT_EQ(vertex1->InDegree(View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
//     {
//       auto ret = vertex1->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex1->OutDegree(View::OLD), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et1);
//         ASSERT_EQ(e.FromVertex(), *vertex1);
//         ASSERT_EQ(e.ToVertex(), *vertex2);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et3);
//         ASSERT_EQ(e.FromVertex(), *vertex1);
//         ASSERT_EQ(e.ToVertex(), *vertex1);
//       }
//     }
//     ASSERT_EQ(vertex1->OutEdges(View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
//     ASSERT_EQ(vertex1->OutDegree(View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
//     {
//       auto ret = vertex2->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex2->InDegree(View::OLD), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et1);
//         ASSERT_EQ(e.FromVertex(), *vertex1);
//         ASSERT_EQ(e.ToVertex(), *vertex2);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et4);
//         ASSERT_EQ(e.FromVertex(), *vertex2);
//         ASSERT_EQ(e.ToVertex(), *vertex2);
//       }
//     }
//     {
//       auto ret = vertex2->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex2->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et4);
//       ASSERT_EQ(e.FromVertex(), *vertex2);
//       ASSERT_EQ(e.ToVertex(), *vertex2);
//     }
//     {
//       auto ret = vertex2->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex2->OutDegree(View::OLD), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et2);
//         ASSERT_EQ(e.FromVertex(), *vertex2);
//         ASSERT_EQ(e.ToVertex(), *vertex1);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et4);
//         ASSERT_EQ(e.FromVertex(), *vertex2);
//         ASSERT_EQ(e.ToVertex(), *vertex2);
//       }
//     }
//     {
//       auto ret = vertex2->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex2->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et4);
//       ASSERT_EQ(e.FromVertex(), *vertex2);
//       ASSERT_EQ(e.ToVertex(), *vertex2);
//     }

//     acc.Abort();
//   }

//   // Check dataset
//   {
//     auto acc = store.Access();
//     auto vertex1 = acc.FindVertex(gid_vertex1, View::NEW);
//     auto vertex2 = acc.FindVertex(gid_vertex2, View::NEW);
//     ASSERT_TRUE(vertex1);
//     ASSERT_TRUE(vertex2);

//     auto et1 = acc.NameToEdgeType("et1");
//     auto et2 = acc.NameToEdgeType("et2");
//     auto et3 = acc.NameToEdgeType("et3");
//     auto et4 = acc.NameToEdgeType("et4");

//     // Check edges
//     {
//       auto ret = vertex1->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex1->InDegree(View::OLD), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et2);
//         ASSERT_EQ(e.FromVertex(), *vertex2);
//         ASSERT_EQ(e.ToVertex(), *vertex1);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et3);
//         ASSERT_EQ(e.FromVertex(), *vertex1);
//         ASSERT_EQ(e.ToVertex(), *vertex1);
//       }
//     }
//     {
//       auto ret = vertex1->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex1->InDegree(View::NEW), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et2);
//         ASSERT_EQ(e.FromVertex(), *vertex2);
//         ASSERT_EQ(e.ToVertex(), *vertex1);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et3);
//         ASSERT_EQ(e.FromVertex(), *vertex1);
//         ASSERT_EQ(e.ToVertex(), *vertex1);
//       }
//     }
//     {
//       auto ret = vertex1->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex1->OutDegree(View::OLD), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et1);
//         ASSERT_EQ(e.FromVertex(), *vertex1);
//         ASSERT_EQ(e.ToVertex(), *vertex2);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et3);
//         ASSERT_EQ(e.FromVertex(), *vertex1);
//         ASSERT_EQ(e.ToVertex(), *vertex1);
//       }
//     }
//     {
//       auto ret = vertex1->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex1->OutDegree(View::NEW), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et1);
//         ASSERT_EQ(e.FromVertex(), *vertex1);
//         ASSERT_EQ(e.ToVertex(), *vertex2);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et3);
//         ASSERT_EQ(e.FromVertex(), *vertex1);
//         ASSERT_EQ(e.ToVertex(), *vertex1);
//       }
//     }
//     {
//       auto ret = vertex2->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex2->InDegree(View::OLD), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et1);
//         ASSERT_EQ(e.FromVertex(), *vertex1);
//         ASSERT_EQ(e.ToVertex(), *vertex2);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et4);
//         ASSERT_EQ(e.FromVertex(), *vertex2);
//         ASSERT_EQ(e.ToVertex(), *vertex2);
//       }
//     }
//     {
//       auto ret = vertex2->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex2->InDegree(View::NEW), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et1);
//         ASSERT_EQ(e.FromVertex(), *vertex1);
//         ASSERT_EQ(e.ToVertex(), *vertex2);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et4);
//         ASSERT_EQ(e.FromVertex(), *vertex2);
//         ASSERT_EQ(e.ToVertex(), *vertex2);
//       }
//     }
//     {
//       auto ret = vertex2->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex2->OutDegree(View::OLD), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et2);
//         ASSERT_EQ(e.FromVertex(), *vertex2);
//         ASSERT_EQ(e.ToVertex(), *vertex1);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et4);
//         ASSERT_EQ(e.FromVertex(), *vertex2);
//         ASSERT_EQ(e.ToVertex(), *vertex2);
//       }
//     }
//     {
//       auto ret = vertex2->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex2->OutDegree(View::NEW), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et2);
//         ASSERT_EQ(e.FromVertex(), *vertex2);
//         ASSERT_EQ(e.ToVertex(), *vertex1);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et4);
//         ASSERT_EQ(e.FromVertex(), *vertex2);
//         ASSERT_EQ(e.ToVertex(), *vertex2);
//       }
//     }

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Detach delete vertex
//   {
//     auto acc = store.Access();
//     auto vertex1 = acc.FindVertex(gid_vertex1, View::NEW);
//     auto vertex2 = acc.FindVertex(gid_vertex2, View::NEW);
//     ASSERT_TRUE(vertex1);
//     ASSERT_TRUE(vertex2);

//     auto et1 = acc.NameToEdgeType("et1");
//     auto et2 = acc.NameToEdgeType("et2");
//     auto et3 = acc.NameToEdgeType("et3");
//     auto et4 = acc.NameToEdgeType("et4");

//     // Delete must fail
//     {
//       auto ret = acc.DeleteVertex(&*vertex1);
//       ASSERT_TRUE(ret.HasError());
//       ASSERT_EQ(ret.GetError(), memgraph::storage::Error::VERTEX_HAS_EDGES);
//     }

//     // Detach delete vertex
//     {
//       auto ret = acc.DetachDeleteVertex(&*vertex1);
//       ASSERT_TRUE(ret.HasValue());
//       ASSERT_TRUE(*ret);
//     }

//     // Check edges
//     {
//       auto ret = vertex1->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex1->InDegree(View::OLD), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et2);
//         ASSERT_EQ(e.FromVertex(), *vertex2);
//         ASSERT_EQ(e.ToVertex(), *vertex1);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et3);
//         ASSERT_EQ(e.FromVertex(), *vertex1);
//         ASSERT_EQ(e.ToVertex(), *vertex1);
//       }
//     }
//     ASSERT_EQ(vertex1->InEdges(View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
//     ASSERT_EQ(vertex1->InDegree(View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
//     {
//       auto ret = vertex1->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex1->OutDegree(View::OLD), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et1);
//         ASSERT_EQ(e.FromVertex(), *vertex1);
//         ASSERT_EQ(e.ToVertex(), *vertex2);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et3);
//         ASSERT_EQ(e.FromVertex(), *vertex1);
//         ASSERT_EQ(e.ToVertex(), *vertex1);
//       }
//     }
//     ASSERT_EQ(vertex1->OutEdges(View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
//     ASSERT_EQ(vertex1->OutDegree(View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
//     {
//       auto ret = vertex2->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex2->InDegree(View::OLD), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et1);
//         ASSERT_EQ(e.FromVertex(), *vertex1);
//         ASSERT_EQ(e.ToVertex(), *vertex2);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et4);
//         ASSERT_EQ(e.FromVertex(), *vertex2);
//         ASSERT_EQ(e.ToVertex(), *vertex2);
//       }
//     }
//     {
//       auto ret = vertex2->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex2->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et4);
//       ASSERT_EQ(e.FromVertex(), *vertex2);
//       ASSERT_EQ(e.ToVertex(), *vertex2);
//     }
//     {
//       auto ret = vertex2->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType();
//       }); ASSERT_EQ(edges.size(), 2); ASSERT_EQ(*vertex2->OutDegree(View::OLD), 2);
//       {
//         auto e = edges[0];
//         ASSERT_EQ(e.EdgeType(), et2);
//         ASSERT_EQ(e.FromVertex(), *vertex2);
//         ASSERT_EQ(e.ToVertex(), *vertex1);
//       }
//       {
//         auto e = edges[1];
//         ASSERT_EQ(e.EdgeType(), et4);
//         ASSERT_EQ(e.FromVertex(), *vertex2);
//         ASSERT_EQ(e.ToVertex(), *vertex2);
//       }
//     }
//     {
//       auto ret = vertex2->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex2->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et4);
//       ASSERT_EQ(e.FromVertex(), *vertex2);
//       ASSERT_EQ(e.ToVertex(), *vertex2);
//     }

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Check dataset
//   {
//     auto acc = store.Access();
//     auto vertex1 = acc.FindVertex(gid_vertex1, View::NEW);
//     auto vertex2 = acc.FindVertex(gid_vertex2, View::NEW);
//     ASSERT_FALSE(vertex1);
//     ASSERT_TRUE(vertex2);

//     auto et4 = acc.NameToEdgeType("et4");

//     // Check edges
//     {
//       auto ret = vertex2->InEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex2->InDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et4);
//       ASSERT_EQ(e.FromVertex(), *vertex2);
//       ASSERT_EQ(e.ToVertex(), *vertex2);
//     }
//     {
//       auto ret = vertex2->InEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex2->InDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et4);
//       ASSERT_EQ(e.FromVertex(), *vertex2);
//       ASSERT_EQ(e.ToVertex(), *vertex2);
//     }
//     {
//       auto ret = vertex2->OutEdges(View::OLD);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex2->OutDegree(View::OLD), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et4);
//       ASSERT_EQ(e.FromVertex(), *vertex2);
//       ASSERT_EQ(e.ToVertex(), *vertex2);
//     }
//     {
//       auto ret = vertex2->OutEdges(View::NEW);
//       ASSERT_TRUE(ret.HasValue());
//       auto edges = ret.GetValue();
//       ASSERT_EQ(edges.size(), 1);
//       ASSERT_EQ(*vertex2->OutDegree(View::NEW), 1);
//       auto e = edges[0];
//       ASSERT_EQ(e.EdgeType(), et4);
//       ASSERT_EQ(e.FromVertex(), *vertex2);
//       ASSERT_EQ(e.ToVertex(), *vertex2);
//     }
//   }
// }

// // NOLINTNEXTLINE(hicpp-special-member-functions)
// TEST(StorageWithProperties, EdgePropertyCommit) {
//   memgraph::storage::Storage store({.items = {.properties_on_edges = true}});
//   memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
//   {
//     auto acc = store.Access();
//     auto vertex = acc.CreateVertex();
//     gid = vertex.Gid();
//     auto et = acc.NameToEdgeType("et5");
//     auto edge = acc.CreateEdge(&vertex, &vertex, et).GetValue();
//     ASSERT_EQ(edge.EdgeType(), et);
//     ASSERT_EQ(edge.FromVertex(), vertex);
//     ASSERT_EQ(edge.ToVertex(), vertex);

//     auto property = acc.NameToProperty("property5");

//     ASSERT_TRUE(edge.GetProperty(property, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::NEW)->size(), 0);

//     {
//       auto old_value = edge.SetProperty(property, memgraph::storage::PropertyValue("temporary"));
//       ASSERT_TRUE(old_value.HasValue());
//       ASSERT_TRUE(old_value->IsNull());
//     }

//     ASSERT_EQ(edge.GetProperty(property, View::NEW)->ValueString(), "temporary");
//     {
//       auto properties = edge.Properties(View::NEW).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property].ValueString(), "temporary");
//     }

//     {
//       auto old_value = edge.SetProperty(property, memgraph::storage::PropertyValue("nandare"));
//       ASSERT_TRUE(old_value.HasValue());
//       ASSERT_FALSE(old_value->IsNull());
//     }

//     ASSERT_EQ(edge.GetProperty(property, View::NEW)->ValueString(), "nandare");
//     {
//       auto properties = edge.Properties(View::NEW).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property].ValueString(), "nandare");
//     }

//     ASSERT_FALSE(acc.Commit().HasError());
//   }
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     auto property = acc.NameToProperty("property5");

//     ASSERT_EQ(edge.GetProperty(property, View::OLD)->ValueString(), "nandare");
//     {
//       auto properties = edge.Properties(View::OLD).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property].ValueString(), "nandare");
//     }

//     ASSERT_EQ(edge.GetProperty(property, View::NEW)->ValueString(), "nandare");
//     {
//       auto properties = edge.Properties(View::NEW).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property].ValueString(), "nandare");
//     }

//     auto other_property = acc.NameToProperty("other");

//     ASSERT_TRUE(edge.GetProperty(other_property, View::OLD)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(other_property, View::NEW)->IsNull());

//     acc.Abort();
//   }
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     auto property = acc.NameToProperty("property5");

//     {
//       auto old_value = edge.SetProperty(property, memgraph::storage::PropertyValue());
//       ASSERT_TRUE(old_value.HasValue());
//       ASSERT_FALSE(old_value->IsNull());
//     }

//     ASSERT_EQ(edge.GetProperty(property, View::OLD)->ValueString(), "nandare");
//     {
//       auto properties = edge.Properties(View::OLD).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property].ValueString(), "nandare");
//     }

//     ASSERT_TRUE(edge.GetProperty(property, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::NEW)->size(), 0);

//     {
//       auto old_value = edge.SetProperty(property, memgraph::storage::PropertyValue());
//       ASSERT_TRUE(old_value.HasValue());
//       ASSERT_TRUE(old_value->IsNull());
//     }

//     ASSERT_FALSE(acc.Commit().HasError());
//   }
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     auto property = acc.NameToProperty("property5");

//     ASSERT_TRUE(edge.GetProperty(property, View::OLD)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(property, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::OLD)->size(), 0);
//     ASSERT_EQ(edge.Properties(View::NEW)->size(), 0);

//     auto other_property = acc.NameToProperty("other");

//     ASSERT_TRUE(edge.GetProperty(other_property, View::OLD)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(other_property, View::NEW)->IsNull());

//     acc.Abort();
//   }
// }

// // NOLINTNEXTLINE(hicpp-special-member-functions)
// TEST(StorageWithProperties, EdgePropertyAbort) {
//   memgraph::storage::Storage store({.items = {.properties_on_edges = true}});
//   memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

//   // Create the vertex.
//   {
//     auto acc = store.Access();
//     auto vertex = acc.CreateVertex();
//     gid = vertex.Gid();
//     auto et = acc.NameToEdgeType("et5");
//     auto edge = acc.CreateEdge(&vertex, &vertex, et).GetValue();
//     ASSERT_EQ(edge.EdgeType(), et);
//     ASSERT_EQ(edge.FromVertex(), vertex);
//     ASSERT_EQ(edge.ToVertex(), vertex);
//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Set property 5 to "nandare", but abort the transaction.
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     auto property = acc.NameToProperty("property5");

//     ASSERT_TRUE(edge.GetProperty(property, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::NEW)->size(), 0);

//     {
//       auto old_value = edge.SetProperty(property, memgraph::storage::PropertyValue("temporary"));
//       ASSERT_TRUE(old_value.HasValue());
//       ASSERT_TRUE(old_value->IsNull());
//     }

//     ASSERT_EQ(edge.GetProperty(property, View::NEW)->ValueString(), "temporary");
//     {
//       auto properties = edge.Properties(View::NEW).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property].ValueString(), "temporary");
//     }

//     {
//       auto old_value = edge.SetProperty(property, memgraph::storage::PropertyValue("nandare"));
//       ASSERT_TRUE(old_value.HasValue());
//       ASSERT_FALSE(old_value->IsNull());
//     }

//     ASSERT_EQ(edge.GetProperty(property, View::NEW)->ValueString(), "nandare");
//     {
//       auto properties = edge.Properties(View::NEW).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property].ValueString(), "nandare");
//     }

//     acc.Abort();
//   }

//   // Check that property 5 is null.
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     auto property = acc.NameToProperty("property5");

//     ASSERT_TRUE(edge.GetProperty(property, View::OLD)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(property, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::OLD)->size(), 0);
//     ASSERT_EQ(edge.Properties(View::NEW)->size(), 0);

//     auto other_property = acc.NameToProperty("other");

//     ASSERT_TRUE(edge.GetProperty(other_property, View::OLD)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(other_property, View::NEW)->IsNull());

//     acc.Abort();
//   }

//   // Set property 5 to "nandare".
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     auto property = acc.NameToProperty("property5");

//     ASSERT_TRUE(edge.GetProperty(property, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::NEW)->size(), 0);

//     {
//       auto old_value = edge.SetProperty(property, memgraph::storage::PropertyValue("temporary"));
//       ASSERT_TRUE(old_value.HasValue());
//       ASSERT_TRUE(old_value->IsNull());
//     }

//     ASSERT_EQ(edge.GetProperty(property, View::NEW)->ValueString(), "temporary");
//     {
//       auto properties = edge.Properties(View::NEW).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property].ValueString(), "temporary");
//     }

//     {
//       auto old_value = edge.SetProperty(property, memgraph::storage::PropertyValue("nandare"));
//       ASSERT_TRUE(old_value.HasValue());
//       ASSERT_FALSE(old_value->IsNull());
//     }

//     ASSERT_EQ(edge.GetProperty(property, View::NEW)->ValueString(), "nandare");
//     {
//       auto properties = edge.Properties(View::NEW).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property].ValueString(), "nandare");
//     }

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Check that property 5 is "nandare".
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     auto property = acc.NameToProperty("property5");

//     ASSERT_EQ(edge.GetProperty(property, View::OLD)->ValueString(), "nandare");
//     {
//       auto properties = edge.Properties(View::OLD).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property].ValueString(), "nandare");
//     }

//     ASSERT_EQ(edge.GetProperty(property, View::NEW)->ValueString(), "nandare");
//     {
//       auto properties = edge.Properties(View::NEW).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property].ValueString(), "nandare");
//     }

//     auto other_property = acc.NameToProperty("other");

//     ASSERT_TRUE(edge.GetProperty(other_property, View::OLD)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(other_property, View::NEW)->IsNull());

//     acc.Abort();
//   }

//   // Set property 5 to null, but abort the transaction.
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     auto property = acc.NameToProperty("property5");

//     ASSERT_EQ(edge.GetProperty(property, View::OLD)->ValueString(), "nandare");
//     {
//       auto properties = edge.Properties(View::OLD).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property].ValueString(), "nandare");
//     }

//     ASSERT_EQ(edge.GetProperty(property, View::NEW)->ValueString(), "nandare");
//     {
//       auto properties = edge.Properties(View::NEW).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property].ValueString(), "nandare");
//     }

//     {
//       auto old_value = edge.SetProperty(property, memgraph::storage::PropertyValue());
//       ASSERT_TRUE(old_value.HasValue());
//       ASSERT_FALSE(old_value->IsNull());
//     }

//     ASSERT_EQ(edge.GetProperty(property, View::OLD)->ValueString(), "nandare");
//     {
//       auto properties = edge.Properties(View::OLD).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property].ValueString(), "nandare");
//     }

//     ASSERT_TRUE(edge.GetProperty(property, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::NEW)->size(), 0);

//     acc.Abort();
//   }

//   // Check that property 5 is "nandare".
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     auto property = acc.NameToProperty("property5");

//     ASSERT_EQ(edge.GetProperty(property, View::OLD)->ValueString(), "nandare");
//     {
//       auto properties = edge.Properties(View::OLD).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property].ValueString(), "nandare");
//     }

//     ASSERT_EQ(edge.GetProperty(property, View::NEW)->ValueString(), "nandare");
//     {
//       auto properties = edge.Properties(View::NEW).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property].ValueString(), "nandare");
//     }

//     auto other_property = acc.NameToProperty("other");

//     ASSERT_TRUE(edge.GetProperty(other_property, View::OLD)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(other_property, View::NEW)->IsNull());

//     acc.Abort();
//   }

//   // Set property 5 to null.
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     auto property = acc.NameToProperty("property5");

//     ASSERT_EQ(edge.GetProperty(property, View::OLD)->ValueString(), "nandare");
//     {
//       auto properties = edge.Properties(View::OLD).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property].ValueString(), "nandare");
//     }

//     ASSERT_EQ(edge.GetProperty(property, View::NEW)->ValueString(), "nandare");
//     {
//       auto properties = edge.Properties(View::NEW).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property].ValueString(), "nandare");
//     }

//     {
//       auto old_value = edge.SetProperty(property, memgraph::storage::PropertyValue());
//       ASSERT_TRUE(old_value.HasValue());
//       ASSERT_FALSE(old_value->IsNull());
//     }

//     ASSERT_EQ(edge.GetProperty(property, View::OLD)->ValueString(), "nandare");
//     {
//       auto properties = edge.Properties(View::OLD).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property].ValueString(), "nandare");
//     }

//     ASSERT_TRUE(edge.GetProperty(property, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::NEW)->size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   // Check that property 5 is null.
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     auto property = acc.NameToProperty("property5");

//     ASSERT_TRUE(edge.GetProperty(property, View::OLD)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(property, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::OLD)->size(), 0);
//     ASSERT_EQ(edge.Properties(View::NEW)->size(), 0);

//     auto other_property = acc.NameToProperty("other");

//     ASSERT_TRUE(edge.GetProperty(other_property, View::OLD)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(other_property, View::NEW)->IsNull());

//     acc.Abort();
//   }
// }

// // NOLINTNEXTLINE(hicpp-special-member-functions)
// TEST(StorageWithProperties, EdgePropertySerializationError) {
//   memgraph::storage::Storage store({.items = {.properties_on_edges = true}});
//   memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
//   {
//     auto acc = store.Access();
//     auto vertex = acc.CreateVertex();
//     gid = vertex.Gid();
//     auto et = acc.NameToEdgeType("et5");
//     auto edge = acc.CreateEdge(&vertex, &vertex, et).GetValue();
//     ASSERT_EQ(edge.EdgeType(), et);
//     ASSERT_EQ(edge.FromVertex(), vertex);
//     ASSERT_EQ(edge.ToVertex(), vertex);
//     ASSERT_FALSE(acc.Commit().HasError());
//   }

//   auto acc1 = store.Access();
//   auto acc2 = store.Access();

//   // Set property 1 to 123 in accessor 1.
//   {
//     auto vertex = acc1.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     auto property1 = acc1.NameToProperty("property1");
//     auto property2 = acc1.NameToProperty("property2");

//     ASSERT_TRUE(edge.GetProperty(property1, View::OLD)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(property1, View::NEW)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(property2, View::OLD)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(property2, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::OLD)->size(), 0);
//     ASSERT_EQ(edge.Properties(View::NEW)->size(), 0);

//     {
//       auto old_value = edge.SetProperty(property1, memgraph::storage::PropertyValue(123));
//       ASSERT_TRUE(old_value.HasValue());
//       ASSERT_TRUE(old_value->IsNull());
//     }

//     ASSERT_TRUE(edge.GetProperty(property1, View::OLD)->IsNull());
//     ASSERT_EQ(edge.GetProperty(property1, View::NEW)->ValueInt(), 123);
//     ASSERT_TRUE(edge.GetProperty(property2, View::OLD)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(property2, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::OLD)->size(), 0);
//     {
//       auto properties = edge.Properties(View::NEW).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property1].ValueInt(), 123);
//     }
//   }

//   // Set property 2 to "nandare" in accessor 2.
//   {
//     auto vertex = acc2.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     auto property1 = acc2.NameToProperty("property1");
//     auto property2 = acc2.NameToProperty("property2");

//     ASSERT_TRUE(edge.GetProperty(property1, View::OLD)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(property1, View::NEW)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(property2, View::OLD)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(property2, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::OLD)->size(), 0);
//     ASSERT_EQ(edge.Properties(View::NEW)->size(), 0);

//     {
//       auto res = edge.SetProperty(property2, memgraph::storage::PropertyValue("nandare"));
//       ASSERT_TRUE(res.HasError());
//       ASSERT_EQ(res.GetError(), memgraph::storage::Error::SERIALIZATION_ERROR);
//     }
//   }

//   // Finalize both accessors.
//   ASSERT_FALSE(acc1.Commit().HasError());
//   acc2.Abort();

//   // Check which properties exist.
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     auto property1 = acc.NameToProperty("property1");
//     auto property2 = acc.NameToProperty("property2");

//     ASSERT_EQ(edge.GetProperty(property1, View::OLD)->ValueInt(), 123);
//     ASSERT_TRUE(edge.GetProperty(property2, View::OLD)->IsNull());
//     {
//       auto properties = edge.Properties(View::OLD).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property1].ValueInt(), 123);
//     }

//     ASSERT_EQ(edge.GetProperty(property1, View::NEW)->ValueInt(), 123);
//     ASSERT_TRUE(edge.GetProperty(property2, View::NEW)->IsNull());
//     {
//       auto properties = edge.Properties(View::NEW).GetValue();
//       ASSERT_EQ(properties.size(), 1);
//       ASSERT_EQ(properties[property1].ValueInt(), 123);
//     }

//     acc.Abort();
//   }
// }

// TEST(StorageWithProperties, EdgePropertyClear) {
//   memgraph::storage::Storage store({.items = {.properties_on_edges = true}});
//   memgraph::storage::Gid gid;
//   auto property1 = store.NameToProperty("property1");
//   auto property2 = store.NameToProperty("property2");
//   {
//     auto acc = store.Access();
//     auto vertex = acc.CreateVertex();
//     gid = vertex.Gid();
//     auto et = acc.NameToEdgeType("et5");
//     auto edge = acc.CreateEdge(&vertex, &vertex, et).GetValue();
//     ASSERT_EQ(edge.EdgeType(), et);
//     ASSERT_EQ(edge.FromVertex(), vertex);
//     ASSERT_EQ(edge.ToVertex(), vertex);

//     auto old_value = edge.SetProperty(property1, memgraph::storage::PropertyValue("value"));
//     ASSERT_TRUE(old_value.HasValue());
//     ASSERT_TRUE(old_value->IsNull());

//     ASSERT_FALSE(acc.Commit().HasError());
//   }
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     ASSERT_EQ(edge.GetProperty(property1, View::OLD)->ValueString(), "value");
//     ASSERT_TRUE(edge.GetProperty(property2, View::OLD)->IsNull());
//     ASSERT_THAT(edge.Properties(View::OLD).GetValue(),
//                 UnorderedElementsAre(std::pair(property1, memgraph::storage::PropertyValue("value"))));

//     {
//       auto old_values = edge.ClearProperties();
//       ASSERT_TRUE(old_values.HasValue());
//       ASSERT_FALSE(old_values->empty());
//     }

//     ASSERT_TRUE(edge.GetProperty(property1, View::NEW)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(property2, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::NEW).GetValue().size(), 0);

//     {
//       auto old_values = edge.ClearProperties();
//       ASSERT_TRUE(old_values.HasValue());
//       ASSERT_TRUE(old_values->empty());
//     }

//     ASSERT_TRUE(edge.GetProperty(property1, View::NEW)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(property2, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::NEW).GetValue().size(), 0);

//     acc.Abort();
//   }
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     auto old_value = edge.SetProperty(property2, memgraph::storage::PropertyValue(42));
//     ASSERT_TRUE(old_value.HasValue());
//     ASSERT_TRUE(old_value->IsNull());

//     ASSERT_FALSE(acc.Commit().HasError());
//   }
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     ASSERT_EQ(edge.GetProperty(property1, View::OLD)->ValueString(), "value");
//     ASSERT_EQ(edge.GetProperty(property2, View::OLD)->ValueInt(), 42);
//     ASSERT_THAT(edge.Properties(View::OLD).GetValue(),
//                 UnorderedElementsAre(std::pair(property1, memgraph::storage::PropertyValue("value")),
//                                      std::pair(property2, memgraph::storage::PropertyValue(42))));

//     {
//       auto old_values = edge.ClearProperties();
//       ASSERT_TRUE(old_values.HasValue());
//       ASSERT_FALSE(old_values->empty());
//     }

//     ASSERT_TRUE(edge.GetProperty(property1, View::NEW)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(property2, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::NEW).GetValue().size(), 0);

//     {
//       auto old_values = edge.ClearProperties();
//       ASSERT_TRUE(old_values.HasValue());
//       ASSERT_TRUE(old_values->empty());
//     }

//     ASSERT_TRUE(edge.GetProperty(property1, View::NEW)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(property2, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::NEW).GetValue().size(), 0);

//     ASSERT_FALSE(acc.Commit().HasError());
//   }
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     ASSERT_TRUE(edge.GetProperty(property1, View::NEW)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(property2, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::NEW).GetValue().size(), 0);

//     acc.Abort();
//   }
// }

// // NOLINTNEXTLINE(hicpp-special-member-functions)
// TEST(StorageWithoutProperties, EdgePropertyAbort) {
//   memgraph::storage::Storage store({.items = {.properties_on_edges = false}});
//   memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
//   {
//     auto acc = store.Access();
//     auto vertex = acc.CreateVertex();
//     gid = vertex.Gid();
//     auto et = acc.NameToEdgeType("et5");
//     auto edge = acc.CreateEdge(&vertex, &vertex, et).GetValue();
//     ASSERT_EQ(edge.EdgeType(), et);
//     ASSERT_EQ(edge.FromVertex(), vertex);
//     ASSERT_EQ(edge.ToVertex(), vertex);
//     ASSERT_FALSE(acc.Commit().HasError());
//   }
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     auto property = acc.NameToProperty("property5");

//     ASSERT_TRUE(edge.GetProperty(property, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::NEW)->size(), 0);

//     {
//       auto res = edge.SetProperty(property, memgraph::storage::PropertyValue("temporary"));
//       ASSERT_TRUE(res.HasError());
//       ASSERT_EQ(res.GetError(), memgraph::storage::Error::PROPERTIES_DISABLED);
//     }

//     ASSERT_TRUE(edge.GetProperty(property, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::NEW)->size(), 0);

//     {
//       auto res = edge.SetProperty(property, memgraph::storage::PropertyValue("nandare"));
//       ASSERT_TRUE(res.HasError());
//       ASSERT_EQ(res.GetError(), memgraph::storage::Error::PROPERTIES_DISABLED);
//     }

//     ASSERT_TRUE(edge.GetProperty(property, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::NEW)->size(), 0);

//     acc.Abort();
//   }
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     auto property = acc.NameToProperty("property5");

//     ASSERT_TRUE(edge.GetProperty(property, View::OLD)->IsNull());
//     ASSERT_EQ(edge.Properties(View::OLD)->size(), 0);

//     ASSERT_TRUE(edge.GetProperty(property, View::NEW)->IsNull());
//     ASSERT_EQ(edge.Properties(View::NEW)->size(), 0);

//     auto other_property = acc.NameToProperty("other");

//     ASSERT_TRUE(edge.GetProperty(other_property, View::OLD)->IsNull());
//     ASSERT_TRUE(edge.GetProperty(other_property, View::NEW)->IsNull());

//     acc.Abort();
//   }
// }

// TEST(StorageWithoutProperties, EdgePropertyClear) {
//   memgraph::storage::Storage store({.items = {.properties_on_edges = false}});
//   memgraph::storage::Gid gid;
//   {
//     auto acc = store.Access();
//     auto vertex = acc.CreateVertex();
//     gid = vertex.Gid();
//     auto et = acc.NameToEdgeType("et5");
//     auto edge = acc.CreateEdge(&vertex, &vertex, et).GetValue();
//     ASSERT_EQ(edge.EdgeType(), et);
//     ASSERT_EQ(edge.FromVertex(), vertex);
//     ASSERT_EQ(edge.ToVertex(), vertex);
//     ASSERT_FALSE(acc.Commit().HasError());
//   }
//   {
//     auto acc = store.Access();
//     auto vertex = acc.FindVertex(gid, View::OLD);
//     ASSERT_TRUE(vertex);
//     auto edge = vertex->OutEdges(View::NEW).GetValue()[0];

//     ASSERT_EQ(edge.ClearProperties().GetError(), memgraph::storage::Error::PROPERTIES_DISABLED);

//     acc.Abort();
//   }
// }

// TEST(StorageWithProperties, EdgeNonexistentPropertyAPI) {
//   memgraph::storage::Storage store({.items = {.properties_on_edges = true}});

//   auto property = store.NameToProperty("property");

//   auto acc = store.Access();
//   auto vertex = acc.CreateVertex();
//   auto edge = acc.CreateEdge(&vertex, &vertex, acc.NameToEdgeType("edge"));
//   ASSERT_TRUE(edge.HasValue());

//   // Check state before (OLD view).
//   ASSERT_EQ(edge->Properties(View::OLD).GetError(), memgraph::storage::Error::NONEXISTENT_OBJECT);
//   ASSERT_EQ(edge->GetProperty(property, View::OLD).GetError(), memgraph::storage::Error::NONEXISTENT_OBJECT);

//   // Check state before (NEW view).
//   ASSERT_EQ(edge->Properties(View::NEW)->size(), 0);
//   ASSERT_EQ(*edge->GetProperty(property, View::NEW), memgraph::storage::PropertyValue());

//   // Modify edge.
//   ASSERT_TRUE(edge->SetProperty(property, memgraph::storage::PropertyValue("value"))->IsNull());

//   // Check state after (OLD view).
//   ASSERT_EQ(edge->Properties(View::OLD).GetError(), memgraph::storage::Error::NONEXISTENT_OBJECT);
//   ASSERT_EQ(edge->GetProperty(property, View::OLD).GetError(), memgraph::storage::Error::NONEXISTENT_OBJECT);

//   // Check state after (NEW view).
//   ASSERT_EQ(edge->Properties(View::NEW)->size(), 1);
//   ASSERT_EQ(*edge->GetProperty(property, View::NEW), memgraph::storage::PropertyValue("value"));

//   ASSERT_FALSE(acc.Commit().HasError());
// }
}  // namespace memgraph::storage::v3
