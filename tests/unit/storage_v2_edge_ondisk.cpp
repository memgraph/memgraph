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

#include "disk_test_utils.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/storage.hpp"

using testing::UnorderedElementsAre;

class StorageEdgeTest : public ::testing::TestWithParam<bool> {};

INSTANTIATE_TEST_CASE_P(EdgesWithProperties, StorageEdgeTest, ::testing::Values(true));
INSTANTIATE_TEST_CASE_P(EdgesWithoutProperties, StorageEdgeTest, ::testing::Values(false));

const std::string testSuite = "storage_v2_edge_ondisk";

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeCreateFromSmallerCommit) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = GetParam();
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid_from = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  memgraph::storage::Gid gid_to = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store->Access();
    auto vertex_from = acc->CreateVertex();
    auto vertex_to = acc->CreateVertex();
    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Create edge
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    auto res = acc->CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et}, &*vertex_to)->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {other_et}, &*vertex_to)->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et}, &*vertex_to)->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {et, other_et}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {et, other_et}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {other_et}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {et, other_et}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {et, other_et}, &*vertex_to)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeCreateFromLargerCommit) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = GetParam();
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid_from = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  memgraph::storage::Gid gid_to = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store->Access();
    auto vertex_to = acc->CreateVertex();
    auto vertex_from = acc->CreateVertex();
    gid_to = vertex_to.Gid();
    gid_from = vertex_from.Gid();
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Create edge
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    auto res = acc->CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeCreateFromSameCommit) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = GetParam();
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid_vertex = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex
  {
    auto acc = store->Access();
    auto vertex = acc->CreateVertex();
    gid_vertex = vertex.Gid();
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Create edge
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid_vertex, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto et = acc->NameToEdgeType("et5");

    auto res = acc->CreateEdge(&*vertex, &*vertex, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex);
    ASSERT_EQ(edge.ToVertex(), *vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {other_et}, &*vertex)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid_vertex, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto et = acc->NameToEdgeType("et5");

    // Check edges without filters
    {
      auto ret = vertex->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {other_et}, &*vertex)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeCreateFromSmallerAbort) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = GetParam();
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid_from = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  memgraph::storage::Gid gid_to = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store->Access();
    auto vertex_from = acc->CreateVertex();
    auto vertex_to = acc->CreateVertex();
    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Create edge, but abort the transaction
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    auto res = acc->CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 0);

    acc->Abort();
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Create edge
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    auto res = acc->CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeCreateFromLargerAbort) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = GetParam();
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid_from = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  memgraph::storage::Gid gid_to = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store->Access();
    auto vertex_to = acc->CreateVertex();
    auto vertex_from = acc->CreateVertex();
    gid_to = vertex_to.Gid();
    gid_from = vertex_from.Gid();
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Create edge, but abort the transaction
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    auto res = acc->CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 0);

    acc->Abort();
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Create edge
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    auto res = acc->CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeCreateFromSameAbort) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = GetParam();
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid_vertex = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex
  {
    auto acc = store->Access();
    auto vertex = acc->CreateVertex();
    gid_vertex = vertex.Gid();
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Create edge, but abort the transaction
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid_vertex, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto et = acc->NameToEdgeType("et5");

    auto res = acc->CreateEdge(&*vertex, &*vertex, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex);
    ASSERT_EQ(edge.ToVertex(), *vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {other_et}, &*vertex)->edges.size(), 0);

    acc->Abort();
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid_vertex, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::NEW), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Create edge
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid_vertex, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto et = acc->NameToEdgeType("et5");

    auto res = acc->CreateEdge(&*vertex, &*vertex, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex);
    ASSERT_EQ(edge.ToVertex(), *vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {other_et}, &*vertex)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid_vertex, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto et = acc->NameToEdgeType("et5");

    // Check edges without filters
    {
      auto ret = vertex->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {other_et}, &*vertex)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeDeleteFromSmallerCommit) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = GetParam();
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid_from = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  memgraph::storage::Gid gid_to = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store->Access();
    auto vertex_from = acc->CreateVertex();
    auto vertex_to = acc->CreateVertex();
    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Create edge
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    auto res = acc->CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Delete edge
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    auto edge = vertex_from->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto res = acc->DeleteEdge(&edge);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeDeleteFromLargerCommit) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = GetParam();
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid_from = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  memgraph::storage::Gid gid_to = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store->Access();
    auto vertex_to = acc->CreateVertex();
    auto vertex_from = acc->CreateVertex();
    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Create edge
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    auto res = acc->CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Delete edge
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    auto edge = vertex_from->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto res = acc->DeleteEdge(&edge);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeDeleteFromSameCommit) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = GetParam();
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid_vertex = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex
  {
    auto acc = store->Access();
    auto vertex = acc->CreateVertex();
    gid_vertex = vertex.Gid();
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Create edge
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid_vertex, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto et = acc->NameToEdgeType("et5");

    auto res = acc->CreateEdge(&*vertex, &*vertex, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex);
    ASSERT_EQ(edge.ToVertex(), *vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {other_et}, &*vertex)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid_vertex, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto et = acc->NameToEdgeType("et5");

    // Check edges without filters
    {
      auto ret = vertex->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {other_et}, &*vertex)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Delete edge
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid_vertex, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto et = acc->NameToEdgeType("et5");

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto res = acc->DeleteEdge(&edge);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters
    {
      auto ret = vertex->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {other_et}, &*vertex)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid_vertex, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::NEW), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeDeleteFromSmallerAbort) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = GetParam();
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid_from = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  memgraph::storage::Gid gid_to = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store->Access();
    auto vertex_from = acc->CreateVertex();
    auto vertex_to = acc->CreateVertex();
    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Create edge
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    auto res = acc->CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Delete the edge, but abort the transaction
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    auto edge = vertex_from->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto res = acc->DeleteEdge(&edge);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 0);

    acc->Abort();
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Delete the edge
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    auto edge = vertex_from->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto res = acc->DeleteEdge(&edge);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeDeleteFromLargerAbort) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = GetParam();
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid_from = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  memgraph::storage::Gid gid_to = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertices
  {
    auto acc = store->Access();
    auto vertex_from = acc->CreateVertex();
    auto vertex_to = acc->CreateVertex();
    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Create edge
  {
    auto acc = store->Access();
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    auto res = acc->CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex_from);
    ASSERT_EQ(edge.ToVertex(), *vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Delete the edge, but abort the transaction
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    auto edge = vertex_from->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto res = acc->DeleteEdge(&edge);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 0);

    acc->Abort();
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW, {}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Delete the edge
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    auto edge = vertex_from->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto res = acc->DeleteEdge(&edge);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_from)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD, {}, &*vertex_to)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    // Check edges without filters
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, EdgeDeleteFromSameAbort) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = GetParam();
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid_vertex = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create vertex
  {
    auto acc = store->Access();
    auto vertex = acc->CreateVertex();
    gid_vertex = vertex.Gid();
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Create edge
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid_vertex, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto et = acc->NameToEdgeType("et5");

    auto res = acc->CreateEdge(&*vertex, &*vertex, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), *vertex);
    ASSERT_EQ(edge.ToVertex(), *vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::OLD), 0);
    {
      auto ret = vertex->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {other_et}, &*vertex)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid_vertex, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto et = acc->NameToEdgeType("et5");

    // Check edges without filters
    {
      auto ret = vertex->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {other_et}, &*vertex)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Delete the edge, but abort the transaction
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid_vertex, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto et = acc->NameToEdgeType("et5");

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto res = acc->DeleteEdge(&edge);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters
    {
      auto ret = vertex->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {other_et}, &*vertex)->edges.size(), 0);

    acc->Abort();
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid_vertex, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto et = acc->NameToEdgeType("et5");

    // Check edges without filters
    {
      auto ret = vertex->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    {
      auto ret = vertex->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW, {other_et}, &*vertex)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Delete the edge
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid_vertex, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto et = acc->NameToEdgeType("et5");

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto res = acc->DeleteEdge(&edge);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());

    // Check edges without filters
    {
      auto ret = vertex->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex);
      ASSERT_EQ(e.ToVertex(), *vertex);
    }
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::NEW), 0);

    auto other_et = acc->NameToEdgeType("other");

    // Check edges with filters
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD, {other_et}, &*vertex)->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {other_et})->edges.size(), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {et, other_et})->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {}, &*vertex)->edges.size(), 1);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD, {other_et}, &*vertex)->edges.size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check whether the edge exists
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid_vertex, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    // Check edges without filters
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex->OutDegree(memgraph::storage::View::NEW), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, VertexDetachDeleteSingleCommit) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = GetParam();
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid_from = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  memgraph::storage::Gid gid_to = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create dataset
  {
    auto acc = store->Access();
    auto vertex_from = acc->CreateVertex();
    auto vertex_to = acc->CreateVertex();

    auto et = acc->NameToEdgeType("et5");

    auto res = acc->CreateEdge(&vertex_from, &vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), vertex_from);
    ASSERT_EQ(edge.ToVertex(), vertex_to);

    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();

    // Check edges
    ASSERT_EQ(vertex_from.InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from.InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_from.OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from.OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), vertex_from);
      ASSERT_EQ(e.ToVertex(), vertex_to);
    }
    {
      auto ret = vertex_to.InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to.InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), vertex_from);
      ASSERT_EQ(e.ToVertex(), vertex_to);
    }
    ASSERT_EQ(vertex_to.OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to.OutDegree(memgraph::storage::View::NEW), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Detach delete vertex
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    // Delete must fail
    {
      auto ret = acc->DeleteVertex(&*vertex_from);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), memgraph::storage::Error::VERTEX_HAS_EDGES);
    }

    // Detach delete vertex
    {
      auto ret = acc->DetachDeleteVertex(&*vertex_from);
      ASSERT_TRUE(ret.HasValue());
      ASSERT_TRUE(*ret);
    }

    // Check edges
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex_from->InDegree(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex_from->OutDegree(memgraph::storage::View::NEW).GetError(),
              memgraph::storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check dataset
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_FALSE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    // Check edges
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, VertexDetachDeleteMultipleCommit) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = GetParam();
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid_vertex1 = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  memgraph::storage::Gid gid_vertex2 = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create dataset
  {
    auto acc = store->Access();
    auto vertex1 = acc->CreateVertex();
    auto vertex2 = acc->CreateVertex();

    gid_vertex1 = vertex1.Gid();
    gid_vertex2 = vertex2.Gid();

    auto et1 = acc->NameToEdgeType("et1");
    auto et2 = acc->NameToEdgeType("et2");
    auto et3 = acc->NameToEdgeType("et3");
    auto et4 = acc->NameToEdgeType("et4");

    auto res1 = acc->CreateEdge(&vertex1, &vertex2, et1);
    ASSERT_TRUE(res1.HasValue());
    auto edge1 = res1.GetValue();
    ASSERT_EQ(edge1.EdgeType(), et1);
    ASSERT_EQ(edge1.FromVertex(), vertex1);
    ASSERT_EQ(edge1.ToVertex(), vertex2);

    auto res2 = acc->CreateEdge(&vertex2, &vertex1, et2);
    ASSERT_TRUE(res2.HasValue());
    auto edge2 = res2.GetValue();
    ASSERT_EQ(edge2.EdgeType(), et2);
    ASSERT_EQ(edge2.FromVertex(), vertex2);
    ASSERT_EQ(edge2.ToVertex(), vertex1);

    auto res3 = acc->CreateEdge(&vertex1, &vertex1, et3);
    ASSERT_TRUE(res3.HasValue());
    auto edge3 = res3.GetValue();
    ASSERT_EQ(edge3.EdgeType(), et3);
    ASSERT_EQ(edge3.FromVertex(), vertex1);
    ASSERT_EQ(edge3.ToVertex(), vertex1);

    auto res4 = acc->CreateEdge(&vertex2, &vertex2, et4);
    ASSERT_TRUE(res4.HasValue());
    auto edge4 = res4.GetValue();
    ASSERT_EQ(edge4.EdgeType(), et4);
    ASSERT_EQ(edge4.FromVertex(), vertex2);
    ASSERT_EQ(edge4.ToVertex(), vertex2);

    // Check edges
    {
      auto ret = vertex1.InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1.InDegree(memgraph::storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
    }
    {
      auto ret = vertex1.OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1.OutDegree(memgraph::storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
    }
    {
      auto ret = vertex2.InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2.InDegree(memgraph::storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
    }
    {
      auto ret = vertex2.OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2.OutDegree(memgraph::storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
    }

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Detach delete vertex
  {
    auto acc = store->Access();
    auto vertex1 = acc->FindVertex(gid_vertex1, memgraph::storage::View::NEW);
    auto vertex2 = acc->FindVertex(gid_vertex2, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex1);
    ASSERT_TRUE(vertex2);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex1);
    acc->PrefetchInEdges(*vertex1);
    acc->PrefetchOutEdges(*vertex2);
    acc->PrefetchInEdges(*vertex2);

    auto et1 = acc->NameToEdgeType("et1");
    auto et2 = acc->NameToEdgeType("et2");
    auto et3 = acc->NameToEdgeType("et3");
    auto et4 = acc->NameToEdgeType("et4");

    // Delete must fail
    {
      auto ret = acc->DeleteVertex(&*vertex1);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), memgraph::storage::Error::VERTEX_HAS_EDGES);
    }

    // Detach delete vertex
    {
      auto ret = acc->DetachDeleteVertex(&*vertex1);
      ASSERT_TRUE(ret.HasValue());
      ASSERT_TRUE(*ret);
    }

    // Check edges
    {
      auto ret = vertex1->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1->InDegree(memgraph::storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    ASSERT_EQ(vertex1->InEdges(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex1->InDegree(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex1->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1->OutDegree(memgraph::storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    ASSERT_EQ(vertex1->OutEdges(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex1->OutDegree(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex2->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2->InDegree(memgraph::storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2->OutDegree(memgraph::storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check dataset
  {
    auto acc = store->Access();
    auto vertex1 = acc->FindVertex(gid_vertex1, memgraph::storage::View::NEW);
    auto vertex2 = acc->FindVertex(gid_vertex2, memgraph::storage::View::NEW);
    ASSERT_FALSE(vertex1);
    ASSERT_TRUE(vertex2);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex2);
    acc->PrefetchInEdges(*vertex2);

    auto et4 = acc->NameToEdgeType("et4");

    // Check edges
    {
      auto ret = vertex2->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, VertexDetachDeleteSingleAbort) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = GetParam();
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid_from = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  memgraph::storage::Gid gid_to = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create dataset
  {
    auto acc = store->Access();
    auto vertex_from = acc->CreateVertex();
    auto vertex_to = acc->CreateVertex();

    auto et = acc->NameToEdgeType("et5");

    auto res = acc->CreateEdge(&vertex_from, &vertex_to, et);
    ASSERT_TRUE(res.HasValue());
    auto edge = res.GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), vertex_from);
    ASSERT_EQ(edge.ToVertex(), vertex_to);

    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();

    // Check edges
    ASSERT_EQ(vertex_from.InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from.InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_from.OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from.OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), vertex_from);
      ASSERT_EQ(e.ToVertex(), vertex_to);
    }
    {
      auto ret = vertex_to.InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to.InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), vertex_from);
      ASSERT_EQ(e.ToVertex(), vertex_to);
    }
    ASSERT_EQ(vertex_to.OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to.OutDegree(memgraph::storage::View::NEW), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Detach delete vertex, but abort the transaction
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    // Delete must fail
    {
      auto ret = acc->DeleteVertex(&*vertex_from);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), memgraph::storage::Error::VERTEX_HAS_EDGES);
    }

    // Detach delete vertex
    {
      auto ret = acc->DetachDeleteVertex(&*vertex_from);
      ASSERT_TRUE(ret.HasValue());
      ASSERT_TRUE(*ret);
    }

    // Check edges
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex_from->InDegree(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex_from->OutDegree(memgraph::storage::View::NEW).GetError(),
              memgraph::storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    acc->Abort();
  }

  // Check dataset
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    // Check edges
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::NEW), 0);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Detach delete vertex
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    auto et = acc->NameToEdgeType("et5");

    // Delete must fail
    {
      auto ret = acc->DeleteVertex(&*vertex_from);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), memgraph::storage::Error::VERTEX_HAS_EDGES);
    }

    // Detach delete vertex
    {
      auto ret = acc->DetachDeleteVertex(&*vertex_from);
      ASSERT_TRUE(ret.HasValue());
      ASSERT_TRUE(*ret);
    }

    // Check edges
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_from->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_from->InEdges(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex_from->InDegree(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex_from->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_from->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_from->OutEdges(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex_from->OutDegree(memgraph::storage::View::NEW).GetError(),
              memgraph::storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex_to->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et);
      ASSERT_EQ(e.FromVertex(), *vertex_from);
      ASSERT_EQ(e.ToVertex(), *vertex_to);
    }
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check dataset
  {
    auto acc = store->Access();
    auto vertex_from = acc->FindVertex(gid_from, memgraph::storage::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, memgraph::storage::View::NEW);
    ASSERT_FALSE(vertex_from);
    ASSERT_TRUE(vertex_to);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex_from);
    acc->PrefetchInEdges(*vertex_from);
    acc->PrefetchOutEdges(*vertex_to);
    acc->PrefetchInEdges(*vertex_to);

    // Check edges
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->InEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->InDegree(memgraph::storage::View::NEW), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::OLD)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::OLD), 0);
    ASSERT_EQ(vertex_to->OutEdges(memgraph::storage::View::NEW)->edges.size(), 0);
    ASSERT_EQ(*vertex_to->OutDegree(memgraph::storage::View::NEW), 0);
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageEdgeTest, VertexDetachDeleteMultipleAbort) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = GetParam();
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid_vertex1 = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  memgraph::storage::Gid gid_vertex2 = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create dataset
  {
    auto acc = store->Access();
    auto vertex1 = acc->CreateVertex();
    auto vertex2 = acc->CreateVertex();

    gid_vertex1 = vertex1.Gid();
    gid_vertex2 = vertex2.Gid();

    auto et1 = acc->NameToEdgeType("et1");
    auto et2 = acc->NameToEdgeType("et2");
    auto et3 = acc->NameToEdgeType("et3");
    auto et4 = acc->NameToEdgeType("et4");

    auto res1 = acc->CreateEdge(&vertex1, &vertex2, et1);
    ASSERT_TRUE(res1.HasValue());
    auto edge1 = res1.GetValue();
    ASSERT_EQ(edge1.EdgeType(), et1);
    ASSERT_EQ(edge1.FromVertex(), vertex1);
    ASSERT_EQ(edge1.ToVertex(), vertex2);

    auto res2 = acc->CreateEdge(&vertex2, &vertex1, et2);
    ASSERT_TRUE(res2.HasValue());
    auto edge2 = res2.GetValue();
    ASSERT_EQ(edge2.EdgeType(), et2);
    ASSERT_EQ(edge2.FromVertex(), vertex2);
    ASSERT_EQ(edge2.ToVertex(), vertex1);

    auto res3 = acc->CreateEdge(&vertex1, &vertex1, et3);
    ASSERT_TRUE(res3.HasValue());
    auto edge3 = res3.GetValue();
    ASSERT_EQ(edge3.EdgeType(), et3);
    ASSERT_EQ(edge3.FromVertex(), vertex1);
    ASSERT_EQ(edge3.ToVertex(), vertex1);

    auto res4 = acc->CreateEdge(&vertex2, &vertex2, et4);
    ASSERT_TRUE(res4.HasValue());
    auto edge4 = res4.GetValue();
    ASSERT_EQ(edge4.EdgeType(), et4);
    ASSERT_EQ(edge4.FromVertex(), vertex2);
    ASSERT_EQ(edge4.ToVertex(), vertex2);

    // Check edges
    {
      auto ret = vertex1.InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1.InDegree(memgraph::storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
    }
    {
      auto ret = vertex1.OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1.OutDegree(memgraph::storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
    }
    {
      auto ret = vertex2.InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2.InDegree(memgraph::storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), vertex1);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
    }
    {
      auto ret = vertex2.OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2.OutDegree(memgraph::storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), vertex2);
        ASSERT_EQ(e.ToVertex(), vertex2);
      }
    }

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Detach delete vertex, but abort the transaction
  {
    auto acc = store->Access();
    auto vertex1 = acc->FindVertex(gid_vertex1, memgraph::storage::View::NEW);
    auto vertex2 = acc->FindVertex(gid_vertex2, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex1);
    ASSERT_TRUE(vertex2);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex1);
    acc->PrefetchInEdges(*vertex1);
    acc->PrefetchOutEdges(*vertex2);
    acc->PrefetchInEdges(*vertex2);

    auto et1 = acc->NameToEdgeType("et1");
    auto et2 = acc->NameToEdgeType("et2");
    auto et3 = acc->NameToEdgeType("et3");
    auto et4 = acc->NameToEdgeType("et4");

    // Delete must fail
    {
      auto ret = acc->DeleteVertex(&*vertex1);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), memgraph::storage::Error::VERTEX_HAS_EDGES);
    }

    // Detach delete vertex
    {
      auto ret = acc->DetachDeleteVertex(&*vertex1);
      ASSERT_TRUE(ret.HasValue());
      ASSERT_TRUE(*ret);
    }

    // Check edges
    {
      auto ret = vertex1->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1->InDegree(memgraph::storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    ASSERT_EQ(vertex1->InEdges(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex1->InDegree(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex1->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1->OutDegree(memgraph::storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    ASSERT_EQ(vertex1->OutEdges(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex1->OutDegree(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex2->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2->InDegree(memgraph::storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2->OutDegree(memgraph::storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }

    acc->Abort();
  }

  // Check dataset
  {
    auto acc = store->Access();
    auto vertex1 = acc->FindVertex(gid_vertex1, memgraph::storage::View::NEW);
    auto vertex2 = acc->FindVertex(gid_vertex2, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex1);
    ASSERT_TRUE(vertex2);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex1);
    acc->PrefetchInEdges(*vertex1);
    acc->PrefetchOutEdges(*vertex2);
    acc->PrefetchInEdges(*vertex2);

    auto et1 = acc->NameToEdgeType("et1");
    auto et2 = acc->NameToEdgeType("et2");
    auto et3 = acc->NameToEdgeType("et3");
    auto et4 = acc->NameToEdgeType("et4");

    // Check edges
    {
      auto ret = vertex1->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1->InDegree(memgraph::storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    {
      auto ret = vertex1->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1->InDegree(memgraph::storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    {
      auto ret = vertex1->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1->OutDegree(memgraph::storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    {
      auto ret = vertex1->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1->OutDegree(memgraph::storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    {
      auto ret = vertex2->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2->InDegree(memgraph::storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2->InDegree(memgraph::storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2->OutDegree(memgraph::storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2->OutDegree(memgraph::storage::View::NEW), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Detach delete vertex
  {
    auto acc = store->Access();
    auto vertex1 = acc->FindVertex(gid_vertex1, memgraph::storage::View::NEW);
    auto vertex2 = acc->FindVertex(gid_vertex2, memgraph::storage::View::NEW);
    ASSERT_TRUE(vertex1);
    ASSERT_TRUE(vertex2);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex1);
    acc->PrefetchInEdges(*vertex1);
    acc->PrefetchOutEdges(*vertex2);
    acc->PrefetchInEdges(*vertex2);

    auto et1 = acc->NameToEdgeType("et1");
    auto et2 = acc->NameToEdgeType("et2");
    auto et3 = acc->NameToEdgeType("et3");
    auto et4 = acc->NameToEdgeType("et4");

    // Delete must fail
    {
      auto ret = acc->DeleteVertex(&*vertex1);
      ASSERT_TRUE(ret.HasError());
      ASSERT_EQ(ret.GetError(), memgraph::storage::Error::VERTEX_HAS_EDGES);
    }

    // Detach delete vertex
    {
      auto ret = acc->DetachDeleteVertex(&*vertex1);
      ASSERT_TRUE(ret.HasValue());
      ASSERT_TRUE(*ret);
    }

    // Check edges
    {
      auto ret = vertex1->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1->InDegree(memgraph::storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    ASSERT_EQ(vertex1->InEdges(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex1->InDegree(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex1->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex1->OutDegree(memgraph::storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et3);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
    }
    ASSERT_EQ(vertex1->OutEdges(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    ASSERT_EQ(vertex1->OutDegree(memgraph::storage::View::NEW).GetError(), memgraph::storage::Error::DELETED_OBJECT);
    {
      auto ret = vertex2->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2->InDegree(memgraph::storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et1);
        ASSERT_EQ(e.FromVertex(), *vertex1);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      std::sort(edges.begin(), edges.end(), [](const auto &a, const auto &b) { return a.EdgeType() < b.EdgeType(); });
      ASSERT_EQ(edges.size(), 2);
      ASSERT_EQ(*vertex2->OutDegree(memgraph::storage::View::OLD), 2);
      {
        auto e = edges[0];
        ASSERT_EQ(e.EdgeType(), et2);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex1);
      }
      {
        auto e = edges[1];
        ASSERT_EQ(e.EdgeType(), et4);
        ASSERT_EQ(e.FromVertex(), *vertex2);
        ASSERT_EQ(e.ToVertex(), *vertex2);
      }
    }
    {
      auto ret = vertex2->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check dataset
  {
    auto acc = store->Access();
    auto vertex1 = acc->FindVertex(gid_vertex1, memgraph::storage::View::NEW);
    auto vertex2 = acc->FindVertex(gid_vertex2, memgraph::storage::View::NEW);
    ASSERT_FALSE(vertex1);
    ASSERT_TRUE(vertex2);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex2);
    acc->PrefetchInEdges(*vertex2);

    auto et4 = acc->NameToEdgeType("et4");

    // Check edges
    {
      auto ret = vertex2->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->InDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->InEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->InDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->OutDegree(memgraph::storage::View::OLD), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
    {
      auto ret = vertex2->OutEdges(memgraph::storage::View::NEW);
      ASSERT_TRUE(ret.HasValue());
      auto edges = ret.GetValue().edges;
      ASSERT_EQ(edges.size(), 1);
      ASSERT_EQ(*vertex2->OutDegree(memgraph::storage::View::NEW), 1);
      auto e = edges[0];
      ASSERT_EQ(e.EdgeType(), et4);
      ASSERT_EQ(e.FromVertex(), *vertex2);
      ASSERT_EQ(e.ToVertex(), *vertex2);
    }
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageWithProperties, EdgePropertyCommit) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = true;
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    auto et = acc->NameToEdgeType("et5");
    auto edge = acc->CreateEdge(&vertex, &vertex, et).GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), vertex);
    ASSERT_EQ(edge.ToVertex(), vertex);

    auto property = acc->NameToProperty("property5");

    ASSERT_TRUE(edge.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::NEW)->size(), 0);

    {
      auto old_value = edge.SetProperty(property, memgraph::storage::PropertyValue("temporary"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(edge.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "temporary");
    {
      auto properties = edge.Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = edge.SetProperty(property, memgraph::storage::PropertyValue("nandare"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(edge.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_FALSE(acc->Commit().HasError());
  }
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto property = acc->NameToProperty("property5");

    ASSERT_EQ(edge.GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(edge.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    auto other_property = acc->NameToProperty("other");

    ASSERT_TRUE(edge.GetProperty(other_property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(other_property, memgraph::storage::View::NEW)->IsNull());

    acc->Abort();
  }
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto property = acc->NameToProperty("property5");

    {
      auto old_value = edge.SetProperty(property, memgraph::storage::PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(edge.GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(edge.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::NEW)->size(), 0);

    {
      auto old_value = edge.SetProperty(property, memgraph::storage::PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_FALSE(acc->Commit().HasError());
  }
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto property = acc->NameToProperty("property5");

    ASSERT_TRUE(edge.GetProperty(property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(edge.Properties(memgraph::storage::View::NEW)->size(), 0);

    auto other_property = acc->NameToProperty("other");

    ASSERT_TRUE(edge.GetProperty(other_property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(other_property, memgraph::storage::View::NEW)->IsNull());

    acc->Abort();
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageWithProperties, EdgePropertyAbort) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = true;
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Create the vertex.
  {
    auto acc = store->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    auto et = acc->NameToEdgeType("et5");
    auto edge = acc->CreateEdge(&vertex, &vertex, et).GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), vertex);
    ASSERT_EQ(edge.ToVertex(), vertex);
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Set property 5 to "nandare", but abort the transaction.
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto property = acc->NameToProperty("property5");

    ASSERT_TRUE(edge.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::NEW)->size(), 0);

    {
      auto old_value = edge.SetProperty(property, memgraph::storage::PropertyValue("temporary"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(edge.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "temporary");
    {
      auto properties = edge.Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = edge.SetProperty(property, memgraph::storage::PropertyValue("nandare"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(edge.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    acc->Abort();
  }

  // Check that property 5 is null.
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);
    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto property = acc->NameToProperty("property5");

    ASSERT_TRUE(edge.GetProperty(property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(edge.Properties(memgraph::storage::View::NEW)->size(), 0);

    auto other_property = acc->NameToProperty("other");

    ASSERT_TRUE(edge.GetProperty(other_property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(other_property, memgraph::storage::View::NEW)->IsNull());

    acc->Abort();
  }

  // Set property 5 to "nandare".
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto property = acc->NameToProperty("property5");

    ASSERT_TRUE(edge.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::NEW)->size(), 0);

    {
      auto old_value = edge.SetProperty(property, memgraph::storage::PropertyValue("temporary"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(edge.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "temporary");
    {
      auto properties = edge.Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = edge.SetProperty(property, memgraph::storage::PropertyValue("nandare"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(edge.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check that property 5 is "nandare".
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto property = acc->NameToProperty("property5");

    ASSERT_EQ(edge.GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(edge.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    auto other_property = acc->NameToProperty("other");

    ASSERT_TRUE(edge.GetProperty(other_property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(other_property, memgraph::storage::View::NEW)->IsNull());

    acc->Abort();
  }

  // Set property 5 to null, but abort the transaction.
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto property = acc->NameToProperty("property5");

    ASSERT_EQ(edge.GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(edge.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    {
      auto old_value = edge.SetProperty(property, memgraph::storage::PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(edge.GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(edge.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::NEW)->size(), 0);

    acc->Abort();
  }

  // Check that property 5 is "nandare".
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto property = acc->NameToProperty("property5");

    ASSERT_EQ(edge.GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(edge.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    auto other_property = acc->NameToProperty("other");

    ASSERT_TRUE(edge.GetProperty(other_property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(other_property, memgraph::storage::View::NEW)->IsNull());

    acc->Abort();
  }

  // Set property 5 to null.
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto property = acc->NameToProperty("property5");

    ASSERT_EQ(edge.GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(edge.GetProperty(property, memgraph::storage::View::NEW)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    {
      auto old_value = edge.SetProperty(property, memgraph::storage::PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(edge.GetProperty(property, memgraph::storage::View::OLD)->ValueString(), "nandare");
    {
      auto properties = edge.Properties(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(edge.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::NEW)->size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // Check that property 5 is null.
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto property = acc->NameToProperty("property5");

    ASSERT_TRUE(edge.GetProperty(property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(edge.Properties(memgraph::storage::View::NEW)->size(), 0);

    auto other_property = acc->NameToProperty("other");

    ASSERT_TRUE(edge.GetProperty(other_property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(other_property, memgraph::storage::View::NEW)->IsNull());

    acc->Abort();
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageWithProperties, EdgePropertySerializationError) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = true;
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    auto et = acc->NameToEdgeType("et5");
    auto edge = acc->CreateEdge(&vertex, &vertex, et).GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), vertex);
    ASSERT_EQ(edge.ToVertex(), vertex);
    ASSERT_FALSE(acc->Commit().HasError());
  }

  auto acc1 = store->Access();
  auto acc2 = store->Access();

  // Set property 1 to 123 in accessor 1.
  {
    auto vertex = acc1->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc1->PrefetchOutEdges(*vertex);
    acc1->PrefetchInEdges(*vertex);

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto property1 = acc1->NameToProperty("property1");
    auto property2 = acc1->NameToProperty("property2");

    ASSERT_TRUE(edge.GetProperty(property1, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property1, memgraph::storage::View::NEW)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property2, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(edge.Properties(memgraph::storage::View::NEW)->size(), 0);

    {
      auto old_value = edge.SetProperty(property1, memgraph::storage::PropertyValue(123));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_TRUE(edge.GetProperty(property1, memgraph::storage::View::OLD)->IsNull());
    ASSERT_EQ(edge.GetProperty(property1, memgraph::storage::View::NEW)->ValueInt(), 123);
    ASSERT_TRUE(edge.GetProperty(property2, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::OLD)->size(), 0);
    {
      auto properties = edge.Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property1].ValueInt(), 123);
    }
  }

  // Set property 2 to "nandare" in accessor 2.
  {
    auto vertex = acc2->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc2->PrefetchOutEdges(*vertex);
    acc2->PrefetchInEdges(*vertex);

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto property1 = acc2->NameToProperty("property1");
    auto property2 = acc2->NameToProperty("property2");

    ASSERT_TRUE(edge.GetProperty(property1, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property1, memgraph::storage::View::NEW)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property2, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::OLD)->size(), 0);
    ASSERT_EQ(edge.Properties(memgraph::storage::View::NEW)->size(), 0);

    {
      // Conflict write.
      // DiskStorage has SNAPSHOT isolation level by default, so it will not see this change until commit.
      // DiskStorage has optimistic transactions so it will fail on Commit.
      auto res = edge.SetProperty(property2, memgraph::storage::PropertyValue("nandare"));
      ASSERT_FALSE(res.HasError());
    }
  }

  // Finalize both accessors.
  ASSERT_FALSE(acc1->Commit().HasError());
  auto res = acc2->Commit();
  ASSERT_TRUE(res.HasError());
  ASSERT_EQ(std::get<memgraph::storage::SerializationError>(res.GetError()), memgraph::storage::SerializationError());

  // Check which properties exist.
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto property1 = acc->NameToProperty("property1");
    auto property2 = acc->NameToProperty("property2");

    ASSERT_EQ(edge.GetProperty(property1, memgraph::storage::View::OLD)->ValueInt(), 123);
    ASSERT_TRUE(edge.GetProperty(property2, memgraph::storage::View::OLD)->IsNull());
    {
      auto properties = edge.Properties(memgraph::storage::View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property1].ValueInt(), 123);
    }

    ASSERT_EQ(edge.GetProperty(property1, memgraph::storage::View::NEW)->ValueInt(), 123);
    ASSERT_TRUE(edge.GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    {
      auto properties = edge.Properties(memgraph::storage::View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property1].ValueInt(), 123);
    }

    acc->Abort();
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

TEST(StorageWithProperties, EdgePropertyClear) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = true;
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid;
  auto property1 = store->NameToProperty("property1");
  auto property2 = store->NameToProperty("property2");
  {
    auto acc = store->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    auto et = acc->NameToEdgeType("et5");
    auto edge = acc->CreateEdge(&vertex, &vertex, et).GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), vertex);
    ASSERT_EQ(edge.ToVertex(), vertex);

    auto old_value = edge.SetProperty(property1, memgraph::storage::PropertyValue("value"));
    ASSERT_TRUE(old_value.HasValue());
    ASSERT_TRUE(old_value->IsNull());

    ASSERT_FALSE(acc->Commit().HasError());
  }
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    ASSERT_EQ(edge.GetProperty(property1, memgraph::storage::View::OLD)->ValueString(), "value");
    ASSERT_TRUE(edge.GetProperty(property2, memgraph::storage::View::OLD)->IsNull());
    ASSERT_THAT(edge.Properties(memgraph::storage::View::OLD).GetValue(),
                UnorderedElementsAre(std::pair(property1, memgraph::storage::PropertyValue("value"))));

    {
      auto old_values = edge.ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_FALSE(old_values->empty());
    }

    ASSERT_TRUE(edge.GetProperty(property1, memgraph::storage::View::NEW)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::NEW).GetValue().size(), 0);

    {
      auto old_values = edge.ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_TRUE(old_values->empty());
    }

    ASSERT_TRUE(edge.GetProperty(property1, memgraph::storage::View::NEW)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::NEW).GetValue().size(), 0);

    acc->Abort();
  }
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto old_value = edge.SetProperty(property2, memgraph::storage::PropertyValue(42));
    ASSERT_TRUE(old_value.HasValue());
    ASSERT_TRUE(old_value->IsNull());

    ASSERT_FALSE(acc->Commit().HasError());
  }
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    ASSERT_EQ(edge.GetProperty(property1, memgraph::storage::View::OLD)->ValueString(), "value");
    ASSERT_EQ(edge.GetProperty(property2, memgraph::storage::View::OLD)->ValueInt(), 42);
    ASSERT_THAT(edge.Properties(memgraph::storage::View::OLD).GetValue(),
                UnorderedElementsAre(std::pair(property1, memgraph::storage::PropertyValue("value")),
                                     std::pair(property2, memgraph::storage::PropertyValue(42))));

    {
      auto old_values = edge.ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_FALSE(old_values->empty());
    }

    ASSERT_TRUE(edge.GetProperty(property1, memgraph::storage::View::NEW)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::NEW).GetValue().size(), 0);

    {
      auto old_values = edge.ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_TRUE(old_values->empty());
    }

    ASSERT_TRUE(edge.GetProperty(property1, memgraph::storage::View::NEW)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::NEW).GetValue().size(), 0);

    ASSERT_FALSE(acc->Commit().HasError());
  }
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    ASSERT_TRUE(edge.GetProperty(property1, memgraph::storage::View::NEW)->IsNull());
    ASSERT_TRUE(edge.GetProperty(property2, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::NEW).GetValue().size(), 0);

    acc->Abort();
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageWithoutProperties, EdgePropertyAbort) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = false;
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid = memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    auto et = acc->NameToEdgeType("et5");
    auto edge = acc->CreateEdge(&vertex, &vertex, et).GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), vertex);
    ASSERT_EQ(edge.ToVertex(), vertex);
    ASSERT_FALSE(acc->Commit().HasError());
  }
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto property = acc->NameToProperty("property5");

    ASSERT_TRUE(edge.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::NEW)->size(), 0);

    {
      auto res = edge.SetProperty(property, memgraph::storage::PropertyValue("temporary"));
      ASSERT_TRUE(res.HasError());
      ASSERT_EQ(res.GetError(), memgraph::storage::Error::PROPERTIES_DISABLED);
    }

    ASSERT_TRUE(edge.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::NEW)->size(), 0);

    {
      auto res = edge.SetProperty(property, memgraph::storage::PropertyValue("nandare"));
      ASSERT_TRUE(res.HasError());
      ASSERT_EQ(res.GetError(), memgraph::storage::Error::PROPERTIES_DISABLED);
    }

    ASSERT_TRUE(edge.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::NEW)->size(), 0);

    acc->Abort();
  }
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    auto property = acc->NameToProperty("property5");

    ASSERT_TRUE(edge.GetProperty(property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::OLD)->size(), 0);

    ASSERT_TRUE(edge.GetProperty(property, memgraph::storage::View::NEW)->IsNull());
    ASSERT_EQ(edge.Properties(memgraph::storage::View::NEW)->size(), 0);

    auto other_property = acc->NameToProperty("other");

    ASSERT_TRUE(edge.GetProperty(other_property, memgraph::storage::View::OLD)->IsNull());
    ASSERT_TRUE(edge.GetProperty(other_property, memgraph::storage::View::NEW)->IsNull());

    acc->Abort();
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

TEST(StorageWithoutProperties, EdgePropertyClear) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = false;
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));
  memgraph::storage::Gid gid;
  {
    auto acc = store->Access();
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    auto et = acc->NameToEdgeType("et5");
    auto edge = acc->CreateEdge(&vertex, &vertex, et).GetValue();
    ASSERT_EQ(edge.EdgeType(), et);
    ASSERT_EQ(edge.FromVertex(), vertex);
    ASSERT_EQ(edge.ToVertex(), vertex);
    ASSERT_FALSE(acc->Commit().HasError());
  }
  {
    auto acc = store->Access();
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    // We prefetch edges implicitly when go thorough query Accessor
    acc->PrefetchOutEdges(*vertex);
    acc->PrefetchInEdges(*vertex);

    auto edge = vertex->OutEdges(memgraph::storage::View::NEW).GetValue().edges[0];

    ASSERT_EQ(edge.ClearProperties().GetError(), memgraph::storage::Error::PROPERTIES_DISABLED);

    acc->Abort();
  }
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}

TEST(StorageWithProperties, EdgeNonexistentPropertyAPI) {
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.items.properties_on_edges = true;
  std::unique_ptr<memgraph::storage::Storage> store(new memgraph::storage::DiskStorage(config));

  auto property = store->NameToProperty("property");

  auto acc = store->Access();
  auto vertex = acc->CreateVertex();
  auto edge = acc->CreateEdge(&vertex, &vertex, acc->NameToEdgeType("edge"));
  ASSERT_TRUE(edge.HasValue());

  // Check state before (OLD view).
  ASSERT_EQ(edge->Properties(memgraph::storage::View::OLD).GetError(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(edge->GetProperty(property, memgraph::storage::View::OLD).GetError(),
            memgraph::storage::Error::NONEXISTENT_OBJECT);

  // Check state before (NEW view).
  ASSERT_EQ(edge->Properties(memgraph::storage::View::NEW)->size(), 0);
  ASSERT_EQ(*edge->GetProperty(property, memgraph::storage::View::NEW), memgraph::storage::PropertyValue());

  // Modify edge.
  ASSERT_TRUE(edge->SetProperty(property, memgraph::storage::PropertyValue("value"))->IsNull());

  // Check state after (OLD view).
  ASSERT_EQ(edge->Properties(memgraph::storage::View::OLD).GetError(), memgraph::storage::Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(edge->GetProperty(property, memgraph::storage::View::OLD).GetError(),
            memgraph::storage::Error::NONEXISTENT_OBJECT);

  // Check state after (NEW view).
  ASSERT_EQ(edge->Properties(memgraph::storage::View::NEW)->size(), 1);
  ASSERT_EQ(*edge->GetProperty(property, memgraph::storage::View::NEW), memgraph::storage::PropertyValue("value"));

  ASSERT_FALSE(acc->Commit().HasError());
  disk_test_utils::RemoveRocksDbDirs(testSuite);
}
