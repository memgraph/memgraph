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

#include <memory>
#include <unordered_set>

#include "disk_test_utils.hpp"
#include "gtest/gtest.h"

#include "query/db_accessor.hpp"
#include "query/graph.hpp"
#include "query/virtual_edge.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/memory.hpp"

class VirtualEdgeTest : public ::testing::Test {
 protected:
  const std::string testSuite = "query_virtual_edge";
  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  std::unique_ptr<memgraph::storage::Storage> db{new memgraph::storage::InMemoryStorage(config)};
};

TEST_F(VirtualEdgeTest, AccessorsAndIdentity) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  auto sv2 = acc->CreateVertex();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  auto v1 = memgraph::query::VertexAccessor(sv1);
  auto v2 = memgraph::query::VertexAccessor(sv2);

  memgraph::query::VirtualEdge ve1(v1, v2, "RELATES_TO");
  memgraph::query::VirtualEdge ve2(v1, v2, "RELATES_TO");

  EXPECT_EQ(ve1.From(), v1);
  EXPECT_EQ(ve1.To(), v2);
  EXPECT_EQ(ve1.EdgeTypeName(), "RELATES_TO");

  // Each virtual edge gets a unique Gid; equality is Gid-based
  EXPECT_NE(ve1.Gid(), ve2.Gid());
  EXPECT_NE(ve1, ve2);
  EXPECT_EQ(ve1, ve1);
  EXPECT_GT(ve1.Gid(), ve2.Gid());  // Gids count down

  // Works in unordered containers
  std::unordered_set<memgraph::query::VirtualEdge> set;
  set.insert(ve1);
  set.insert(ve2);
  set.insert(ve1);  // duplicate
  EXPECT_EQ(set.size(), 2);
}

TEST_F(VirtualEdgeTest, GraphStoresVirtualEdgesSeparately) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  auto sv2 = acc->CreateVertex();
  auto edge_type = acc->NameToEdgeType("KNOWS");
  auto se = acc->CreateEdge(&sv1, &sv2, edge_type);
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  auto v1 = memgraph::query::VertexAccessor(sv1);
  auto v2 = memgraph::query::VertexAccessor(sv2);

  memgraph::query::Graph graph(memgraph::utils::NewDeleteResource());
  graph.InsertVertex(v1);
  graph.InsertVertex(v2);
  graph.InsertEdge(memgraph::query::EdgeAccessor(*se));

  memgraph::query::VirtualEdge ve(v1, v2, "VIRTUAL");
  graph.virtual_edge_store().Insert(ve);
  graph.virtual_edge_store().Insert(ve);  // duplicate is a no-op

  EXPECT_EQ(graph.edges().size(), 1);
  EXPECT_EQ(graph.virtual_edge_store().size(), 1);
  EXPECT_TRUE(graph.virtual_edge_store().Contains(ve));
}

TEST_F(VirtualEdgeTest, SubgraphVertexAccessorFiltersVirtualEdges) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  auto sv2 = acc->CreateVertex();
  auto sv3 = acc->CreateVertex();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  auto v1 = memgraph::query::VertexAccessor(sv1);
  auto v2 = memgraph::query::VertexAccessor(sv2);
  auto v3 = memgraph::query::VertexAccessor(sv3);

  memgraph::query::Graph graph(memgraph::utils::NewDeleteResource());
  graph.InsertVertex(v1);
  graph.InsertVertex(v2);
  graph.InsertVertex(v3);

  // v1->v2, v1->v3, v2->v3
  graph.virtual_edge_store().Insert(memgraph::query::VirtualEdge(v1, v2, "A"));
  graph.virtual_edge_store().Insert(memgraph::query::VirtualEdge(v1, v3, "B"));
  graph.virtual_edge_store().Insert(memgraph::query::VirtualEdge(v2, v3, "C"));

  // v1 has 2 out, 0 in
  memgraph::query::SubgraphVertexAccessor sva1(v1, &graph);
  EXPECT_EQ(sva1.VirtualOutEdges().size(), 2);
  EXPECT_EQ(sva1.VirtualInEdges().size(), 0);

  // v3 has 0 out, 2 in
  memgraph::query::SubgraphVertexAccessor sva3(v3, &graph);
  EXPECT_EQ(sva3.VirtualOutEdges().size(), 0);
  EXPECT_EQ(sva3.VirtualInEdges().size(), 2);
}

TEST_F(VirtualEdgeTest, SelfLoopAppearsInBothDirections) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  auto v1 = memgraph::query::VertexAccessor(sv1);

  memgraph::query::Graph graph(memgraph::utils::NewDeleteResource());
  graph.InsertVertex(v1);
  graph.virtual_edge_store().Insert(memgraph::query::VirtualEdge(v1, v1, "SELF"));

  memgraph::query::SubgraphVertexAccessor sva(v1, &graph);
  EXPECT_EQ(sva.VirtualOutEdges().size(), 1);
  EXPECT_EQ(sva.VirtualInEdges().size(), 1);
}
