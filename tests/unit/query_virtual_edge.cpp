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

#include <algorithm>
#include <memory>
#include <unordered_set>
#include <vector>

#include "disk_test_utils.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "query/db_accessor.hpp"
#include "query/graph.hpp"
#include "query/virtual_edge.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/memory.hpp"

// =============================================================================
// Phase 0: VirtualEdge unit tests
// =============================================================================

class VirtualEdgeTest : public ::testing::Test {
 protected:
  const std::string testSuite = "query_virtual_edge";
  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  std::unique_ptr<memgraph::storage::Storage> db{new memgraph::storage::InMemoryStorage(config)};

  struct VertexPair {
    memgraph::query::VertexAccessor v1;
    memgraph::query::VertexAccessor v2;
  };

  void TearDown() override {}
};

TEST_F(VirtualEdgeTest, ConstructionAndAccessors) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  auto sv2 = acc->CreateVertex();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  auto v1 = memgraph::query::VertexAccessor(sv1);
  auto v2 = memgraph::query::VertexAccessor(sv2);

  memgraph::query::VirtualEdge ve(v1, v2, "RELATES_TO");

  EXPECT_EQ(ve.From(), v1);
  EXPECT_EQ(ve.To(), v2);
  EXPECT_EQ(ve.EdgeTypeName(), "RELATES_TO");
  EXPECT_TRUE(ve.Properties().empty());
}

TEST_F(VirtualEdgeTest, UniqueGids) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  auto sv2 = acc->CreateVertex();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  auto v1 = memgraph::query::VertexAccessor(sv1);
  auto v2 = memgraph::query::VertexAccessor(sv2);

  memgraph::query::VirtualEdge ve1(v1, v2, "TYPE_A");
  memgraph::query::VirtualEdge ve2(v1, v2, "TYPE_A");
  memgraph::query::VirtualEdge ve3(v2, v1, "TYPE_B");

  EXPECT_NE(ve1.Gid(), ve2.Gid());
  EXPECT_NE(ve1.Gid(), ve3.Gid());
  EXPECT_NE(ve2.Gid(), ve3.Gid());
}

TEST_F(VirtualEdgeTest, Equality) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  auto sv2 = acc->CreateVertex();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  auto v1 = memgraph::query::VertexAccessor(sv1);
  auto v2 = memgraph::query::VertexAccessor(sv2);

  memgraph::query::VirtualEdge ve1(v1, v2, "TYPE");
  memgraph::query::VirtualEdge ve2(v1, v2, "TYPE");

  // Different Gids means different virtual edges
  EXPECT_NE(ve1, ve2);
  EXPECT_EQ(ve1, ve1);
}

TEST_F(VirtualEdgeTest, Hashing) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  auto sv2 = acc->CreateVertex();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  auto v1 = memgraph::query::VertexAccessor(sv1);
  auto v2 = memgraph::query::VertexAccessor(sv2);

  memgraph::query::VirtualEdge ve1(v1, v2, "TYPE");
  memgraph::query::VirtualEdge ve2(v1, v2, "TYPE");

  std::unordered_set<memgraph::query::VirtualEdge> set;
  set.insert(ve1);
  set.insert(ve2);
  EXPECT_EQ(set.size(), 2);

  set.insert(ve1);
  EXPECT_EQ(set.size(), 2);
}

TEST_F(VirtualEdgeTest, GetPropertyReturnsNullForMissing) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  auto v1 = memgraph::query::VertexAccessor(sv1);
  memgraph::query::VirtualEdge ve(v1, v1, "SELF_LOOP");

  auto prop = ve.GetProperty(memgraph::storage::PropertyId::FromUint(999));
  EXPECT_TRUE(prop.IsNull());
}

TEST_F(VirtualEdgeTest, GidsCountDownFromMax) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  auto v1 = memgraph::query::VertexAccessor(sv1);
  memgraph::query::VirtualEdge ve1(v1, v1, "A");
  memgraph::query::VirtualEdge ve2(v1, v1, "B");

  EXPECT_GT(ve1.Gid(), ve2.Gid());
}

// =============================================================================
// Phase 1: Graph with virtual edges
// =============================================================================

class GraphVirtualEdgeTest : public ::testing::Test {
 protected:
  const std::string testSuite = "query_virtual_edge_graph";
  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  std::unique_ptr<memgraph::storage::Storage> db{new memgraph::storage::InMemoryStorage(config)};
};

TEST_F(GraphVirtualEdgeTest, InsertAndContains) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  auto sv2 = acc->CreateVertex();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  auto v1 = memgraph::query::VertexAccessor(sv1);
  auto v2 = memgraph::query::VertexAccessor(sv2);

  memgraph::query::Graph graph(memgraph::utils::NewDeleteResource());
  graph.InsertVertex(v1);
  graph.InsertVertex(v2);

  memgraph::query::VirtualEdge ve(v1, v2, "RELATED");
  graph.InsertVirtualEdge(ve);

  EXPECT_TRUE(graph.ContainsVirtualEdge(ve));
  EXPECT_EQ(graph.virtual_edges().size(), 1);
}

TEST_F(GraphVirtualEdgeTest, VirtualEdgesDoNotAffectRealEdges) {
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
  graph.InsertVirtualEdge(ve);

  EXPECT_EQ(graph.edges().size(), 1);
  EXPECT_EQ(graph.virtual_edges().size(), 1);
}

TEST_F(GraphVirtualEdgeTest, DuplicateVirtualEdgeInsertion) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  auto v1 = memgraph::query::VertexAccessor(sv1);
  memgraph::query::VirtualEdge ve(v1, v1, "SELF");

  memgraph::query::Graph graph(memgraph::utils::NewDeleteResource());
  graph.InsertVertex(v1);
  graph.InsertVirtualEdge(ve);
  graph.InsertVirtualEdge(ve);

  EXPECT_EQ(graph.virtual_edges().size(), 1);
}

TEST_F(GraphVirtualEdgeTest, CopyPreservesVirtualEdges) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  auto sv2 = acc->CreateVertex();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  auto v1 = memgraph::query::VertexAccessor(sv1);
  auto v2 = memgraph::query::VertexAccessor(sv2);

  memgraph::query::Graph graph(memgraph::utils::NewDeleteResource());
  graph.InsertVertex(v1);
  graph.InsertVertex(v2);

  memgraph::query::VirtualEdge ve(v1, v2, "TYPE");
  graph.InsertVirtualEdge(ve);

  memgraph::query::Graph copy(graph);
  EXPECT_EQ(copy.virtual_edges().size(), 1);
  EXPECT_TRUE(copy.ContainsVirtualEdge(ve));
}

TEST_F(GraphVirtualEdgeTest, MovePreservesVirtualEdges) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  auto v1 = memgraph::query::VertexAccessor(sv1);

  memgraph::query::Graph graph(memgraph::utils::NewDeleteResource());
  graph.InsertVertex(v1);

  memgraph::query::VirtualEdge ve(v1, v1, "TYPE");
  graph.InsertVirtualEdge(ve);

  memgraph::query::Graph moved(std::move(graph));
  EXPECT_EQ(moved.virtual_edges().size(), 1);
  EXPECT_TRUE(moved.ContainsVirtualEdge(ve));
}

// =============================================================================
// Phase 3: SubgraphVertexAccessor virtual edge methods
// =============================================================================

class SubgraphVirtualEdgeTest : public ::testing::Test {
 protected:
  const std::string testSuite = "query_virtual_edge_subgraph";
  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  std::unique_ptr<memgraph::storage::Storage> db{new memgraph::storage::InMemoryStorage(config)};
};

TEST_F(SubgraphVirtualEdgeTest, VirtualOutEdges) {
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

  graph.InsertVirtualEdge(memgraph::query::VirtualEdge(v1, v2, "A"));
  graph.InsertVirtualEdge(memgraph::query::VirtualEdge(v1, v3, "B"));
  graph.InsertVirtualEdge(memgraph::query::VirtualEdge(v2, v3, "C"));

  memgraph::query::SubgraphVertexAccessor sva(v1, &graph);
  auto out = sva.VirtualOutEdges();
  EXPECT_EQ(out.size(), 2);
}

TEST_F(SubgraphVirtualEdgeTest, VirtualInEdges) {
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

  graph.InsertVirtualEdge(memgraph::query::VirtualEdge(v1, v3, "A"));
  graph.InsertVirtualEdge(memgraph::query::VirtualEdge(v2, v3, "B"));
  graph.InsertVirtualEdge(memgraph::query::VirtualEdge(v1, v2, "C"));

  memgraph::query::SubgraphVertexAccessor sva(v3, &graph);
  auto in = sva.VirtualInEdges();
  EXPECT_EQ(in.size(), 2);
}

TEST_F(SubgraphVirtualEdgeTest, NoVirtualEdgesReturnsEmpty) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  auto v1 = memgraph::query::VertexAccessor(sv1);

  memgraph::query::Graph graph(memgraph::utils::NewDeleteResource());
  graph.InsertVertex(v1);

  memgraph::query::SubgraphVertexAccessor sva(v1, &graph);
  EXPECT_TRUE(sva.VirtualOutEdges().empty());
  EXPECT_TRUE(sva.VirtualInEdges().empty());
}

TEST_F(SubgraphVirtualEdgeTest, SelfLoopAppearsInBothDirections) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  auto v1 = memgraph::query::VertexAccessor(sv1);

  memgraph::query::Graph graph(memgraph::utils::NewDeleteResource());
  graph.InsertVertex(v1);

  memgraph::query::VirtualEdge ve(v1, v1, "SELF");
  graph.InsertVirtualEdge(ve);

  memgraph::query::SubgraphVertexAccessor sva(v1, &graph);
  EXPECT_EQ(sva.VirtualOutEdges().size(), 1);
  EXPECT_EQ(sva.VirtualInEdges().size(), 1);
}
