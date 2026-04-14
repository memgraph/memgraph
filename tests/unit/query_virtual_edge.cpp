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
#include "query/virtual_graph.hpp"
#include "query/virtual_node.hpp"
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

  memgraph::query::VirtualNode vn1(sv1.Gid(), {"L1"}, {});
  memgraph::query::VirtualNode vn2(sv2.Gid(), {"L2"}, {});

  memgraph::query::VirtualEdge ve1(vn1, vn2, "RELATES_TO");
  memgraph::query::VirtualEdge ve2(vn1, vn2, "RELATES_TO");

  EXPECT_EQ(ve1.From().Gid(), vn1.Gid());
  EXPECT_EQ(ve1.To().Gid(), vn2.Gid());
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
  const auto edge_type = acc->NameToEdgeType("KNOWS");
  auto se = acc->CreateEdge(&sv1, &sv2, edge_type);
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  const auto v1 = memgraph::query::VertexAccessor(sv1);
  const auto v2 = memgraph::query::VertexAccessor(sv2);

  // real edges live on Graph
  memgraph::query::Graph graph(memgraph::utils::NewDeleteResource());
  graph.InsertVertex(v1);
  graph.InsertVertex(v2);
  graph.InsertEdge(memgraph::query::EdgeAccessor(*se));

  // virtual edges live on VirtualGraph
  memgraph::query::VirtualGraph vg(memgraph::utils::NewDeleteResource());
  const auto &vn1 = vg.node_store().InsertOrGet(memgraph::query::VirtualNode(sv1.Gid(), {}, {}));
  const auto &vn2 = vg.node_store().InsertOrGet(memgraph::query::VirtualNode(sv2.Gid(), {}, {}));
  memgraph::query::VirtualEdge ve(vn1, vn2, "VIRTUAL");
  vg.edge_store().Insert(ve);
  vg.edge_store().Insert(ve);  // duplicate is a no-op

  EXPECT_EQ(graph.edges().size(), 1);
  EXPECT_EQ(vg.edge_store().size(), 1);
  EXPECT_TRUE(vg.edge_store().Contains(ve));
}

TEST_F(VirtualEdgeTest, VirtualGraphFiltersEdgesByVertex) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  auto sv2 = acc->CreateVertex();
  auto sv3 = acc->CreateVertex();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  memgraph::query::VirtualGraph vg(memgraph::utils::NewDeleteResource());
  const auto &vn1 = vg.node_store().InsertOrGet(memgraph::query::VirtualNode(sv1.Gid(), {}, {}));
  const auto &vn2 = vg.node_store().InsertOrGet(memgraph::query::VirtualNode(sv2.Gid(), {}, {}));
  const auto &vn3 = vg.node_store().InsertOrGet(memgraph::query::VirtualNode(sv3.Gid(), {}, {}));

  // v1->v2, v1->v3, v2->v3
  vg.edge_store().Insert(memgraph::query::VirtualEdge(vn1, vn2, "A"));
  vg.edge_store().Insert(memgraph::query::VirtualEdge(vn1, vn3, "B"));
  vg.edge_store().Insert(memgraph::query::VirtualEdge(vn2, vn3, "C"));

  // edge store is indexed by synthetic VirtualNode Gid
  // vn1 has 2 out, 0 in
  EXPECT_EQ(vg.edge_store().OutEdges(vn1.Gid()).size(), 2);
  EXPECT_EQ(vg.edge_store().InEdges(vn1.Gid()).size(), 0);

  // vn3 has 0 out, 2 in
  EXPECT_EQ(vg.edge_store().OutEdges(vn3.Gid()).size(), 0);
  EXPECT_EQ(vg.edge_store().InEdges(vn3.Gid()).size(), 2);
}

TEST_F(VirtualEdgeTest, SelfLoopAppearsInBothDirections) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  memgraph::query::VirtualGraph vg(memgraph::utils::NewDeleteResource());
  const auto &vn1 = vg.node_store().InsertOrGet(memgraph::query::VirtualNode(sv1.Gid(), {}, {}));
  vg.edge_store().Insert(memgraph::query::VirtualEdge(vn1, vn1, "SELF"));

  // edge store is indexed by synthetic VirtualNode Gid
  EXPECT_EQ(vg.edge_store().OutEdges(vn1.Gid()).size(), 1);
  EXPECT_EQ(vg.edge_store().InEdges(vn1.Gid()).size(), 1);
}
