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
  auto vn1 = std::make_shared<const memgraph::query::VirtualNode>(memgraph::query::VirtualNode({"L1"}, {}));
  auto vn2 = std::make_shared<const memgraph::query::VirtualNode>(memgraph::query::VirtualNode({"L2"}, {}));

  memgraph::query::VirtualEdge ve1(vn1, vn2, "RELATES_TO");
  memgraph::query::VirtualEdge ve2(vn1, vn2, "RELATES_TO");

  EXPECT_EQ(ve1.From().Gid(), vn1->Gid());
  EXPECT_EQ(ve1.To().Gid(), vn2->Gid());
  EXPECT_EQ(ve1.EdgeTypeName(), "RELATES_TO");

  // same triple → distinct synthetic gids but semantic equality.
  EXPECT_NE(ve1.Gid(), ve2.Gid());
  EXPECT_GT(ve1.Gid(), ve2.Gid());  // gids count down
  EXPECT_EQ(ve1, ve2);

  const memgraph::query::VirtualEdge ve_other_type(vn1, vn2, "OTHER_TYPE");
  EXPECT_NE(ve1, ve_other_type);

  const memgraph::query::VirtualEdge ve_reversed(vn2, vn1, "RELATES_TO");
  EXPECT_NE(ve1, ve_reversed);

  std::unordered_set<memgraph::query::VirtualEdge> set;
  set.insert(ve1);
  set.insert(ve2);
  set.insert(ve_other_type);
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

  memgraph::query::Graph graph(memgraph::utils::NewDeleteResource());
  graph.InsertVertex(v1);
  graph.InsertVertex(v2);
  graph.InsertEdge(memgraph::query::EdgeAccessor(*se));

  memgraph::query::VirtualGraph vg(memgraph::utils::NewDeleteResource());
  const auto &vn1 = vg.InsertNode(memgraph::query::VirtualNode({}, {}));
  const auto &vn2 = vg.InsertNode(memgraph::query::VirtualNode({}, {}));
  memgraph::query::VirtualEdge ve(vg.FindNode(vn1.Gid()), vg.FindNode(vn2.Gid()), "VIRTUAL");
  EXPECT_TRUE(vg.InsertEdgeIfNew(ve));
  EXPECT_FALSE(vg.InsertEdgeIfNew(ve));

  EXPECT_EQ(graph.edges().size(), 1);
  EXPECT_EQ(vg.edges().size(), 1);
  EXPECT_TRUE(vg.ContainsEdge(ve));
}

TEST_F(VirtualEdgeTest, VirtualGraphFiltersEdgesByVertex) {
  memgraph::query::VirtualGraph vg(memgraph::utils::NewDeleteResource());
  const auto &vn1 = vg.InsertNode(memgraph::query::VirtualNode({}, {}));
  const auto &vn2 = vg.InsertNode(memgraph::query::VirtualNode({}, {}));
  const auto &vn3 = vg.InsertNode(memgraph::query::VirtualNode({}, {}));

  auto s1 = vg.FindNode(vn1.Gid());
  auto s2 = vg.FindNode(vn2.Gid());
  auto s3 = vg.FindNode(vn3.Gid());
  vg.InsertEdgeIfNew(memgraph::query::VirtualEdge(s1, s2, "A"));
  vg.InsertEdgeIfNew(memgraph::query::VirtualEdge(s1, s3, "B"));
  vg.InsertEdgeIfNew(memgraph::query::VirtualEdge(s2, s3, "C"));

  EXPECT_EQ(vg.OutEdges(vn1.Gid()).size(), 2);
  EXPECT_EQ(vg.InEdges(vn1.Gid()).size(), 0);
  EXPECT_EQ(vg.OutEdges(vn3.Gid()).size(), 0);
  EXPECT_EQ(vg.InEdges(vn3.Gid()).size(), 2);
}

TEST_F(VirtualEdgeTest, SelfLoopAppearsInBothDirections) {
  memgraph::query::VirtualGraph vg(memgraph::utils::NewDeleteResource());
  const auto &vn1 = vg.InsertNode(memgraph::query::VirtualNode({}, {}));
  auto s1 = vg.FindNode(vn1.Gid());
  vg.InsertEdgeIfNew(memgraph::query::VirtualEdge(s1, s1, "SELF"));

  EXPECT_EQ(vg.OutEdges(vn1.Gid()).size(), 1);
  EXPECT_EQ(vg.InEdges(vn1.Gid()).size(), 1);
}

TEST_F(VirtualEdgeTest, MergeRewritesEdgesViaExplicitAliasMap) {
  // Simulate two parallel aggregate branches that both derived a node from the same real vertex,
  // producing distinct synth gids. The caller (aggregator) supplies an alias map telling Merge
  // that `other`'s synth is an alias for `main`'s canonical synth.
  memgraph::query::VirtualGraph main(memgraph::utils::NewDeleteResource());
  memgraph::query::VirtualGraph other(memgraph::utils::NewDeleteResource());

  const auto &main_shared = main.InsertNode(memgraph::query::VirtualNode({"main"}, {}));
  const auto &other_shared = other.InsertNode(memgraph::query::VirtualNode({"other"}, {}));
  const auto &other_only = other.InsertNode(memgraph::query::VirtualNode({}, {}));
  // give `other` an edge between the two nodes it owns.
  other.InsertEdgeIfNew(
      memgraph::query::VirtualEdge(other.FindNode(other_shared.Gid()), other.FindNode(other_only.Gid()), "E"));

  memgraph::query::VirtualGraphAliasMap aliases(memgraph::utils::NewDeleteResource());
  aliases.try_emplace(other_shared.Gid(), main_shared.Gid());

  main.Merge(other, aliases);

  // main's canonical for the shared vertex kept its identity and label.
  const auto canonical = main.FindNode(main_shared.Gid());
  ASSERT_NE(canonical, nullptr);
  EXPECT_EQ(canonical->Labels()[0], "main");
  // other_shared was aliased; its synth gid is NOT a key in main's node map.
  EXPECT_EQ(main.FindNode(other_shared.Gid()), nullptr);
  // other_only was carried through under its original synth gid.
  ASSERT_NE(main.FindNode(other_only.Gid()), nullptr);
  EXPECT_EQ(main.nodes().size(), 2);
  // Edge was rewritten: source rebound from other_shared → main_shared.
  EXPECT_EQ(main.OutEdges(main_shared.Gid()).size(), 1);
  EXPECT_EQ(main.InEdges(other_only.Gid()).size(), 1);
}

TEST_F(VirtualEdgeTest, InsertIfNewDedupsDistinctEdgeGidsByTriple) {
  memgraph::query::VirtualGraph vg(memgraph::utils::NewDeleteResource());
  const auto &vn1 = vg.InsertNode(memgraph::query::VirtualNode({}, {}));
  const auto &vn2 = vg.InsertNode(memgraph::query::VirtualNode({}, {}));
  auto s1 = vg.FindNode(vn1.Gid());
  auto s2 = vg.FindNode(vn2.Gid());

  EXPECT_TRUE(vg.InsertEdgeIfNew(memgraph::query::VirtualEdge(s1, s2, "X")));
  EXPECT_FALSE(vg.InsertEdgeIfNew(memgraph::query::VirtualEdge(s1, s2, "X")));
  EXPECT_TRUE(vg.InsertEdgeIfNew(memgraph::query::VirtualEdge(s1, s2, "Y")));

  EXPECT_EQ(vg.edges().size(), 2);
  EXPECT_EQ(vg.OutEdges(vn1.Gid()).size(), 2);
}
