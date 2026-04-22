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
  memgraph::query::VirtualNode vn1({"L1"}, {});
  memgraph::query::VirtualNode vn2({"L2"}, {});

  memgraph::query::VirtualEdge ve1(vn1, vn2, "RELATES_TO");
  memgraph::query::VirtualEdge ve2(vn1, vn2, "RELATES_TO");

  EXPECT_EQ(ve1.From().Gid(), vn1.Gid());
  EXPECT_EQ(ve1.To().Gid(), vn2.Gid());
  EXPECT_EQ(ve1.EdgeTypeName(), "RELATES_TO");

  // Same triple → distinct synthetic gids but semantic equality.
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
  const auto &vn1 = vg.InsertOrGetNode(sv1.Gid(), memgraph::query::VirtualNode({}, {}));
  const auto &vn2 = vg.InsertOrGetNode(sv2.Gid(), memgraph::query::VirtualNode({}, {}));
  memgraph::query::VirtualEdge ve(vn1, vn2, "VIRTUAL");
  EXPECT_TRUE(vg.InsertEdgeIfNew(ve));
  EXPECT_FALSE(vg.InsertEdgeIfNew(ve));

  EXPECT_EQ(graph.edges().size(), 1);
  EXPECT_EQ(vg.edges().size(), 1);
  EXPECT_TRUE(vg.ContainsEdge(ve));
}

TEST_F(VirtualEdgeTest, VirtualGraphFiltersEdgesByVertex) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  auto sv2 = acc->CreateVertex();
  auto sv3 = acc->CreateVertex();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  memgraph::query::VirtualGraph vg(memgraph::utils::NewDeleteResource());
  const auto &vn1 = vg.InsertOrGetNode(sv1.Gid(), memgraph::query::VirtualNode({}, {}));
  const auto &vn2 = vg.InsertOrGetNode(sv2.Gid(), memgraph::query::VirtualNode({}, {}));
  const auto &vn3 = vg.InsertOrGetNode(sv3.Gid(), memgraph::query::VirtualNode({}, {}));

  vg.InsertEdgeIfNew(memgraph::query::VirtualEdge(vn1, vn2, "A"));
  vg.InsertEdgeIfNew(memgraph::query::VirtualEdge(vn1, vn3, "B"));
  vg.InsertEdgeIfNew(memgraph::query::VirtualEdge(vn2, vn3, "C"));

  EXPECT_EQ(vg.OutEdges(vn1.Gid()).size(), 2);
  EXPECT_EQ(vg.InEdges(vn1.Gid()).size(), 0);
  EXPECT_EQ(vg.OutEdges(vn3.Gid()).size(), 0);
  EXPECT_EQ(vg.InEdges(vn3.Gid()).size(), 2);
}

TEST_F(VirtualEdgeTest, SelfLoopAppearsInBothDirections) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  memgraph::query::VirtualGraph vg(memgraph::utils::NewDeleteResource());
  const auto &vn1 = vg.InsertOrGetNode(sv1.Gid(), memgraph::query::VirtualNode({}, {}));
  vg.InsertEdgeIfNew(memgraph::query::VirtualEdge(vn1, vn1, "SELF"));

  EXPECT_EQ(vg.OutEdges(vn1.Gid()).size(), 1);
  EXPECT_EQ(vg.InEdges(vn1.Gid()).size(), 1);
}

TEST_F(VirtualEdgeTest, MergePreservesCanonicalAndAliasesOtherSynth) {
  auto acc = db->Access(memgraph::storage::WRITE);
  const auto shared_gid = acc->CreateVertex().Gid();
  const auto other_only_gid = acc->CreateVertex().Gid();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  memgraph::query::VirtualGraph main(memgraph::utils::NewDeleteResource());
  memgraph::query::VirtualGraph other(memgraph::utils::NewDeleteResource());

  const auto main_synth = main.InsertOrGetNode(shared_gid, memgraph::query::VirtualNode({"main"}, {})).Gid();
  const auto other_synth = other.InsertOrGetNode(shared_gid, memgraph::query::VirtualNode({"other"}, {})).Gid();
  const auto other_only_synth = other.InsertOrGetNode(other_only_gid, memgraph::query::VirtualNode({}, {})).Gid();

  main.Merge(other);

  const auto *canonical = main.FindNodeByRealGid(shared_gid);
  ASSERT_NE(canonical, nullptr);
  EXPECT_EQ(canonical->Gid(), main_synth);
  ASSERT_EQ(canonical->Labels().size(), 1);
  EXPECT_EQ(canonical->Labels()[0], "main");
  EXPECT_EQ(main.FindNode(main_synth), canonical);
  // Aliased synths (other's synth for the shared vertex) are not keys in main's node map;
  // they're resolved via real_to_virtual_ only.
  EXPECT_EQ(main.FindNode(other_synth), nullptr);
  ASSERT_NE(main.FindNode(other_only_synth), nullptr);
  EXPECT_EQ(main.FindNodeByRealGid(other_only_gid)->Gid(), other_only_synth);
  EXPECT_EQ(main.nodes().size(), 2);
}

TEST_F(VirtualEdgeTest, InsertIfNewDedupsDistinctEdgeGidsByTriple) {
  auto acc = db->Access(memgraph::storage::WRITE);
  auto sv1 = acc->CreateVertex();
  auto sv2 = acc->CreateVertex();
  acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());

  memgraph::query::VirtualGraph vg(memgraph::utils::NewDeleteResource());
  const auto &vn1 = vg.InsertOrGetNode(sv1.Gid(), memgraph::query::VirtualNode({}, {}));
  const auto &vn2 = vg.InsertOrGetNode(sv2.Gid(), memgraph::query::VirtualNode({}, {}));

  EXPECT_TRUE(vg.InsertEdgeIfNew(memgraph::query::VirtualEdge(vn1, vn2, "X")));
  EXPECT_FALSE(vg.InsertEdgeIfNew(memgraph::query::VirtualEdge(vn1, vn2, "X")));
  EXPECT_TRUE(vg.InsertEdgeIfNew(memgraph::query::VirtualEdge(vn1, vn2, "Y")));

  EXPECT_EQ(vg.edges().size(), 2);
  EXPECT_EQ(vg.OutEdges(vn1.Gid()).size(), 2);
}
