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

// Graph Versioning: the procedure API on a branch. mgp_execute_query needs the DbmsHandler and interpreter-context
// singletons, so its nested session is covered by tests/e2e/versioning/versioning_mgp_test.py.

#include <cstdint>
#include <optional>
#include <string>

#include <gtest/gtest.h>

#include "mg_procedure.h"
#include "query/procedure/mg_procedure_impl.hpp"
#include "versioning_fixture.hpp"

namespace {

class VersioningMgpTest : public VersioningGatedTest {
 protected:
  // Opens a transaction on the session's branch and the procedure graph over it.
  mgp_graph &Begin() {
    faker.Interpret("BEGIN");
    auto &dba = *faker.interpreter.current_db_.execution_db_accessor_;
    return graph_.emplace(&dba, memgraph::storage::View::NEW, nullptr, dba.GetStorageMode());
  }

  int64_t Id(const std::string &query) { return faker.Interpret(query).GetResults().at(0).at(0).ValueInt(); }

  mgp_vertex Vertex(int64_t id) {
    auto vertex = graph_->getImpl()->FindVertex(memgraph::storage::Gid::FromInt(id), memgraph::storage::View::NEW);
    EXPECT_TRUE(vertex);
    return mgp_vertex{*vertex, &*graph_, mgp_vertex::allocator_type{memgraph::utils::NewDeleteResource()}};
  }

  // The single out-edge of vertex `id`.
  mgp_edge OutEdge(int64_t id) {
    auto vertex = graph_->getImpl()->FindVertex(memgraph::storage::Gid::FromInt(id), memgraph::storage::View::NEW);
    EXPECT_TRUE(vertex);
    auto out = vertex->OutEdges(memgraph::storage::View::NEW);
    EXPECT_TRUE(out && out->edges.size() == 1);
    return mgp_edge{out->edges[0], &*graph_, mgp_edge::allocator_type{memgraph::utils::NewDeleteResource()}};
  }

  static bool IsDeleted(mgp_vertex &vertex) {
    int deleted = -1;
    EXPECT_EQ(mgp_vertex_is_deleted(&vertex, &deleted), mgp_error::MGP_ERROR_NO_ERROR);
    return deleted == 1;
  }

  static bool IsDeleted(mgp_edge &edge) {
    int deleted = -1;
    EXPECT_EQ(mgp_edge_is_deleted(&edge, &deleted), mgp_error::MGP_ERROR_NO_ERROR);
    return deleted == 1;
  }

  std::optional<mgp_graph> graph_;
};

}  // namespace

// A fork vertex is deleted only if the branch deleted it: main deleting it after the fork does not show through,
// whether or not the branch changed it. A branch-created vertex reports its own delete.
TEST_F(VersioningMgpTest, VertexIsDeletedFollowsTheBranch) {
  faker.Interpret("CREATE (:V {id: 1}), (:V {id: 2}), (:V {id: 3})");
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("MATCH (n:V) WHERE n.id > 1 DELETE n");
  faker.Interpret("USE BRANCH b");
  faker.Interpret("CREATE (:Native)");

  Begin();
  faker.Interpret("MATCH (n:V {id: 3}) SET n.x = 1");
  auto deleted_on_main = Vertex(Id("MATCH (n:V {id: 2}) RETURN id(n)"));
  auto changed_then_deleted_on_main = Vertex(Id("MATCH (n:V {id: 3}) RETURN id(n)"));
  auto fork = Vertex(Id("MATCH (n:V {id: 1}) RETURN id(n)"));
  auto native = Vertex(Id("MATCH (n:Native) RETURN id(n)"));
  EXPECT_FALSE(IsDeleted(deleted_on_main));
  EXPECT_FALSE(IsDeleted(changed_then_deleted_on_main));
  EXPECT_FALSE(IsDeleted(fork));
  EXPECT_FALSE(IsDeleted(native));

  faker.Interpret("MATCH (n:V {id: 1}) DELETE n");
  faker.Interpret("MATCH (n:Native) DELETE n");
  EXPECT_TRUE(IsDeleted(fork));
  EXPECT_TRUE(IsDeleted(native));
  faker.Interpret("ROLLBACK");
}

// Likewise for a fork edge: one the branch hid (deleted it or an endpoint) is deleted, one main deleted is not.
TEST_F(VersioningMgpTest, EdgeIsDeletedFollowsTheBranch) {
  faker.Interpret("CREATE (:A {id: 1})-[:E]->(:B), (:A {id: 2})-[:E]->(:B), (:A {id: 3})-[:E]->(:B {id: 3})");
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("MATCH (:A {id: 2})-[e:E]->() DELETE e");
  faker.Interpret("USE BRANCH b");

  Begin();
  auto deleted_on_main = OutEdge(Id("MATCH (n:A {id: 2}) RETURN id(n)"));
  auto deleted_on_branch = OutEdge(Id("MATCH (n:A {id: 1}) RETURN id(n)"));
  auto endpoint_deleted_on_branch = OutEdge(Id("MATCH (n:A {id: 3}) RETURN id(n)"));
  EXPECT_FALSE(IsDeleted(deleted_on_main));
  EXPECT_FALSE(IsDeleted(deleted_on_branch));
  EXPECT_FALSE(IsDeleted(endpoint_deleted_on_branch));

  faker.Interpret("MATCH (:A {id: 1})-[e:E]->() DELETE e");
  faker.Interpret("MATCH (b:B {id: 3}) DETACH DELETE b");
  EXPECT_FALSE(IsDeleted(deleted_on_main));
  EXPECT_TRUE(IsDeleted(deleted_on_branch));
  EXPECT_TRUE(IsDeleted(endpoint_deleted_on_branch));
  faker.Interpret("ROLLBACK");
}

// The diff engine holds only touched and branch-created objects, so the counts are main's.
TEST_F(VersioningMgpTest, ApproximateCountsOnABranchAreMains) {
  faker.Interpret("CREATE (:V)-[:E]->(:V)-[:E]->(:V)");
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("USE BRANCH b");

  auto &graph = Begin();
  faker.Interpret("MATCH (n:V) SET n.x = 1");
  size_t vertices = 0;
  size_t edges = 0;
  ASSERT_EQ(mgp_graph_approximate_vertex_count(&graph, &vertices), mgp_error::MGP_ERROR_NO_ERROR);
  ASSERT_EQ(mgp_graph_approximate_edge_count(&graph, &edges), mgp_error::MGP_ERROR_NO_ERROR);
  EXPECT_EQ(vertices, 3U);
  EXPECT_EQ(edges, 2U);
  faker.Interpret("ROLLBACK");
}
