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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "mg_procedure.h"
#include "query/context.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "versioning_fixture.hpp"

namespace {

class VersioningMgpTest : public VersioningGatedTest {
 protected:
  mgp_graph &Begin() {
    faker.Interpret("BEGIN");
    auto &dba = *faker.interpreter.current_db_.execution_db_accessor_;
    return graph_.emplace(&dba, memgraph::storage::View::NEW, nullptr, dba.GetStorageMode());
  }

  // As Begin, with the execution context the procedure API's writes record their statistics in.
  mgp_graph &BeginWritable() {
    faker.Interpret("BEGIN");
    auto &dba = *faker.interpreter.current_db_.execution_db_accessor_;
    return graph_.emplace(&dba, memgraph::storage::View::NEW, &execution_context_, dba.GetStorageMode());
  }

  int64_t V(int64_t id) {
    auto vertex = Vertex(id);
    mgp_memory memory{memgraph::utils::NewDeleteResource()};
    mgp_value *value = nullptr;
    EXPECT_EQ(mgp_vertex_get_property(&vertex, "v", &memory, &value), mgp_error::MGP_ERROR_NO_ERROR);
    int64_t out = -1;
    EXPECT_EQ(mgp_value_get_int(value, &out), mgp_error::MGP_ERROR_NO_ERROR);
    mgp_value_destroy(value);
    return out;
  }

  int64_t Id(const std::string &query) { return faker.Interpret(query).GetResults().at(0).at(0).ValueInt(); }

  mgp_vertex Vertex(int64_t id) {
    auto vertex = graph_->getImpl()->FindVertex(memgraph::storage::Gid::FromInt(id), memgraph::storage::View::NEW);
    EXPECT_TRUE(vertex);
    return mgp_vertex{*vertex, &*graph_, mgp_vertex::allocator_type{memgraph::utils::NewDeleteResource()}};
  }

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

  // Edge `id` as the edge-property index scan (type R, property w) returns it on the branch.
  mgp_edge IndexedEdge(int64_t id) {
    auto &dba = *graph_->getImpl();
    auto edges = dba.Edges(memgraph::storage::View::NEW,
                           dba.NameToEdgeType("R"),
                           dba.NameToProperty("w"),
                           memgraph::storage::PropertyValueRange::IsNotNull());
    for (auto edge : edges) {
      if (edge.Gid().AsInt() == id) {
        return mgp_edge{edge, &*graph_, mgp_edge::allocator_type{memgraph::utils::NewDeleteResource()}};
      }
    }
    ADD_FAILURE() << "edge " << id << " is not in the edge-property index scan";
    throw std::logic_error{"edge not found"};
  }

  mgp_value *IntValue(int64_t value) {
    mgp_value *out = nullptr;
    EXPECT_EQ(mgp_value_make_int(value, &memory_, &out), mgp_error::MGP_ERROR_NO_ERROR);
    return out;
  }

  // The diff engine's label+properties index entries, as gid -> the key's values read off the stub itself.
  std::map<int64_t, std::vector<int64_t>> IndexedKeys(const std::string &label,
                                                      const std::vector<std::string> &properties) {
    auto &storage = *db->storage();
    auto acc = faker.interpreter.current_db_.branch_context()->diff_engine().Access(memgraph::storage::READ);
    std::vector<memgraph::storage::PropertyId> ids;
    for (const auto &property : properties) ids.push_back(storage.NameToProperty(property));
    std::vector<memgraph::storage::PropertyPath> paths(ids.begin(), ids.end());
    std::map<int64_t, std::vector<int64_t>> keys;
    for (auto vertex : acc->Vertices(storage.NameToLabel(label), paths, memgraph::storage::View::NEW)) {
      auto &key = keys[vertex.Gid().AsInt()];
      for (auto id : ids) key.push_back(vertex.GetProperty(id, memgraph::storage::View::NEW)->ValueInt());
    }
    return keys;
  }

  mgp_memory memory_{memgraph::utils::NewDeleteResource()};
  memgraph::query::ExecutionContext execution_context_;

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

// A fork edge the branch set a property on and then deleted is deleted for the procedure, also when it is read back
// through the edge-property index; one main deleted after the fork is found there and is not.
TEST_F(VersioningMgpTest, EdgeDeletedAfterSetIsDeletedAndFoundThroughTheEdgePropertyIndex) {
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE (:A {id: 1})-[:R {w: 1}]->(:B), (:A {id: 2})-[:R {w: 2}]->(:B)");
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("MATCH (:A {id: 2})-[e:R]->() DELETE e");
  faker.Interpret("USE BRANCH b");

  Begin();
  auto deleted_on_main = IndexedEdge(Id("MATCH (:A {id: 2})-[e:R]->() RETURN id(e)"));
  EXPECT_FALSE(IsDeleted(deleted_on_main));
  faker.Interpret("MATCH (:A {id: 1})-[e:R]->() SET e.w = 5");
  {
    auto written = IndexedEdge(Id("MATCH (:A {id: 1})-[e:R]->() RETURN id(e)"));
    EXPECT_FALSE(IsDeleted(written));
    faker.Interpret("MATCH (:A {id: 1})-[e:R]->() DELETE e");
    EXPECT_TRUE(IsDeleted(written));
  }
  faker.Interpret("ROLLBACK");
}

// The outer branch transaction holds main's storage guard shared; a UNIQUE request (SetStorageMode, SET GLOBAL
// ISOLATION LEVEL) queued behind it must make the nested session time out, as on main, not wait forever.
TEST_F(VersioningMgpTest, NestedSessionOnABranchTimesOutBehindAQueuedUniqueRequestInsteadOfHanging) {
  faker.Interpret("CREATE (:K {v: 1})");
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("USE BRANCH b");
  Begin();
  faker.Interpret("MATCH (n:K) SET n.v = 2");

  InterpreterFaker nested{&interpreter_context, db};
  nested.interpreter.current_db_.AttachBranchOf(db->version_store(), *faker.interpreter.current_db_.branch_context());

  std::atomic<bool> unique_requested{false};
  std::thread unique([&] {
    unique_requested = true;
    auto accessor = db->storage()->UniqueAccess();
  });
  while (!unique_requested) std::this_thread::yield();
  std::this_thread::sleep_for(std::chrono::milliseconds(300));  // The request is queued behind the outer READ guard.

  auto nested_run = std::async(std::launch::async, [&] {
    return memgraph::tests::versioning_fixture::ThrownMessage([&] { nested.Interpret("MATCH (n:K) RETURN n.v"); });
  });
  const auto finished = nested_run.wait_for(std::chrono::seconds(20)) == std::future_status::ready;
  // Ending the outer transaction unblocks a hung nested session, so the test reports instead of hanging.
  faker.Interpret("ROLLBACK");
  unique.join();
  const auto message = nested_run.get();
  ASSERT_TRUE(finished) << "the nested session waited on main's storage guard without a timeout";
  EXPECT_NE(message, "<no exception>");
}

// A procedure's graph is the outer transaction's: repeated reads see one snapshot while another branch session and main
// commit, the transaction's own write shows through, and a rollback leaves the committed state.
TEST_F(VersioningMgpTest, ProcedureReadsOnABranchSeeTheOuterSnapshotAndItsOwnWrites) {
  faker.Interpret("CREATE (:K {id: 1, v: 10}), (:K {id: 2, v: 20})");
  faker.Interpret("CREATE BRANCH b");
  InterpreterFaker main_session{&interpreter_context, db};
  auto sessions = SessionsOnBranch(1, "b");
  auto &writer = *sessions[0];
  const auto first = Id("MATCH (n:K {id: 1}) RETURN id(n)");
  const auto second = Id("MATCH (n:K {id: 2}) RETURN id(n)");
  faker.Interpret("USE BRANCH b");

  Begin();
  EXPECT_EQ(V(first), 10);
  writer.Interpret("MATCH (n:K {id: 1}) SET n.v = 11");
  main_session.Interpret("MATCH (n:K {id: 2}) SET n.v = 99");
  EXPECT_EQ(V(first), 10);
  EXPECT_EQ(V(second), 20);
  faker.Interpret("MATCH (n:K {id: 2}) SET n.v = 21");
  EXPECT_EQ(V(first), 10);
  EXPECT_EQ(V(second), 21);
  faker.Interpret("ROLLBACK");

  Begin();
  EXPECT_EQ(V(first), 11);
  EXPECT_EQ(V(second), 20);
  faker.Interpret("ROLLBACK");
}

// A procedure's writes take the same path as a query's, so a fork vertex the procedure changed sits in the diff
// engine's label+properties index under its whole key: a property set, a label added and a map set that moves two keys.
TEST_F(VersioningMgpTest, ProcedureVertexWritesKeepTheStubUnderItsWholeIndexKey) {
  faker.Interpret("CREATE INDEX ON :L(a, b)");
  faker.Interpret("CREATE INDEX ON :M(c, d)");
  faker.Interpret(
      "CREATE (:L:M {id: 1, a: 1, b: 2, c: 3, d: 4}), (:Plain {id: 2, a: 5, b: 6}), (:L {id: 3, a: 7, b: 8})");
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("USE BRANCH b");

  BeginWritable();
  auto const both = Id("MATCH (n {id: 1}) RETURN id(n)");
  auto const plain = Id("MATCH (n {id: 2}) RETURN id(n)");
  auto const single = Id("MATCH (n {id: 3}) RETURN id(n)");
  {
    auto vertex = Vertex(both);
    mgp_map *map = nullptr;
    ASSERT_EQ(mgp_map_make_empty(&memory_, &map), mgp_error::MGP_ERROR_NO_ERROR);
    auto *ten = IntValue(10);
    auto *thirty = IntValue(30);
    ASSERT_EQ(mgp_map_insert(map, "a", ten), mgp_error::MGP_ERROR_NO_ERROR);
    ASSERT_EQ(mgp_map_insert(map, "c", thirty), mgp_error::MGP_ERROR_NO_ERROR);
    ASSERT_EQ(mgp_vertex_set_properties(&vertex, map), mgp_error::MGP_ERROR_NO_ERROR);
    mgp_value_destroy(ten);
    mgp_value_destroy(thirty);
    mgp_map_destroy(map);
  }
  {
    auto vertex = Vertex(plain);
    ASSERT_EQ(mgp_vertex_add_label(&vertex, mgp_label{"L"}), mgp_error::MGP_ERROR_NO_ERROR);
  }
  {
    auto vertex = Vertex(single);
    auto *value = IntValue(70);
    ASSERT_EQ(mgp_vertex_set_property(&vertex, "a", value), mgp_error::MGP_ERROR_NO_ERROR);
    mgp_value_destroy(value);
  }
  faker.Interpret("COMMIT");

  EXPECT_EQ(IndexedKeys("L", {"a", "b"}),
            (std::map<int64_t, std::vector<int64_t>>{{both, {10, 2}}, {plain, {5, 6}}, {single, {70, 8}}}));
  EXPECT_EQ(IndexedKeys("M", {"c", "d"}), (std::map<int64_t, std::vector<int64_t>>{{both, {30, 4}}}));
}

// The same for a fork edge: its stub appears in the diff engine's edge property index at the written value.
TEST_F(VersioningMgpTest, ProcedureEdgeWritesPutTheStubUnderTheEdgePropertyIndex) {
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE (:A {id: 1})-[:R {w: 1, z: 9}]->(:B)");
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("USE BRANCH b");

  BeginWritable();
  auto const edge_id = Id("MATCH (:A {id: 1})-[e:R]->() RETURN id(e)");
  {
    auto edge = OutEdge(Id("MATCH (n:A) RETURN id(n)"));
    auto *value = IntValue(5);
    ASSERT_EQ(mgp_edge_set_property(&edge, "w", value), mgp_error::MGP_ERROR_NO_ERROR);
    mgp_value_destroy(value);
  }
  faker.Interpret("COMMIT");

  auto &storage = *db->storage();
  auto acc = faker.interpreter.current_db_.branch_context()->diff_engine().Access(memgraph::storage::READ);
  std::map<int64_t, int64_t> found;
  for (auto edge : acc->Edges(storage.NameToEdgeType("R"), storage.NameToProperty("w"), memgraph::storage::View::NEW)) {
    found.emplace(edge.Gid().AsInt(),
                  edge.GetProperty(storage.NameToProperty("w"), memgraph::storage::View::NEW)->ValueInt());
  }
  EXPECT_EQ(found, (std::map<int64_t, int64_t>{{edge_id, 5}}));
}
