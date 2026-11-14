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

// Branch edge writes through Cypher and the procedure-facing DbAccessor calls: branch-native edges (CREATE) and
// fork-edge property writes and deletes, isolated from main. Branches are created and attached through the C++ API.

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "query/db_accessor.hpp"
#include "query/edge_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/vertex_accessor.hpp"
#include "storage/v2/property_value.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning_fixture.hpp"

using memgraph::tests::versioning_fixture::ExplainText;
using memgraph::tests::versioning_fixture::IntRows;
using memgraph::tests::versioning_fixture::kNullInt;
using memgraph::tests::versioning_fixture::PlanHasOperator;
using memgraph::tests::versioning_fixture::Rows;
using memgraph::tests::versioning_fixture::ThrownMessage;

namespace {

namespace mq = memgraph::query;
namespace ms = memgraph::storage;

template <typename Base>
class EdgeTest : public Base {
 protected:
  void UseMain() { this->faker.interpreter.current_db_.LeaveBranch(); }

  int64_t Int(const std::string &query) {
    auto rows = IntRows(this->faker, query);
    EXPECT_EQ(rows.size(), 1U) << query;
    return rows.empty() ? kNullInt : rows[0][0];
  }
};

using VersioningEdgeTest = EdgeTest<VersioningGatedTest>;

class VersioningReferenceEdgeTest : public EdgeTest<VersioningFixture<false>> {
 protected:
  void SetUp() override {
    VersioningFixture<false>::SetUp();
    SatisfyGate();
  }
};

std::set<std::string> EdgePropertyKeys(InterpreterFaker &session, const std::string &query) {
  auto stream = session.Interpret(query);
  if (stream.GetResults().size() != 1U) return {"<not one row>"};
  std::set<std::string> keys;
  for (const auto &[key, _] : stream.GetResults()[0][0].ValueMap()) keys.insert(key);
  return keys;
}

}  // namespace

// Includes a fork endpoint that already has a label-write stub, and a self-loop.
TEST_F(VersioningEdgeTest, BranchEdgeCreateAndEndpointShadowingIsUnionAware) {
  faker.Interpret(
      "CREATE (:A {name: 'a', id: 1, p: 7}), (:B {name: 'b', id: 2, p: 8}), (:S {name: 's'})-[:E]->(:A {name: 'a2'}), "
      "(:Loop {id: 3})");
  Fork();

  faker.Interpret("MATCH (n:A {name: 'a'}) SET n:Extra");
  faker.Interpret("MATCH (x:A {name: 'a'}), (y:B {name: 'b'}) CREATE (x)-[:R]->(y)");
  EXPECT_EQ(IntRows(faker, "MATCH (:A {name: 'a'})-[:R]->(y) RETURN toInteger(y.name = 'b')"), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:B {name: 'b'})<-[:R]-(x) RETURN toInteger(x.name = 'a')"), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, "MATCH (x:A {name: 'a'}) RETURN outDegree(x), inDegree(x)"), (Rows{{1, 0}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:Extra)-[:R]->(m) RETURN n.id, n.p, m.id, m.p"), (Rows{{1, 7, 2, 8}}));

  faker.Interpret("MATCH (x:Loop) CREATE (x)-[:L]->(x)");
  EXPECT_EQ(IntRows(faker, "MATCH (x:Loop) RETURN outDegree(x), inDegree(x)"), (Rows{{1, 1}}));
  EXPECT_EQ(IntRows(faker, "MATCH (x)-[:L]->(x) RETURN count(*)"), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:Loop)-[:L]->(y) RETURN y.id"), (Rows{{3}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:Loop)<-[:L]-(y) RETURN y.id"), (Rows{{3}}));

  faker.Interpret("MATCH (a2:A {name: 'a2'}) SET a2.name = 'a2_mod'");
  EXPECT_EQ(IntRows(faker, "MATCH (:S)-[:E]->(a2) RETURN toInteger(a2.name = 'a2_mod')"), (Rows{{1}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:L]->() RETURN count(r)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH (x:Loop) RETURN outDegree(x), inDegree(x)"), (Rows{{0, 0}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:S)-[:E]->(a2) RETURN toInteger(a2.name = 'a2')"), (Rows{{1}}));
}

// A branch-created edge's gid is at or above the fork floor and never reused by main.
TEST_F(VersioningEdgeTest, BranchFindEdgeByIdResolvesForkAndBranchNativeEdges) {
  faker.Interpret("CREATE (:S)-[:E]->(:A)");
  auto const fork_edge = Int("MATCH ()-[e:E]->() RETURN id(e)");
  Fork();
  auto const floor = static_cast<int64_t>(faker.interpreter.current_db_.branch_context()->core()->edge_gid_floor());

  auto const native_edge = Int("MATCH (a:A) CREATE (a)-[e:F]->(:B) RETURN id(e)");
  EXPECT_GE(native_edge, floor);
  auto const is_type = [&](int64_t gid, std::string_view type) {
    return "MATCH ()-[e]->() WHERE id(e) = " + std::to_string(gid) + " RETURN toInteger(type(e) = '" +
           std::string{type} + "')";
  };
  ASSERT_TRUE(PlanHasOperator(ExplainText(faker, is_type(fork_edge, "E")), "ScanAllByEdgeId"));
  EXPECT_EQ(IntRows(faker, is_type(fork_edge, "E")), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, is_type(native_edge, "F")), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, is_type(999999, "E")), (Rows{}));

  UseMain();
  EXPECT_GT(Int("MATCH (a:A) CREATE (a)-[e:G]->(:B) RETURN id(e)"), native_edge)
      << "main's counter already handed out the branch edge's gid";
}

// Writing one edge leaves untouched siblings, a self-loop and parallel edges between the same endpoints at their fork
// values.
TEST_F(VersioningEdgeTest, BranchEdgeWritesResolvePerEdgeObject) {
  faker.Interpret("CREATE (s:S)-[:E1 {w: 1}]->(:A), (s)-[:E3 {w: 3}]->(:C), (s)-[:SELF {w: 4}]->(s)");
  faker.Interpret("CREATE (:S2)-[:E2 {w: 2}]->(:B), (t:S3)-[:P1 {w: 10}]->(u:T3), (t)-[:P2 {w: 20}]->(u)");
  Fork();

  faker.Interpret("MATCH (:S)-[e:E1]->(:A) SET e.w = 100");
  faker.Interpret("MATCH (:S)-[e:SELF]->(:S) SET e.w = 999");
  faker.Interpret("MATCH (:S3)-[e:P1]->(:T3) SET e.w = 111");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[e]->() RETURN e.w ORDER BY e.w"), (Rows{{2}, {3}, {20}, {100}, {111}, {999}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:S3)-[e]->(:T3) RETURN e.w ORDER BY e.w"), (Rows{{20}, {111}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH ()-[e]->() RETURN e.w ORDER BY e.w"), (Rows{{1}, {2}, {3}, {4}, {10}, {20}}));
}

TEST_F(VersioningEdgeTest, BranchForkEdgePropertyWritesReadBackFromBothEndsAndLeaveMainAlone) {
  faker.Interpret(
      "CREATE (:A)-[:R0 {w: 1, x: 7}]->(:B), (:A)-[:R1 {w: 1, x: 7}]->(:B), (:A)-[:R2 {w: 1, x: 7, z: 3}]->(:B), "
      "(:A)-[:R3 {w: 1, x: 7}]->(:B)");
  Fork();

  EXPECT_EQ(IntRows(faker, "MATCH (:A)-[r:R0]->(:B) SET r.w = 2 RETURN r.w"), (Rows{{2}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:A)-[r:R0]->(:B) RETURN r.w, r.x"), (Rows{{2, 7}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:B)<-[r:R0]-(:A) RETURN r.w, r.x"), (Rows{{2, 7}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() RETURN count(r)"), (Rows{{4}})) << "the write adds no second edge";
  EXPECT_EQ(EdgePropertyKeys(faker, "MATCH ()-[r:R0]->() RETURN properties(r)"), (std::set<std::string>{"w", "x"}));

  faker.Interpret("MATCH ()-[r:R1]->() REMOVE r.w");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R1]->() RETURN r.w, r.x"), (Rows{{kNullInt, 7}}));
  EXPECT_EQ(EdgePropertyKeys(faker, "MATCH ()-[r:R1]->() RETURN properties(r)"), (std::set<std::string>{"x"}));

  faker.Interpret("MATCH ()-[r:R2]->() SET r = {x: 1}");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R2]->() RETURN r.w, r.x"), (Rows{{kNullInt, 1}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R2]->() RETURN r.z"), (Rows{{kNullInt}}))
      << "a single lookup takes GetProperty";
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R2]->() RETURN r.x"), (Rows{{1}}));
  EXPECT_EQ(EdgePropertyKeys(faker, "MATCH ()-[r:R2]->() RETURN properties(r)"), (std::set<std::string>{"x"}));

  faker.Interpret("MATCH ()-[r:R3]->() SET r.w = null");
  faker.Interpret("MATCH ()-[r:R3]->() SET r += {y: 5}");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R3]->() RETURN r.y"), (Rows{{5}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R3]->() RETURN r.w, r.x, r.y"), (Rows{{kNullInt, 7, 5}}));
  faker.Interpret("MATCH ()-[r:R3]->() SET r.w = 9");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R3]->() RETURN r.w, r.x, r.y"), (Rows{{9, 7, 5}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() RETURN r.w, r.x, r.y"),
            (Rows{{1, 7, kNullInt}, {1, 7, kNullInt}, {1, 7, kNullInt}, {1, 7, kNullInt}}));
}

TEST_F(VersioningEdgeTest, BranchForkEdgeWriteAfterRollbackSucceeds) {
  faker.Interpret("CREATE (:A)-[:R {w: 1}]->(:B)");
  Fork();

  faker.Interpret("BEGIN");
  faker.Interpret("MATCH ()-[r:R]->() SET r.w = 2");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.w"), (Rows{{2}}));
  faker.Interpret("ROLLBACK");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.w"), (Rows{{1}}));

  faker.Interpret("MATCH ()-[r:R]->() SET r.w = 3");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.w"), (Rows{{3}}));
}

TEST_F(VersioningEdgeTest, BranchDeleteWrittenForkEdgeIsGoneEverywhere) {
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret(
      "CREATE (a:A {id: 1}), (b:B {id: 2}), (a)-[:R {w: 1}]->(b), (a)-[:R {w: 4}]->(b), "
      "(:A {id: 3})-[:R {w: 5}]->(:B {id: 4}), (:A {id: 5})-[:R {w: 6}]->(:B {id: 6})");
  Fork();

  faker.Interpret("MATCH (:A {id: 1})-[r:R {w: 1}]->() SET r.w = 2");
  faker.Interpret("MATCH (:A {id: 1})-[r:R {w: 2}]->() DELETE r");
  EXPECT_EQ(IntRows(faker, "MATCH (:A {id: 1})-[r]->() RETURN r.w"), (Rows{{4}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:B {id: 2})<-[r]-() RETURN r.w"), (Rows{{4}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w > 0 RETURN r.w ORDER BY r.w"), (Rows{{4}, {5}, {6}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w = 2 RETURN count(r)"), (Rows{{0}}));

  faker.Interpret("MATCH (:A {id: 3})-[r:R]->() SET r.w = 8");
  faker.Interpret("MATCH (a:A {id: 3}) DETACH DELETE a");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.w ORDER BY r.w"), (Rows{{4}, {6}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w > 0 RETURN r.w ORDER BY r.w"), (Rows{{4}, {6}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w = 8 RETURN count(r)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:B {id: 4})<-[r]-() RETURN count(r)"), (Rows{{0}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.w ORDER BY r.w"), (Rows{{1}, {4}, {5}, {6}}));
}

TEST_F(VersioningEdgeTest, BranchEdgeBetweenForkAndNativeVertices) {
  faker.Interpret("CREATE (:F {id: 1}), (:F {id: 2})");
  Fork();
  faker.Interpret("CREATE (:N {id: 3})");

  faker.Interpret("MATCH (f:F {id: 1}), (n:N) CREATE (f)-[:FN {w: 1, x: 5}]->(n)");
  faker.Interpret("MATCH (f:F {id: 2}), (n:N) CREATE (n)-[:NF {w: 2}]->(f)");
  EXPECT_EQ(IntRows(faker, "MATCH (:F {id: 1})-[e:FN]->(:N) RETURN e.w, e.x"), (Rows{{1, 5}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:N)<-[e:FN]-(:F {id: 1}) RETURN e.w, e.x"), (Rows{{1, 5}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:N)-[e:NF]->(:F {id: 2}) RETURN e.w"), (Rows{{2}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:F {id: 2})<-[e:NF]-(:N) RETURN e.w"), (Rows{{2}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:N) RETURN outDegree(n), inDegree(n)"), (Rows{{1, 1}}));
  EXPECT_EQ(IntRows(faker, "MATCH (f:F) RETURN f.id, outDegree(f), inDegree(f) ORDER BY f.id"),
            (Rows{{1, 1, 0}, {2, 0, 1}}));

  faker.Interpret("MATCH ()-[e:FN]->() SET e.w = 10");
  faker.Interpret("MATCH ()-[e:FN]->() REMOVE e.x");
  faker.Interpret("MATCH ()-[e:NF]->() SET e += {y: 3}");
  faker.Interpret("MATCH ()-[e:NF]->() SET e.w = null");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[e:FN]->() RETURN e.w, e.x"), (Rows{{10, kNullInt}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[e:NF]->() RETURN e.w, e.y"), (Rows{{kNullInt, 3}}));
  EXPECT_EQ(EdgePropertyKeys(faker, "MATCH ()-[e:FN]->() RETURN properties(e)"), (std::set<std::string>{"w"}));

  faker.Interpret("MATCH ()-[e:FN]->() SET e = {z: 4}");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[e:FN]->() RETURN e.w, e.x, e.z"), (Rows{{kNullInt, kNullInt, 4}}));
  EXPECT_EQ(EdgePropertyKeys(faker, "MATCH ()-[e:FN]->() RETURN properties(e)"), (std::set<std::string>{"z"}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH (f:F) RETURN outDegree(f) + inDegree(f)"), (Rows{{0}, {0}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[e]->() RETURN count(e)"), (Rows{{0}}));
}

// The WITH makes the DELETE take effect before the CREATE.
TEST_F(VersioningEdgeTest, BranchCreateEdgeOnVertexDeletedEarlierInTheStatementIsRefused) {
  faker.Interpret("CREATE (:K {id: 1}), (:K {id: 2})");
  Fork();
  faker.Interpret("CREATE (:K {id: 3})");

  constexpr auto kRefusal = "Trying to create an edge on a deleted node.";
  for (const char *deleted : {"DELETE b", "DETACH DELETE b"}) {
    for (const char *create : {"CREATE (a)-[:N]->(b)", "CREATE (b)-[:N]->(a)"}) {
      auto const query = [&](int id) {
        return "MATCH (a:K {id: 1}), (b:K {id: " + std::to_string(id) + "}) " + deleted + " WITH a, b " + create;
      };
      for (int const id : {2, 3}) {  // a fork vertex, then a branch-native one
        SCOPED_TRACE(query(id));
        EXPECT_THAT(ThrownMessage([&] { faker.Interpret(query(id)); }), ::testing::HasSubstr(kRefusal));
        EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN n.id ORDER BY n.id"), (Rows{{1}, {2}, {3}}));
        EXPECT_EQ(IntRows(faker, "MATCH ()-[e]->() RETURN count(e)"), (Rows{{0}}));
      }
      UseMain();
      EXPECT_THAT(ThrownMessage([&] { faker.Interpret(query(2)); }), ::testing::HasSubstr(kRefusal))
          << "main refuses the same way";
      Attach(faker, "b");
    }
  }
}

// The first query binds the target in an earlier MATCH (s-t cursor); the second leaves it unbound (single-source).
// Both must see the stubbed vertex as one identity, so the 2-hop arm wins over the 3-hop one.
TEST_F(VersioningEdgeTest, BranchShortestPathsAcrossStubbedVertexHaveConsistentIdentity) {
  faker.Interpret(
      "CREATE (s:Chain {id: 1}), (m:Chain {id: 2}), (x:Chain {id: 5}), (y:Chain {id: 6}), (t:Chain {id: 4}), "
      "(s)-[:R]->(m), (s)-[:R]->(x), (x)-[:R]->(y), (y)-[:R]->(t)");
  Fork();

  faker.Interpret("MATCH (n:Chain {id: 2}) SET n.tag = 'cowd'");
  faker.Interpret("MATCH (m:Chain {id: 2}), (t:Chain {id: 4}) CREATE (m)-[:R]->(t)");

  for (const char *query :
       {"MATCH (a:Chain {id: 1}), (d:Chain {id: 4}) MATCH p = (a)-[*bfs..10]->(d) RETURN [x IN nodes(p) | x.id]",
        "MATCH p = (a:Chain {id: 1})-[*bfs..10]->(d:Chain {id: 4}) RETURN [x IN nodes(p) | x.id]"}) {
    auto stream = faker.Interpret(query);
    ASSERT_EQ(stream.GetResults().size(), 1U) << query;
    std::vector<int64_t> ids;
    for (const auto &v : stream.GetResults()[0][0].ValueList()) ids.push_back(v.ValueInt());
    EXPECT_EQ(ids, (std::vector<int64_t>{1, 2, 4})) << query;
  }
}

// The procedure-facing single-edge calls: InsertEdge and RemoveEdge on fork and native edges, and InitProperties on a
// fork edge, which Cypher never reaches.
TEST_F(VersioningEdgeTest, ProcedureFacingEdgeWritesAndDeletesOnABranch) {
  faker.Interpret("CREATE (:A)-[:R]->(:B), (:A)-[:Q {w: 1}]->(:B)");
  auto const a = Int("MATCH (n:A)-[:R]->() RETURN id(n)");
  auto const b = Int("MATCH ()-[:R]->(n:B) RETURN id(n)");
  auto const r = Int("MATCH ()-[e:R]->() RETURN id(e)");
  auto const q = Int("MATCH ()-[e:Q]->() RETURN id(e)");
  Fork();

  auto &ctx = *faker.interpreter.current_db_.branch_context();
  auto const w = db->storage()->NameToProperty("w");
  auto const n_type = db->storage()->NameToEdgeType("N");
  {
    auto diff = ctx.diff_engine().Access(ms::WRITE);
    ASSERT_TRUE(ctx.BeginTransaction(diff.get()).has_value());
    mq::DbAccessor dba{ctx.current_diff_txn(), &ctx};
    auto vertex = [&](int64_t gid) { return *dba.FindVertex(ms::Gid::FromInt(gid), ms::View::NEW); };
    auto edge = [&](int64_t gid) { return *dba.FindEdge(ms::Gid::FromInt(gid), ms::View::NEW); };

    auto fork_r = edge(r);
    std::map<ms::PropertyId, ms::PropertyValue> init{{w, ms::PropertyValue{5}}};
    ASSERT_TRUE(fork_r.InitProperties(init).value());
    EXPECT_EQ(*edge(r).GetProperty(ms::View::NEW, w), ms::PropertyValue{5});

    auto from = vertex(b);
    auto to = vertex(a);
    auto native = dba.InsertEdge(&from, &to, n_type);
    ASSERT_TRUE(native.has_value());
    EXPECT_EQ(native->branch_ctx(), &ctx);
    EXPECT_EQ(native->From().Gid().AsInt(), b);
    EXPECT_EQ(vertex(a).InDegree(ms::View::NEW).value(), 1U) << "the endpoint's adjacency includes the new edge";

    auto fork_q = edge(q);
    auto removed = dba.RemoveEdge(&fork_q);
    ASSERT_TRUE(removed.has_value() && removed->has_value());
    EXPECT_EQ((*removed)->Gid().AsInt(), q);
    auto removed_again = dba.RemoveEdge(&fork_q);
    ASSERT_TRUE(removed_again.has_value());
    EXPECT_FALSE(removed_again->has_value()) << "a second delete is a no-op";
    auto removed_native = dba.RemoveEdge(&*native);
    ASSERT_TRUE(removed_native.has_value());
    EXPECT_TRUE(removed_native->has_value());
    ASSERT_TRUE(ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx.EndTransaction();
  }
  EXPECT_EQ(IntRows(faker, "MATCH ()-[e]->() RETURN id(e), e.w"), (Rows{{r, 5}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH ()-[e]->() RETURN e.w ORDER BY type(e)"), (Rows{{1}, {kNullInt}}));
}

TEST_F(VersioningReferenceEdgeTest, BranchReferenceEdgeReadsNullAndRefusesPropertyWrites) {
  faker.Interpret("CREATE (:S)-[:E]->(:A)");
  Fork();

  EXPECT_EQ(IntRows(faker, "MATCH (:S)-[e:E]->(:A) RETURN e.w"), (Rows{{kNullInt}}));
  EXPECT_EQ(IntRows(faker, "MATCH (a:A) CREATE (a)-[e:E2]->(:B) RETURN e.w"), (Rows{{kNullInt}}));
  EXPECT_THROW(faker.Interpret("MATCH (:S)-[e:E]->(:A) SET e.w = 1"), mq::QueryRuntimeException);
  EXPECT_THROW(faker.Interpret("MATCH (:S)-[e:E]->(:A) SET e += {w: 1}"), mq::QueryRuntimeException);
  EXPECT_THROW(faker.Interpret("MATCH (a:A) CREATE (a)-[:E3 {w: 1}]->(:B)"), mq::QueryRuntimeException)
      << "a branch-native edge has no properties either";
  EXPECT_THROW(faker.Interpret("MATCH ()-[e:E2]->() SET e = {w: 1}"), mq::QueryRuntimeException);
  EXPECT_THROW(faker.Interpret("MATCH ()-[e:E2]->() SET e += {w: 1}"), mq::QueryRuntimeException);
  EXPECT_THROW(faker.Interpret("MATCH ()-[e:E2]->() SET e.w = 1"), mq::QueryRuntimeException);
  EXPECT_EQ(IntRows(faker, "MATCH ()-[e]->() RETURN count(e)"), (Rows{{2}})) << "the failed CREATE left no edge";

  UseMain();
  EXPECT_THROW(faker.Interpret("MATCH (:S)-[e:E]->(:A) SET e.w = 1"), mq::QueryRuntimeException);
  EXPECT_EQ(IntRows(faker, "MATCH ()-[e]->() RETURN count(e)"), (Rows{{1}}));
}

// A main without edge properties can hold no edge index, and the diff engine (which always has edge properties) must
// not grow one: edge-type matches on the branch stay plain scans over the branch's adjacency, which see the branch's
// deletes and native edges.
TEST_F(VersioningReferenceEdgeTest, BranchEdgeTypeMatchFollowsBranchWritesWithoutEdgeIndexes) {
  faker.Interpret(
      "CREATE (a:A {id: 1}), (b:B {id: 2}), (c:B {id: 3}), (d:B {id: 4}), (e:B {id: 5}), (a)-[:E]->(b), (a)-[:E]->(c), "
      "(b)-[:F]->(a), (d)-[:E]->(e)");
  Fork();

  auto const scan = "MATCH (x)-[:E]->(y) RETURN x.id * 10 + y.id AS e ORDER BY e";
  EXPECT_FALSE(PlanHasOperator(ExplainText(faker, scan), "ScanAllByEdgeType"));
  EXPECT_EQ(IntRows(faker, scan), (Rows{{12}, {13}, {45}}));
  faker.Interpret("MATCH (:A)-[e:E]->(:B {id: 2}) DELETE e");
  faker.Interpret("MATCH (d:B {id: 4}) DETACH DELETE d");
  faker.Interpret("MATCH (a:A), (e:B {id: 5}) CREATE (a)-[:E]->(e)");
  EXPECT_EQ(IntRows(faker, scan), (Rows{{13}, {15}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[e:E]->() RETURN count(e)"), (Rows{{2}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, scan), (Rows{{12}, {13}, {45}}));
}
