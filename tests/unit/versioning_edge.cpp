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

// Keys of `properties(r)` for the single edge `query` matches.
std::set<std::string> EdgePropertyKeys(InterpreterFaker &session, const std::string &query) {
  auto stream = session.Interpret(query);
  if (stream.GetResults().size() != 1U) return {"<not one row>"};
  std::set<std::string> keys;
  for (const auto &[key, _] : stream.GetResults()[0][0].ValueMap()) keys.insert(key);
  return keys;
}

}  // namespace

// An edge created on the branch is seen from both endpoints, a later write of a fork endpoint reads through it, and
// main sees neither.
TEST_F(VersioningEdgeTest, BranchEdgeCreateAndEndpointShadowingIsUnionAware) {
  faker.Interpret("CREATE (:A {name: 'a'}), (:B {name: 'b'}), (:S {name: 's'})-[:E]->(:A {name: 'a2'})");
  Fork();

  faker.Interpret("MATCH (x:A {name: 'a'}), (y:B {name: 'b'}) CREATE (x)-[:R]->(y)");
  EXPECT_EQ(IntRows(faker, "MATCH (:A {name: 'a'})-[:R]->(y) RETURN toInteger(y.name = 'b')"), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:B {name: 'b'})<-[:R]-(x) RETURN toInteger(x.name = 'a')"), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, "MATCH (x:A {name: 'a'}) RETURN outDegree(x), inDegree(x)"), (Rows{{1, 0}}));

  faker.Interpret("MATCH (a2:A {name: 'a2'}) SET a2.name = 'a2_mod'");
  EXPECT_EQ(IntRows(faker, "MATCH (:S)-[:E]->(a2) RETURN toInteger(a2.name = 'a2_mod')"), (Rows{{1}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:S)-[:E]->(a2) RETURN toInteger(a2.name = 'a2')"), (Rows{{1}}));
}

// A Cypher-created edge takes its gid from main's counter (at or above the fork floor, never reused by main) and
// resolves by id next to a fork edge.
TEST_F(VersioningEdgeTest, BranchFindEdgeByIdResolvesForkAndBranchNativeEdges) {
  faker.Interpret("CREATE (:S)-[:E]->(:A)");
  auto const fork_edge = Int("MATCH ()-[e:E]->() RETURN id(e)");
  Fork();
  auto const floor = static_cast<int64_t>(faker.interpreter.current_db_.branch_context()->core()->edge_gid_floor());

  auto const native_edge = Int("MATCH (a:A) CREATE (a)-[e:F]->(:B) RETURN id(e)");
  EXPECT_GE(native_edge, floor);
  auto const type_by_id = [&](int64_t gid) {
    return "MATCH ()-[e]->() WHERE id(e) = " + std::to_string(gid) + " RETURN toInteger(type(e) IN ['E', 'F'])";
  };
  ASSERT_TRUE(PlanHasOperator(ExplainText(faker, type_by_id(fork_edge)), "ScanAllByEdgeId"));
  EXPECT_EQ(IntRows(faker, type_by_id(fork_edge)), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, type_by_id(native_edge)), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, type_by_id(999999)), (Rows{}));

  UseMain();
  EXPECT_GT(Int("MATCH (a:A) CREATE (a)-[e:G]->(:B) RETURN id(e)"), native_edge)
      << "main's counter already handed out the branch edge's gid";
}

// An untouched edge reads its fork value through the branched-bit fast path even when a sibling edge's write branched
// its endpoints; the bit is per edge, so a self-loop and parallel edges resolve independently.
TEST_F(VersioningEdgeTest, BranchEdgeBitFastPathIsPerEdgeObject) {
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

// SET on a fork edge is visible from both ends and in whole-map reads, keeps the other properties and leaves main
// alone.
TEST_F(VersioningEdgeTest, BranchForkEdgeSetPropertyIsVisibleBothWaysAndIsolated) {
  faker.Interpret("CREATE (:A {id: 1})-[:R {w: 1, x: 7}]->(:B {id: 2})");
  Fork();

  EXPECT_EQ(IntRows(faker, "MATCH (:A)-[r:R]->(:B) SET r.w = 2 RETURN r.w"), (Rows{{2}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:A)-[r:R]->(:B) RETURN r.w, r.x"), (Rows{{2, 7}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:B)<-[r:R]-(:A) RETURN r.w, r.x"), (Rows{{2, 7}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() RETURN count(r)"), (Rows{{1}})) << "the write adds no second edge";
  EXPECT_EQ(EdgePropertyKeys(faker, "MATCH ()-[r:R]->() RETURN properties(r)"), (std::set<std::string>{"w", "x"}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.w, r.x"), (Rows{{1, 7}}));
}

// REMOVE, SET to null, SET = {...} and SET += {...} on fork edges: removed fork properties stay gone and a re-set one
// comes back.
TEST_F(VersioningEdgeTest, BranchForkEdgeRemoveAndReplacePropertiesOverlayCorrectly) {
  faker.Interpret(
      "CREATE (:A)-[:R1 {w: 1, x: 7}]->(:B), (:A)-[:R2 {w: 1, x: 7, z: 3}]->(:B), (:A)-[:R3 {w: 1, x: 7}]->(:B)");
  Fork();

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
            (Rows{{1, 7, kNullInt}, {1, 7, kNullInt}, {1, 7, kNullInt}}));
}

// A rolled-back fork-edge write leaves the fork value and does not wedge the edge.
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

// A written fork edge, deleted directly or through a DETACH DELETE of its endpoint, is gone from both ends and from
// filtered scans while its parallel sibling stays; main keeps all of them.
TEST_F(VersioningEdgeTest, BranchDeleteWrittenForkEdgeIsGoneEverywhere) {
  faker.Interpret(
      "CREATE (a:A {id: 1}), (b:B {id: 2}), (a)-[:R {w: 1}]->(b), (a)-[:R {w: 4}]->(b), "
      "(:A {id: 3})-[:R {w: 5}]->(:B {id: 4}), (:A {id: 5})-[:R {w: 6}]->(:B {id: 6})");
  Fork();

  faker.Interpret("MATCH (:A {id: 1})-[r:R {w: 1}]->() SET r.w = 2");
  faker.Interpret("MATCH (:A {id: 1})-[r:R {w: 2}]->() DELETE r");
  EXPECT_EQ(IntRows(faker, "MATCH (:A {id: 1})-[r]->() RETURN r.w"), (Rows{{4}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:B {id: 2})<-[r]-() RETURN r.w"), (Rows{{4}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w > 0 RETURN r.w ORDER BY r.w"), (Rows{{4}, {5}, {6}}));

  faker.Interpret("MATCH (:A {id: 3})-[r:R]->() SET r.w = 8");
  faker.Interpret("MATCH (a:A {id: 3}) DETACH DELETE a");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.w ORDER BY r.w"), (Rows{{4}, {6}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:B {id: 4})<-[r]-() RETURN count(r)"), (Rows{{0}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.w ORDER BY r.w"), (Rows{{1}, {4}, {5}, {6}}));
}

// A label write and a new edge on fork vertices leave their properties at the fork value; a whole-map replace drops
// the fork properties.
TEST_F(VersioningEdgeTest, BranchStubPropertyReadsFollowFineFilterNotJustStubPresence) {
  faker.Interpret("CREATE (:L {id: 1, p: 7}), (:M {id: 2, p: 8}), (:R {id: 3, p: 9, q: 10})");
  Fork();

  faker.Interpret("MATCH (n:L) SET n:Extra");
  faker.Interpret("MATCH (a:L), (b:M) CREATE (a)-[:E]->(b)");
  EXPECT_EQ(IntRows(faker, "MATCH (n:Extra)-[:E]->(m) RETURN n.id, n.p, m.id, m.p"), (Rows{{1, 7, 2, 8}}));

  faker.Interpret("MATCH (n:R) SET n = {id: 3}");
  EXPECT_EQ(IntRows(faker, "MATCH (n:R) RETURN n.id, n.p, n.q"), (Rows{{3, kNullInt, kNullInt}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH (n:R) RETURN n.p, n.q"), (Rows{{9, 10}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[e]->() RETURN count(e)"), (Rows{{0}}));
}

// A fork vertex reached through a fork edge and leaving through a branch-created edge is one identity for both
// shortest-path cursors (bidirectional and single-source), so the 2-hop arm wins over the 3-hop one.
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
    EXPECT_EQ(native->branch_ctx_, &ctx);
    EXPECT_EQ(native->From().Gid().AsInt(), b);
    EXPECT_EQ(vertex(a).InDegree(ms::View::NEW).value(), 1U) << "the endpoint's adjacency includes the new edge";

    auto fork_q = edge(q);
    auto removed = dba.RemoveEdge(&fork_q);
    ASSERT_TRUE(removed.has_value() && removed->has_value());
    EXPECT_EQ((*removed)->Gid().AsInt(), q);
    EXPECT_FALSE(dba.RemoveEdge(&fork_q)->has_value()) << "a second delete is a no-op";
    EXPECT_TRUE(dba.RemoveEdge(&*native)->has_value());
    ASSERT_TRUE(ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx.EndTransaction();
  }
  EXPECT_EQ(IntRows(faker, "MATCH ()-[e]->() RETURN id(e), e.w"), (Rows{{r, 5}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH ()-[e]->() RETURN e.w ORDER BY type(e)"), (Rows{{1}, {kNullInt}}));
}

// With reference edges (no properties on edges) a fork or branch-created edge reads a missing property as null, and
// writing one fails on the branch as on main.
TEST_F(VersioningReferenceEdgeTest, BranchReferenceEdgeReadsNullAndRefusesPropertyWrites) {
  faker.Interpret("CREATE (:S)-[:E]->(:A)");
  Fork();

  EXPECT_EQ(IntRows(faker, "MATCH (:S)-[e:E]->(:A) RETURN e.w"), (Rows{{kNullInt}}));
  EXPECT_EQ(IntRows(faker, "MATCH (a:A) CREATE (a)-[e:E2]->(:B) RETURN e.w"), (Rows{{kNullInt}}));
  EXPECT_THROW(faker.Interpret("MATCH (:S)-[e:E]->(:A) SET e.w = 1"), mq::QueryRuntimeException);

  UseMain();
  EXPECT_THROW(faker.Interpret("MATCH (:S)-[e:E]->(:A) SET e.w = 1"), mq::QueryRuntimeException);
  EXPECT_EQ(IntRows(faker, "MATCH ()-[e]->() RETURN count(e)"), (Rows{{1}}));
}
