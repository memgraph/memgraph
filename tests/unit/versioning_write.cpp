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

// Branch vertex and edge writes and deletes through Cypher: isolation from main and siblings, read-your-writes,
// rollback and conflicts between sessions on one branch.

#include <array>
#include <barrier>
#include <cstdint>
#include <exception>
#include <map>
#include <string>
#include <string_view>
#include <thread>
#include <typeinfo>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "query/db_accessor.hpp"
#include "query/edge_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/graph.hpp"
#include "query/vertex_accessor.hpp"
#include "storage/v2/property_value.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/memory.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning_fixture.hpp"

using memgraph::tests::versioning_fixture::IntRows;
using memgraph::tests::versioning_fixture::kNullInt;
using memgraph::tests::versioning_fixture::Rows;
using memgraph::tests::versioning_fixture::SortedNames;
using memgraph::tests::versioning_fixture::ThrownMessage;

namespace {

namespace mq = memgraph::query;
namespace ms = memgraph::storage;
namespace mv = memgraph::versioning;
using Value = memgraph::communication::bolt::Value;

class VersioningWriteTest : public VersioningGatedTest {
 protected:
  void UseMain(InterpreterFaker &session) { session.interpreter.current_db_.LeaveBranch(); }

  void UseMain() { UseMain(faker); }

  void Use(InterpreterFaker &session, const std::string &name) {
    UseMain(session);
    Attach(session, name);
  }

  void Use(const std::string &name) { Use(faker, name); }

  // A branch of main's current state; the session stays where it is.
  void CreateBranch(const std::string &name) {
    auto created = db->version_store()->CreateBranch(name);
    ASSERT_TRUE(created) << created.error();
  }
};

}  // namespace

// A property write leaves the labels and other properties alone, and a later label write leaves the property alone.
TEST_F(VersioningWriteTest, BranchChangeFilterKindIsolation) {
  faker.Interpret("CREATE (:User {id: 1, age: 40, name: 'a'})");
  Fork();

  faker.Interpret("MATCH (n {id: 1}) SET n.age = 99");
  {
    auto stream = faker.Interpret("MATCH (n {id: 1}) RETURN n.age, labels(n), n.name");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    const auto &row = stream.GetResults()[0];
    EXPECT_EQ(row[0].ValueInt(), 99);
    EXPECT_EQ(SortedNames(row[1]), (std::vector<std::string>{"User"}));
    EXPECT_EQ(row[2].ValueString(), "a") << "an unchanged property reads the base state";
  }

  faker.Interpret("CREATE (:User {id: 2, age: 50})");
  faker.Interpret("MATCH (n {id: 1}) SET n:VIP");
  EXPECT_EQ(IntRows(faker, "MATCH (n {id: 1}) RETURN n.age, toInteger('VIP' IN labels(n))"), (Rows{{99, 1}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) RETURN n.age ORDER BY n.age"), (Rows{{50}, {99}}));
}

// b2 reads the base value of a vertex or edge changed on b1, and does not see main's post-fork write either.
TEST_F(VersioningWriteTest, BranchCrossBranchIsolationReadsOwnFork) {
  faker.Interpret("CREATE (:User {id: 1, age: 10}), (:User {id: 2, age: 20}), (:S)-[:E {w: 1}]->(:A)");

  Fork("b1");
  faker.Interpret("MATCH (n {id: 1}) SET n.age = 111, n:Changed");
  faker.Interpret("MATCH (:S)-[e:E]->(:A) SET e.w = 999");
  EXPECT_EQ(IntRows(faker, "MATCH (n {id: 1}) RETURN n.age"), (Rows{{111}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:S)-[e:E]->(:A) RETURN e.w"), (Rows{{999}}));
  UseMain();

  CreateBranch("b2");
  faker.Interpret("MATCH (n {id: 2}) SET n.age = 222");
  EXPECT_EQ(IntRows(faker, "MATCH (n {id: 2}) RETURN n.age"), (Rows{{222}}));
  Use("b2");

  EXPECT_EQ(IntRows(faker, "MATCH (n {id: 1}) RETURN n.age, toInteger('Changed' IN labels(n))"), (Rows{{10, 0}}))
      << "b2 must not see b1's changes";
  EXPECT_EQ(IntRows(faker, "MATCH (n {id: 2}) RETURN n.age"), (Rows{{20}})) << "b2 must not see main's later write";
  EXPECT_EQ(IntRows(faker, "MATCH (:S)-[e:E]->(:A) RETURN e.w"), (Rows{{1}})) << "b2 must not see b1's edge write";
}

TEST_F(VersioningWriteTest, BranchDeleteAndLabelRemovalOnOneBranchDoNotAffectUntouchedSibling) {
  faker.Interpret("CREATE (:K {i:1})-[:R]->(:K {i:2})-[:R]->(:K {i:3}), (:K:Z {i:4})");
  CreateBranch("b2");
  Fork("b1");

  faker.Interpret("MATCH (:K {i:1})-[r:R]->() DELETE r");
  faker.Interpret("MATCH (n:K {i:2}) DETACH DELETE n");
  faker.Interpret("MATCH (n:K {i:4}) REMOVE n:Z");
  EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN count(n)"), (Rows{{3}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:Z) RETURN count(n)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{0}}));

  Use("b2");
  EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN count(n)"), (Rows{{4}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:Z) RETURN n.i"), (Rows{{4}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:K {i:1})-[r:R]->(m) RETURN m.i"), (Rows{{2}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:K {i:3})<-[r:R]-(m) RETURN m.i"), (Rows{{2}}));
}

// A write creates an overlay for the touched base vertex only, and an overlay is not counted as a branch vertex.
TEST_F(VersioningWriteTest, BranchWriteOverlaysOnlyTheTouchedVertex) {
  faker.Interpret("CREATE (:A {x:1}), (:B {y:9})");
  Fork();

  faker.Interpret("MATCH (n) WHERE 'A' IN labels(n) SET n.x = 2");

  auto *ctx = faker.interpreter.current_db_.branch_context();
  size_t overlay_count = 0;
  ctx->core()->overlay_index().ForEachOverlay([&](ms::Vertex *) {
    ++overlay_count;
    return true;
  });
  EXPECT_EQ(overlay_count, 1U) << "only A has an overlay";

  EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN n.x, n.y ORDER BY n.y"), (Rows{{kNullInt, 9}, {2, kNullInt}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n)"), (Rows{{2}}));
  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH (n:A) RETURN n.x"), (Rows{{1}}));
}

TEST_F(VersioningWriteTest, BranchWholeMapPropertyWriteOnBaseVertex) {
  faker.Interpret("CREATE (:A {x:1}), (:P {id: 1, p: 7, q: 8})");
  Fork();

  faker.Interpret("MATCH (n:A) SET n += {y: 2}");
  EXPECT_EQ(IntRows(faker, "MATCH (n:A) RETURN n.x, n.y"), (Rows{{1, 2}}));

  faker.Interpret("MATCH (n:P) SET n = {x: 1}");
  {
    auto stream = faker.Interpret("MATCH (n:P) RETURN keys(n), n.x");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(SortedNames(stream.GetResults()[0][0]), (std::vector<std::string>{"x"}));
    EXPECT_EQ(stream.GetResults()[0][1].ValueInt(), 1);
  }
  EXPECT_EQ(IntRows(faker, "MATCH (n:P) RETURN n.p"), (Rows{{kNullInt}})) << "a single lookup takes GetProperty";

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH (n:A) RETURN n.y"), (Rows{{kNullInt}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:P) RETURN n.id, n.p, n.q, n.x"), (Rows{{1, 7, 8, kNullInt}}));
}

// The frame's accessor predates the SET, yet the same statement's RETURN reads the new value.
TEST_F(VersioningWriteTest, BranchSingleStatementSetThenReturnSeesNewValue) {
  faker.Interpret("CREATE (:A {x:1})");
  Fork();
  EXPECT_EQ(IntRows(faker, "MATCH (n) SET n.x = 2 RETURN n.x"), (Rows{{2}}));
}

// A branch-created vertex takes its gid from main's counter; main sees neither it nor its delete.
TEST_F(VersioningWriteTest, BranchNativeVertexCreateAndDelete) {
  faker.Interpret("CREATE (), ()");
  Fork();
  auto const floor = static_cast<int64_t>(faker.interpreter.current_db_.branch_context()->core()->vertex_gid_floor());

  faker.Interpret("CREATE (:X {id:1})");
  EXPECT_EQ(IntRows(faker, "MATCH (n:X) RETURN toInteger(id(n) >= " + std::to_string(floor) + ")"), (Rows{{1}}))
      << "a branch-native gid is drawn from main's counter, at or above the fork floor";
  auto const native_id = IntRows(faker, "MATCH (n:X) RETURN id(n)")[0][0];
  faker.Interpret("MATCH (n:X) SET n.id = 2, n:Y");
  EXPECT_EQ(IntRows(faker, "MATCH (n:X:Y) RETURN n.id"), (Rows{{2}}));
  faker.Interpret("MATCH (n:X) DELETE n");
  EXPECT_EQ(IntRows(faker, "MATCH (n:X) RETURN count(n)"), (Rows{{0}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n)"), (Rows{{2}}));
  faker.Interpret("CREATE (:Later)");
  EXPECT_NE(IntRows(faker, "MATCH (n:Later) RETURN id(n)")[0][0], native_id)
      << "main's next gid is past the one the branch drew";
}

TEST_F(VersioningWriteTest, BranchPeriodicCommitWritesToBranch) {
  faker.Interpret("CREATE (:F {id: 0})");
  Fork();

  faker.Interpret("USING PERIODIC COMMIT 10 UNWIND range(1, 100) AS x CREATE (:Batch {id: x})");
  faker.Interpret("UNWIND range(1, 20) AS x CALL { WITH x MATCH (f:F) SET f.id = x } IN TRANSACTIONS OF 5 ROWS");
  EXPECT_EQ(IntRows(faker, "MATCH (n:Batch) RETURN count(n)"), (Rows{{100}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:F) RETURN n.id"), (Rows{{20}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH (n:Batch) RETURN count(n)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:F) RETURN n.id"), (Rows{{0}}));
  Use("b");
  EXPECT_EQ(IntRows(faker, "MATCH (n:Batch) RETURN count(n)"), (Rows{{100}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:F) RETURN n.id"), (Rows{{20}}));
}

TEST_F(VersioningWriteTest, BranchDoubleDeleteIsNoOpNotCrash) {
  faker.Interpret("CREATE (:A)-[:R]->(:B), (:Iso {tag:1})");
  Fork();

  faker.Interpret("MATCH (:A)-[r:R]->(:B) DETACH DELETE r DELETE r");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[e:R]->() RETURN count(e)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE n:A OR n:B RETURN count(n)"), (Rows{{2}}));

  faker.Interpret("MATCH (n:Iso) DETACH DELETE n DELETE n");
  EXPECT_EQ(IntRows(faker, "MATCH (n:Iso) RETURN count(n)"), (Rows{{0}}));
}

TEST_F(VersioningWriteTest, BranchDetachDeleteBaseVertexHidesIncidentEdgesFromNeighbours) {
  faker.Interpret("CREATE (:N {n:1})-[:R]->(:X)-[:S]->(:N {n:2})");
  Fork();

  faker.Interpret("MATCH (x:X) DETACH DELETE x");
  EXPECT_EQ(IntRows(faker, "MATCH (:N {n:1})-[r]->() RETURN count(r)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:N {n:2})<-[r]-() RETURN count(r)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:N) OPTIONAL MATCH (n)-[r]-() RETURN n.n, count(r) ORDER BY n.n"),
            (Rows{{1, 0}, {2, 0}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() RETURN count(r)"), (Rows{{0}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH (n:N) OPTIONAL MATCH (n)-[r]-() RETURN n.n, count(r) ORDER BY n.n"),
            (Rows{{1, 1}, {2, 1}}));
}

TEST_F(VersioningWriteTest, BranchPlainDeleteBaseVertexWithEdgesFailsLikeMain) {
  faker.Interpret("CREATE (:A)-[:R]->(:B)");
  Fork();

  ASSERT_THROW(faker.Interpret("MATCH (a:A) DELETE a"), mq::RemoveAttachedVertexException);
  ASSERT_THROW(faker.Interpret("MATCH (b:B) DELETE b"), mq::RemoveAttachedVertexException);
  EXPECT_EQ(IntRows(faker, "MATCH (a:A)-[:R]->(:B) RETURN count(a)"), (Rows{{1}}));

  faker.Interpret("MATCH (a:A)-[r:R]->(:B) DELETE r, a");
  auto stream = faker.Interpret("MATCH (n) RETURN labels(n)");
  ASSERT_EQ(stream.GetResults().size(), 1U);
  EXPECT_EQ(SortedNames(stream.GetResults()[0][0]), (std::vector<std::string>{"B"}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{0}}));
}

TEST_F(VersioningWriteTest, BranchDeleteSingleBaseEdgeIsGoneInBothDirections) {
  faker.Interpret("CREATE (a:A), (b:B), (a)-[:R {w:1}]->(b), (a)-[:R {w:2}]->(b)");
  Fork();

  faker.Interpret("MATCH ()-[r:R {w:1}]->() DELETE r");
  for (const char *query : {"MATCH (:A)-[r:R]->(:B) RETURN r.w", "MATCH (:B)<-[r:R]-(:A) RETURN r.w"}) {
    EXPECT_EQ(IntRows(faker, query), (Rows{{2}})) << query;
  }

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{2}}));
}

TEST_F(VersioningWriteTest, BranchDeleteThenRollbackRestoresEverything) {
  faker.Interpret("CREATE (:K {i:1})-[:R]->(:K {i:2})-[:R]->(:K {i:3})");
  Fork();
  auto *ctx = faker.interpreter.current_db_.branch_context();

  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (n:K {i:3}) DETACH DELETE n");
  faker.Interpret("MATCH (:K {i:1})-[r:R]->(:K {i:2}) DELETE r");
  EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN count(n)"), (Rows{{2}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{0}}));
  faker.Interpret("ROLLBACK");

  EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN count(n)"), (Rows{{3}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{2}}));
  EXPECT_EQ(ctx->core()->BaseDeletes(), (mv::ObjectCounts{0, 0})) << "a rolled-back delete is never counted";
}

TEST_F(VersioningWriteTest, BranchScanSeesSameQueryDeleteLikeMain) {
  faker.Interpret("CREATE (:A {i:1}), (:A {i:2}), (:A {i:3}), (:A {i:4})");
  Fork();

  const std::string full_scan = "MATCH (z:A {i: 4}) MERGE (n) FOREACH (x IN [1] | DELETE z) RETURN count(n)";
  const auto on_branch = IntRows(faker, full_scan);
  UseMain();
  EXPECT_EQ(on_branch, IntRows(faker, full_scan));
}

namespace {

struct DeletedReadCase {
  const char *name;
  const char *seed;
  const char *query;
  // Expected error substring, so a different failure does not pass.
  const char *message;
  // Run after the failed statement to show the seed is intact; returns the single integer `probe_expected`.
  const char *probe;
  int64_t probe_expected;
  // Committed on the branch before `query`.
  const char *branch_write = "";
};

// The *Endpoint* cases read a base edge with a clear branched bit, hidden only by its endpoint's delete.
constexpr DeletedReadCase kDeletedReadCases[] = {
    {"VertexProperty",
     "CREATE (:P {prop: 1})",
     "MATCH (n:P) DETACH DELETE n RETURN n.prop",
     "deleted object",
     "MATCH (n:P) RETURN n.prop",
     1},
    {"VertexLabels",
     "CREATE (:P:Q)",
     "MATCH (n:P) DETACH DELETE n RETURN labels(n)",
     "deleted node",
     "MATCH (n:P:Q) RETURN count(n)",
     1},
    {"EdgeProperty",
     "CREATE (:S)-[:E {w: 1}]->(:A)",
     "MATCH (:S)-[e:E]->(:A) DELETE e RETURN e.w",
     "deleted object",
     "MATCH (:S)-[e:E]->(:A) RETURN e.w",
     1},
    {"FromEndpointEdgeProperty",
     "CREATE (:A)-[:R {p: 1}]->(:B)",
     "MATCH (a:A)-[e:R]->(:B) DETACH DELETE a RETURN e.p",
     "deleted object",
     "MATCH (:A)-[e:R]->(:B) RETURN e.p",
     1},
    {"ToEndpointEdgeProperty",
     "CREATE (:A)-[:R {p: 1}]->(:B)",
     "MATCH (:A)-[e:R]->(b:B) DETACH DELETE b RETURN e.p",
     "deleted object",
     "MATCH (:A)-[e:R]->(:B) RETURN e.p",
     1},
    {"FromEndpointEdgeProperties",
     "CREATE (:A)-[:R {p: 1}]->(:B)",
     "MATCH (a:A)-[e:R]->(:B) DETACH DELETE a RETURN properties(e)",
     "deleted object",
     "MATCH (:A)-[e:R]->(:B) RETURN e.p",
     1},
    {"ToEndpointEdgeProperties",
     "CREATE (:A)-[:R {p: 1}]->(:B)",
     "MATCH (:A)-[e:R]->(b:B) DETACH DELETE b RETURN properties(e)",
     "deleted object",
     "MATCH (:A)-[e:R]->(:B) RETURN e.p",
     1},
    // The edge already has an overlay version: the failed DELETE leaves the branch's value.
    {"EdgeModifiedThenDeleted",
     "CREATE (:S)-[:E {w: 1}]->(:A)",
     "MATCH (:S)-[e:E]->(:A) DELETE e RETURN e.w",
     "deleted object",
     "MATCH (:S)-[e:E]->(:A) RETURN e.w",
     42,
     "MATCH (:S)-[e:E]->(:A) SET e.w = 42"},
};

class VersioningDeletedReadTest : public VersioningWriteTest, public ::testing::WithParamInterface<DeletedReadCase> {};

}  // namespace

TEST_P(VersioningDeletedReadTest, BranchReadOfEntityDeletedInSameQueryRaises) {
  const auto &c = GetParam();
  faker.Interpret(c.seed);
  Fork();
  if (*c.branch_write != '\0') faker.Interpret(c.branch_write);

  try {
    faker.Interpret(c.query);
    FAIL() << "a read of an entity deleted earlier in the same query must raise";
  } catch (const mq::QueryRuntimeException &e) {
    EXPECT_THAT(e.what(), ::testing::HasSubstr(c.message));
  }
  EXPECT_EQ(IntRows(faker, c.probe), (Rows{{c.probe_expected}}));
}

INSTANTIATE_TEST_SUITE_P(Reads, VersioningDeletedReadTest, ::testing::ValuesIn(kDeletedReadCases),
                         [](const ::testing::TestParamInfo<DeletedReadCase> &info) {
                           return std::string{info.param.name};
                         });

// Unlike the RETURN cases above, a write after DETACH DELETE with no RETURN raises nothing, as on main.
TEST_F(VersioningWriteTest, BranchDeleteThenWritePropertyIsNoOpNotError) {
  for (const char *write : {"SET n.prop = 2", "REMOVE n.prop"}) {
    SCOPED_TRACE(write);
    UseMain();
    faker.Interpret("MATCH (n) DETACH DELETE n");
    faker.Interpret("CREATE (:P {prop: 1})");
    Fork(write[0] == 'S' ? "set" : "remove");

    EXPECT_EQ(faker.Interpret(std::string{"MATCH (n:P) DETACH DELETE n "} + write).GetResults().size(), 0U);
    EXPECT_EQ(IntRows(faker, "MATCH (n:P) RETURN count(n)"), (Rows{{0}}));
  }
}

// Writing an edge deleted earlier in the same query is main's silent no-op, so the edge stays deleted; a read of it
// afterwards raises main's error and aborts the statement, so the edge stays.
TEST_F(VersioningWriteTest, BranchWriteOfAnEdgeDeletedInTheSameQueryBehavesLikeMain) {
  struct Case {
    const char *write;
    const char *outcome;
    int64_t edges_left;
  };

  constexpr const char *kNoError = "<no exception>";
  constexpr const char *kDeletedRead = "Trying to get a property from a deleted object.";
  constexpr Case cases[] = {{"SET e.w = 2", kNoError, 0},
                            {"REMOVE e.w", kNoError, 0},
                            {"SET e = {w: 2}", kNoError, 0},
                            {"SET e += {w: 2}", kNoError, 0},
                            {"SET e.w = 2 RETURN e.w", kDeletedRead, 1}};

  int index = 0;
  for (const auto &c : cases) {
    SCOPED_TRACE(c.write);
    UseMain();
    faker.Interpret("MATCH (n) DETACH DELETE n");
    faker.Interpret("CREATE (:S)-[:E {w: 1}]->(:A)");
    Fork("b" + std::to_string(index++));
    auto const query = std::string{"MATCH (:S)-[e:E]->(:A) DELETE e "} + c.write;
    constexpr auto kEdges = "MATCH ()-[e:E]->() RETURN count(e)";

    EXPECT_EQ(ThrownMessage([&] { faker.Interpret(query); }), c.outcome);
    EXPECT_EQ(IntRows(faker, kEdges), (Rows{{c.edges_left}}));
    UseMain();
    EXPECT_EQ(IntRows(faker, kEdges), (Rows{{1}})) << "main keeps its edge";
    EXPECT_EQ(ThrownMessage([&] { faker.Interpret(query); }), c.outcome) << "main";
    EXPECT_EQ(IntRows(faker, kEdges), (Rows{{c.edges_left}})) << "main";
  }
}

TEST_F(VersioningWriteTest, BranchRemovalsLeaveNoTraceReAddsRestoreAndMgPrefixedNamesAreOrdinary) {
  faker.Interpret("CREATE (:P:L {id: 1, p: 7})");
  Fork();

  faker.Interpret("MATCH (n:P) REMOVE n:L, n.p");
  auto stream = faker.Interpret("MATCH (n:P) RETURN labels(n), keys(n), properties(n), n{.*}, n:L, n.p");
  ASSERT_EQ(stream.GetResults().size(), 1U);
  const auto &row = stream.GetResults()[0];
  EXPECT_EQ(SortedNames(row[0]), (std::vector<std::string>{"P"}));
  EXPECT_EQ(SortedNames(row[1]), (std::vector<std::string>{"id"}));
  for (const auto idx : {2U, 3U}) {
    ASSERT_EQ(row[idx].ValueMap().size(), 1U);
    EXPECT_TRUE(row[idx].ValueMap().contains("id"));
  }
  EXPECT_FALSE(row[4].ValueBool());
  EXPECT_EQ(row[5].type(), Value::Type::Null);

  faker.Interpret("MATCH (n:P) SET n.p = 5");
  faker.Interpret("MATCH (n:P) SET n:L");
  auto restored = faker.Interpret("MATCH (n:P) RETURN n.p, keys(n), labels(n), n:L");
  ASSERT_EQ(restored.GetResults().size(), 1U);
  EXPECT_EQ(restored.GetResults()[0][0].ValueInt(), 5);
  EXPECT_EQ(SortedNames(restored.GetResults()[0][1]), (std::vector<std::string>{"id", "p"}));
  EXPECT_EQ(SortedNames(restored.GetResults()[0][2]), (std::vector<std::string>{"L", "P"}));
  EXPECT_TRUE(restored.GetResults()[0][3].ValueBool());

  faker.Interpret("MATCH (n:P) SET n:__mg_x, n.__mg_y = 1");
  auto after = faker.Interpret("MATCH (n:P) RETURN labels(n), keys(n)");
  ASSERT_EQ(after.GetResults().size(), 1U);
  EXPECT_EQ(SortedNames(after.GetResults()[0][0]), (std::vector<std::string>{"L", "P", "__mg_x"}));
  EXPECT_EQ(SortedNames(after.GetResults()[0][1]), (std::vector<std::string>{"__mg_y", "id", "p"}));

  UseMain();
  auto on_main = faker.Interpret("MATCH (n:P) RETURN labels(n), keys(n)");
  ASSERT_EQ(on_main.GetResults().size(), 1U);
  EXPECT_EQ(SortedNames(on_main.GetResults()[0][0]), (std::vector<std::string>{"L", "P"}));
  EXPECT_EQ(SortedNames(on_main.GetResults()[0][1]), (std::vector<std::string>{"id", "p"}));
}

// 'baseenum' holds an enum it does not write: its overlay must still read back the exact value.
TEST_F(VersioningWriteTest, BranchEnumMirroredFromMainSupportsNativeCreateAndSetOnBaseVertex) {
  faker.Interpret("CREATE ENUM Status VALUES { Good, Bad };");
  faker.Interpret("CREATE (:X {tag: 'base'}), (:X {tag: 'baseenum', status: Status::Good})");
  Fork();

  faker.Interpret("CREATE (:X {tag: 'native', status: Status::Good})");
  faker.Interpret("MATCH (n:X {tag: 'base'}) SET n.status = Status::Bad");
  faker.Interpret("MATCH (n:X {tag: 'baseenum'}) SET n.other = 1");

  auto const enum_value = [](const Value &val) -> std::string {
    if (!val.IsMap()) return "<not an enum>";
    EXPECT_EQ(val.ValueMap().at("__type").ValueString(), "mg_enum");
    return val.ValueMap().at("__value").ValueString();
  };
  auto stream = faker.Interpret("MATCH (n:X) RETURN n.tag, n.status ORDER BY n.tag");
  ASSERT_EQ(stream.GetResults().size(), 3U);
  std::vector<std::string> rows;
  for (const auto &row : stream.GetResults()) rows.push_back(row[0].ValueString() + "=" + enum_value(row[1]));
  EXPECT_EQ(rows, (std::vector<std::string>{"base=Status::Bad", "baseenum=Status::Good", "native=Status::Good"}));

  UseMain();
  auto on_main = faker.Interpret("MATCH (n:X {tag: 'baseenum'}) RETURN n.status, n.other");
  ASSERT_EQ(on_main.GetResults().size(), 1U);
  EXPECT_EQ(enum_value(on_main.GetResults()[0][0]), "Status::Good");
  EXPECT_EQ(on_main.GetResults()[0][1].type(), Value::Type::Null);
}

// mgp-facing single-object paths: RemoveVertex/DetachRemoveVertex, SubgraphVertexAccessor::UpdateProperties and
// DeletedEdge*Vertex, whose endpoints must stay on the branch.
TEST_F(VersioningWriteTest, ProcedureFacingVertexWritesAndDeletesOnABranch) {
  faker.Interpret("CREATE (:A {p: 1})-[:R]->(:B), (:Iso)");
  auto const a = IntRows(faker, "MATCH (n:A) RETURN id(n)")[0][0];
  auto const b = IntRows(faker, "MATCH (n:B) RETURN id(n)")[0][0];
  auto const iso = IntRows(faker, "MATCH (n:Iso) RETURN id(n)")[0][0];
  auto const edge_gid = IntRows(faker, "MATCH ()-[e:R]->() RETURN id(e)")[0][0];
  Fork();

  auto &ctx = *faker.interpreter.current_db_.branch_context();
  auto const p = db->storage()->NameToProperty("p");
  {
    auto diff = ctx.diff_engine().Access(ms::WRITE);
    ASSERT_TRUE(ctx.BeginTransaction(diff.get()).has_value());
    mq::DbAccessor dba{ctx.current_diff_txn(), &ctx};
    auto vertex = [&](int64_t gid) { return *dba.FindVertex(ms::Gid::FromInt(gid), ms::View::NEW); };

    mq::Graph graph{memgraph::utils::NewDeleteResource()};
    mq::SubgraphVertexAccessor sub{vertex(a), &graph};
    std::map<ms::PropertyId, ms::PropertyValue> update{{p, ms::PropertyValue{2}}};
    ASSERT_TRUE(sub.UpdateProperties(update).has_value());

    auto base_edge = dba.FindEdge(ms::Gid::FromInt(edge_gid), ms::View::NEW);
    ASSERT_TRUE(base_edge.has_value());
    auto const from = base_edge->DeletedEdgeFromVertex();
    EXPECT_EQ(from.branch_ctx(), &ctx);
    EXPECT_EQ(*from.GetProperty(ms::View::NEW, p), ms::PropertyValue{2}) << "the endpoint reads the branch's write";
    EXPECT_EQ(base_edge->DeletedEdgeToVertex().branch_ctx(), &ctx);

    auto attached = vertex(a);
    auto refused = dba.RemoveVertex(&attached);
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(refused.error(), ms::Error::VERTEX_HAS_EDGES);
    auto detached = dba.DetachRemoveVertex(&attached);
    ASSERT_TRUE(detached.has_value() && detached->has_value());
    EXPECT_EQ((*detached)->first.Gid().AsInt(), a);
    ASSERT_EQ((*detached)->second.size(), 1U);
    EXPECT_EQ((*detached)->second[0].Gid().AsInt(), edge_gid);

    auto lone = vertex(iso);
    auto removed = dba.RemoveVertex(&lone);
    ASSERT_TRUE(removed.has_value() && removed->has_value());
    EXPECT_FALSE(dba.RemoveVertex(&lone)->has_value()) << "a second delete is a no-op";

    auto native = dba.InsertVertex();
    EXPECT_TRUE(dba.RemoveVertex(&native)->has_value());
    ASSERT_TRUE(ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx.EndTransaction();
  }
  EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN id(n)"), (Rows{{b}}));
  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH (n:A)-[:R]->() RETURN n.p"), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n)"), (Rows{{3}}));
}

// Summary stats and the branch's base-delete counts count each deleted object once, including an edge between two
// deleted vertices.
TEST_F(VersioningWriteTest, BranchDetachDeleteCountsEachDeletedObjectOnce) {
  faker.Interpret("CREATE (:A)-[:R]->(:B)-[:S]->(:C), (:D)-[:T]->(:E), (:F)-[:U]->(:G)");
  Fork();
  auto *ctx = faker.interpreter.current_db_.branch_context();

  auto [stream, qid] = faker.Prepare("MATCH (a:A), (b:B) DETACH DELETE a, b, a");
  faker.Pull(&stream);
  auto stats = stream.GetSummary().at("stats").ValueMap();
  EXPECT_EQ(stats["nodes-deleted"].ValueInt(), 2);
  EXPECT_EQ(stats["relationships-deleted"].ValueInt(), 2);
  EXPECT_EQ(ctx->core()->BaseDeletes(), (mv::ObjectCounts{2, 2}));
  EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n)"), (Rows{{5}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() RETURN count(r)"), (Rows{{2}}));

  faker.Interpret("MATCH ()-[r:T]->() DELETE r");
  EXPECT_EQ(ctx->core()->BaseDeletes(), (mv::ObjectCounts{2, 3}));

  faker.Interpret("USING PERIODIC COMMIT 1 MATCH ()-[r:U]->() DELETE r");
  EXPECT_EQ(ctx->core()->BaseDeletes(), (mv::ObjectCounts{2, 4})) << "a periodic batch's deletes are counted too";

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() RETURN count(r)"), (Rows{{4}}));
}

// Writes to a base vertex deleted earlier in the transaction are refused, not applied to its overlay.
TEST_F(VersioningWriteTest, BranchVertexWriteOfAnEarlierDeletedBaseVertexIsRefused) {
  faker.Interpret("CREATE (:P {prop: 1})");
  Fork();
  auto *ctx = faker.interpreter.current_db_.branch_context();

  auto diff = ctx->diff_engine().Access(ms::WRITE);
  ASSERT_TRUE(ctx->BeginTransaction(diff.get()).has_value());
  mq::DbAccessor dba{ctx->current_diff_txn(), ctx};
  auto vertex = *dba.Vertices(ms::View::NEW).begin();
  auto const label = db->storage()->NameToLabel("X");
  auto const prop = db->storage()->NameToProperty("prop");

  auto deleted = dba.DetachRemoveVertex(&vertex);
  ASSERT_TRUE(deleted.has_value() && deleted->has_value());
  EXPECT_EQ(vertex.AddLabel(label).error(), ms::Error::DELETED_OBJECT);
  EXPECT_EQ(vertex.RemoveLabel(label).error(), ms::Error::DELETED_OBJECT);
  EXPECT_EQ(vertex.SetProperty(prop, ms::PropertyValue{2}).error(), ms::Error::DELETED_OBJECT);
  EXPECT_EQ(vertex.ClearProperties().error(), ms::Error::DELETED_OBJECT);
  std::map<ms::PropertyId, ms::PropertyValue> properties{{prop, ms::PropertyValue{3}}};
  EXPECT_EQ(vertex.InitProperties(properties).error(), ms::Error::DELETED_OBJECT);
  EXPECT_EQ(vertex.UpdateProperties(properties).error(), ms::Error::DELETED_OBJECT);
  EXPECT_FALSE(dba.DetachRemoveVertex(&vertex)->has_value()) << "a second delete is a no-op";
  ctx->EndTransaction();
}

// DETACH DELETE of a vertex whose overlay holds native edges and earlier base-edge deletes: native edges go,
// visible base edges are hidden, earlier deletes stay, each edge counts once.
TEST_F(VersioningWriteTest, BranchDetachDeleteOfABaseVertexWithNativeEdgesAndEarlierBaseEdgeDeletes) {
  faker.Interpret("CREATE (a:A)-[:R {k: 1}]->(:B), (a)-[:R {k: 2}]->(:B), (a)-[:R {k: 3}]->(:B), (:C)");
  Fork();
  auto &ctx = *faker.interpreter.current_db_.branch_context();
  auto const type = db->storage()->NameToEdgeType("N");
  auto const a = IntRows(faker, "MATCH (n:A) RETURN id(n)")[0][0];
  auto const c = IntRows(faker, "MATCH (n:C) RETURN id(n)")[0][0];

  faker.Interpret("MATCH (:A)-[r:R {k: 1}]->() DELETE r");
  {
    auto diff = ctx.diff_engine().Access(ms::WRITE);
    ASSERT_TRUE(ctx.BeginTransaction(diff.get()).has_value());
    auto from = ctx.EnsureDiffVertex(ms::Gid::FromInt(a));
    auto to = ctx.EnsureDiffVertex(ms::Gid::FromInt(c));
    ASSERT_TRUE(from.has_value() && to.has_value());
    ASSERT_TRUE(ctx.CreateNativeEdge(*from, *to, type).has_value());
    ASSERT_TRUE(ctx.CreateNativeEdge(*to, *from, type).has_value());
    ASSERT_TRUE(ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx.EndTransaction();
  }
  {
    auto [lone, lone_qid] = faker.Prepare("MATCH (:C)-[r:N]->(:A) DELETE r");
    faker.Pull(&lone);
    auto lone_stats = lone.GetSummary().at("stats").ValueMap();
    EXPECT_EQ(lone_stats["relationships-deleted"].ValueInt(), 1);
  }
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:N]->() RETURN count(r)"), (Rows{{1}}))
      << "a branch-created edge deleted by itself";
  EXPECT_EQ(IntRows(faker, "MATCH (:A)-[r]->() RETURN count(r)"), (Rows{{3}})) << "two base edges and the native one";

  // C's only edge is the branch-created one, incoming.
  ASSERT_THROW(faker.Interpret("MATCH (a:A) DELETE a"), mq::RemoveAttachedVertexException);
  ASSERT_THROW(faker.Interpret("MATCH (c:C) DELETE c"), mq::RemoveAttachedVertexException);
  EXPECT_EQ(IntRows(faker, "MATCH (:A)-[r]->() RETURN count(r)"), (Rows{{3}}));

  auto [stream, qid] = faker.Prepare("MATCH (a:A) DETACH DELETE a");
  faker.Pull(&stream);
  auto stats = stream.GetSummary().at("stats").ValueMap();
  EXPECT_EQ(stats["nodes-deleted"].ValueInt(), 1);
  EXPECT_EQ(stats["relationships-deleted"].ValueInt(), 3);
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() RETURN count(r)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:B) RETURN count(n)"), (Rows{{3}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:B)<-[r]-() RETURN count(r)"), (Rows{{0}})) << "the neighbours lost their edges";
  EXPECT_EQ(ctx.core()->BaseDeletes(), (mv::ObjectCounts{1, 3})) << "the earlier delete, then the two it hid";
}

// Sessions on one branch share its diff engine; A = faker, B = faker2, interleaved on one thread.
class VersioningMultiSessionTest : public VersioningWriteTest {
 protected:
  InterpreterFaker faker2{&interpreter_context, db};

  void TearDown() override {
    // faker2 is destroyed before the base members; do not leave the context pointing at its auth checker.
    interpreter_context.auth_checker = &faker.auth_checker;
    VersioningWriteTest::TearDown();
  }

  // main: (:K {id:1, v:10, w:5}), (:K {id:2, v:20, w:6}); both sessions on branch 'b' forked from it.
  void SetUpTwoSessionsOnBranch() {
    faker.Interpret("CREATE (:K {id:1, v:10, w:5}), (:K {id:2, v:20, w:6})");
    Fork();
    Attach(faker2, "b");
  }

  static constexpr const char *kReadAll = "MATCH (n:K) RETURN n.id, n.v ORDER BY n.id";
};

namespace {
// `a` runs a_write, then `b` runs b_write, in overlapping explicit transactions; both are rolled back. Returns the type
// name of what b's write raised, or "" on success.
std::string OverlappingWriteOutcome(InterpreterFaker &a, InterpreterFaker &b, const std::string &a_write,
                                    const std::string &b_write) {
  a.Interpret("BEGIN");
  b.Interpret("BEGIN");
  a.Interpret(a_write);
  std::string outcome;
  try {
    b.Interpret(b_write);
  } catch (const std::exception &e) {
    outcome = typeid(e).name();
  }
  b.Interpret("ROLLBACK");
  a.Interpret("ROLLBACK");
  return outcome;
}
}  // namespace

TEST_F(VersioningMultiSessionTest, SecondSessionSeesFirstSessionsCommittedWrites) {
  faker.Interpret("CREATE (:K {id:1, v:10}), (:K {id:2, v:20})");
  Fork();

  faker.Interpret("MATCH (n:K {id:1}) SET n.v = 11");
  faker.Interpret("MATCH (n:K {id:2}) DELETE n");
  faker.Interpret("CREATE (:K {id:3, v:30})");

  Attach(faker2, "b");
  EXPECT_EQ(IntRows(faker2, kReadAll), (Rows{{1, 11}, {3, 30}}));
  UseMain(faker2);
  EXPECT_EQ(IntRows(faker2, kReadAll), (Rows{{1, 10}, {2, 20}}));
}

// Overlapping writes on the same base vertex or edge, or on an edge and its endpoint, conflict as on main; the losing
// transactions leave nothing behind. A base-edge delete writes only the edge, so it does not conflict with a write of
// an endpoint, another edge at it, or an edge created there (branch_conflicts = false), as main would.
TEST_F(VersioningMultiSessionTest, OverlappingWritesConflictLikeMain) {
  faker.Interpret("CREATE (:K {id:1, v:10, w:5}), (:K {id:2, v:20, w:6})");
  faker.Interpret("CREATE (a:A {id:1})-[:R {w:1, x:2}]->(:B {id:2}), (a)-[:S]->(:C {id:3})");
  CreateBranch("b");

  struct Case {
    const char *name;
    // Committed on the branch first so the entity already has an overlay; empty: the race creates it.
    const char *overlay_write;
    const char *a_write;
    const char *b_write;
    bool branch_conflicts = true;
  };

  static constexpr const char *kCreateEdge = "MATCH (a:K {id:1}), (b:K {id:2}) CREATE (a)-[:N]->(b)";

  static constexpr Case cases[] = {
      {"VertexFirstTouch", "", "MATCH (n:K {id:2}) SET n.v = 21", "MATCH (n:K {id:2}) SET n.w = 61"},
      {"VertexOverlaid",
       "MATCH (n:K {id:1}) SET n.z = 0",
       "MATCH (n:K {id:1}) SET n.v = 11",
       "MATCH (n:K {id:1}) SET n.v = 12"},
      {"VertexSetThenDelete", "", "MATCH (n:K {id:2}) SET n.v = 21", "MATCH (n:K {id:2}) DELETE n"},
      {"EdgeFirstTouch", "", "MATCH ()-[r:R]->() SET r.w = 10", "MATCH ()-[r:R]->() SET r.x = 20"},
      {"EdgeOverlaid",
       "MATCH ()-[r:R]->() SET r.y = 0",
       "MATCH ()-[r:R]->() SET r.w = 10",
       "MATCH ()-[r:R]->() SET r.x = 20"},
      {"EdgeDeleteThenEdgeSet", "", "MATCH ()-[r:R]->() DELETE r", "MATCH ()-[r:R]->() SET r.x = 20"},
      {"EdgeSetThenEdgeDelete", "", "MATCH ()-[r:R]->() SET r.w = 10", "MATCH ()-[r:R]->() DELETE r"},
      {"EdgeDeleteThenEdgeDelete", "", "MATCH ()-[r:R]->() DELETE r", "MATCH ()-[r:R]->() DELETE r"},
      {"EdgeDeleteThenEndpointDetachDelete", "", "MATCH ()-[r:R]->() DELETE r", "MATCH (n:A) DETACH DELETE n"},
      {"EdgeDeleteThenEndpointWrite", "", "MATCH ()-[r:R]->() DELETE r", "MATCH (n:B) SET n.z = 1", false},
      {"EndpointWriteThenEdgeDelete", "", "MATCH (n:B) SET n.z = 1", "MATCH ()-[r:R]->() DELETE r", false},
      {"EdgeDeleteThenEdgeDeleteAtASharedEndpoint",
       "",
       "MATCH ()-[r:R]->() DELETE r",
       "MATCH ()-[r:S]->() DELETE r",
       false},
      {"EdgeDeleteThenEdgeCreateAtAnEndpoint",
       "",
       "MATCH ()-[r:R]->() DELETE r",
       "MATCH (a:A), (k:K {id:1}) CREATE (a)-[:N]->(k)",
       false},
      {"EdgeCreateAtAnEndpointThenEdgeDelete",
       "",
       "MATCH (a:A), (k:K {id:1}) CREATE (a)-[:N]->(k)",
       "MATCH ()-[r:R]->() DELETE r",
       false},
      {"EdgeCreateThenEndpointDetachDelete", "", kCreateEdge, "MATCH (n:K {id:2}) DETACH DELETE n"},
      {"EndpointDetachDeleteThenEdgeCreate", "", "MATCH (n:K {id:2}) DETACH DELETE n", kCreateEdge},
      {"EdgeCreateThenEndpointDelete", "", kCreateEdge, "MATCH (n:K {id:2}) DELETE n"},
      {"EdgeCreateThenEndpointDetachDeleteAnchored", kCreateEdge, kCreateEdge, "MATCH (n:K {id:2}) DETACH DELETE n"},
      {"EndpointDetachDeleteThenEdgeCreateAnchored", kCreateEdge, "MATCH (n:K {id:2}) DETACH DELETE n", kCreateEdge},
  };

  for (const auto &c : cases) {
    SCOPED_TRACE(c.name);
    Use(faker, "b");
    Use(faker2, "b");
    if (*c.overlay_write != '\0') faker.Interpret(c.overlay_write);
    const auto branch_outcome = OverlappingWriteOutcome(faker, faker2, c.a_write, c.b_write);

    UseMain(faker);
    UseMain(faker2);
    const auto main_outcome = OverlappingWriteOutcome(faker, faker2, c.a_write, c.b_write);

    EXPECT_THAT(main_outcome, ::testing::HasSubstr("TransactionSerializationException"));
    EXPECT_EQ(branch_outcome, c.branch_conflicts ? main_outcome : std::string{});
  }

  Use(faker, "b");
  EXPECT_EQ(IntRows(faker, kReadAll), (Rows{{1, 10}, {2, 20}})) << "every transaction was rolled back";
  EXPECT_EQ(IntRows(faker, "MATCH (n:K {id:2}) RETURN n.w"), (Rows{{6}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:A)-[r:R]->(:B) RETURN r.w, r.x"), (Rows{{1, 2}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[n:N]->() RETURN count(n)"), (Rows{{2}}))
      << "only the two anchors' committed edges";
  Use(faker2, "b");
  EXPECT_NO_THROW(faker2.Interpret("MATCH (n:K {id:2}) SET n.v = 22")) << "a retried write succeeds";
  EXPECT_NO_THROW(faker2.Interpret("MATCH ()-[r:R]->() SET r.x = 21"));
  EXPECT_EQ(IntRows(faker, "MATCH (n:K {id:2}) RETURN n.v"), (Rows{{22}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.w, r.x"), (Rows{{1, 21}}));
}

// The overlapping writes that do not conflict on a branch leave both effects when both commit, and only the other
// transaction's effect when the base-edge deleter aborts.
TEST_F(VersioningMultiSessionTest, NonConflictingOverlappingWritesCommitOrAbortIndependently) {
  faker.Interpret("CREATE (:K {id:1, v:10, w:5}), (:K {id:2, v:20, w:6})");
  faker.Interpret("CREATE (a:A {id:1})-[:R {w:1, x:2}]->(:B {id:2}), (a)-[:S]->(:C {id:3})");
  const auto r_gid = IntRows(faker, "MATCH ()-[r:R]->() RETURN id(r)")[0][0];

  struct Case {
    const char *name;
    const char *a_write;
    const char *b_write;
    bool a_deletes_r;  // the transaction that aborts in the abort run
    int b_z;           // B.z left by the other transaction
    int n_edges;       // :N edges A->K left by the other transaction
    bool s_deleted;
  };

  static constexpr const char *kDeleteR = "MATCH ()-[r:R]->() DELETE r";
  static constexpr const char *kCreateN = "MATCH (a:A), (k:K {id:1}) CREATE (a)-[:N]->(k)";
  static constexpr Case cases[] = {
      {"EdgeDeleteThenEndpointWrite", kDeleteR, "MATCH (n:B) SET n.z = 1", true, 1, 0, false},
      {"EndpointWriteThenEdgeDelete", "MATCH (n:B) SET n.z = 1", kDeleteR, false, 1, 0, false},
      {"EdgeDeleteThenEdgeDeleteAtASharedEndpoint", kDeleteR, "MATCH ()-[r:S]->() DELETE r", true, 0, 0, true},
      {"EdgeDeleteThenEdgeCreateAtAnEndpoint", kDeleteR, kCreateN, true, 0, 1, false},
      {"EdgeCreateAtAnEndpointThenEdgeDelete", kCreateN, kDeleteR, false, 0, 1, false},
  };

  const auto count = [&](const std::string &pattern) {
    return IntRows(faker, "MATCH " + pattern + " RETURN count(r)");
  };
  int branch_index = 0;
  for (const bool deleter_aborts : {false, true}) {
    for (const auto &c : cases) {
      SCOPED_TRACE(std::string(c.name) + (deleter_aborts ? " (deleter aborts)" : " (both commit)"));
      const auto name = "nc" + std::to_string(branch_index++);
      CreateBranch(name);
      Use(faker, name);
      Use(faker2, name);

      faker.Interpret("BEGIN");
      faker2.Interpret("BEGIN");
      faker.Interpret(c.a_write);
      EXPECT_NO_THROW(faker2.Interpret(c.b_write));
      auto &deleter = c.a_deletes_r ? faker : faker2;
      auto &other = c.a_deletes_r ? faker2 : faker;
      other.Interpret("COMMIT");
      deleter.Interpret(deleter_aborts ? "ROLLBACK" : "COMMIT");

      const Rows r_visible{{deleter_aborts ? 1 : 0}};
      EXPECT_EQ(count("(:A)-[r:R]->(:B)"), r_visible);
      EXPECT_EQ(count("(:B)<-[r:R]-(:A)"), r_visible);
      EXPECT_EQ(count("(:A)-[r:R]-()"), r_visible);
      EXPECT_EQ(count("(:B)-[r:R]-()"), r_visible);
      EXPECT_EQ(count("()-[r]->() WHERE id(r) = " + std::to_string(r_gid)), r_visible);
      EXPECT_EQ(IntRows(faker, "MATCH (n:B) RETURN coalesce(n.z, 0)"), (Rows{{c.b_z}}));
      EXPECT_EQ(count("(:A)-[r:N]->(:K)"), (Rows{{c.n_edges}}));
      EXPECT_EQ(count("(:K)<-[r:N]-(:A)"), (Rows{{c.n_edges}}));
      EXPECT_EQ(count("(:A)-[r:S]->(:C)"), (Rows{{c.s_deleted ? 0 : 1}}));
      EXPECT_EQ(count("(:C)<-[r:S]-(:A)"), (Rows{{c.s_deleted ? 0 : 1}}));
    }
  }
}

// A transaction that began before another committed a DETACH DELETE of a base vertex, or an edge create on it,
// conflicts with its own late edge create or delete there; the same late write on an unrelated vertex does not.
TEST_F(VersioningMultiSessionTest, BaseVertexDeleteAndEdgeCreateConflictAcrossCommit) {
  faker.Interpret("CREATE (:K {id:1}), (:K {id:2}), (:K {id:3})");

  constexpr const char *kCreateEdge = "MATCH (a:K {id:1}), (b:K {id:2}) CREATE (a)-[:N]->(b)";

  struct Case {
    const char *committed;
    const char *late;
    bool conflicts;
  };

  static constexpr Case cases[] = {
      {"MATCH (n:K {id:2}) DETACH DELETE n", kCreateEdge, true},
      {kCreateEdge, "MATCH (n:K {id:2}) DETACH DELETE n", true},
      {kCreateEdge, "MATCH (n:K {id:2}) DELETE n", true},
      {"MATCH (n:K {id:3}) DETACH DELETE n", kCreateEdge, false},
      {kCreateEdge, "MATCH (n:K {id:3}) DETACH DELETE n", false},
  };

  int index = 0;
  for (const auto &c : cases) {
    SCOPED_TRACE(index);
    auto const name = "c" + std::to_string(index++);
    CreateBranch(name);
    Use(faker, name);
    Use(faker2, name);

    faker2.Interpret("BEGIN");
    faker2.Interpret("MATCH (n:K) RETURN count(n)");  // B's snapshot predates A's commit
    faker.Interpret(c.committed);
    bool conflicted = false;
    try {
      faker2.Interpret(c.late);
    } catch (const mq::TransactionSerializationException &) {
      conflicted = true;
    }
    faker2.Interpret("ROLLBACK");
    EXPECT_EQ(conflicted, c.conflicts);
  }
}

// A base edge written after the DETACH DELETE of either endpoint committed, by a transaction that began before it, is
// a serialization conflict; the retry runs on the post-delete state and succeeds.
TEST_F(VersioningMultiSessionTest, BaseEdgeWriteAfterACommittedEndpointDetachDeleteConflicts) {
  faker.Interpret("CREATE (:A)-[:R {w: 1}]->(:B)");

  int index = 0;
  for (const char *endpoint : {"A", "B"}) {
    for (const char *late : {"SET r.w = 9", "DELETE r"}) {
      SCOPED_TRACE(std::string{endpoint} + " / " + late);
      auto const name = "c" + std::to_string(index++);
      CreateBranch(name);
      Use(faker, name);
      Use(faker2, name);
      auto const write = std::string{"MATCH ()-[r:R]->() "} + late;

      faker2.Interpret("BEGIN");
      EXPECT_EQ(IntRows(faker2, "MATCH ()-[r:R]->() RETURN r.w"), (Rows{{1}}));  // B's snapshot predates A's commit
      faker.Interpret(std::string{"MATCH (n:"} + endpoint + ") DETACH DELETE n");
      EXPECT_THROW(faker2.Interpret(write), mq::TransactionSerializationException);
      faker2.Interpret("ROLLBACK");

      EXPECT_NO_THROW(faker2.Interpret(write));
      EXPECT_EQ(IntRows(faker2, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{0}}));
    }
  }
}

TEST_F(VersioningMultiSessionTest, DeleteIsInvisibleToOtherSessionsUntilCommit) {
  SetUpTwoSessionsOnBranch();

  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (n:K {id:1}) DELETE n");
  EXPECT_EQ(IntRows(faker, kReadAll), (Rows{{2, 20}}));
  EXPECT_EQ(IntRows(faker2, kReadAll), (Rows{{1, 10}, {2, 20}}));
  faker2.Interpret("BEGIN");
  EXPECT_EQ(IntRows(faker2, kReadAll), (Rows{{1, 10}, {2, 20}}));

  faker.Interpret("COMMIT");
  EXPECT_EQ(IntRows(faker2, kReadAll), (Rows{{1, 10}, {2, 20}}));
  faker2.Interpret("COMMIT");
  EXPECT_EQ(IntRows(faker2, kReadAll), (Rows{{2, 20}}));
}

// Main's writes never conflict with the branch's, on a vertex or an edge; the branch reads the base value (w=5) of
// what it did not write.
TEST_F(VersioningMultiSessionTest, MainWritesNeverConflictWithBranchWrites) {
  faker.Interpret("CREATE (:K {id:1, v:10, w:5})-[:R {w:1, x:2}]->(:T)");
  Fork();

  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (n:K {id:1}) SET n.v = 11");
  faker.Interpret("MATCH ()-[r:R]->() SET r.w = 3");
  faker.Interpret("MATCH (n:K), (t:T) CREATE (n)-[:N]->(t)");
  faker2.Interpret("MATCH ()-[r:R]->() SET r.w = 98, r.x = 99");
  faker2.Interpret("MATCH (n:K {id:1}) SET n.v = 99");
  faker2.Interpret("MATCH (n:K {id:1}) SET n.w = 55");
  faker2.Interpret("MATCH (n:K {id:1}) DETACH DELETE n");

  EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN n.v, n.w"), (Rows{{11, 5}}));
  faker.Interpret("COMMIT");
  EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN n.v, n.w"), (Rows{{11, 5}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:K)-[r]->(:T) RETURN r.w, r.x ORDER BY type(r)"),
            (Rows{{kNullInt, kNullInt}, {3, 2}}));
  EXPECT_EQ(IntRows(faker2, "MATCH (n:K) RETURN count(n)"), (Rows{{0}})) << "main holds only its own writes";
}

TEST_F(VersioningMultiSessionTest, ReadsThenWritesInOneTransactionReachOtherSessionsOnlyOnCommit) {
  SetUpTwoSessionsOnBranch();
  auto const before = Rows{{1, 10}, {2, 20}};
  auto const own = Rows{{1, 11}, {3, 30}};

  for (auto const *end : {"ROLLBACK", "COMMIT"}) {
    SCOPED_TRACE(end);
    faker.Interpret("BEGIN");
    EXPECT_EQ(IntRows(faker, kReadAll), before);
    EXPECT_EQ(IntRows(faker, kReadAll), before);
    faker.Interpret("MATCH (n:K {id:1}) SET n.v = n.v + 1");
    faker.Interpret("CREATE (:K {id:3, v:30})");
    faker.Interpret("MATCH (n:K {id:2}) DELETE n");
    EXPECT_EQ(IntRows(faker, kReadAll), own);
    EXPECT_EQ(IntRows(faker, kReadAll), own);
    EXPECT_EQ(IntRows(faker2, kReadAll), before) << "uncommitted writes stay invisible";
    faker.Interpret(end);
    EXPECT_EQ(IntRows(faker2, kReadAll), std::string_view{end} == "COMMIT" ? own : before);
  }

  UseMain(faker2);
  EXPECT_EQ(IntRows(faker2, kReadAll), before) << "main never saw any of it";
}

TEST_F(VersioningMultiSessionTest, DisjointConcurrentWritersBothCommitAndAnOlderReaderSeesNeither) {
  SetUpTwoSessionsOnBranch();
  auto readers = SessionsOnBranch(1);
  auto &reader = *readers[0];
  auto const before = Rows{{1, 10}, {2, 20}};

  reader.Interpret("BEGIN");
  EXPECT_EQ(IntRows(reader, kReadAll), before);
  faker.Interpret("BEGIN");
  faker2.Interpret("BEGIN");
  faker.Interpret("MATCH (n:K {id:1}) SET n.v = 11");
  EXPECT_NO_THROW(faker2.Interpret("MATCH (n:K {id:2}) SET n.v = 21"));
  EXPECT_NO_THROW(faker.Interpret("COMMIT"));
  EXPECT_NO_THROW(faker2.Interpret("COMMIT"));

  EXPECT_EQ(IntRows(reader, kReadAll), before);
  reader.Interpret("COMMIT");
  EXPECT_EQ(IntRows(reader, kReadAll), (Rows{{1, 11}, {2, 21}}));
}

// Edge writes keep both endpoints in step: inside the writing transaction, after a rollback, and after a commit; a
// reader whose transaction began earlier keeps seeing the old edges from both ends.
TEST_F(VersioningMultiSessionTest, EdgeWritesKeepBothEndpointsConsistentForWriterAndOlderReader) {
  faker.Interpret("CREATE (:A {id:1})-[:R {w:1}]->(:B {id:2})");
  Fork();
  Attach(faker2, "b");

  // Returns {count, sum of w} of the A->B edges, after checking both endpoints and the degrees agree.
  auto const ends = [](InterpreterFaker &session) {
    auto const out = IntRows(session, "MATCH (:A)-[r]->(:B) RETURN count(r), sum(coalesce(r.w, 0))");
    auto const in = IntRows(session, "MATCH (:B)<-[r]-(:A) RETURN count(r), sum(coalesce(r.w, 0))");
    EXPECT_EQ(out, in) << "both endpoints must agree";
    EXPECT_EQ(IntRows(session, "MATCH (a:A) RETURN outDegree(a)"), (Rows{{out[0][0]}}));
    EXPECT_EQ(IntRows(session, "MATCH (b:B) RETURN inDegree(b)"), (Rows{{in[0][0]}}));
    return out;
  };
  auto const before = Rows{{1, 1}};

  faker2.Interpret("BEGIN");
  EXPECT_EQ(ends(faker2), before);

  faker.Interpret("BEGIN");
  EXPECT_EQ(ends(faker), before);
  faker.Interpret("MATCH ()-[r:R]->() SET r.w = 2");
  faker.Interpret("MATCH (a:A), (b:B) CREATE (a)-[:N {w: 5}]->(b)");
  EXPECT_EQ(ends(faker), (Rows{{2, 7}}));
  faker.Interpret("ROLLBACK");
  EXPECT_EQ(ends(faker), before) << "a rollback leaves nothing on either endpoint";

  faker.Interpret("BEGIN");
  faker.Interpret("MATCH ()-[r:R]->() DELETE r");
  faker.Interpret("MATCH (a:A), (b:B) CREATE (a)-[:N {w: 5}]->(b)");
  EXPECT_EQ(ends(faker), (Rows{{1, 5}}));
  faker.Interpret("COMMIT");
  EXPECT_EQ(ends(faker), (Rows{{1, 5}}));

  EXPECT_EQ(ends(faker2), before) << "the older reader's snapshot never moved";
  faker2.Interpret("COMMIT");
  EXPECT_EQ(ends(faker2), (Rows{{1, 5}}));
  UseMain(faker2);
  EXPECT_EQ(ends(faker2), before) << "main never saw any of it";
}

// Attaching a branch-created edge to a base vertex and a DETACH DELETE of that vertex, in overlapping transactions on
// two sessions: exactly one of them commits, and what is left is the state of that one alone.
TEST_F(VersioningMultiSessionTest, EdgeAttachAndDetachDeleteOfBaseEndpointExactlyOneCommits) {
  constexpr int kRounds = 20;
  for (int i = 0; i < kRounds; ++i)
    faker.Interpret("CREATE (:P {i: " + std::to_string(i) + "}), (:Q {i: " + std::to_string(i) + "})");
  Fork();
  auto sessions = SessionsOnBranch(2);

  for (int i = 0; i < kRounds; ++i) {
    SCOPED_TRACE(i);
    auto const n = std::to_string(i);
    std::string const attach = "MATCH (p:P {i: " + n + "}), (q:Q {i: " + n + "}) CREATE (p)-[:N]->(q)";
    std::string const detach = "MATCH (q:Q {i: " + n + "}) DETACH DELETE q";
    std::array<std::string, 2> const writes{attach, detach};
    std::array<bool, 2> committed{false, false};
    std::barrier written{2};

    {
      std::vector<std::jthread> threads;
      for (int w = 0; w < 2; ++w) {
        threads.emplace_back([&, w] {
          auto &session = *sessions[w];
          session.Interpret("BEGIN");
          bool ok = true;
          try {
            session.Interpret(writes[w]);
          } catch (const mq::TransactionSerializationException &) {
            ok = false;
          }
          written.arrive_and_wait();  // both writes are in flight before either commits
          if (ok) {
            session.Interpret("COMMIT");
            committed[w] = true;
          } else {
            session.Interpret("ROLLBACK");
          }
        });
      }
    }

    EXPECT_NE(committed[0], committed[1]) << "exactly one of the overlapping transactions commits";
    if (committed[0]) {
      EXPECT_EQ(IntRows(faker, "MATCH (:P {i: " + n + "})-[:N]->(q:Q) RETURN count(q)"), (Rows{{1}}));
      EXPECT_EQ(IntRows(faker, "MATCH (q:Q {i: " + n + "}) RETURN inDegree(q)"), (Rows{{1}}));
    } else {
      EXPECT_EQ(IntRows(faker, "MATCH (q:Q {i: " + n + "}) RETURN count(q)"), (Rows{{0}}));
      EXPECT_EQ(IntRows(faker, "MATCH (p:P {i: " + n + "}) RETURN outDegree(p)"), (Rows{{0}}));
    }
  }
}
