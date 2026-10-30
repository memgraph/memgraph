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

// Branch writes and deletes through Cypher (query::VertexAccessor/EdgeAccessor mutators, DbAccessor
// InsertVertex/InsertEdge/DetachDelete): isolation from main and sibling branches, read-your-writes, deletes, rollback
// and conflicts between sessions on one branch. Branches are created and attached through the C++ API.

#include <array>
#include <atomic>
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

// A property write must not disturb the vertex's labels or its other properties, and a later label write must not
// disturb the property; both read back on the branch.
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
    EXPECT_EQ(row[2].ValueString(), "a") << "an unchanged property reads the fork state";
  }

  faker.Interpret("CREATE (:User {id: 2, age: 50})");
  faker.Interpret("MATCH (n {id: 1}) SET n:VIP");
  EXPECT_EQ(IntRows(faker, "MATCH (n {id: 1}) RETURN n.age, toInteger('VIP' IN labels(n))"), (Rows{{99, 1}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) RETURN n.age ORDER BY n.age"), (Rows{{50}, {99}}));
}

// A vertex or edge changed on b1 reads its fork value on b2, which never sees b1's change nor main's post-fork write.
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

// Deletes and a label removal on b1 do not leak to a sibling b2 forked from the same main.
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

// A changed property reached through a traversal reads the new value, an unchanged one on the same vertex the fork
// value; main stays untouched.
TEST_F(VersioningWriteTest, BranchFinePropertyChangeReadViaTraversal) {
  faker.Interpret("CREATE (:User {id: 1})-[:E]->(:User {id: 2, age: 40, keep: 7})");
  Fork();
  faker.Interpret("MATCH (v {id: 2}) SET v.age = 99");
  EXPECT_EQ(IntRows(faker, "MATCH ({id: 1})-[:E]->(v) RETURN v.age, v.keep"), (Rows{{99, 7}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH (v {id: 2}) RETURN v.age"), (Rows{{40}}));
}

// A fork-vertex property write creates a stub for that vertex only: the other fork vertex gets none, and the stub is
// not a vertex of the branch.
TEST_F(VersioningWriteTest, BranchWriteStubsOnlyTheTouchedVertex) {
  faker.Interpret("CREATE (:A {x:1}), (:B {y:9})");
  Fork();

  faker.Interpret("MATCH (n) WHERE 'A' IN labels(n) SET n.x = 2");

  auto *ctx = faker.interpreter.current_db_.branch_context();
  size_t stub_count = 0;
  ctx->core()->stub_index().ForEachStub([&](ms::Vertex *) {
    ++stub_count;
    return true;
  });
  EXPECT_EQ(stub_count, 1U) << "only A has a stub";

  EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN n.x, n.y ORDER BY n.y"), (Rows{{kNullInt, 9}, {2, kNullInt}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n)"), (Rows{{2}}));
  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH (n:A) RETURN n.x"), (Rows{{1}}));
}

// Whole-map writes on a fork vertex (`SET n += {...}`, `SET n = {...}`) are isolated from main.
TEST_F(VersioningWriteTest, BranchWholeMapPropertyWriteOnForkVertex) {
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

// `SET n.x = 2 RETURN n.x` sees the new value although the frame's accessor predates the write.
TEST_F(VersioningWriteTest, BranchSingleStatementSetThenReturnSeesNewValue) {
  faker.Interpret("CREATE (:A {x:1})");
  Fork();
  EXPECT_EQ(IntRows(faker, "MATCH (n) SET n.x = 2 RETURN n.x"), (Rows{{2}}));
}

// A vertex created on the branch takes its gid from main's counter, and main never sees it, nor its later delete.
TEST_F(VersioningWriteTest, BranchNativeVertexCreateAndDelete) {
  faker.Interpret("CREATE (), ()");
  Fork();
  auto const floor = static_cast<int64_t>(faker.interpreter.current_db_.branch_context()->core()->vertex_gid_floor());

  faker.Interpret("CREATE (:X {id:1})");
  EXPECT_EQ(IntRows(faker, "MATCH (n:X) RETURN toInteger(id(n) >= " + std::to_string(floor) + ")"), (Rows{{1}}))
      << "a branch-native gid is drawn from main's counter, at or above the fork floor";
  faker.Interpret("MATCH (n:X) SET n.id = 2, n:Y");
  EXPECT_EQ(IntRows(faker, "MATCH (n:X:Y) RETURN n.id"), (Rows{{2}}));
  faker.Interpret("MATCH (n:X) DELETE n");
  EXPECT_EQ(IntRows(faker, "MATCH (n:X) RETURN count(n)"), (Rows{{0}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n)"), (Rows{{2}}));
}

// Periodic commits (`USING PERIODIC COMMIT`, `CALL {} IN TRANSACTIONS`) flush every batch to the branch.
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

// Deleting an object twice in one statement is a no-op the second time, as on main.
TEST_F(VersioningWriteTest, BranchDoubleDeleteIsNoOpNotCrash) {
  faker.Interpret("CREATE (:A)-[:R]->(:B), (:Iso {tag:1})");
  Fork();

  faker.Interpret("MATCH (:A)-[r:R]->(:B) DETACH DELETE r DELETE r");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[e:R]->() RETURN count(e)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE n:A OR n:B RETURN count(n)"), (Rows{{2}}));

  faker.Interpret("MATCH (n:Iso) DETACH DELETE n DELETE n");
  EXPECT_EQ(IntRows(faker, "MATCH (n:Iso) RETURN count(n)"), (Rows{{0}}));
}

TEST_F(VersioningWriteTest, BranchDeleteForkVertexIsGoneOnBranchAndPresentOnMain) {
  faker.Interpret("CREATE (:K {i:1}), (:K {i:2}), (:K {i:3})");
  Fork();

  faker.Interpret("MATCH (n:K {i:2}) DELETE n");
  EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN n.i ORDER BY n.i"), (Rows{{1}, {3}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:K {i:2}) RETURN count(n)"), (Rows{{0}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN count(n)"), (Rows{{3}}));
}

// DETACH DELETE of a fork vertex hides its edges from its untouched neighbours in both directions.
TEST_F(VersioningWriteTest, BranchDetachDeleteForkVertexHidesIncidentEdgesFromNeighbours) {
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

// A plain DELETE of a fork vertex with a visible edge fails like main; deleting the edge in the same statement lets
// it through.
TEST_F(VersioningWriteTest, BranchPlainDeleteForkVertexWithEdgesFailsLikeMain) {
  faker.Interpret("CREATE (:A)-[:R]->(:B)");
  Fork();

  ASSERT_THROW(faker.Interpret("MATCH (a:A) DELETE a"), mq::RemoveAttachedVertexException);
  EXPECT_EQ(IntRows(faker, "MATCH (a:A)-[:R]->(:B) RETURN count(a)"), (Rows{{1}}));

  faker.Interpret("MATCH (a:A)-[r:R]->(:B) DELETE r, a");
  auto stream = faker.Interpret("MATCH (n) RETURN labels(n)");
  ASSERT_EQ(stream.GetResults().size(), 1U);
  EXPECT_EQ(SortedNames(stream.GetResults()[0][0]), (std::vector<std::string>{"B"}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{0}}));
}

// DELETE of one fork edge: gone from both directions while its parallel edge stays; main is untouched.
TEST_F(VersioningWriteTest, BranchDeleteSingleForkEdgeIsGoneInBothDirections) {
  faker.Interpret("CREATE (a:A), (b:B), (a)-[:R {w:1}]->(b), (a)-[:R {w:2}]->(b)");
  Fork();

  faker.Interpret("MATCH ()-[r:R {w:1}]->() DELETE r");
  for (const char *query : {"MATCH (:A)-[r:R]->(:B) RETURN r.w", "MATCH (:B)<-[r:R]-(:A) RETURN r.w"}) {
    EXPECT_EQ(IntRows(faker, query), (Rows{{2}})) << query;
  }

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{2}}));
}

// An explicit transaction that deletes a fork vertex and a fork edge and rolls back leaves everything visible.
TEST_F(VersioningWriteTest, BranchDeleteThenRollbackRestoresEverything) {
  faker.Interpret("CREATE (:K {i:1})-[:R]->(:K {i:2})-[:R]->(:K {i:3})");
  Fork();

  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (n:K {i:3}) DETACH DELETE n");
  faker.Interpret("MATCH (:K {i:1})-[r:R]->(:K {i:2}) DELETE r");
  EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN count(n)"), (Rows{{2}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{0}}));
  faker.Interpret("ROLLBACK");

  EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN count(n)"), (Rows{{3}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{2}}));
}

// A full-scan MERGE sees a delete the same query makes, exactly as main does.
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
  // Substring of the error: a deleted-object read rather than any other failure.
  const char *message;
  // Run after the failed statement; must return the single integer `probe_expected`.
  const char *probe;
  int64_t probe_expected;
  // Committed on the branch before `query`.
  const char *branch_write = "";
};

// The endpoint cases' fork edge has a clear branched bit: only its endpoint's delete hides it.
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
    {"SetThenReturn",
     "CREATE (:P {prop: 1})",
     "MATCH (n:P) DETACH DELETE n SET n.prop = 2 RETURN n.prop",
     "deleted object",
     "MATCH (n:P) RETURN n.prop",
     1},
    {"RemoveThenReturn",
     "CREATE (:P {prop: 1})",
     "MATCH (n:P) DETACH DELETE n REMOVE n.prop RETURN n.prop",
     "deleted object",
     "MATCH (n:P) RETURN n.prop",
     1},
    {"SetLabelThenReturn",
     "CREATE (:P {prop: 1})",
     "MATCH (n:P) DETACH DELETE n SET n:X RETURN labels(n)",
     "deleted node",
     "MATCH (n:P) WHERE NOT 'X' IN labels(n) RETURN n.prop",
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

// Reading, in the same query, an entity deleted earlier in it raises as on main; the failed statement's delete is
// discarded with its transaction.
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

// SET/REMOVE after a DETACH DELETE in the same query, without a RETURN, is a silent no-op as on main.
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

TEST_F(VersioningWriteTest, BranchSetPropertyAfterRemoveRestoresIt) {
  faker.Interpret("CREATE (:P {id: 1, p: 7})");
  Fork();

  faker.Interpret("MATCH (n:P) REMOVE n.p");
  EXPECT_EQ(IntRows(faker, "MATCH (n:P) RETURN n.p"), (Rows{{kNullInt}}));
  faker.Interpret("MATCH (n:P) SET n.p = 5");
  auto stream = faker.Interpret("MATCH (n:P) RETURN n.p, keys(n)");
  ASSERT_EQ(stream.GetResults().size(), 1U);
  EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 5);
  EXPECT_EQ(SortedNames(stream.GetResults()[0][1]), (std::vector<std::string>{"id", "p"}));
}

TEST_F(VersioningWriteTest, BranchRemoveThenReAddLabelLeaksNoMarker) {
  faker.Interpret("CREATE (:P:L {id: 1})");
  Fork();

  struct Step {
    const char *write;
    std::vector<std::string> labels;
  };

  for (const auto &[write, labels] : {Step{"REMOVE n:L", {"P"}}, Step{"SET n:L", {"L", "P"}}}) {
    faker.Interpret(std::string{"MATCH (n:P) "} + write);
    auto stream = faker.Interpret("MATCH (n:P) RETURN labels(n), n:L");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(SortedNames(stream.GetResults()[0][0]), labels) << write;
    EXPECT_EQ(stream.GetResults()[0][1].ValueBool(), labels.size() == 2U) << write;
  }
}

// Removals leave no trace in any read form, and names with the `__mg_` prefix are ordinary labels and properties.
TEST_F(VersioningWriteTest, BranchRemovalsLeaveNoTraceAndMgPrefixedNamesAreOrdinary) {
  faker.Interpret("CREATE (:P:L {id: 1, p: 7})");
  Fork();

  faker.Interpret("MATCH (n:P) REMOVE n:L, n.p");
  auto stream = faker.Interpret("MATCH (n:P) RETURN labels(n), keys(n), properties(n), n{.*}");
  ASSERT_EQ(stream.GetResults().size(), 1U);
  const auto &row = stream.GetResults()[0];
  EXPECT_EQ(SortedNames(row[0]), (std::vector<std::string>{"P"}));
  EXPECT_EQ(SortedNames(row[1]), (std::vector<std::string>{"id"}));
  for (const auto idx : {2U, 3U}) {
    ASSERT_EQ(row[idx].ValueMap().size(), 1U);
    EXPECT_TRUE(row[idx].ValueMap().contains("id"));
  }

  faker.Interpret("MATCH (n:P) SET n:__mg_x, n.__mg_y = 1");
  auto after = faker.Interpret("MATCH (n:P) RETURN labels(n), keys(n)");
  ASSERT_EQ(after.GetResults().size(), 1U);
  EXPECT_EQ(SortedNames(after.GetResults()[0][0]), (std::vector<std::string>{"P", "__mg_x"}));
  EXPECT_EQ(SortedNames(after.GetResults()[0][1]), (std::vector<std::string>{"__mg_y", "id"}));

  UseMain();
  auto on_main = faker.Interpret("MATCH (n:P) RETURN labels(n), keys(n)");
  ASSERT_EQ(on_main.GetResults().size(), 1U);
  EXPECT_EQ(SortedNames(on_main.GetResults()[0][0]), (std::vector<std::string>{"L", "P"}));
  EXPECT_EQ(SortedNames(on_main.GetResults()[0][1]), (std::vector<std::string>{"id", "p"}));
}

// The branch mirrors main's enums: a native CREATE and a fork-vertex SET store and read back exact enum values.
TEST_F(VersioningWriteTest, BranchEnumMirroredFromMainSupportsNativeCreateAndSetOnForkVertex) {
  faker.Interpret("CREATE ENUM Status VALUES { Good, Bad };");
  faker.Interpret("CREATE (:X {tag: 'fork'}), (:X {tag: 'forkenum', status: Status::Good})");
  Fork();

  faker.Interpret("CREATE (:X {tag: 'native', status: Status::Good})");
  faker.Interpret("MATCH (n:X {tag: 'fork'}) SET n.status = Status::Bad");
  faker.Interpret("MATCH (n:X {tag: 'forkenum'}) SET n.other = 1");

  auto const enum_value = [](const Value &val) -> std::string {
    if (!val.IsMap()) return "<not an enum>";
    EXPECT_EQ(val.ValueMap().at("__type").ValueString(), "mg_enum");
    return val.ValueMap().at("__value").ValueString();
  };
  auto stream = faker.Interpret("MATCH (n:X) RETURN n.tag, n.status ORDER BY n.tag");
  ASSERT_EQ(stream.GetResults().size(), 3U);
  std::vector<std::string> rows;
  for (const auto &row : stream.GetResults()) rows.push_back(row[0].ValueString() + "=" + enum_value(row[1]));
  EXPECT_EQ(rows, (std::vector<std::string>{"fork=Status::Bad", "forkenum=Status::Good", "native=Status::Good"}));

  UseMain();
  auto on_main = faker.Interpret("MATCH (n:X {tag: 'forkenum'}) RETURN n.status, n.other");
  ASSERT_EQ(on_main.GetResults().size(), 1U);
  EXPECT_EQ(enum_value(on_main.GetResults()[0][0]), "Status::Good");
  EXPECT_EQ(on_main.GetResults()[0][1].type(), Value::Type::Null);
}

// The mgp-facing single-object paths: RemoveVertex/DetachRemoveVertex, SubgraphVertexAccessor::UpdateProperties and a
// fork edge's DeletedEdge*Vertex endpoints, which must stay on the branch.
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

    auto fork_edge = dba.FindEdge(ms::Gid::FromInt(edge_gid), ms::View::NEW);
    ASSERT_TRUE(fork_edge.has_value());
    auto const from = fork_edge->DeletedEdgeFromVertex();
    EXPECT_EQ(from.branch_ctx(), &ctx);
    EXPECT_EQ(*from.GetProperty(ms::View::NEW, p), ms::PropertyValue{2}) << "the endpoint reads the branch's write";
    EXPECT_EQ(fork_edge->DeletedEdgeToVertex().branch_ctx(), &ctx);

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

// DETACH DELETE counts the vertices and the fork edges it deletes in the statement's summary and, once committed, in
// the branch's fork-delete counts; an edge between two deleted vertices is counted once.
TEST_F(VersioningWriteTest, BranchDetachDeleteCountsEachDeletedObjectOnce) {
  faker.Interpret("CREATE (:A)-[:R]->(:B)-[:S]->(:C)");
  Fork();
  auto *ctx = faker.interpreter.current_db_.branch_context();

  auto [stream, qid] = faker.Prepare("MATCH (a:A), (b:B) DETACH DELETE a, b");
  faker.Pull(&stream);
  auto stats = stream.GetSummary().at("stats").ValueMap();
  EXPECT_EQ(stats["nodes-deleted"].ValueInt(), 2);
  EXPECT_EQ(stats["relationships-deleted"].ValueInt(), 2);
  EXPECT_EQ(ctx->core()->ForkDeletes(), (mv::ObjectCounts{2, 2}));
  EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n)"), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() RETURN count(r)"), (Rows{{0}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() RETURN count(r)"), (Rows{{2}}));
}

// A single fork edge deleted on its own counts as one edge and no vertex.
TEST_F(VersioningWriteTest, BranchDeleteOfAForkEdgeCountsOneEdge) {
  faker.Interpret("CREATE (:A)-[:R]->(:B)");
  Fork();
  auto *ctx = faker.interpreter.current_db_.branch_context();

  faker.Interpret("MATCH ()-[r:R]->() DELETE r");
  EXPECT_EQ(ctx->core()->ForkDeletes(), (mv::ObjectCounts{0, 1}));
}

// Writes of a fork vertex after a DETACH DELETE of it in an earlier statement of the transaction are refused, not
// silently applied to a stub.
TEST_F(VersioningWriteTest, BranchVertexWriteOfAnEarlierDeletedForkVertexIsRefused) {
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

// A branch-created edge on a fork vertex is deleted with it by a DETACH DELETE and keeps a plain DELETE from
// succeeding; it is returned with the fork edges the statement deleted.
TEST_F(VersioningWriteTest, BranchDetachDeleteOfAForkVertexDeletesItsNativeEdges) {
  faker.Interpret("CREATE (:A)-[:R]->(:B), (:C)");
  Fork();
  auto &ctx = *faker.interpreter.current_db_.branch_context();
  auto const type = db->storage()->NameToEdgeType("N");
  auto const a = IntRows(faker, "MATCH (n:A) RETURN id(n)")[0][0];
  auto const c = IntRows(faker, "MATCH (n:C) RETURN id(n)")[0][0];

  {
    auto diff = ctx.diff_engine().Access(ms::WRITE);
    ASSERT_TRUE(ctx.BeginTransaction(diff.get()).has_value());
    auto from = ctx.EnsureDiffVertex(ms::Gid::FromInt(a));
    auto to = ctx.EnsureDiffVertex(ms::Gid::FromInt(c));
    ASSERT_TRUE(from.has_value() && to.has_value());
    ASSERT_TRUE(ctx.CreateNativeEdge(*from, *to, type).has_value());
    ASSERT_TRUE(ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx.EndTransaction();
  }
  EXPECT_EQ(IntRows(faker, "MATCH (:A)-[r:N]->(:C) RETURN count(r)"), (Rows{{1}}));

  ASSERT_THROW(faker.Interpret("MATCH (a:A) DELETE a"), mq::RemoveAttachedVertexException);
  EXPECT_EQ(IntRows(faker, "MATCH (:A)-[r]->() RETURN count(r)"), (Rows{{2}}));

  auto [stream, qid] = faker.Prepare("MATCH (a:A) DETACH DELETE a");
  faker.Pull(&stream);
  auto stats = stream.GetSummary().at("stats").ValueMap();
  EXPECT_EQ(stats["nodes-deleted"].ValueInt(), 1);
  EXPECT_EQ(stats["relationships-deleted"].ValueInt(), 2);
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() RETURN count(r)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n)"), (Rows{{2}}));
  EXPECT_EQ(ctx.core()->ForkDeletes(), (mv::ObjectCounts{1, 1}));
}

// Sessions on one branch share its diff engine; A = faker and B = faker2, interleaved deterministically on one thread.
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

// A's committed SET, DELETE and CREATE are what B sees on the branch; main sees none of them.
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

// B's open transaction keeps its snapshot while A commits to the same vertex; B's next transaction sees the commit.
TEST_F(VersioningMultiSessionTest, OpenTransactionKeepsItsSnapshotWhileAnotherSessionCommits) {
  SetUpTwoSessionsOnBranch();

  faker2.Interpret("BEGIN");
  EXPECT_EQ(IntRows(faker2, kReadAll), (Rows{{1, 10}, {2, 20}}));
  faker.Interpret("MATCH (n:K {id:1}) SET n.v = 11");
  EXPECT_EQ(IntRows(faker, kReadAll), (Rows{{1, 11}, {2, 20}}));
  EXPECT_EQ(IntRows(faker2, kReadAll), (Rows{{1, 10}, {2, 20}}));
  faker2.Interpret("COMMIT");
  EXPECT_EQ(IntRows(faker2, kReadAll), (Rows{{1, 11}, {2, 20}}));
}

// Overlapping writes of the same fork vertex or fork edge, and edge creates racing a delete of an endpoint, conflict
// exactly as on main; nothing is left behind.
TEST_F(VersioningMultiSessionTest, OverlappingWritesConflictLikeMain) {
  faker.Interpret("CREATE (:K {id:1, v:10, w:5}), (:K {id:2, v:20, w:6})");
  faker.Interpret("CREATE (:A {id:1})-[:R {w:1, x:2}]->(:B {id:2})");
  CreateBranch("b");

  struct Case {
    const char *name;
    // Committed on the branch first, so the entity's stub (or the created edge's anchors) already exists; empty: the
    // race creates it.
    const char *stub_write;
    const char *a_write;
    const char *b_write;
  };

  static constexpr const char *kCreateEdge = "MATCH (a:K {id:1}), (b:K {id:2}) CREATE (a)-[:N]->(b)";

  static constexpr Case cases[] = {
      {"VertexFirstTouch", "", "MATCH (n:K {id:2}) SET n.v = 21", "MATCH (n:K {id:2}) SET n.w = 61"},
      {"VertexStubbed",
       "MATCH (n:K {id:1}) SET n.z = 0",
       "MATCH (n:K {id:1}) SET n.v = 11",
       "MATCH (n:K {id:1}) SET n.v = 12"},
      {"VertexSetThenDelete", "", "MATCH (n:K {id:2}) SET n.v = 21", "MATCH (n:K {id:2}) DELETE n"},
      {"EdgeFirstTouch", "", "MATCH ()-[r:R]->() SET r.w = 10", "MATCH ()-[r:R]->() SET r.x = 20"},
      {"EdgeStubbed",
       "MATCH ()-[r:R]->() SET r.y = 0",
       "MATCH ()-[r:R]->() SET r.w = 10",
       "MATCH ()-[r:R]->() SET r.x = 20"},
      {"EdgeDeleteThenEdgeSet", "", "MATCH ()-[r:R]->() DELETE r", "MATCH ()-[r:R]->() SET r.x = 20"},
      {"EdgeSetThenEdgeDelete", "", "MATCH ()-[r:R]->() SET r.w = 10", "MATCH ()-[r:R]->() DELETE r"},
      {"EdgeDeleteThenEdgeDelete", "", "MATCH ()-[r:R]->() DELETE r", "MATCH ()-[r:R]->() DELETE r"},
      {"EdgeDeleteThenEndpointDetachDelete", "", "MATCH ()-[r:R]->() DELETE r", "MATCH (n:A) DETACH DELETE n"},
      {"EdgeCreateThenEndpointDetachDelete", "", kCreateEdge, "MATCH (n:K {id:2}) DETACH DELETE n"},
      {"EndpointDetachDeleteThenEdgeCreate", "", "MATCH (n:K {id:2}) DETACH DELETE n", kCreateEdge},
      {"EdgeCreateThenEndpointDelete", "", kCreateEdge, "MATCH (n:K {id:2}) DELETE n"},
      {"EndpointDeleteThenEdgeCreate", "", "MATCH (n:K {id:2}) DELETE n", kCreateEdge},
      {"EdgeCreateThenEndpointDetachDeleteAnchored", kCreateEdge, kCreateEdge, "MATCH (n:K {id:2}) DETACH DELETE n"},
      {"EndpointDetachDeleteThenEdgeCreateAnchored", kCreateEdge, "MATCH (n:K {id:2}) DETACH DELETE n", kCreateEdge},
  };

  for (const auto &c : cases) {
    SCOPED_TRACE(c.name);
    Use(faker, "b");
    Use(faker2, "b");
    if (*c.stub_write != '\0') faker.Interpret(c.stub_write);
    const auto branch_outcome = OverlappingWriteOutcome(faker, faker2, c.a_write, c.b_write);

    UseMain(faker);
    UseMain(faker2);
    const auto main_outcome = OverlappingWriteOutcome(faker, faker2, c.a_write, c.b_write);

    EXPECT_THAT(main_outcome, ::testing::HasSubstr("TransactionSerializationException"));
    EXPECT_EQ(branch_outcome, main_outcome);
  }

  Use(faker, "b");
  EXPECT_EQ(IntRows(faker, kReadAll), (Rows{{1, 10}, {2, 20}})) << "every transaction was rolled back";
  EXPECT_EQ(IntRows(faker, "MATCH (n:K {id:2}) RETURN n.w"), (Rows{{6}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:A)-[r:R]->(:B) RETURN r.w, r.x"), (Rows{{1, 2}}));
  Use(faker2, "b");
  EXPECT_NO_THROW(faker2.Interpret("MATCH (n:K {id:2}) SET n.v = 22")) << "a retried write succeeds";
  EXPECT_NO_THROW(faker2.Interpret("MATCH ()-[r:R]->() SET r.x = 21"));
  EXPECT_EQ(IntRows(faker, "MATCH (n:K {id:2}) RETURN n.v"), (Rows{{22}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.w, r.x"), (Rows{{1, 21}}));
}

// A transaction that began before another committed a delete of a fork vertex (or an edge created on it) conflicts
// with the late write; a write to an unrelated vertex does not. Each case runs on a fresh branch of main.
TEST_F(VersioningMultiSessionTest, ForkVertexDeleteAndEdgeCreateConflictAcrossCommit) {
  faker.Interpret("CREATE (:K {id:1}), (:K {id:2}), (:K {id:3})");

  constexpr const char *kCreateEdge = "MATCH (a:K {id:1}), (b:K {id:2}) CREATE (a)-[:N]->(b)";

  struct Case {
    const char *committed;  // autocommitted by A after B began
    const char *late;       // run by B
    bool conflicts;
  };

  static constexpr Case cases[] = {
      {"MATCH (n:K {id:2}) DETACH DELETE n", kCreateEdge, true},
      {"MATCH (n:K {id:2}) DELETE n", kCreateEdge, true},
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

// A's uncommitted DELETE is invisible to B; B's transaction opened before A's commit never sees it.
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

// A branch transaction and main writes to the same vertex or edge never conflict; the branch keeps the fork value of
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

// Reads, then writes, in one explicit branch transaction: it reads one snapshot throughout and sees its own writes,
// other sessions see none of them before COMMIT, and a ROLLBACK leaves nothing behind.
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

// Writers of different vertices overlap and both commit; a reader that began before either keeps its snapshot, one
// that begins afterwards sees both.
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

// Bounded stress: two writer sessions move one unit between two vertices in overlapping explicit transactions,
// retrying after a serialization failure; reader sessions must see a total of 100 in every transaction, one
// unchanging pair across its statements, and no committed transfer may be lost.
TEST_F(VersioningMultiSessionTest, ConcurrentTransfersLoseNoUpdateAndReadersSeeAConstantTotal) {
  constexpr int kTransfers = 100;
  faker.Interpret("CREATE (:K {id:1, v:100}), (:K {id:2, v:0})");
  Fork();
  auto sessions = SessionsOnBranch(4);
  std::atomic<int> writers_left{2};

  {
    std::vector<std::jthread> threads;
    for (int w = 0; w < 2; ++w) {
      threads.emplace_back([&session = *sessions[w], &writers_left] {
        for (int done = 0; done < kTransfers;) {
          try {
            session.Interpret("BEGIN");
            session.Interpret("MATCH (a:K {id:1}), (b:K {id:2}) SET a.v = a.v - 1, b.v = b.v + 1");
            std::this_thread::yield();
            session.Interpret("COMMIT");
            ++done;
          } catch (const memgraph::query::TransactionSerializationException &) {
            session.Interpret("ROLLBACK");
          }
        }
        --writers_left;
      });
    }
    for (int r = 2; r < 4; ++r) {
      threads.emplace_back([&session = *sessions[r], &writers_left] {
        int64_t last_moved = 0;
        do {
          session.Interpret("BEGIN");
          auto const first = IntRows(session, "MATCH (n:K) RETURN sum(n.v)");
          auto const pair = IntRows(session, "MATCH (n:K) RETURN n.v ORDER BY n.id");
          auto const again = IntRows(session, "MATCH (n:K) RETURN sum(n.v)");
          session.Interpret("COMMIT");
          ASSERT_EQ(first, (Rows{{100}})) << "a half-applied transfer was visible";
          ASSERT_EQ(again, first);
          ASSERT_EQ(pair.size(), 2U);
          ASSERT_EQ(pair[0][0] + pair[1][0], 100);
          ASSERT_GE(pair[1][0], last_moved) << "committed transfers went backwards";
          last_moved = pair[1][0];
        } while (writers_left.load() > 0);
      });
    }
  }

  EXPECT_EQ(IntRows(faker, kReadAll), (Rows{{1, 100 - 2 * kTransfers}, {2, 2 * kTransfers}}))
      << "every committed transfer is applied exactly once";
}

// Edge writes keep both endpoints in step: inside the writing transaction, after a rollback, and after a commit; a
// reader whose transaction began earlier keeps seeing the old edges from both ends.
TEST_F(VersioningMultiSessionTest, EdgeWritesKeepBothEndpointsConsistentForWriterAndOlderReader) {
  faker.Interpret("CREATE (:A {id:1})-[:R {w:1}]->(:B {id:2})");
  Fork();
  Attach(faker2, "b");

  // (out of A: count, sum of w; into B: count, sum of w) as the session sees them.
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

// Bounded stress over edges: two writer sessions move one unit between the weights of two fork edges, retrying after
// a serialization failure; readers must see a total of 100 from both endpoints in every transaction, and no
// committed transfer may be lost.
TEST_F(VersioningMultiSessionTest, ConcurrentEdgeTransfersLoseNoUpdateAndBothEndpointsStayConsistent) {
  constexpr int kTransfers = 100;
  faker.Interpret("CREATE (a:A {id:1}), (b:B {id:2}), (a)-[:R {id:1, w:100}]->(b), (a)-[:R {id:2, w:0}]->(b)");
  Fork();
  auto sessions = SessionsOnBranch(4);
  std::atomic<int> writers_left{2};

  {
    std::vector<std::jthread> threads;
    for (int w = 0; w < 2; ++w) {
      threads.emplace_back([&session = *sessions[w], &writers_left] {
        for (int done = 0; done < kTransfers;) {
          try {
            session.Interpret("BEGIN");
            session.Interpret("MATCH ()-[x:R {id:1}]->(), ()-[y:R {id:2}]->() SET x.w = x.w - 1, y.w = y.w + 1");
            std::this_thread::yield();
            session.Interpret("COMMIT");
            ++done;
          } catch (const memgraph::query::TransactionSerializationException &) {
            session.Interpret("ROLLBACK");
          }
        }
        --writers_left;
      });
    }
    for (int r = 2; r < 4; ++r) {
      threads.emplace_back([&session = *sessions[r], &writers_left] {
        do {
          session.Interpret("BEGIN");
          auto const out = IntRows(session, "MATCH (:A)-[r:R]->(:B) RETURN sum(r.w), count(r)");
          auto const in = IntRows(session, "MATCH (:B)<-[r:R]-(:A) RETURN sum(r.w), count(r)");
          auto const again = IntRows(session, "MATCH (:A)-[r:R]->(:B) RETURN sum(r.w), count(r)");
          session.Interpret("COMMIT");
          ASSERT_EQ(out, (Rows{{100, 2}})) << "a half-applied transfer was visible";
          ASSERT_EQ(in, out) << "the endpoints disagree";
          ASSERT_EQ(again, out) << "the snapshot moved inside one transaction";
        } while (writers_left.load() > 0);
      });
    }
  }

  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.id, r.w ORDER BY r.id"),
            (Rows{{1, 100 - 2 * kTransfers}, {2, 2 * kTransfers}}))
      << "every committed transfer is applied exactly once";
}

// Attaching a branch-created edge to a fork vertex and a DETACH DELETE of that vertex, in overlapping transactions on
// two sessions: exactly one of them commits, and what is left is the state of that one alone.
TEST_F(VersioningMultiSessionTest, EdgeAttachAndDetachDeleteOfForkEndpointExactlyOneCommits) {
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
