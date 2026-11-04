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

// Graph Versioning: main's constraints on a branch, and the features a branch handles differently in v1 (point index,
// parallel execution, text and vector search). Branches are created and attached through the C++ API.

#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "mg_procedure.h"
#include "query/db_accessor.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning_fixture.hpp"

using memgraph::tests::versioning_fixture::ExplainText;
using memgraph::tests::versioning_fixture::IntRows;
using memgraph::tests::versioning_fixture::PlanHasOperator;
using memgraph::tests::versioning_fixture::Rows;
using memgraph::tests::versioning_fixture::ThrownMessage;

namespace {

class VersioningConstraintsTest : public VersioningGatedTest {
 protected:
  void UseMain() { faker.interpreter.current_db_.LeaveBranch(); }

  std::string Outcome(const std::string &query) {
    try {
      faker.Interpret(query);
      return "ok";
    } catch (const std::exception &e) {
      return e.what();
    }
  }

  int64_t Count(const std::string &query) {
    auto const rows = IntRows(faker, query);
    EXPECT_EQ(rows.size(), 1U) << query;
    return rows.at(0).at(0);
  }
};

}  // namespace

// A branch commit validates main's existence constraints on every touched or created vertex (the diff engine mirrors
// none) and fails with main's error, leaving the branch unchanged.
TEST_F(VersioningConstraintsTest, BranchCommitValidatesExistenceConstraints) {
  faker.Interpret("CREATE CONSTRAINT ON (n:P) ASSERT EXISTS (n.id)");
  faker.Interpret("CREATE CONSTRAINT ON (n:P) ASSERT n.age IS TYPED INTEGER");
  faker.Interpret("CREATE (:P {id: 1, age: 10}), (:P {id: 2, age: 20}), (:Q {tag: 'q'})");
  Fork();

  constexpr auto kViolation = "Unable to commit due to existence constraint violation on :P(id)";
  EXPECT_THAT(Outcome("MATCH (n:P {id: 1}) REMOVE n.id"), ::testing::HasSubstr(kViolation));
  EXPECT_EQ(Count("MATCH (n:P) RETURN count(n.id)"), 2) << "the failed commit must leave the fork vertex unchanged";
  EXPECT_THAT(Outcome("MATCH (q:Q) SET q:P"), ::testing::HasSubstr(kViolation));
  EXPECT_EQ(Count("MATCH (n:P) RETURN count(n)"), 2);
  EXPECT_THAT(Outcome("CREATE (:P {age: 1})"), ::testing::HasSubstr(kViolation));
  EXPECT_EQ(Count("MATCH (n:P) RETURN count(n)"), 2);

  faker.Interpret("MATCH (n:P {id: 1}) SET n.age = 11, n.extra = 'e'");
  faker.Interpret("MATCH (q:Q) SET q:P, q.id = 5");
  faker.Interpret("CREATE (:P {id: 9, age: 3})");
  EXPECT_EQ(Count("MATCH (n:P) RETURN count(n)"), 4);
  EXPECT_EQ(Count("MATCH (n:P {id: 1}) RETURN n.age"), 11);
  faker.Interpret("MATCH (n:P {id: 1}) REMOVE n.age");  // type constraints do not enforce existence
  EXPECT_EQ(Count("MATCH (n:P {id: 1}) RETURN count(n.age)"), 0);

  // A vertex deleted in the same transaction is not validated.
  faker.Interpret("MATCH (n:P {id: 2}) REMOVE n.id DELETE n");
  EXPECT_EQ(Count("MATCH (n:P) RETURN count(n)"), 3);

  // Explicit transaction: the violation is detected at COMMIT.
  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (n:P {id: 9}) REMOVE n.id");
  EXPECT_THAT(Outcome("COMMIT"), ::testing::HasSubstr(kViolation));
  faker.Interpret("ROLLBACK");
  EXPECT_EQ(Count("MATCH (n:P) RETURN count(n.id)"), 3);

  // PERIODIC COMMIT commits through a different path and is validated too.
  EXPECT_THAT(Outcome("USING PERIODIC COMMIT 2 UNWIND range(1, 5) AS x CREATE (:P {age: x})"),
              ::testing::HasSubstr("PeriodicCommit failed: Unable to commit due to constraint violation"));
  EXPECT_EQ(Count("MATCH (n:P) RETURN count(n)"), 3);
  faker.Interpret("USING PERIODIC COMMIT 2 UNWIND range(1, 5) AS x CREATE (:P {id: 100 + x, age: x})");
  EXPECT_EQ(Count("MATCH (n:P) RETURN count(n)"), 8);

  UseMain();
  EXPECT_EQ(Count("MATCH (n:P) RETURN count(n)"), 2);
  EXPECT_EQ(Count("MATCH (n:P {id: 1}) RETURN n.age"), 10);
  EXPECT_EQ(Count("MATCH (n:P:Q) RETURN count(n)"), 0);
}

// A branch raises type-constraint violations at the statement (not at COMMIT) with main's own message, for fork and
// native vertices and CREATE/MERGE alike.
TEST_F(VersioningConstraintsTest, BranchTypeConstraintViolationsRaiseAtTheStatement) {
  faker.Interpret("CREATE CONSTRAINT ON (n:P) ASSERT n.age IS TYPED INTEGER");
  faker.Interpret("CREATE (:P {id: 1, age: 10}), (:Q {id: 2, age: 'x'})");
  ASSERT_TRUE(db->version_store()->CreateBranch("b", "main"));

  constexpr auto kMessage = "IS TYPED INTEGER violation on P(age)";
  std::vector<std::string> const statements{
      "MATCH (n:P) SET n.age = 'x'",
      "MATCH (n:P) SET n += {age: 'x'}",
      "MATCH (n:P) SET n = {age: 'x'}",
      "MATCH (q:Q) SET q:P",
      "CREATE (:P {age: 'x'})",
      "CREATE (:P:Z {age: 'x'})",
      "MERGE (:P {age: 'x'})",
  };
  // Inside an explicit transaction, so a violation the commit would catch does not mask a missing statement check.
  auto const in_txn = [&](std::string const &statement) {
    Outcome("BEGIN");
    auto outcome = Outcome(statement);
    Outcome("ROLLBACK");
    return outcome;
  };
  std::vector<std::string> main_messages;
  for (auto const &statement : statements) {
    main_messages.push_back(in_txn(statement));
    EXPECT_THAT(main_messages.back(), ::testing::HasSubstr(kMessage)) << statement;
  }

  Attach(faker, "b");
  for (size_t i = 0; i < statements.size(); ++i) {
    EXPECT_EQ(in_txn(statements[i]), main_messages[i]) << statements[i];
  }
  EXPECT_EQ(Count("MATCH (n:P) RETURN count(n)"), 1) << "failed statements must leave no partial write";
  EXPECT_EQ(Count("MATCH (n:P) RETURN n.age"), 10);
  EXPECT_EQ(Count("MATCH (q:P) RETURN count(q)"), 1) << "SET q:P must not have added the label";

  // A native vertex (created on the branch) is checked too; null never violates.
  faker.Interpret("CREATE (:P {id: 3, age: 3}), (:R {age: 'x'})");
  faker.Interpret("MATCH (n:P {id: 3}) SET n.age = 4");
  EXPECT_EQ(in_txn("MATCH (n:P {id: 3}) SET n.age = 'x'"), main_messages[0]);
  EXPECT_EQ(in_txn("MATCH (r:R) SET r:P"), main_messages[3]);
  EXPECT_EQ(Count("MATCH (n:P {id: 3}) RETURN n.age"), 4);
  faker.Interpret("MATCH (n:P {id: 3}) REMOVE n.age");
  EXPECT_EQ(Outcome("MATCH (n:P {id: 3}) SET n.age = 7"), "ok");

  // Explicit transaction: the statement itself fails, and everything after it behaves exactly as on main.
  auto const explicit_txn_trace = [&]() {
    std::vector<std::string> trace;
    trace.push_back(Outcome("BEGIN"));
    trace.push_back(Outcome("MATCH (n:P {id: 1}) SET n.age = 'x'"));
    trace.push_back(Outcome("MATCH (n:P {id: 1}) RETURN n.age"));
    trace.push_back(Outcome("COMMIT"));
    Outcome("ROLLBACK");
    return trace;
  };
  auto const branch_trace = explicit_txn_trace();
  UseMain();
  EXPECT_EQ(branch_trace, explicit_txn_trace());
  EXPECT_THAT(branch_trace.at(1), ::testing::HasSubstr(kMessage)) << "the SET must fail inside the txn, not at COMMIT";
}

// The commit checks main's current type constraints on everything a touched vertex holds, so a fork-time value that a
// constraint created on main later forbids fails the commit with main's statement-time message.
TEST_F(VersioningConstraintsTest, BranchCommitChecksForkValuesAgainstMainsCurrentTypeConstraints) {
  faker.Interpret("CREATE (:P {id: 1, age: 'x'})");
  Fork();
  UseMain();
  faker.Interpret("MATCH (n:P) SET n.age = 1");
  faker.Interpret("CREATE CONSTRAINT ON (n:P) ASSERT n.age IS TYPED INTEGER");
  Attach(faker, "b");

  EXPECT_THAT(Outcome("MATCH (n:P) SET n.other = 1"), ::testing::HasSubstr("IS TYPED INTEGER violation on P(age)"));
  EXPECT_EQ(Count("MATCH (n:P) RETURN count(n.other)"), 0);
  EXPECT_THAT(Outcome("USING PERIODIC COMMIT 1 MATCH (n:P) SET n.other = 1"),
              ::testing::HasSubstr("PeriodicCommit failed: Unable to commit due to constraint violation"));
  faker.Interpret("MATCH (n:P) SET n.age = 2, n.other = 1");
  EXPECT_EQ(Count("MATCH (n:P) RETURN n.other"), 1);
}

TEST_F(VersioningConstraintsTest, ShowConstraintInfoMarksUniqueAsNotEnforcedOnABranch) {
  faker.Interpret("CREATE CONSTRAINT ON (n:P) ASSERT n.id IS UNIQUE");
  faker.Interpret("CREATE CONSTRAINT ON (n:P) ASSERT EXISTS (n.id)");
  auto const types = [&] {
    std::vector<std::string> out;
    for (auto const &row : faker.Interpret("SHOW CONSTRAINT INFO").GetResults()) out.push_back(row[0].ValueString());
    return out;
  };
  EXPECT_THAT(types(), ::testing::UnorderedElementsAre("exists", "unique"));
  Fork();
  EXPECT_THAT(types(), ::testing::UnorderedElementsAre("exists", "unique (not enforced on branch)"));
}

// Point functions on a branch are answered by a scan plus filter: main's point index reflects only main's current
// state, so the planner must not use it.
TEST_F(VersioningConstraintsTest, PointFunctionsOnABranchScanInsteadOfUsingThePointIndex) {
  faker.Interpret("CREATE POINT INDEX ON :Pt(loc)");
  faker.Interpret("CREATE (:Pt {id: 1, loc: point({x: 0, y: 0})}), (:Pt {id: 2, loc: point({x: 10, y: 10})})");
  Fork();
  faker.Interpret("MATCH (n:Pt {id: 2}) SET n.loc = point({x: 1, y: 1})");
  faker.Interpret("CREATE (:Pt {id: 3, loc: point({x: 2, y: 2})})");

  std::string const distance =
      "MATCH (n:Pt) WHERE point.distance(n.loc, point({x: 0, y: 0})) < 5 RETURN n.id ORDER BY n.id";
  std::string const bbox =
      "MATCH (n:Pt) WHERE point.withinbbox(n.loc, point({x: -1, y: -1}), point({x: 3, y: 3})) RETURN n.id ORDER BY "
      "n.id";
  EXPECT_EQ(IntRows(faker, distance), (Rows{{1}, {2}, {3}}));
  EXPECT_EQ(IntRows(faker, bbox), (Rows{{1}, {2}, {3}}));
  EXPECT_FALSE(PlanHasOperator(ExplainText(faker, distance), "ScanAllByPointDistance"));
  EXPECT_FALSE(PlanHasOperator(ExplainText(faker, bbox), "ScanAllByPointWithinbbox"));

  UseMain();
  EXPECT_EQ(IntRows(faker, distance), (Rows{{1}}));
  EXPECT_TRUE(PlanHasOperator(ExplainText(faker, distance), "ScanAllByPointDistance"));
  EXPECT_TRUE(PlanHasOperator(ExplainText(faker, bbox), "ScanAllByPointWithinbbox"));
}

// Branch scans cannot be split into chunks, so a parallel query on a branch runs single-threaded on the branch view.
TEST_F(VersioningConstraintsTest, ParallelExecutionOnABranchRunsSingleThreadedOnTheBranchView) {
  faker.Interpret("CREATE INDEX ON :K");
  faker.Interpret("CREATE EDGE INDEX ON :R");
  faker.Interpret("CREATE (:K {i: 1})-[:R]->(:K {i: 2}), (:K {i: 3})");
  Fork();
  faker.Interpret("CREATE (:K {i: 4})");
  faker.Interpret("MATCH (n:K {i: 3}) DELETE n");
  faker.Interpret("MATCH (a:K {i: 1}), (b:K {i: 4}) CREATE (a)-[:R]->(b)");

  for (int run = 0; run < 2; ++run) {  // the second run takes the branch plan cache
    EXPECT_EQ(IntRows(faker, "USING PARALLEL EXECUTION MATCH (n) RETURN count(n)"), (Rows{{3}}));
    EXPECT_EQ(IntRows(faker, "USING PARALLEL EXECUTION MATCH (n:K) RETURN n.i ORDER BY n.i"), (Rows{{1}, {2}, {4}}));
    EXPECT_EQ(IntRows(faker, "USING PARALLEL EXECUTION MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{2}}));
  }

  UseMain();
  EXPECT_EQ(IntRows(faker, "USING PARALLEL EXECUTION MATCH (n:K) RETURN n.i ORDER BY n.i"), (Rows{{1}, {2}, {3}}));
  EXPECT_EQ(IntRows(faker, "USING PARALLEL EXECUTION MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{1}}));
}

// Text and vector indexes reflect only main's current state, so a search on a branch errors instead of answering stale.
TEST_F(VersioningConstraintsTest, TextAndVectorSearchOnABranchError) {
  Fork();
  auto &ctx = *faker.interpreter.current_db_.branch_context();
  auto diff = ctx.diff_engine().Access(memgraph::storage::WRITE);
  ASSERT_TRUE(ctx.BeginTransaction(diff.get()).has_value());
  memgraph::query::DbAccessor dba{ctx.current_diff_txn(), &ctx};

  constexpr auto kText = "Text search is not supported on a branch in v1.";
  constexpr auto kVector = "Vector search is not supported on a branch in v1.";
  auto const mode = text_search_mode::SPECIFIED_PROPERTIES;
  EXPECT_EQ(ThrownMessage([&] { dba.TextIndexSearch("idx", "data.x:1", mode, {}); }), kText);
  EXPECT_EQ(ThrownMessage([&] { dba.SearchEdgeTextIndex("idx", "data.x:1", mode, {}); }), kText);
  EXPECT_EQ(ThrownMessage([&] { dba.TextIndexAggregate("idx", "data.x:1", "{}"); }), kText);
  EXPECT_EQ(ThrownMessage([&] { dba.TextEdgeIndexAggregate("idx", "data.x:1", "{}"); }), kText);
  EXPECT_EQ(ThrownMessage([&] { dba.VectorIndexSearchOnNodes("idx", 1, {1.0F}); }), kVector);
  EXPECT_EQ(ThrownMessage([&] { dba.VectorIndexSearchOnEdges("idx", 1, {1.0F}); }), kVector);
  ctx.EndTransaction();
}
