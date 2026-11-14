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
// text and vector search, triggers). Branches are created and attached through the C++ API.

#include <atomic>
#include <exception>
#include <string>
#include <thread>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "mg_procedure.h"
#include "query/db_accessor.hpp"
#include "query/exceptions.hpp"
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

  static std::string Outcome(InterpreterFaker &session, const std::string &query) {
    try {
      session.Interpret(query);
      return "ok";
    } catch (const std::exception &e) {
      return e.what();
    }
  }

  std::string Outcome(const std::string &query) { return Outcome(faker, query); }

  int64_t Count(const std::string &query) {
    auto const rows = IntRows(faker, query);
    EXPECT_EQ(rows.size(), 1U) << query;
    return rows.at(0).at(0);
  }
};

}  // namespace

// A branch commit enforces main's existence constraints on created and relabelled vertices (explicit transactions at
// COMMIT), skips a vertex deleted in the same statement, and a refused write leaves the branch unchanged.
TEST_F(VersioningConstraintsTest, BranchCommitValidatesExistenceConstraints) {
  faker.Interpret("CREATE CONSTRAINT ON (n:P) ASSERT EXISTS (n.id)");
  faker.Interpret("CREATE CONSTRAINT ON (n:P) ASSERT n.age IS TYPED INTEGER");
  faker.Interpret("CREATE (:P {id: 1, age: 10}), (:P {id: 2, age: 20}), (:Q {tag: 'q'})");
  Fork();

  constexpr auto kViolation = "Unable to commit due to existence constraint violation on :P(id)";
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
  ASSERT_TRUE(db->version_store()->CreateBranch("b"));

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
  EXPECT_EQ(Outcome("MATCH (n:P {id: 1}) SET n.age = null"), "ok");  // also on a fork vertex
  EXPECT_EQ(Count("MATCH (n:P {id: 1}) RETURN count(n.age)"), 0);
  faker.Interpret("MATCH (n:P {id: 1}) SET n.age = 10");

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

  // Adding a label the vertex already has is not checked at the statement, as on main.
  faker.Interpret("BEGIN");
  EXPECT_EQ(Outcome("MATCH (n:P) SET n:P"), "ok");
  faker.Interpret("ROLLBACK");

  EXPECT_THAT(Outcome("MATCH (n:P) SET n.other = 1"), ::testing::HasSubstr("IS TYPED INTEGER violation on P(age)"));
  EXPECT_EQ(Count("MATCH (n:P) RETURN count(n.other)"), 0);
  EXPECT_THAT(Outcome("USING PERIODIC COMMIT 1 MATCH (n:P) SET n.other = 1"),
              ::testing::HasSubstr("PeriodicCommit failed: Unable to commit due to constraint violation"));
  faker.Interpret("MATCH (n:P) SET n.age = 2, n.other = 1");
  EXPECT_EQ(Count("MATCH (n:P) RETURN n.other"), 1);
}

// Fork vertices are written through diff-engine stubs, which the commit must still find and validate. Every violation
// is refused with main's text and leaves branch and main unchanged; a satisfying write then commits.
TEST_F(VersioningConstraintsTest, BranchCommitValidatesForkVerticesWrittenThroughStubs) {
  faker.Interpret("CREATE CONSTRAINT ON (n:P) ASSERT EXISTS (n.id)");
  faker.Interpret("CREATE (:P {id: 1, age: 10}), (:P {id: 2, age: 20}), (:Q {tag: 'q'})");
  Fork();

  constexpr auto kViolation = "Unable to commit due to existence constraint violation on :P(id)";
  std::vector<std::string> const refused{
      "MATCH (n:P {id: 1}) REMOVE n.id",
      "MATCH (n:P {id: 1}) SET n.id = null",
      "MATCH (n:P {id: 1}) SET n = {age: 5}",
      "MATCH (q:Q) SET q:P",
      "MATCH (q:Q) SET q:P, q.age = 1",
  };
  for (auto const &statement : refused) {
    EXPECT_THAT(Outcome(statement), ::testing::HasSubstr(kViolation)) << statement;
    EXPECT_EQ(Count("MATCH (n:P) RETURN count(n.id)"), 2) << statement;
    EXPECT_EQ(Count("MATCH (n:P) RETURN count(n)"), 2) << statement;
    EXPECT_EQ(Count("MATCH (n:P {id: 1}) RETURN n.age"), 10) << statement;
  }

  // The failed commits left no stub state behind: a valid write on the same vertices commits.
  faker.Interpret("MATCH (n:P {id: 1}) REMOVE n.age");
  faker.Interpret("MATCH (q:Q) SET q:P, q.id = 7");
  EXPECT_EQ(Count("MATCH (n:P) RETURN count(n)"), 3);
  EXPECT_EQ(Count("MATCH (n:P {id: 1}) RETURN count(n.age)"), 0);

  // A fork vertex whose id is removed and put back in one transaction satisfies the constraint.
  faker.Interpret("MATCH (n:P {id: 2}) REMOVE n.id SET n.id = 2");
  EXPECT_EQ(Count("MATCH (n:P {id: 2}) RETURN n.age"), 20);

  UseMain();
  EXPECT_EQ(Count("MATCH (n:P) RETURN count(n.id)"), 2);
  EXPECT_EQ(Count("MATCH (n:P {id: 1}) RETURN n.age"), 10);
  EXPECT_EQ(Count("MATCH (n:P) RETURN count(n)"), 2);
}

// PERIODIC COMMIT validates each batch it commits, fork vertices included; a violating batch leaves nothing behind.
TEST_F(VersioningConstraintsTest, BranchPeriodicCommitValidatesForkVertices) {
  faker.Interpret("CREATE CONSTRAINT ON (n:P) ASSERT EXISTS (n.id)");
  faker.Interpret("UNWIND range(1, 4) AS x CREATE (:P {id: x, age: x})");
  Fork();

  EXPECT_THAT(Outcome("USING PERIODIC COMMIT 2 MATCH (n:P) REMOVE n.id"),
              ::testing::HasSubstr("PeriodicCommit failed: Unable to commit due to constraint violation"));
  EXPECT_EQ(Count("MATCH (n:P) RETURN count(n.id)"), 4) << "the failing batch must leave nothing behind";

  faker.Interpret("USING PERIODIC COMMIT 2 MATCH (n:P) SET n.age = n.age + 100");
  EXPECT_EQ(Count("MATCH (n:P) RETURN sum(n.age)"), 410);

  UseMain();
  EXPECT_EQ(Count("MATCH (n:P) RETURN sum(n.age)"), 10);
}

// A constraint created on main while a branch transaction is open is checked at that transaction's commit: fork
// and native vertices alike, for type and existence constraints.
TEST_F(VersioningConstraintsTest, BranchCommitChecksConstraintsCreatedOnMainDuringTheTransaction) {
  faker.Interpret("CREATE (:P {id: 1, age: 10}), (:Q {tag: 'q'})");
  Fork();
  InterpreterFaker main_session{&interpreter_context, db};
  interpreter_context.auth_checker = &faker.auth_checker;

  struct Case {
    std::string constraint;
    std::string statement;
    std::string expected;
  };

  std::vector<Case> const cases{
      {"CREATE CONSTRAINT ON (n:P) ASSERT n.age IS TYPED INTEGER",
       "MATCH (n:P) SET n.age = 'x'",
       "IS TYPED INTEGER violation on P(age)"},
      {"CREATE CONSTRAINT ON (n:P) ASSERT n.age IS TYPED INTEGER",
       "CREATE (:P {age: 'x'})",
       "IS TYPED INTEGER violation on P(age)"},
      {"CREATE CONSTRAINT ON (n:P) ASSERT EXISTS (n.id)", "MATCH (n:P) REMOVE n.id", "existence constraint violation"},
      {"CREATE CONSTRAINT ON (n:P) ASSERT EXISTS (n.id)", "MATCH (q:Q) SET q:P", "existence constraint violation"},
      {"CREATE CONSTRAINT ON (n:P) ASSERT EXISTS (n.id)", "CREATE (:P {age: 1})", "existence constraint violation"},
  };
  for (auto const &c : cases) {
    faker.Interpret("BEGIN");
    faker.Interpret(c.statement);
    main_session.Interpret(c.constraint);
    EXPECT_THAT(Outcome("COMMIT"), ::testing::HasSubstr(c.expected)) << c.statement;
    Outcome("ROLLBACK");
    main_session.Interpret("DROP" + c.constraint.substr(6));
    EXPECT_EQ(Count("MATCH (n:P) RETURN count(n)"), 1) << c.statement;
    EXPECT_EQ(Count("MATCH (n:P) RETURN n.age"), 10) << c.statement;
    EXPECT_EQ(Count("MATCH (q:Q) RETURN count(q)"), 1) << c.statement;
  }
}

// Deleting a vertex never violates a constraint: a fork vertex that is deleted after its required property was removed
// is not validated, and a second session cannot write to it meanwhile.
TEST_F(VersioningConstraintsTest, BranchCommitNeverValidatesADeletedForkVertex) {
  faker.Interpret("CREATE CONSTRAINT ON (n:P) ASSERT EXISTS (n.id)");
  faker.Interpret("CREATE (:P {id: 1}), (:P {id: 2})");
  ASSERT_TRUE(db->version_store()->CreateBranch("b"));
  auto sessions = SessionsOnBranch(2);
  auto &deleter = *sessions[0];
  auto &other = *sessions[1];

  deleter.Interpret("BEGIN");
  deleter.Interpret("MATCH (n:P {id: 1}) REMOVE n.id");
  deleter.Interpret("MATCH (n:P) WHERE n.id IS NULL DELETE n");
  EXPECT_THROW(other.Interpret("MATCH (n:P {id: 1}) SET n.age = 1"), std::exception);
  EXPECT_EQ(Outcome(deleter, "COMMIT"), "ok");
  EXPECT_EQ(IntRows(other, "MATCH (n:P) RETURN n.id ORDER BY n.id"), (Rows{{2}}));
  EXPECT_THAT(Outcome(other, "MATCH (n:P {id: 2}) REMOVE n.id"),
              ::testing::HasSubstr("existence constraint violation"));
  EXPECT_EQ(IntRows(other, "MATCH (n:P) RETURN n.id ORDER BY n.id"), (Rows{{2}}));
}

// In v1 a branch does not enforce unique constraints, so SHOW CONSTRAINT INFO marks them there and leaves main as is.
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

// Triggers defined on main do not fire for writes on a branch in v1.
TEST_F(VersioningConstraintsTest, MainTriggersDoNotFireForBranchWrites) {
  faker.Interpret("CREATE TRIGGER log_create ON () CREATE BEFORE COMMIT EXECUTE CREATE (:Log)");
  faker.Interpret("CREATE (:P)");
  ASSERT_EQ(Count("MATCH (l:Log) RETURN count(l)"), 1) << "the trigger fires for a write on main";
  Fork();
  faker.Interpret("CREATE (:P)");
  EXPECT_EQ(Count("MATCH (n:P) RETURN count(n)"), 2);
  EXPECT_EQ(Count("MATCH (l:Log) RETURN count(l)"), 1);
  UseMain();
  EXPECT_EQ(Count("MATCH (l:Log) RETURN count(l)"), 1);
  EXPECT_EQ(Count("MATCH (n:P) RETURN count(n)"), 1);
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

// Text and vector indexes reflect only main's current state, so a search on a branch errors instead of answering stale.
TEST_F(VersioningConstraintsTest, TextAndVectorSearchOnABranchError) {
  Fork();
  auto &ctx = *faker.interpreter.current_db_.branch_context();
  auto diff = ctx.diff_engine().Access(memgraph::storage::WRITE);
  ASSERT_TRUE(ctx.BeginTransaction(diff.get()).has_value());
  memgraph::query::DbAccessor dba{ctx.current_diff_txn(), &ctx};

  constexpr auto kText = "Text search is not supported on a branch in v1. Run it on main: USE BRANCH main.";
  constexpr auto kVector = "Vector search is not supported on a branch in v1. Run it on main: USE BRANCH main.";
  auto const mode = text_search_mode::SPECIFIED_PROPERTIES;
  EXPECT_EQ(ThrownMessage([&] { dba.TextIndexSearch("idx", "data.x:1", mode, {}); }), kText);
  EXPECT_EQ(ThrownMessage([&] { dba.SearchEdgeTextIndex("idx", "data.x:1", mode, {}); }), kText);
  EXPECT_EQ(ThrownMessage([&] { dba.TextIndexAggregate("idx", "data.x:1", "{}"); }), kText);
  EXPECT_EQ(ThrownMessage([&] { dba.TextEdgeIndexAggregate("idx", "data.x:1", "{}"); }), kText);
  EXPECT_EQ(ThrownMessage([&] { dba.VectorIndexSearchOnNodes("idx", 1, {1.0F}); }), kVector);
  EXPECT_EQ(ThrownMessage([&] { dba.VectorIndexSearchOnEdges("idx", 1, {1.0F}); }), kVector);
  ctx.EndTransaction();
}

// Bounded stress: violating sessions race sessions committing valid writes on one branch. No violating write lands,
// no valid increment is lost, and a reader's transaction sees one unchanging state.
TEST_F(VersioningConstraintsTest, ConcurrentViolatingAndValidWritesNeverLandABadValueOnABranch) {
  constexpr int kIncrements = 100;
  constexpr int kViolations = 100;
  faker.Interpret("CREATE CONSTRAINT ON (n:P) ASSERT EXISTS (n.id)");
  faker.Interpret("CREATE CONSTRAINT ON (n:P) ASSERT n.age IS TYPED INTEGER");
  faker.Interpret("CREATE (:P {id: 1, age: 0}), (:P {id: 2, age: 0})");
  Fork();
  auto sessions = SessionsOnBranch(4);
  std::atomic<int> writers_left{3};

  {
    std::vector<std::jthread> threads;
    for (int w = 0; w < 2; ++w) {
      threads.emplace_back([&session = *sessions[w], &writers_left] {
        for (int done = 0; done < kIncrements;) {
          try {
            session.Interpret("BEGIN");
            session.Interpret("MATCH (n:P {id: 1}) SET n.age = n.age + 1");
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
    threads.emplace_back([&session = *sessions[2], &writers_left] {
      for (int i = 0; i < kViolations; ++i) {
        // Type violation: raised at the statement. Existence violation: raised at COMMIT.
        session.Interpret("BEGIN");
        EXPECT_THROW(session.Interpret("MATCH (n:P {id: 2}) SET n.age = 'x'"), std::exception);
        session.Interpret("ROLLBACK");
        session.Interpret("BEGIN");
        session.Interpret("MATCH (n:P {id: 2}) REMOVE n.id");
        EXPECT_THROW(session.Interpret("COMMIT"), std::exception);
        session.Interpret("ROLLBACK");
        std::this_thread::yield();
      }
      --writers_left;
    });
    threads.emplace_back([&session = *sessions[3], &writers_left] {
      int64_t last_age = 0;
      do {
        session.Interpret("BEGIN");
        auto const first = IntRows(session, "MATCH (n:P) RETURN n.id, n.age ORDER BY n.id");
        auto const again = IntRows(session, "MATCH (n:P) RETURN n.id, n.age ORDER BY n.id");
        session.Interpret("COMMIT");
        ASSERT_EQ(first.size(), 2U);
        ASSERT_EQ(again, first) << "the snapshot moved inside one transaction";
        ASSERT_EQ(first[1], (std::vector<int64_t>{2, 0})) << "a violating write landed";
        ASSERT_EQ(first[0][0], 1);
        ASSERT_GE(first[0][1], last_age) << "committed increments went backwards";
        last_age = first[0][1];
      } while (writers_left.load() > 0);
    });
  }

  EXPECT_EQ(IntRows(faker, "MATCH (n:P) RETURN n.id, n.age ORDER BY n.id"), (Rows{{1, 2 * kIncrements}, {2, 0}}));
  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH (n:P) RETURN n.id, n.age ORDER BY n.id"), (Rows{{1, 0}, {2, 0}}));
}
