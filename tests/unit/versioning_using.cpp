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

// Graph Versioning: USING BRANCH runs one autocommit query on a branch (or main) and leaves the session where it is.

#include <atomic>
#include <filesystem>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "query/exceptions.hpp"
#include "versioning/gate.hpp"
#include "versioning_fixture.hpp"

using memgraph::tests::versioning_fixture::ExplainText;
using memgraph::tests::versioning_fixture::IntRows;
using memgraph::tests::versioning_fixture::PlanHasOperator;
using memgraph::tests::versioning_fixture::Rows;
using memgraph::tests::versioning_fixture::ThrownMessage;
using memgraph::versioning::GateError;

namespace {
std::string GateMessage(GateError err) { return std::string{memgraph::versioning::GateErrorMessage(err)}; }
}  // namespace

// From a branch session: the query reads and writes the named branch, its checkout lasts only for the query, and the
// session's own branch is untouched, also when the query fails.
TEST_F(VersioningGatedTest, UsingBranchRunsOneQueryOnAnotherBranch) {
  faker.Interpret("CREATE (:Base)");
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("CREATE BRANCH c");
  faker.Interpret("USE BRANCH c");
  faker.Interpret("CREATE (:OnC {v: 3})");
  faker.Interpret("USE BRANCH b");
  faker.Interpret("CREATE (:OnB {v: 2})");
  auto *store = db->version_store();
  auto &current_db = faker.interpreter.current_db_;

  EXPECT_EQ(IntRows(faker, "USING BRANCH c MATCH (n) WHERE n:OnB OR n:OnC RETURN n.v"), (Rows{{3}}));
  faker.Interpret("USING BRANCH 'c' CREATE (:AlsoOnC)");
  EXPECT_EQ(IntRows(faker, "USING PERIODIC COMMIT 1, BRANCH c UNWIND range(1, 3) AS i CREATE (:P) RETURN count(*)"),
            (Rows{{3}}));
  EXPECT_THROW(faker.Interpret("USING BRANCH c MATCH (n:OnC) RETURN 1 / (n.v - 3)"),
               memgraph::query::QueryRuntimeException);
  EXPECT_EQ(current_db.CurrentVersion(), "b");
  EXPECT_EQ(store->CheckoutCount("b"), 1U);
  EXPECT_EQ(store->CheckoutCount("c"), 0U) << "the query's checkout ends with its transaction";

  EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE n:OnB OR n:OnC OR n:AlsoOnC OR n:P RETURN n.v"), (Rows{{2}}));
  faker.Interpret("USING BRANCH b CREATE (:AlsoOnB)");
  EXPECT_EQ(IntRows(faker, "MATCH (n:AlsoOnB) RETURN count(n)"), (Rows{{1}})) << "the session's own branch";
  faker.Interpret("USE BRANCH c");
  EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE n:AlsoOnC OR n:P RETURN count(n)"), (Rows{{4}}));
  EXPECT_EQ(IntRows(faker, "USING BRANCH main MATCH (n) RETURN count(n)"), (Rows{{1}}));
}

// From main: USING BRANCH attaches only for the query, so a finished one does not keep the branch from being dropped.
TEST_F(VersioningGatedTest, UsingBranchFromMainAttachesOnlyForTheQuery) {
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("USING BRANCH b CREATE (:OnB {v: 2})");
  auto *store = db->version_store();

  EXPECT_EQ(IntRows(faker, "USING BRANCH 'b' MATCH (n:OnB) RETURN n.v"), (Rows{{2}}));
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  EXPECT_EQ(faker.interpreter.current_db_.branch_context(), nullptr);
  EXPECT_EQ(store->CheckoutCount("b"), 0U);
  EXPECT_EQ(IntRows(faker, "MATCH (n:OnB) RETURN count(n)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "USING BRANCH main MATCH (n:OnB) RETURN count(n)"), (Rows{{0}}));
  faker.Interpret("DROP BRANCH b");
}

// USING BRANCH main from a branch session reads and writes main only, and the branch's writes stay off main.
TEST_F(VersioningGatedTest, UsingBranchMainWhileOnABranchRunsOnMain) {
  faker.Interpret("CREATE (:MainOnly)");
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("USE BRANCH b");
  faker.Interpret("CREATE (:BranchOnly)");

  EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n)"), (Rows{{2}}));
  EXPECT_EQ(IntRows(faker, "USING BRANCH main MATCH (n) RETURN count(n)"), (Rows{{1}}));
  faker.Interpret("USING BRANCH main CREATE (:M)");
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  EXPECT_EQ(IntRows(faker, "MATCH (n:M) RETURN count(n)"), (Rows{{0}}));
  faker.Interpret("USE BRANCH main");
  EXPECT_EQ(IntRows(faker, "MATCH (n:M) RETURN count(n)"), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:BranchOnly) RETURN count(n)"), (Rows{{0}}));
}

TEST_F(VersioningGatedTest, UsingBranchIsHonouredByExplainProfileAndOtherDirectives) {
  faker.Interpret("CREATE (:Base {v: 1})");
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("USING BRANCH b CREATE (:OnB {v: 2})");

  EXPECT_FALSE(faker.Interpret("EXPLAIN USING BRANCH b MATCH (n:OnB) RETURN n").GetResults().empty());
  EXPECT_FALSE(faker.Interpret("PROFILE USING BRANCH b MATCH (n:OnB) RETURN n").GetResults().empty());
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 0U);
  for (const auto *prefix : {"EXPLAIN", "PROFILE"}) {
    EXPECT_EQ(ThrownMessage([&] { faker.Interpret(std::string{prefix} + " USING BRANCH nope MATCH (n) RETURN n"); }),
              "Branch 'nope' does not exist.")
        << prefix;
  }
  EXPECT_FALSE(faker.Interpret("PROFILE USING BRANCH b CREATE (:Profiled)").GetResults().empty());
  EXPECT_EQ(IntRows(faker, "USING BRANCH b, HOPS LIMIT 5 MATCH (n) WHERE n:Base OR n:OnB RETURN n.v ORDER BY n.v"),
            (Rows{{1}, {2}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:OnB) RETURN count(n)"), (Rows{{0}}));
}

TEST_F(VersioningGatedTest, UsingAnUnknownBranchFailsAndLeavesTheSessionUsable) {
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("USE BRANCH b");
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("USING BRANCH 'nope' MATCH (n) RETURN n"); }),
            "Branch 'nope' does not exist.");
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n)"), (Rows{{0}}));
}

// A transaction binds one storage engine when it begins.
TEST_F(VersioningGatedTest, UsingBranchIsRefusedInsideAnExplicitTransaction) {
  faker.Interpret("CREATE BRANCH b");
  for (const auto *session_branch : {"main", "b"}) {
    faker.Interpret(std::string{"USE BRANCH "} + session_branch);
    faker.Interpret("BEGIN");
    for (const auto *query : {"USING BRANCH b MATCH (n) RETURN n", "USING BRANCH main CREATE (:X)"}) {
      EXPECT_EQ(ThrownMessage([&] { faker.Interpret(query); }),
                "USING BRANCH is not allowed inside an explicit transaction.")
          << session_branch << ": " << query;
    }
    faker.Interpret("ROLLBACK");
  }
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 1U);
  EXPECT_EQ(IntRows(faker, "USING BRANCH main MATCH (n:X) RETURN count(n)"), (Rows{{0}}));
}

// A failed attach (here the first use of `c` cannot persist its gid floors) restores the session's branch.
TEST_F(VersioningGatedTest, AFailedUsingAttachKeepsTheSessionOnItsBranch) {
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("CREATE BRANCH c");
  faker.Interpret("USE BRANCH b");
  faker.Interpret("CREATE (:OnB)");

  const auto blocker = config.durability.storage_directory / "branches" / "ledger.tmp";
  std::filesystem::create_directory(blocker);
  EXPECT_THAT(ThrownMessage([&] { faker.Interpret("USING BRANCH c MATCH (n) RETURN n"); }),
              ::testing::HasSubstr("Branch ledger"));
  std::filesystem::remove(blocker);

  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 1U);
  EXPECT_EQ(db->version_store()->CheckoutCount("c"), 0U);
  EXPECT_EQ(IntRows(faker, "MATCH (n:OnB) RETURN count(n)"), (Rows{{1}}));
}

// A session on a dropped branch gets the dead-branch error for USING BRANCH too: it never runs anything until it
// switches.
TEST_F(VersioningGatedTest, UsingBranchOnASessionWhoseBranchIsGoneFails) {
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("CREATE BRANCH c");
  faker.Interpret("USE BRANCH b");
  ASSERT_EQ(db->version_store()->ForceDropBranch("b"), memgraph::versioning::DropResult::kDropped);
  for (const auto *query : {"USING BRANCH main MATCH (n) RETURN n", "USING BRANCH c MATCH (n) RETURN n"}) {
    EXPECT_EQ(ThrownMessage([&] { faker.Interpret(query); }),
              "Branch 'b' no longer exists. Run USE BRANCH or USE DATABASE to continue.")
        << query;
  }
}

// USING BRANCH checks the flag and the storage mode, then that the instance is MAIN.
TEST_F(VersioningInterpreterTest, UsingBranchGateOrder) {
  memgraph::license::global_license_checker.EnableTesting();
  ASSERT_TRUE(db->version_store()->CreateBranch("b"));
  const auto refused_with = [&](const char *query) { return ThrownMessage([&] { faker.Interpret(query); }); };

  EXPECT_EQ(refused_with("USING BRANCH b MATCH (n) RETURN n"), GateMessage(GateError::kDisabled));
  SetVersioningFlag(true);
  ASSERT_TRUE(repl_state.Lock()->SetReplicationRoleReplica(
      memgraph::replication::ReplicationServerConfig{.repl_server =
                                                         memgraph::io::network::Endpoint("127.0.0.1", 30246)},
      /*maybe_main_uuid=*/std::nullopt));
  for (const auto *query : {"USING BRANCH b MATCH (n) RETURN n", "USING BRANCH main MATCH (n) RETURN n"}) {
    EXPECT_EQ(refused_with(query), "Branch operations are forbidden on a REPLICA. Run them on the MAIN instance.")
        << query;
  }
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 0U);
}

// Without a valid license USING BRANCH reads any branch, but writes on a branch are refused; main stays writable.
TEST_F(VersioningGatedTest, WithoutLicenseUsingBranchOnlyReads) {
  faker.Interpret("CREATE (:V)");
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("CREATE BRANCH c");
  faker.Interpret("USE BRANCH c");
  memgraph::license::global_license_checker.DisableTesting();
  const auto no_license = GateMessage(GateError::kNoLicense);

  EXPECT_EQ(IntRows(faker, "USING BRANCH b MATCH (n:V) RETURN count(n)"), (Rows{{1}}));
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("USING BRANCH b CREATE (:V)"); }), no_license);
  faker.Interpret("USING BRANCH main CREATE (:V)");
  faker.Interpret("USE BRANCH main");
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("USING BRANCH b MATCH (n:V) SET n.x = 1"); }), no_license);
  EXPECT_EQ(IntRows(faker, "USING BRANCH b MATCH (n:V) WHERE n.x IS NULL RETURN count(n)"), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:V) RETURN count(n)"), (Rows{{2}}));
}

// BRANCH_USE plus the query's own privileges: a write on a branch needs WRITE-side privileges, not BRANCH_EDIT.
TEST_F(VersioningGatedTest, UsingBranchNeedsBranchUseAndTheQuerysPrivileges) {
  using memgraph::query::AuthQuery;
  faker.Interpret("CREATE BRANCH b");
  const auto privileges_of = [&](const std::string &query) {
    auto prepared =
        faker.interpreter.Prepare(query, [](auto *) { return memgraph::storage::ExternalPropertyValue::map_t{}; }, {});
    faker.Abort();
    return prepared.privileges;
  };
  EXPECT_THAT(privileges_of("USING BRANCH b MATCH (n) RETURN n"),
              ::testing::UnorderedElementsAre(AuthQuery::Privilege::MATCH, AuthQuery::Privilege::BRANCH_USE));
  EXPECT_THAT(privileges_of("USING BRANCH b CREATE (n)"),
              ::testing::UnorderedElementsAre(AuthQuery::Privilege::CREATE, AuthQuery::Privilege::BRANCH_USE));
}

// Every target shares one stripped query text ("USING BRANCH $0 ..."), so plans must follow the target, never the text:
// main has an index created after the fork, the branch does not.
TEST_F(VersioningGatedTest, UsingBranchPlansFollowTheTargetNotTheQueryText) {
  faker.Interpret("CREATE (:User {id: 1, tag: 'main'})");
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("CREATE INDEX ON :User(id)");
  faker.Interpret("USING BRANCH b CREATE (:User {id: 100, tag: 'branch'})");
  const auto index_scan = [&](const std::string &target) {
    return PlanHasOperator(ExplainText(faker, "USING BRANCH " + target + " MATCH (n:User {id: 100}) RETURN n.tag"),
                           "ScanAllByLabelProperties");
  };

  for (int round = 0; round < 2; ++round) {
    EXPECT_TRUE(index_scan("'main'")) << round;
    EXPECT_FALSE(index_scan("'b'")) << round;
    EXPECT_EQ(faker.Interpret("USING BRANCH 'main' MATCH (n:User {id: 100}) RETURN n.tag").GetResults().size(), 0U);
    auto on_branch = faker.Interpret("USING BRANCH 'b' MATCH (n:User {id: 100}) RETURN n.tag");
    ASSERT_EQ(on_branch.GetResults().size(), 1U);
    EXPECT_EQ(on_branch.GetResults()[0][0].ValueString(), "branch");
  }

  // From a session on the branch, USING BRANCH main neither fills nor reads the branch's plan cache.
  faker.Interpret("USE BRANCH b");
  EXPECT_TRUE(index_scan("main"));
  EXPECT_FALSE(
      PlanHasOperator(ExplainText(faker, "MATCH (n:User {id: 100}) RETURN n.tag"), "ScanAllByLabelProperties"));
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) RETURN n.id ORDER BY n.id"), (Rows{{1}, {100}}));
  EXPECT_TRUE(index_scan("main"));
}

// Only the outermost query's directive picks the branch: in a subquery it would be silently ignored.
TEST_F(VersioningGatedTest, UsingBranchInASubqueryIsRefused) {
  faker.Interpret("CREATE BRANCH b");
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("CALL { USING BRANCH b MATCH (n) RETURN count(n) AS c } RETURN c"); }),
            "USING BRANCH applies to a whole query, not to a subquery.");
}

// Commit-lock narrowing on main is not a prerequisite: USING BRANCH reads and writes a branch with it on.
TEST_F(VersioningNarrowingGatedTest, UsingBranchWorksWithCommitLockNarrowingOn) {
  ASSERT_TRUE(db->storage()->config_.experimental_commit_lock_narrowing);
  faker.Interpret("CREATE (:M {v: 1})");
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("USING BRANCH b CREATE (:B {v: 2})");
  EXPECT_EQ(IntRows(faker, "USING BRANCH b MATCH (n) WHERE n:M OR n:B RETURN n.v ORDER BY n.v"), (Rows{{1}, {2}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:B) RETURN count(n)"), (Rows{{0}}));
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 0U);
}

// Every USING BRANCH query is its own transaction on the target: it sees what other sessions committed there
// before it began and never what was written to another branch, whichever branch the issuing session is on.
TEST_F(VersioningGatedTest, UsingBranchSeesTheTargetsLatestCommitsAndNeverAnotherBranchs) {
  faker.Interpret("CREATE (:K {v: 1})");
  faker.Interpret("CREATE BRANCH x");
  faker.Interpret("CREATE BRANCH y");
  auto on_x = SessionsOnBranch(1, "x");
  auto on_y = SessionsOnBranch(1, "y");
  auto &writer = *on_x[0];
  constexpr auto kRead = "MATCH (n:K) RETURN n.v ORDER BY n.v";
  auto const using_branch = [&](InterpreterFaker &session, std::string_view target) {
    return IntRows(session, fmt::format("USING BRANCH {} {}", target, kRead));
  };

  EXPECT_EQ(using_branch(faker, "x"), (Rows{{1}}));
  writer.Interpret("MATCH (n:K) SET n.v = 10");
  writer.Interpret("CREATE (:K {v: 11})");
  EXPECT_EQ(using_branch(faker, "x"), (Rows{{10}, {11}}));
  EXPECT_EQ(using_branch(*on_y[0], "x"), (Rows{{10}, {11}})) << "from another branch's session";
  EXPECT_EQ(using_branch(writer, "x"), (Rows{{10}, {11}})) << "targeting the session's own branch";
  EXPECT_EQ(using_branch(faker, "y"), (Rows{{1}}));
  EXPECT_EQ(using_branch(writer, "main"), (Rows{{1}}));

  on_y[0]->Interpret("USING BRANCH x MATCH (n:K {v: 11}) SET n.v = 12");
  EXPECT_EQ(IntRows(*on_y[0], kRead), (Rows{{1}})) << "the session's own branch is untouched";
  EXPECT_EQ(IntRows(writer, kRead), (Rows{{10}, {12}}));
  writer.Interpret("USING BRANCH y CREATE (:K {v: 20})");
  EXPECT_EQ(IntRows(writer, kRead), (Rows{{10}, {12}}));
  EXPECT_EQ(IntRows(*on_y[0], kRead), (Rows{{1}, {20}}));
  EXPECT_EQ(IntRows(faker, kRead), (Rows{{1}}));
  EXPECT_EQ(db->version_store()->CheckoutCount("x"), 1U) << "only the session on x holds a checkout";
  EXPECT_EQ(db->version_store()->CheckoutCount("y"), 1U);
}

// Bounded stress: two sessions on branch x move one unit between two vertices in overlapping explicit transactions
// while sessions on main and on branch y read x through USING BRANCH. Every such read is one consistent snapshot
// (total 100, one pair), x only moves forward, y and main never change, and no transfer is lost.
TEST_F(VersioningGatedTest, UsingBranchReadsAreConsistentSnapshotsWhileTransfersCommit) {
  constexpr int kTransfers = 80;
  faker.Interpret("CREATE (:K {id: 1, v: 100}), (:K {id: 2, v: 0})");
  faker.Interpret("CREATE BRANCH x");
  faker.Interpret("CREATE BRANCH y");
  auto writers = SessionsOnBranch(2, "x");
  auto on_y = SessionsOnBranch(1, "y");
  std::atomic<int> writers_left{2};

  {
    std::vector<std::jthread> threads;
    for (auto &writer : writers) {
      threads.emplace_back([&session = *writer, &writers_left] {
        for (int done = 0; done < kTransfers;) {
          try {
            session.Interpret("BEGIN");
            session.Interpret("MATCH (a:K {id: 1}), (b:K {id: 2}) SET a.v = a.v - 1, b.v = b.v + 1");
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
    for (InterpreterFaker *reader : {&faker, on_y[0].get()}) {
      threads.emplace_back([reader, &writers_left] {
        int64_t last_moved = 0;
        do {
          auto const pair = IntRows(*reader, "USING BRANCH x MATCH (n:K) RETURN n.v ORDER BY n.id");
          auto const total = IntRows(*reader, "USING BRANCH x MATCH (n:K) RETURN sum(n.v)");
          ASSERT_EQ(pair.size(), 2U);
          ASSERT_EQ(pair[0][0] + pair[1][0], 100) << "a half-applied transfer was visible";
          ASSERT_EQ(total, (Rows{{100}}));
          ASSERT_GE(pair[1][0], last_moved) << "committed transfers went backwards";
          last_moved = pair[1][0];
        } while (writers_left.load() > 0);
      });
    }
  }

  EXPECT_EQ(IntRows(faker, "USING BRANCH x MATCH (n:K) RETURN n.v ORDER BY n.id"),
            (Rows{{100 - 2 * kTransfers}, {2 * kTransfers}}));
  EXPECT_EQ(IntRows(faker, "USING BRANCH y MATCH (n:K) RETURN n.v ORDER BY n.id"), (Rows{{100}, {0}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN n.v ORDER BY n.id"), (Rows{{100}, {0}}));
  EXPECT_EQ(db->version_store()->CheckoutCount("x"), 2U) << "only the writers hold x; every USING released its attach";
}
