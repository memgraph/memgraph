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

#include <filesystem>
#include <string>
#include <string_view>

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
  ASSERT_TRUE(db->version_store()->ForceDropBranch("b").value_or(false));
  for (const auto *query : {"USING BRANCH main MATCH (n) RETURN n", "USING BRANCH c MATCH (n) RETURN n"}) {
    EXPECT_EQ(ThrownMessage([&] { faker.Interpret(query); }),
              "Branch 'b' no longer exists. Run USE BRANCH or USE DATABASE to continue.")
        << query;
  }
}

// USING BRANCH checks the flag, the storage mode and commit-lock narrowing, then that the instance is MAIN.
TEST_F(VersioningInterpreterTest, UsingBranchGateOrder) {
  memgraph::license::global_license_checker.EnableTesting();
  ASSERT_TRUE(db->version_store()->CreateBranch("b", "main"));
  const auto refused_with = [&](const char *query) { return ThrownMessage([&] { faker.Interpret(query); }); };
  db->storage()->config_.experimental_commit_lock_narrowing = true;

  EXPECT_EQ(refused_with("USING BRANCH b MATCH (n) RETURN n"), GateMessage(GateError::kDisabled));
  SetVersioningFlag(true);
  EXPECT_EQ(refused_with("USING BRANCH b MATCH (n) RETURN n"), GateMessage(GateError::kCommitLockNarrowing));
  db->storage()->config_.experimental_commit_lock_narrowing = false;
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
