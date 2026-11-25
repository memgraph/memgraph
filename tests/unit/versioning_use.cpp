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

// Graph Versioning: USE BRANCH and what a session does on a branch that is dropped under it.

#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "metrics/prometheus_metrics.hpp"
#include "query/exceptions.hpp"
#include "query/query_user.hpp"
#include "versioning/gate.hpp"
#include "versioning_fixture.hpp"

using memgraph::tests::versioning_fixture::CheckoutCount;
using memgraph::tests::versioning_fixture::GateMessage;
using memgraph::tests::versioning_fixture::IntRows;
using memgraph::tests::versioning_fixture::LackingPrivilegesUser;
using memgraph::tests::versioning_fixture::Rows;
using memgraph::tests::versioning_fixture::ThrownMessage;
using memgraph::versioning::GateError;

namespace {
void ForceDrop(memgraph::versioning::VersionStore *store, std::string_view name) {
  const auto dropped = store->DropBranch(name, true);
  ASSERT_TRUE(dropped) << dropped.error();
  ASSERT_EQ(*dropped, memgraph::versioning::DropResult::kDropped);
}
}  // namespace

namespace {

constexpr std::string_view kDeadBranch = "Branch 'b' no longer exists. Run USE BRANCH or USE DATABASE to continue.";

std::string Status(InterpreterFaker &session, const std::string &query) {
  auto stream = session.Interpret(query);
  EXPECT_EQ(stream.GetHeader(), std::vector<std::string>{"STATUS"}) << query;
  EXPECT_EQ(stream.GetResults().size(), 1U) << query;
  return stream.GetResults().at(0).at(0).ValueString();
}

}  // namespace

// The session reads and writes the branch it switched to until it switches again; an unknown branch leaves it where it
// was, and dropping its own branch moves it to main.
TEST_F(VersioningGatedTest, UseBranchMovesTheSessionUntilTheNextSwitch) {
  faker.Interpret("CREATE (:A {v: 1})");
  faker.Interpret("CREATE BRANCH feature");
  auto &current_db = faker.interpreter.current_db_;

  EXPECT_EQ(Status(faker, "USE BRANCH main"), "Already using branch main");
  EXPECT_EQ(Status(faker, "USE BRANCH feature"), "Using branch feature");
  EXPECT_EQ(Status(faker, "USE BRANCH 'feature'"), "Already using branch feature");
  EXPECT_EQ(current_db.CurrentVersion(), "feature");
  EXPECT_EQ(faker.Interpret("SHOW BRANCH").GetResults().at(0)[0].ValueString(), "feature");
  faker.Interpret("MATCH (n:A) SET n.v = 2");

  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("USE BRANCH 'nope'"); }), "Branch 'nope' does not exist.");
  EXPECT_EQ(current_db.CurrentVersion(), "feature") << "a failed switch leaves the session where it was";

  EXPECT_EQ(Status(faker, "USE BRANCH main"), "Using branch main");
  EXPECT_FALSE(current_db.CurrentVersion().has_value());
  EXPECT_EQ(IntRows(faker, "MATCH (n:A) RETURN n.v"), (Rows{{1}}));
  faker.Interpret("CREATE (:A {v: 3})");

  faker.Interpret("USE BRANCH feature");
  EXPECT_EQ(IntRows(faker, "MATCH (n:A) RETURN n.v"), (Rows{{2}})) << "main's later write is not the branch's";
  faker.Interpret("DROP BRANCH feature");
  EXPECT_FALSE(current_db.CurrentVersion().has_value());
  EXPECT_EQ(current_db.branch_context(), nullptr);
  EXPECT_EQ(IntRows(faker, "MATCH (n:A) RETURN n.v ORDER BY n.v"), (Rows{{1}, {3}}));
}

// A branch startup set aside cannot be used: the error names the reason and the way out, the session stays put and
// holds no checkout; DROP BRANCH needs no FORCE.
TEST_F(VersioningGatedTest, UseBranchOnAnUnavailableBranchSaysWhyAndLeavesTheSessionWhereItWas) {
  faker.Interpret("CREATE BRANCH feature");
  faker.Interpret("CREATE BRANCH bad");
  auto *store = db->version_store();
  store->MarkUnavailable(store->Get("bad")->info.id, "the write-ahead log directory is missing");
  faker.Interpret("USE BRANCH feature");

  const auto message = ThrownMessage([&] { faker.Interpret("USE BRANCH bad"); });
  EXPECT_THAT(message, ::testing::HasSubstr("Branch 'bad' is unavailable: the write-ahead log directory is missing."));
  EXPECT_THAT(message, ::testing::HasSubstr("restart Memgraph"));
  EXPECT_THAT(message, ::testing::HasSubstr("DROP BRANCH 'bad'"));
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "feature");
  EXPECT_EQ(CheckoutCount(store, "bad"), 0U);
  EXPECT_EQ(CheckoutCount(store, "feature"), 1U);

  faker.Interpret("DROP BRANCH bad");
  EXPECT_FALSE(store->Exists("bad"));
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("USE BRANCH bad"); }), "Branch 'bad' does not exist.");
}

// x -> y -> main on one connection: the session holds one checkout at a time (counted once in the active-sessions
// gauge), and branches forked from main are isolated from each other and from main.
TEST_F(VersioningGatedTest, SwitchingBranchesKeepsOneCheckoutPerSession) {
  faker.Interpret("CREATE (:Base)");
  faker.Interpret("CREATE BRANCH x");
  faker.Interpret("CREATE BRANCH y");
  auto *store = db->version_store();
  auto &sessions = *memgraph::metrics::Metrics().global.versioning_active_sessions;
  const auto sessions0 = sessions.Value();

  faker.Interpret("USE BRANCH x");
  faker.Interpret("CREATE (:OnX)");
  EXPECT_EQ(CheckoutCount(store, "x"), 1U);
  EXPECT_DOUBLE_EQ(sessions.Value(), sessions0 + 1);
  faker.Interpret("USE BRANCH y");
  EXPECT_DOUBLE_EQ(sessions.Value(), sessions0 + 1) << "a switch moves the session, it does not add one";
  EXPECT_EQ(CheckoutCount(store, "x"), 0U);
  EXPECT_EQ(CheckoutCount(store, "y"), 1U);
  faker.Interpret("CREATE (:OnY)");
  EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE n:Base OR n:OnX OR n:OnY RETURN count(n)"), (Rows{{2}}));
  faker.Interpret("USE BRANCH main");
  EXPECT_EQ(CheckoutCount(store, "y"), 0U);
  EXPECT_DOUBLE_EQ(sessions.Value(), sessions0);
  faker.Interpret("CREATE (:OnMain)");
  EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE n:Base OR n:OnMain RETURN count(n)"), (Rows{{2}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE n:OnX OR n:OnY RETURN count(n)"), (Rows{{0}}));
  faker.Interpret("USE BRANCH x");
  EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE n:OnX OR n:OnY OR n:OnMain RETURN count(n)"), (Rows{{1}}));
}

// The old branch is released only once the new one is attached: a switch that fails half-way (here `c` is unavailable)
// keeps the session on its branch.
TEST_F(VersioningGatedTest, AFailedSwitchKeepsTheSessionOnItsBranch) {
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("CREATE BRANCH c");
  faker.Interpret("CREATE BRANCH d");
  faker.Interpret("USE BRANCH b");
  faker.Interpret("CREATE (:OnB)");
  auto *store = db->version_store();

  store->MarkUnavailable(store->Get("c")->info.id, "the write-ahead log directory is missing");
  EXPECT_THAT(ThrownMessage([&] { faker.Interpret("USE BRANCH c"); }),
              ::testing::HasSubstr("Branch 'c' is unavailable"));

  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  EXPECT_EQ(CheckoutCount(store, "b"), 1U);
  EXPECT_EQ(CheckoutCount(store, "c"), 0U);
  EXPECT_EQ(IntRows(faker, "MATCH (n:OnB) RETURN count(n)"), (Rows{{1}}));
  faker.Interpret("USE BRANCH d");
  EXPECT_EQ(CheckoutCount(store, "b"), 0U);
  EXPECT_EQ(CheckoutCount(store, "d"), 1U);
  EXPECT_EQ(IntRows(faker, "MATCH (n:OnB) RETURN count(n)"), (Rows{{0}}));
}

// PROFILE wraps an inner query; after USE BRANCH main it runs on main like a bare query.
TEST_F(VersioningGatedTest, ProfileOnMainAfterUseMain) {
  faker.Interpret("CREATE (:Seed)");
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("USE BRANCH b");
  faker.Interpret("USE BRANCH main");

  EXPECT_FALSE(faker.Interpret("PROFILE CREATE (:Profiled)").GetResults().empty());
  EXPECT_FALSE(faker.Interpret("PROFILE MATCH (n:Seed) RETURN n").GetResults().empty());
  faker.Interpret("CREATE (:Bare)");
  EXPECT_EQ(IntRows(faker, "MATCH (n:Bare) RETURN count(n)"), (Rows{{1}}));
}

// USE BRANCH checks the flag and the storage mode, then that the instance is MAIN; a refusal leaves the session where
// it was.
TEST_F(VersioningInterpreterTest, UseBranchGateOrder) {
  memgraph::license::global_license_checker.EnableTesting();
  ASSERT_TRUE(db->version_store()->CreateBranch("b"));
  const auto refused_with = [&](const char *query) { return ThrownMessage([&] { faker.Interpret(query); }); };

  EXPECT_EQ(refused_with("USE BRANCH b"), GateMessage(GateError::kDisabled));
  SetVersioningFlag(true);
  ASSERT_TRUE(repl_state.Lock()->SetReplicationRoleReplica(
      memgraph::replication::ReplicationServerConfig{.repl_server =
                                                         memgraph::io::network::Endpoint("127.0.0.1", 30246)},
      /*maybe_main_uuid=*/std::nullopt));
  for (const auto *query : {"USE BRANCH b", "USE BRANCH main"}) {
    EXPECT_EQ(refused_with(query),
              "Graph Versioning branches are not supported with replication in v1: branch operations are refused on a "
              "REPLICA. Run them on the MAIN instance.")
        << query;
  }
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  EXPECT_EQ(CheckoutCount(db->version_store(), "b"), 0U);
}

// Without a valid license a session can still move onto a branch, read it and leave it; only writes are refused.
TEST_F(VersioningGatedTest, WithoutLicenseUseBranchReadsAndLeaves) {
  faker.Interpret("CREATE (:V)");
  faker.Interpret("CREATE BRANCH b");
  memgraph::license::global_license_checker.DisableTesting();

  faker.Interpret("USE BRANCH b");
  EXPECT_EQ(IntRows(faker, "MATCH (n:V) RETURN count(n)"), (Rows{{1}}));
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("CREATE (:V)"); }), GateMessage(GateError::kNoLicense));
  faker.Interpret("USE BRANCH main");
  faker.Interpret("CREATE (:V)");
  EXPECT_EQ(IntRows(faker, "MATCH (n:V) RETURN count(n)"), (Rows{{2}}));
}

TEST_F(VersioningGatedTest, UseBranchIsRefusedInATransactionAndOnADatabasePinnedSession) {
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("BEGIN");
  EXPECT_THROW(faker.Interpret("USE BRANCH b"), memgraph::query::VersioningQueryInMulticommandTxException);
  faker.Interpret("ROLLBACK");

  auto &current_db = faker.interpreter.current_db_;
  current_db.SetCurrentDB(*db_gk.access(), /*in_explicit_db=*/true);
  for (const auto *query : {"USE BRANCH b", "USE BRANCH main"}) {
    EXPECT_EQ(ThrownMessage([&] { faker.Interpret(query); }),
              "Branch switching is prohibited if the session explicitly defines the database.")
        << query;
  }
  EXPECT_FALSE(current_db.CurrentVersion().has_value());
}

// The Bolt layer authorises the privileges Prepare returns, so the decision is exercised the same way.
std::string RefusalOf(InterpreterFaker &faker, const std::string &query) {
  auto prepared =
      faker.interpreter.Prepare(query, [](auto *) { return memgraph::storage::ExternalPropertyValue::map_t{}; }, {});
  auto message = ThrownMessage([&] { faker.interpreter.CheckAuthorized(prepared.privileges, prepared.db); });
  faker.Abort();
  return message == "<no exception>" ? "" : message;
}

constexpr std::string_view kNeedsBranchUse =
    "You are not authorized to execute this query on database \"\"! Missing privilege: BRANCH_USE. Ask your "
    "database administrator to grant it.";

TEST_F(VersioningGatedTest, UseBranchWithoutBranchUseIsRefusedAndTheSessionStaysPut) {
  faker.Interpret("CREATE BRANCH b");
  faker.interpreter.SetUser(
      std::make_shared<LackingPrivilegesUser>(std::set{memgraph::query::AuthQuery::Privilege::BRANCH_USE}));
  EXPECT_EQ(RefusalOf(faker, "USE BRANCH b"), kNeedsBranchUse);
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
}

// BRANCH_USE is checked when a session attaches to a branch, as MULTI_DATABASE_USE is for a switch; a revoke leaves the
// attached session running and blocks the next attach. main needs database access only.
TEST_F(VersioningGatedTest, RevokedBranchUseBlocksTheNextAttachNotTheAttachedSession) {
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("CREATE BRANCH c");
  faker.Interpret("USE BRANCH b");
  faker.interpreter.SetUser(
      std::make_shared<LackingPrivilegesUser>(std::set{memgraph::query::AuthQuery::Privilege::BRANCH_USE}));

  EXPECT_EQ(RefusalOf(faker, "MATCH (n) RETURN n"), "");
  EXPECT_EQ(RefusalOf(faker, "USE BRANCH b"), kNeedsBranchUse);
  EXPECT_EQ(RefusalOf(faker, "USE BRANCH c"), kNeedsBranchUse);
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");

  EXPECT_EQ(RefusalOf(faker, "USE BRANCH main"), "");
  EXPECT_EQ(Status(faker, "USE BRANCH main"), "Using branch main");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  EXPECT_EQ(RefusalOf(faker, "MATCH (n) RETURN n"), "");
}

// A branch dropped under the session never falls back to main: every query fails, the dead checkout is released once,
// and USE BRANCH main gets the session out.
TEST_F(VersioningGatedTest, SessionOnDroppedBranchFailsEveryQueryUntilItSwitches) {
  auto &sessions = *memgraph::metrics::Metrics().global.versioning_active_sessions;
  faker.Interpret("CREATE (:A {x: 1})");
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("USE BRANCH b");
  faker.Interpret("CREATE (:A {x: 2})");
  const auto sessions0 = sessions.Value();
  ForceDrop(db->version_store(), "b");

  for (const auto *query :
       {"MATCH (n:A) RETURN n.x", "RETURN 1", "SHOW BRANCH", "SHOW BRANCHES", "SHOW INDEX INFO", "CREATE (:B)"}) {
    EXPECT_EQ(ThrownMessage([&] { faker.Interpret(query); }), kDeadBranch) << query;
  }
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b") << "the session is not silently moved to main";
  EXPECT_DOUBLE_EQ(sessions.Value(), sessions0 - 1);
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("BEGIN"); }), kDeadBranch);
  faker.Interpret("ROLLBACK");

  EXPECT_EQ(Status(faker, "USE BRANCH main"), "Using branch main");
  EXPECT_DOUBLE_EQ(sessions.Value(), sessions0 - 1);
  EXPECT_EQ(IntRows(faker, "MATCH (n:A) RETURN n.x"), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:B) RETURN count(n)"), (Rows{{0}}));
}

TEST_F(VersioningGatedTest, SessionOnDroppedBranchCanUseAnotherBranch) {
  faker.Interpret("CREATE (:A {x: 1})");
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("CREATE BRANCH c");
  faker.Interpret("USE BRANCH b");
  ForceDrop(db->version_store(), "b");

  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("MATCH (n:A) RETURN n.x"); }), kDeadBranch);
  EXPECT_EQ(Status(faker, "USE BRANCH c"), "Using branch c");
  EXPECT_EQ(IntRows(faker, "MATCH (n:A) RETURN n.x"), (Rows{{1}}));
}

// CREATE SNAPSHOT, SHOW SNAPSHOTS, FREE MEMORY, the global isolation level and the data directory lock statements act
// on main: a session on a branch is told so, a session on main is not.
TEST_F(VersioningGatedTest, DatabaseWideStatementsFromABranchSessionSayTheyRanOnMain) {
  faker.Interpret("CREATE (:A {x: 1})");
  faker.Interpret("CREATE BRANCH b");
  const auto notifications = [&](const std::string &query) {
    auto const stream = faker.Interpret(query);
    std::vector<std::pair<std::string, std::string>> result;
    if (stream.GetSummary().contains("notifications")) {
      for (auto const &notification : stream.GetSummary().at("notifications").ValueList()) {
        auto const &map = notification.ValueMap();
        result.emplace_back(map.at("code").ValueString(), map.at("title").ValueString());
      }
    }
    return result;
  };
  using Notes = std::vector<std::pair<std::string, std::string>>;

  EXPECT_TRUE(notifications("FREE MEMORY").empty());
  faker.Interpret("USE BRANCH b");
  EXPECT_EQ(notifications("FREE MEMORY"),
            (Notes{{"RanOnMainFromBranch", "FREE MEMORY ran on main, not on branch 'b'."}}));
  EXPECT_EQ(notifications("CREATE SNAPSHOT"),
            (Notes{{"RanOnMainFromBranch", "CREATE SNAPSHOT ran on main, not on branch 'b'."}}));
  EXPECT_EQ(notifications("LOCK DATA DIRECTORY"),
            (Notes{{"RanOnMainFromBranch", "LOCK DATA DIRECTORY ran on main, not on branch 'b'."}}));
  EXPECT_EQ(notifications("DATA DIRECTORY LOCK STATUS"),
            (Notes{{"RanOnMainFromBranch", "DATA DIRECTORY LOCK STATUS ran on main, not on branch 'b'."}}));
  EXPECT_EQ(faker.Interpret("DATA DIRECTORY LOCK STATUS").GetResults().at(0).at(0).ValueString(),
            "Data directory is locked.");
  EXPECT_EQ(notifications("UNLOCK DATA DIRECTORY"),
            (Notes{{"RanOnMainFromBranch", "UNLOCK DATA DIRECTORY ran on main, not on branch 'b'."}}));
  EXPECT_EQ(notifications("SHOW SNAPSHOTS"),
            (Notes{{"RanOnMainFromBranch", "SHOW SNAPSHOTS ran on main, not on branch 'b'."}}));
  EXPECT_EQ(notifications("SHOW NEXT SNAPSHOT"),
            (Notes{{"RanOnMainFromBranch", "SHOW NEXT SNAPSHOT ran on main, not on branch 'b'."}}));
  EXPECT_EQ(notifications("SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED"),
            (Notes{{"RanOnMainFromBranch", "SET GLOBAL TRANSACTION ISOLATION LEVEL ran on main, not on branch 'b'."}}));
  EXPECT_TRUE(notifications("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED").empty());
  EXPECT_TRUE(notifications("SET NEXT TRANSACTION ISOLATION LEVEL READ COMMITTED").empty());
  faker.Interpret("USE BRANCH main");
  EXPECT_TRUE(notifications("FREE MEMORY").empty());
  EXPECT_TRUE(notifications("DATA DIRECTORY LOCK STATUS").empty());
  EXPECT_TRUE(notifications("SHOW SNAPSHOTS").empty());
  EXPECT_TRUE(notifications("SET GLOBAL TRANSACTION ISOLATION LEVEL SNAPSHOT ISOLATION").empty());
}

// A new branch with the dropped one's name is another branch: the session stays dead until it switches to it.
TEST_F(VersioningGatedTest, SessionOnDroppedBranchStaysDeadWhenTheNameIsReused) {
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("USE BRANCH b");
  faker.Interpret("CREATE (:Old)");
  ForceDrop(db->version_store(), "b");
  ASSERT_TRUE(db->version_store()->CreateBranch("b"));

  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("MATCH (n) RETURN n"); }),
            "Branch 'b' was dropped (a branch with the same name was created since). Run USE BRANCH b to use the new "
            "one, or USE DATABASE to continue.");
  EXPECT_EQ(CheckoutCount(db->version_store(), "b"), 0U);
  EXPECT_EQ(Status(faker, "USE BRANCH b"), "Using branch b");
  EXPECT_EQ(CheckoutCount(db->version_store(), "b"), 1U);
  EXPECT_EQ(IntRows(faker, "MATCH (n:Old) RETURN count(n)"), (Rows{{0}}));
}

// The dead-branch error names USE DATABASE as the way out: it leaves a gone branch (on a REPLICA USE BRANCH is
// refused) and keeps a live one.
TEST_F(VersioningGatedTest, UseDatabaseLeavesADroppedBranch) {
  db->Rename("tenant");
  const auto use_db = std::string{"USE DATABASE tenant"};
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("USE BRANCH b");
  EXPECT_EQ(Status(faker, use_db), "Already using tenant") << "a live branch is kept";
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  ForceDrop(db->version_store(), "b");
  ASSERT_TRUE(repl_state.Lock()->SetReplicationRoleReplica(
      memgraph::replication::ReplicationServerConfig{.repl_server =
                                                         memgraph::io::network::Endpoint("127.0.0.1", 30246)},
      /*maybe_main_uuid=*/std::nullopt));
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("USE BRANCH main"); }),
            "Graph Versioning branches are not supported with replication in v1: branch operations are refused on a "
            "REPLICA. Run them on the MAIN instance.");
  EXPECT_EQ(Status(faker, use_db), "Using tenant");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  EXPECT_EQ(IntRows(faker, "RETURN 1"), (Rows{{1}}));
}

// An idle session on a branch holds none of main's locks: main's DDL is not blocked, and the branch keeps its fork
// point.
TEST_F(VersioningGatedTest, IdleBranchSessionDoesNotBlockMainDdl) {
  faker.Interpret("CREATE (:K {id: 1, v: 10}), (:K {id: 2, v: 20})");
  faker.Interpret("CREATE BRANCH b");
  InterpreterFaker other{&interpreter_context, db};
  other.Interpret("USE BRANCH b");
  ASSERT_EQ(IntRows(other, "MATCH (n:K) RETURN n.v ORDER BY n.id"), (Rows{{10}, {20}}));

  faker.Interpret("CREATE INDEX ON :K(id)");
  faker.Interpret("MATCH (n:K {id: 1}) SET n.v = 11");
  EXPECT_EQ(IntRows(faker, "MATCH (n:K {id: 1}) RETURN n.v"), (Rows{{11}}));
  EXPECT_EQ(IntRows(other, "MATCH (n:K) RETURN n.v ORDER BY n.id"), (Rows{{10}, {20}}));
}

// Each USE BRANCH starts the next transaction on that branch's committed state, including another session's commits;
// a USE refused inside an open transaction keeps the session on its branch and snapshot.
TEST_F(VersioningGatedTest, SwitchingBranchesBetweenTransactionsSeesEachBranchsOwnSnapshot) {
  faker.Interpret("CREATE (:K {v: 1})");
  faker.Interpret("CREATE BRANCH x");
  faker.Interpret("CREATE BRANCH y");
  auto sessions = SessionsOnBranch(1, "x");
  auto &writer = *sessions[0];
  constexpr auto kRead = "MATCH (n:K) RETURN n.v ORDER BY n.v";

  faker.Interpret("USE BRANCH x");
  faker.Interpret("BEGIN");
  EXPECT_EQ(IntRows(faker, kRead), (Rows{{1}}));
  writer.Interpret("MATCH (n:K) SET n.v = 10");
  writer.Interpret("CREATE (:K {v: 11})");
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("USE BRANCH y"); }),
            ThrownMessage([&] { faker.Interpret("USE BRANCH main"); }));
  EXPECT_EQ(IntRows(faker, kRead), (Rows{{1}})) << "a refused USE must not move the open snapshot";
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "x");
  faker.Interpret("ROLLBACK");  // the refusal leaves only a rollback possible
  EXPECT_EQ(IntRows(faker, kRead), (Rows{{10}, {11}}));

  faker.Interpret("USE BRANCH y");
  EXPECT_EQ(IntRows(faker, kRead), (Rows{{1}})) << "x's commits are not y's";
  faker.Interpret("CREATE (:K {v: 20})");
  faker.Interpret("USE BRANCH main");
  EXPECT_EQ(IntRows(faker, kRead), (Rows{{1}}));
  faker.Interpret("MATCH (n:K) SET n.v = 30");
  faker.Interpret("USE BRANCH x");
  EXPECT_EQ(IntRows(faker, kRead), (Rows{{10}, {11}}));
  writer.Interpret("MATCH (n:K {v: 11}) SET n.v = 12");
  EXPECT_EQ(IntRows(faker, kRead), (Rows{{10}, {12}})) << "the next transaction sees the other session's commit";
  faker.Interpret("USE BRANCH y");
  EXPECT_EQ(IntRows(faker, kRead), (Rows{{1}, {20}}));
  faker.Interpret("USE BRANCH main");
  EXPECT_EQ(IntRows(faker, kRead), (Rows{{30}}));
}

// RENAME DATABASE under a session on a branch: the accessor's name (read by auth checks) is the new one, in the open
// transaction and in the next.
TEST_F(VersioningGatedTest, BranchSessionDatabaseNameFollowsARename) {
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("USE BRANCH b");
  auto &current_db = faker.interpreter.current_db_;
  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (n) RETURN n");
  ASSERT_TRUE(current_db.execution_db_accessor_.has_value());
  ASSERT_EQ(current_db.execution_db_accessor_->DatabaseName(), db->name());

  db->Rename("renamed_db");
  EXPECT_EQ(current_db.execution_db_accessor_->DatabaseName(), "renamed_db");
  faker.Interpret("ROLLBACK");

  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (n) RETURN n");
  ASSERT_TRUE(current_db.execution_db_accessor_.has_value());
  EXPECT_EQ(current_db.execution_db_accessor_->DatabaseName(), "renamed_db");
  faker.Interpret("ROLLBACK");
}
