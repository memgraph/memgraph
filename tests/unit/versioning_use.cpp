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

// Graph Versioning: USE BRANCH, the Bolt db="<database>@<branch>" pin, and what a session does on a branch that is
// dropped under it.

#include <array>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "metrics/prometheus_metrics.hpp"
#include "query/exceptions.hpp"
#include "versioning/gate.hpp"
#include "versioning_fixture.hpp"

using memgraph::tests::versioning_fixture::IntRows;
using memgraph::tests::versioning_fixture::kNullInt;
using memgraph::tests::versioning_fixture::Rows;
using memgraph::tests::versioning_fixture::ThrownMessage;
using memgraph::versioning::GateError;

namespace {

constexpr std::string_view kDeadBranch = "Branch 'b' no longer exists. Run USE BRANCH or USE DATABASE to continue.";

std::string GateMessage(GateError err) { return std::string{memgraph::versioning::GateErrorMessage(err)}; }

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
  store->MarkUnavailable(store->Get("bad")->number, "the write-ahead log directory is missing");
  faker.Interpret("USE BRANCH feature");

  const auto message = ThrownMessage([&] { faker.Interpret("USE BRANCH bad"); });
  EXPECT_THAT(message, ::testing::HasSubstr("Branch 'bad' is unavailable: the write-ahead log directory is missing."));
  EXPECT_THAT(message, ::testing::HasSubstr("restart Memgraph"));
  EXPECT_THAT(message, ::testing::HasSubstr("DROP BRANCH 'bad'"));
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "feature");
  EXPECT_EQ(store->CheckoutCount("bad"), 0U);
  EXPECT_EQ(store->CheckoutCount("feature"), 1U);

  faker.Interpret("DROP BRANCH bad");
  EXPECT_FALSE(store->Exists("bad"));
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("USE BRANCH bad"); }), "Branch 'bad' does not exist.");
}

// With the ledger unreadable the branches are unknown: USE BRANCH fails with the ledger's message, whatever the name.
TEST_F(VersioningGatedTest, UseBranchWithAnUnreadableLedgerSaysSo) {
  db->version_store()->RestoreLedgerUnreadable("The branch ledger cannot be read; restore it.");
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("USE BRANCH x"); }), "The branch ledger cannot be read; restore it.");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
}

// Two or more lookups on one symbol are read in one CacheProperties pass; on a branch it must read the branch's values,
// not main's at the fork.
TEST_F(VersioningGatedTest, SeveralPropertyLookupsOnABranchReadTheBranchsValues) {
  faker.Interpret("CREATE (:A {a: 1, b: 2})-[:R {a: 1, b: 2}]->(:A {a: 5, b: 6})");
  faker.Interpret("CREATE BRANCH feature");
  faker.Interpret("USE BRANCH feature");
  faker.Interpret("MATCH (n:A {a: 1}) SET n.a = 10 REMOVE n.b");
  faker.Interpret("MATCH ()-[r:R]->() SET r.a = 10 REMOVE r.b");

  constexpr auto kNodes = "MATCH (n:A) RETURN n.a, n.b ORDER BY n.a";
  constexpr auto kEdges = "MATCH ()-[r:R]->() RETURN r.a, r.b";
  for (auto const query : {kNodes, kEdges}) {
    bool cached = false;
    for (auto const &row : faker.Interpret(std::string{"EXPLAIN "} + query).GetResults())
      cached = cached || row.at(0).ValueString().contains("CacheProperties");
    EXPECT_TRUE(cached) << "the plan must read the lookups in one pass: " << query;
  }

  EXPECT_EQ(IntRows(faker, kNodes), (Rows{{5, 6}, {10, kNullInt}}));
  EXPECT_EQ(IntRows(faker, kEdges), (Rows{{10, kNullInt}}));

  faker.Interpret("USE BRANCH main");
  EXPECT_EQ(IntRows(faker, kNodes), (Rows{{1, 2}, {5, 6}}));
  EXPECT_EQ(IntRows(faker, kEdges), (Rows{{1, 2}}));
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
  EXPECT_EQ(store->CheckoutCount("x"), 1U);
  EXPECT_DOUBLE_EQ(sessions.Value(), sessions0 + 1);
  faker.Interpret("USE BRANCH y");
  EXPECT_DOUBLE_EQ(sessions.Value(), sessions0 + 1) << "a switch moves the session, it does not add one";
  EXPECT_EQ(store->CheckoutCount("x"), 0U);
  EXPECT_EQ(store->CheckoutCount("y"), 1U);
  faker.Interpret("CREATE (:OnY)");
  EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE n:Base OR n:OnX OR n:OnY RETURN count(n)"), (Rows{{2}}));
  faker.Interpret("USE BRANCH main");
  EXPECT_EQ(store->CheckoutCount("y"), 0U);
  EXPECT_DOUBLE_EQ(sessions.Value(), sessions0);
  faker.Interpret("CREATE (:OnMain)");
  EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE n:Base OR n:OnMain RETURN count(n)"), (Rows{{2}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE n:OnX OR n:OnY RETURN count(n)"), (Rows{{0}}));
  faker.Interpret("USE BRANCH x");
  EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE n:OnX OR n:OnY OR n:OnMain RETURN count(n)"), (Rows{{1}}));
}

// The old branch is released only once the new one is attached: a switch that fails half-way (here `c` cannot persist
// its gid floors) keeps the session on its branch.
TEST_F(VersioningGatedTest, AFailedSwitchKeepsTheSessionOnItsBranch) {
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("CREATE BRANCH c");
  faker.Interpret("USE BRANCH b");
  faker.Interpret("CREATE (:OnB)");
  auto *store = db->version_store();

  // The registry is replaced through `ledger.tmp`; a directory in its place makes that write fail.
  const auto blocker = config.durability.storage_directory / "branches" / "ledger.tmp";
  std::filesystem::create_directory(blocker);
  EXPECT_THAT(ThrownMessage([&] { faker.Interpret("USE BRANCH c"); }), ::testing::HasSubstr("Branch ledger"));
  std::filesystem::remove(blocker);

  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  EXPECT_EQ(store->CheckoutCount("b"), 1U);
  EXPECT_EQ(store->CheckoutCount("c"), 0U);
  EXPECT_EQ(IntRows(faker, "MATCH (n:OnB) RETURN count(n)"), (Rows{{1}}));
  faker.Interpret("USE BRANCH c");
  EXPECT_EQ(store->CheckoutCount("b"), 0U);
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
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 0U);
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

TEST_F(VersioningGatedTest, UseBranchNeedsBranchUse) {
  faker.Interpret("CREATE BRANCH b");
  auto prepared = faker.interpreter.Prepare(
      "USE BRANCH b", [](auto *) { return memgraph::storage::ExternalPropertyValue::map_t{}; }, {});
  faker.Abort();
  EXPECT_THAT(prepared.privileges, ::testing::ElementsAre(memgraph::query::AuthQuery::Privilege::BRANCH_USE));
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
  ASSERT_TRUE(db->version_store()->FreezeAndDropAll().has_value());
  db->version_store()->Unfreeze();

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
  ASSERT_EQ(db->version_store()->ForceDropBranch("b"), memgraph::versioning::DropResult::kDropped);

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
  ASSERT_EQ(db->version_store()->ForceDropBranch("b"), memgraph::versioning::DropResult::kDropped);
  ASSERT_TRUE(db->version_store()->CreateBranch("b"));

  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("MATCH (n) RETURN n"); }),
            "Branch 'b' was dropped (a branch with the same name was created since). Run USE BRANCH b to use the new "
            "one, or USE DATABASE to continue.");
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 0U);
  EXPECT_EQ(Status(faker, "USE BRANCH b"), "Using branch b");
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 1U);
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
  ASSERT_TRUE(db->version_store()->FreezeAndDropAll().has_value());
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

// Commit-lock narrowing on main is not a prerequisite: a session moves between main and a branch, and writes on each.
TEST_F(VersioningNarrowingGatedTest, UseBranchWorksWithCommitLockNarrowingOn) {
  ASSERT_TRUE(db->storage()->config_.experimental_commit_lock_narrowing);
  faker.Interpret("CREATE (:M {v: 1})");
  faker.Interpret("CREATE BRANCH b");
  EXPECT_EQ(Status(faker, "USE BRANCH b"), "Using branch b");
  faker.Interpret("CREATE (:B {v: 2})");
  EXPECT_EQ(Status(faker, "USE BRANCH main"), "Using branch main");
  faker.Interpret("CREATE (:M {v: 3})");
  EXPECT_EQ(IntRows(faker, "MATCH (n:B) RETURN count(n)"), (Rows{{0}}));
  faker.Interpret("USE BRANCH b");
  EXPECT_EQ(IntRows(faker, "MATCH (n:M) RETURN n.v"), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:B) RETURN n.v"), (Rows{{2}}));
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

// Bounded stress: sessions on main, x and y write their own label while a fourth hops between all three in explicit
// transactions. Nobody sees another branch's label, and no write is lost.
TEST_F(VersioningGatedTest, ConcurrentSessionsOnTwoBranchesAndMainNeverSeeEachOthersWrites) {
  constexpr int kWrites = 60;
  static constexpr std::array<const char *, 3> kLabels{"OnMain", "OnX", "OnY"};
  static constexpr std::array<const char *, 3> kBranches{"main", "x", "y"};
  faker.Interpret("CREATE (:Base)");
  faker.Interpret("CREATE BRANCH x");
  faker.Interpret("CREATE BRANCH y");
  auto on_x = SessionsOnBranch(1, "x");
  auto on_y = SessionsOnBranch(1, "y");
  auto hopper = std::make_unique<InterpreterFaker>(&interpreter_context, db);
  interpreter_context.auth_checker = &faker.auth_checker;
  std::array<InterpreterFaker *, 3> const owners{&faker, on_x[0].get(), on_y[0].get()};

  auto const foreign_count = [](InterpreterFaker &session, int branch) {
    std::string predicate;
    for (int other = 0; other < 3; ++other) {
      if (other == branch) continue;
      predicate += fmt::format("{}n:{}", predicate.empty() ? "" : " OR ", kLabels[other]);
    }
    return IntRows(session, fmt::format("MATCH (n) WHERE {} RETURN count(n)", predicate));
  };

  {
    std::vector<std::jthread> threads;
    for (int branch = 0; branch < 3; ++branch) {
      threads.emplace_back([&, branch] {
        auto &session = *owners[branch];
        for (int i = 1; i <= kWrites; ++i) {
          session.Interpret("BEGIN");
          session.Interpret(fmt::format("CREATE (:{})", kLabels[branch]));
          auto const own = IntRows(session, fmt::format("MATCH (n:{}) RETURN count(n)", kLabels[branch]));
          auto const foreign = foreign_count(session, branch);
          session.Interpret("COMMIT");
          ASSERT_EQ(own, (Rows{{i}}));
          ASSERT_EQ(foreign, (Rows{{0}})) << "a write of another branch is visible on " << kBranches[branch];
        }
      });
    }
    threads.emplace_back([&] {
      std::array<int, 3> hopped{};
      for (int round = 0; round < kWrites; ++round) {
        for (int branch = 0; branch < 3; ++branch) {
          hopper->Interpret(fmt::format("USE BRANCH {}", kBranches[branch]));
          hopper->Interpret("BEGIN");
          hopper->Interpret(fmt::format("CREATE (:H {{b: {}}})", branch));
          auto const own = IntRows(*hopper, fmt::format("MATCH (n:H {{b: {}}}) RETURN count(n)", branch));
          auto const alien = IntRows(*hopper, fmt::format("MATCH (n:H) WHERE n.b <> {} RETURN count(n)", branch));
          auto const foreign = foreign_count(*hopper, branch);
          hopper->Interpret("COMMIT");
          ++hopped[branch];
          ASSERT_EQ(own, (Rows{{hopped[branch]}})) << "lost or foreign hopper write on " << kBranches[branch];
          ASSERT_EQ(alien, (Rows{{0}}));
          ASSERT_EQ(foreign, (Rows{{0}}));
        }
      }
    });
  }

  for (int branch = 0; branch < 3; ++branch) {
    hopper->Interpret(fmt::format("USE BRANCH {}", kBranches[branch]));
    EXPECT_EQ(IntRows(*hopper, fmt::format("MATCH (n:{}) RETURN count(n)", kLabels[branch])), (Rows{{kWrites}}));
    EXPECT_EQ(IntRows(*hopper, fmt::format("MATCH (n:H {{b: {}}}) RETURN count(n)", branch)), (Rows{{kWrites}}));
    EXPECT_EQ(IntRows(*hopper, "MATCH (n:Base) RETURN count(n)"), (Rows{{1}}));
  }
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

// USE BRANCH builds a branch on its ledger's gid floors, not main's current counters: main objects created since the
// fork are not fork objects, and a native create never takes a gid main has handed out.
TEST_F(VersioningGatedTest, UseBranchBuildsTheCoreOnTheLedgersGidFloors) {
  faker.Interpret("CREATE (:V {k: 1}), (:V {k: 2})");
  auto &main = static_cast<memgraph::storage::InMemoryStorage &>(*db->storage());
  ASSERT_TRUE(db->version_store()->CreateBranch("b"));
  const auto number = db->version_store()->Get("b")->number;
  const auto vertex_floor = main.vertex_id_.load();
  const auto edge_floor = main.edge_id_.load();
  ASSERT_TRUE(db->version_store()->RecordGidFloors(number, vertex_floor, edge_floor));
  faker.Interpret("CREATE (:V {k: 3}), (:V {k: 4}), (:V {k: 5})");
  const auto main_vertex_counter = main.vertex_id_.load();
  ASSERT_GT(main_vertex_counter, vertex_floor);

  faker.Interpret("USE BRANCH b");

  const auto *core = faker.interpreter.current_db_.branch_context()->core().get();
  EXPECT_EQ(core->vertex_gid_floor(), vertex_floor);
  EXPECT_EQ(core->edge_gid_floor(), edge_floor);
  EXPECT_EQ(IntRows(faker, "MATCH (n:V) RETURN count(n)"), (Rows{{2}}));
  const auto created = IntRows(faker, "CREATE (n:N) RETURN id(n)");
  ASSERT_EQ(created.size(), 1U);
  EXPECT_GE(created[0][0], static_cast<int64_t>(main_vertex_counter));
  EXPECT_EQ(IntRows(faker, "MATCH (n:V) RETURN count(n)"), (Rows{{2}}));
}

// A branch session commits through the branch context, so a periodic batch's fork deletes are counted once.
TEST_F(VersioningGatedTest, SessionCommitsOnABranchCountForkDeletes) {
  faker.Interpret("CREATE (:V), (:V), (:V)");
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("USE BRANCH b");
  faker.Interpret("MATCH (n:V) WITH n LIMIT 2 DETACH DELETE n");
  EXPECT_EQ(faker.interpreter.current_db_.branch_context()->core()->ForkDeletes(),
            (memgraph::versioning::ObjectCounts{2, 0}));
  faker.Interpret("USING PERIODIC COMMIT 1 MATCH (n:V) DETACH DELETE n");
  EXPECT_EQ(faker.interpreter.current_db_.branch_context()->core()->ForkDeletes(),
            (memgraph::versioning::ObjectCounts{3, 0}));
}

// Bolt db="<database>@<branch>": RuntimeConfig::Configure pins the session to the database (in_explicit_db) and calls
// SelectPinnedBranch with the branch part; these drive that pair without a Bolt session.
TEST_F(VersioningGatedTest, PinnedBranchRunsOnTheBranchAndAPlainPinIsMain) {
  faker.Interpret("CREATE BRANCH b");
  InterpreterFaker other{&interpreter_context, db};
  auto &current_db = faker.interpreter.current_db_;
  faker.interpreter.SetCurrentDB(*db_gk.access(), /*in_explicit_db=*/true);

  faker.interpreter.SelectPinnedBranch("b");
  EXPECT_EQ(current_db.CurrentVersion(), "b");
  faker.Interpret("CREATE (:OnB)");
  EXPECT_EQ(IntRows(faker, "MATCH (n:OnB) RETURN count(n)"), (Rows{{1}}));
  EXPECT_EQ(IntRows(other, "MATCH (n:OnB) RETURN count(n)"), (Rows{{0}})) << "the write is the branch's";

  faker.interpreter.SelectPinnedBranch(std::nullopt);
  EXPECT_FALSE(current_db.CurrentVersion().has_value());
  EXPECT_EQ(IntRows(faker, "MATCH (n:OnB) RETURN count(n)"), (Rows{{0}}));
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 0U);

  faker.interpreter.SelectPinnedBranch("b");
  faker.interpreter.SelectPinnedBranch("main");
  EXPECT_FALSE(current_db.CurrentVersion().has_value()) << "'x@main' is main";
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 0U);
}

TEST_F(VersioningGatedTest, PinnedUnknownBranchFailsAndLeavesTheSessionWhereItWas) {
  faker.Interpret("CREATE BRANCH b");
  faker.interpreter.SetCurrentDB(*db_gk.access(), true);
  EXPECT_EQ(ThrownMessage([&] { faker.interpreter.SelectPinnedBranch("nope"); }), "Branch 'nope' does not exist.");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());

  faker.interpreter.SelectPinnedBranch("b");
  EXPECT_EQ(ThrownMessage([&] { faker.interpreter.SelectPinnedBranch("nope"); }), "Branch 'nope' does not exist.");
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 1U);
}

TEST_F(VersioningGatedTest, PinnedUnavailableBranchFailsWithTheReasonAndLeavesTheSessionWhereItWas) {
  faker.Interpret("CREATE BRANCH bad");
  auto *store = db->version_store();
  store->MarkUnavailable(store->Get("bad")->number, "the branch directory is not a directory");
  faker.interpreter.SetCurrentDB(*db_gk.access(), true);

  const auto message = ThrownMessage([&] { faker.interpreter.SelectPinnedBranch("bad"); });
  EXPECT_THAT(message, ::testing::HasSubstr("Branch 'bad' is unavailable: the branch directory is not a directory."));
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  EXPECT_EQ(store->CheckoutCount("bad"), 0U);
}

TEST_F(VersioningGatedTest, PinnedBranchPassesTheGateOfUseBranch) {
  faker.Interpret("CREATE (:V)");
  faker.Interpret("CREATE BRANCH b");
  faker.interpreter.SetCurrentDB(*db_gk.access(), true);

  SetVersioningFlag(false);
  EXPECT_EQ(ThrownMessage([&] { faker.interpreter.SelectPinnedBranch("b"); }), GateMessage(GateError::kDisabled));
  EXPECT_NO_THROW(faker.interpreter.SelectPinnedBranch(std::nullopt)) << "no '@' part, no gate";
  SetVersioningFlag(true);

  memgraph::license::global_license_checker.DisableTesting();
  faker.interpreter.SelectPinnedBranch("b");
  EXPECT_EQ(IntRows(faker, "MATCH (n:V) RETURN count(n)"), (Rows{{1}}));
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("CREATE (:V)"); }), GateMessage(GateError::kNoLicense));
  faker.interpreter.SelectPinnedBranch(std::nullopt);

  ASSERT_TRUE(repl_state.Lock()->SetReplicationRoleReplica(
      memgraph::replication::ReplicationServerConfig{.repl_server =
                                                         memgraph::io::network::Endpoint("127.0.0.1", 30246)},
      /*maybe_main_uuid=*/std::nullopt));
  EXPECT_EQ(ThrownMessage([&] { faker.interpreter.SelectPinnedBranch("b"); }),
            "Graph Versioning branches are not supported with replication in v1: branch operations are refused on a "
            "REPLICA. Run them on the MAIN instance.");
}

TEST_F(VersioningGatedTest, PinnedSessionRefusesUseBranchAndUseDatabaseButAllowsUsingBranch) {
  faker.Interpret("CREATE BRANCH b");
  faker.Interpret("USING BRANCH b CREATE (:OnB)");
  faker.interpreter.SetCurrentDB(*db_gk.access(), true);
  faker.interpreter.SelectPinnedBranch("b");

  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("USE BRANCH main"); }),
            "Branch switching is prohibited if the session explicitly defines the database.");
  EXPECT_THAT(ThrownMessage([&] { faker.Interpret("USE DATABASE memgraph"); }),
              ::testing::HasSubstr("Database switching is prohibited"));
  EXPECT_EQ(IntRows(faker, "USING BRANCH main MATCH (n:OnB) RETURN count(n)"), (Rows{{0}}));
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b") << "USING BRANCH changes no session state";
  EXPECT_EQ(IntRows(faker, "MATCH (n:OnB) RETURN count(n)"), (Rows{{1}}));
}

TEST_F(VersioningGatedTest, PinnedBranchDroppedFailsEveryTransactionUntilTheDbChanges) {
  auto &sessions = *memgraph::metrics::Metrics().global.versioning_active_sessions;
  faker.Interpret("CREATE (:K {v: 10})");
  faker.Interpret("CREATE BRANCH b");
  faker.interpreter.SetCurrentDB(*db_gk.access(), true);
  const auto sessions0 = sessions.Value();
  faker.interpreter.SelectPinnedBranch("b");
  faker.Interpret("CREATE (:K {v: 20})");
  ASSERT_DOUBLE_EQ(sessions.Value(), sessions0 + 1);
  ASSERT_EQ(db->version_store()->ForceDropBranch("b"), memgraph::versioning::DropResult::kDropped);

  for (const auto *query : {"MATCH (n:K) RETURN n.v", "CREATE (:K {v: 30})", "BEGIN"}) {
    EXPECT_EQ(ThrownMessage([&] { faker.Interpret(query); }), "Branch 'b' no longer exists.") << query;
  }
  faker.Interpret("ROLLBACK");
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");

  faker.interpreter.SelectPinnedBranch(std::nullopt);
  EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN n.v"), (Rows{{10}}));
  EXPECT_DOUBLE_EQ(sessions.Value(), sessions0) << "the dead checkout was released exactly once";
}

TEST_F(VersioningGatedTest, PinnedBranchTransactionsReadASnapshotThenWriteAndRollBack) {
  faker.Interpret("CREATE (:K {id: 1, v: 10}), (:K {id: 2, v: 20})");
  faker.Interpret("CREATE BRANCH b");
  InterpreterFaker main_session{&interpreter_context, db};
  auto sessions = SessionsOnBranch(1, "b");
  auto &writer = *sessions[0];
  faker.interpreter.SetCurrentDB(*db_gk.access(), /*in_explicit_db=*/true);
  faker.interpreter.SelectPinnedBranch("b");
  constexpr auto kRead = "MATCH (n:K) RETURN n.v ORDER BY n.id";
  const auto before = Rows{{10}, {20}};

  faker.Interpret("BEGIN");
  EXPECT_EQ(IntRows(faker, kRead), before);
  writer.Interpret("MATCH (n:K {id: 1}) SET n.v = 11");
  writer.Interpret("CREATE (:K {id: 3, v: 30})");
  main_session.Interpret("MATCH (n:K {id: 2}) SET n.v = 99");
  EXPECT_EQ(IntRows(faker, kRead), before) << "neither the branch's nor main's later commits enter the snapshot";
  EXPECT_EQ(IntRows(faker, kRead), before);
  faker.Interpret("MATCH (n:K {id: 2}) SET n.v = 21");
  EXPECT_EQ(IntRows(faker, kRead), (Rows{{10}, {21}})) << "own write over the snapshot";
  faker.Interpret("ROLLBACK");
  EXPECT_EQ(IntRows(faker, kRead), (Rows{{11}, {20}, {30}})) << "the rolled-back write left no trace";

  faker.Interpret("BEGIN");
  EXPECT_EQ(IntRows(faker, "MATCH (n:K {id: 1}) RETURN n.v"), (Rows{{11}}));
  writer.Interpret("MATCH (n:K {id: 1}) SET n.v = 12");
  EXPECT_EQ(IntRows(faker, "MATCH (n:K {id: 1}) RETURN n.v"), (Rows{{11}}));
  EXPECT_THAT(ThrownMessage([&] { faker.Interpret("MATCH (n:K {id: 1}) SET n.v = 13"); }),
              ::testing::HasSubstr("Cannot resolve conflicting transactions"))
      << "a write over a commit that landed after the snapshot must conflict";
  faker.Interpret("ROLLBACK");
  EXPECT_EQ(IntRows(faker, kRead), (Rows{{12}, {20}, {30}}));
  EXPECT_EQ(IntRows(main_session, kRead), (Rows{{10}, {99}})) << "main never saw the branch's writes";
}
