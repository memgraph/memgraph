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

// Graph Versioning: the branch management statements, their gate and their privileges. Sessions are attached to
// branches through the C++ API.

#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "query/exceptions.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/query_user.hpp"
#include "utils/temporal.hpp"
#include "versioning/gate.hpp"
#include "versioning_fixture.hpp"

using memgraph::tests::versioning_fixture::ThrownMessage;
using memgraph::versioning::GateError;

namespace {

constexpr std::string_view kReplicaError =
    "Graph Versioning branches are not supported with replication in v1: branch operations are refused on a REPLICA. "
    "Run them on the MAIN instance.";

std::string GateMessage(GateError err) { return std::string{memgraph::versioning::GateErrorMessage(err)}; }

const std::vector<std::string> kBranchColumns{"name",
                                              "created_at",
                                              "sessions",
                                              "current",
                                              "fork_timestamp",
                                              "age_seconds",
                                              "holds_back_main_gc",
                                              "main_unreleased_deltas"};

void ExpectShowRefusedWith(InterpreterFaker &session, std::string_view message) {
  for (const auto *query : {"SHOW BRANCHES", "SHOW BRANCH"}) {
    EXPECT_EQ(ThrownMessage([&] { session.Interpret(query); }), message) << query;
  }
}

// Authorizes every privilege, but only on the databases it was built with, and records what each database was asked
// for.
struct ScopedDatabaseUser final : memgraph::query::QueryUserOrRole {
  using Asked = std::vector<std::pair<std::string, std::vector<memgraph::query::AuthQuery::Privilege>>>;

  explicit ScopedDatabaseUser(std::set<std::string, std::less<>> dbs)
      : memgraph::query::QueryUserOrRole{"scoped", {}}, dbs_{std::move(dbs)} {}

  bool IsAuthorized(const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
                    std::optional<std::string_view> db_name, memgraph::query::UserPolicy * /*policy*/) const override {
    if (db_name) asked_->emplace_back(std::string{*db_name}, privileges);
    return !db_name || dbs_.contains(*db_name);
  }

  std::shared_ptr<QueryUserOrRole> clone() const override { return std::make_shared<ScopedDatabaseUser>(*this); }

  std::vector<std::string> GetRolenames(std::optional<std::string> /*db_name*/) const override { return {}; }

  bool CanImpersonate(const std::string & /*target*/, memgraph::query::UserPolicy * /*policy*/,
                      std::optional<std::string_view> /*db_name*/) const override {
    return false;
  }

  std::string GetDefaultDB() const override { return "memgraph"; }

  std::set<std::string, std::less<>> dbs_;
  std::shared_ptr<Asked> asked_ = std::make_shared<Asked>();
};

// Durable branches need the WAL, so this database could never create one.
class VersioningNoWalTest : public VersioningFixture<true, false> {
 protected:
  void SetUp() override {
    VersioningFixture<true, false>::SetUp();
    SetVersioningFlag(true);
  }
};

}  // namespace

// Branch queries other than CREATE BRANCH check the flag and the storage mode, then that the instance is MAIN, and
// report the first failure.
TEST_F(VersioningInterpreterTest, ShowGateOrder) {
  memgraph::license::global_license_checker.EnableTesting();
  faker.Interpret("STORAGE MODE IN_MEMORY_ANALYTICAL");

  ExpectShowRefusedWith(faker, GateMessage(GateError::kDisabled));
  SetVersioningFlag(true);
  ExpectShowRefusedWith(faker, GateMessage(GateError::kUnsupportedStorageMode));
  faker.Interpret("STORAGE MODE IN_MEMORY_TRANSACTIONAL");
  ASSERT_TRUE(repl_state.Lock()->SetReplicationRoleReplica(
      memgraph::replication::ReplicationServerConfig{.repl_server =
                                                         memgraph::io::network::Endpoint("127.0.0.1", 30246)},
      /*maybe_main_uuid=*/std::nullopt));
  ExpectShowRefusedWith(faker, kReplicaError);
}

// Commit-lock narrowing on main is not a prerequisite: listing branches works with it on.
TEST_F(VersioningNarrowingGatedTest, ShowBranchesWorksWithCommitLockNarrowingOn) {
  ASSERT_TRUE(db->storage()->config_.experimental_commit_lock_narrowing);
  ASSERT_TRUE(db->version_store()->CreateBranch("feature"));
  EXPECT_EQ(faker.Interpret("SHOW BRANCHES").GetResults().size(), 2U);
  EXPECT_EQ(faker.Interpret("SHOW BRANCH").GetResults().size(), 1U);
}

// Only creating branches needs the license and durable branches: listing them works without either.
TEST_F(VersioningNoWalTest, ShowNeedsNeitherLicenseNorDurability) {
  EXPECT_EQ(faker.Interpret("SHOW BRANCHES").GetResults().size(), 1U);
  EXPECT_EQ(faker.Interpret("SHOW BRANCH").GetResults().size(), 1U);
}

// A database gets its branch registry only when it starts in IN_MEMORY_TRANSACTIONAL.
TEST_F(VersioningGatedTest, DatabaseStartedInAnotherStorageModeHasNoBranches) {
  auto analytical_config = config;
  analytical_config.durability.storage_directory = data_directory / "analytical_db";
  analytical_config.disk.main_storage_directory = data_directory / "analytical_db" / "disk";
  analytical_config.salient.storage_mode = memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL;
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> gk{analytical_config};
  InterpreterFaker session{&interpreter_context, *gk.access()};
  session.Interpret("STORAGE MODE IN_MEMORY_TRANSACTIONAL");

  ExpectShowRefusedWith(session,
                        "Graph versioning is not available for this database: it did not start in "
                        "IN_MEMORY_TRANSACTIONAL storage mode.");
}

TEST_F(VersioningGatedTest, ShowBranchesListsMainFirstWithSessionsAndCurrent) {
  auto *version_store = db->version_store();
  ASSERT_TRUE(version_store->CreateBranch("feature"));
  ASSERT_TRUE(version_store->CreateBranch("other"));
  InterpreterFaker on_feature{&interpreter_context, db};
  Attach(on_feature, "feature");
  const auto created_at = memgraph::utils::LocalDateTime(*version_store->Get("feature")->created_at_us).ToStringWTZ();

  {
    auto stream = faker.Interpret("SHOW BRANCHES");
    ASSERT_EQ(stream.GetHeader(), kBranchColumns);
    const auto &rows = stream.GetResults();
    ASSERT_EQ(rows.size(), 3U);
    EXPECT_EQ(rows[0][0].ValueString(), "main");
    EXPECT_TRUE(rows[0][1].type() == memgraph::communication::bolt::Value::Type::Null);
    EXPECT_EQ(rows[0][2].ValueInt(), 1);
    EXPECT_TRUE(rows[0][3].ValueBool());
    EXPECT_EQ(rows[1][0].ValueString(), "feature");
    EXPECT_EQ(rows[1][1].ValueString(), created_at);
    EXPECT_EQ(rows[1][2].ValueInt(), 1);
    EXPECT_FALSE(rows[1][3].ValueBool());
    EXPECT_EQ(rows[2][0].ValueString(), "other");
    EXPECT_EQ(rows[2][2].ValueInt(), 0);
    EXPECT_FALSE(rows[2][3].ValueBool());
  }
  {
    auto stream = on_feature.Interpret("SHOW BRANCHES");
    const auto &rows = stream.GetResults();
    ASSERT_EQ(rows.size(), 3U);
    EXPECT_FALSE(rows[0][3].ValueBool());
    EXPECT_TRUE(rows[1][3].ValueBool());
    EXPECT_FALSE(rows[2][3].ValueBool());
  }
}

// The columns an operator reads to find a forgotten branch holding main's GC back: the fork timestamp, the age, whether
// this is the oldest pin and main's unreleased delta count (a gauge, the same on every row). Main has no fork.
TEST_F(VersioningGatedTest, ShowBranchesReportsForkPinAgeAndWhichBranchHoldsMainsGcBack) {
  auto *version_store = db->version_store();
  ASSERT_TRUE(version_store->CreateBranch("older"));
  faker.Interpret("CREATE (:A)");
  ASSERT_TRUE(version_store->CreateBranch("newer"));
  const auto older = *version_store->Get("older");
  const auto newer = *version_store->Get("newer");
  ASSERT_LT(older.fork_ts, newer.fork_ts);

  const auto expect_rows = [&](const std::string &query, size_t first) {
    auto stream = faker.Interpret(query);
    ASSERT_EQ(stream.GetHeader(), kBranchColumns) << query;
    const auto &rows = stream.GetResults();
    for (const auto &row : rows) {
      ASSERT_EQ(row.size(), kBranchColumns.size()) << query;
      EXPECT_GE(row[7].ValueInt(), 0) << query;
      if (row[0].ValueString() == "main") {
        EXPECT_TRUE(row[4].type() == memgraph::communication::bolt::Value::Type::Null) << query;
        EXPECT_TRUE(row[5].type() == memgraph::communication::bolt::Value::Type::Null) << query;
        EXPECT_TRUE(row[6].type() == memgraph::communication::bolt::Value::Type::Null) << query;
      } else if (row[0].ValueString() == "older") {
        EXPECT_EQ(row[4].ValueInt(), static_cast<int64_t>(older.fork_ts)) << query;
        EXPECT_GE(row[5].ValueInt(), 0) << query;
        EXPECT_TRUE(row[6].ValueBool()) << query;
      } else {
        EXPECT_EQ(row[0].ValueString(), "newer") << query;
        EXPECT_EQ(row[4].ValueInt(), static_cast<int64_t>(newer.fork_ts)) << query;
        EXPECT_FALSE(row[6].ValueBool()) << query;
      }
    }
    EXPECT_EQ(rows.size(), first) << query;
  };
  expect_rows("SHOW BRANCHES", 3U);
  expect_rows("SHOW BRANCH older", 1U);
  expect_rows("SHOW BRANCH newer", 1U);

  ASSERT_EQ(version_store->DropBranch("older"), memgraph::versioning::DropResult::kDropped);
  auto stream = faker.Interpret("SHOW BRANCH newer");
  EXPECT_TRUE(stream.GetResults().at(0)[6].ValueBool()) << "once the older pin is gone this one is the oldest";
}

// SHOW BRANCH without a name reports the session's branch; a quoted name is a stripped literal, so the cached AST must
// not keep the first name it was parsed with.
TEST_F(VersioningGatedTest, ShowBranchReportsOneBranch) {
  ASSERT_TRUE(db->version_store()->CreateBranch("feature"));
  ASSERT_TRUE(db->version_store()->CreateBranch("other"));
  InterpreterFaker on_feature{&interpreter_context, db};
  Attach(on_feature, "feature");

  const auto row = [](InterpreterFaker &session, const std::string &query) {
    auto stream = session.Interpret(query);
    EXPECT_EQ(stream.GetHeader(), kBranchColumns) << query;
    EXPECT_EQ(stream.GetResults().size(), 1U) << query;
    const auto &r = stream.GetResults().at(0);
    return std::tuple{r[0].ValueString(),
                      r[1].type() == memgraph::communication::bolt::Value::Type::Null,
                      r[2].ValueInt(),
                      r[3].ValueBool()};
  };
  EXPECT_EQ(row(faker, "SHOW BRANCH"), std::tuple("main", true, 1, true));
  EXPECT_EQ(row(on_feature, "SHOW BRANCH"), std::tuple("feature", false, 1, true));
  EXPECT_EQ(row(on_feature, "SHOW BRANCH main"), std::tuple("main", true, 1, false));
  EXPECT_EQ(row(faker, "SHOW BRANCH 'feature'"), std::tuple("feature", false, 1, false));
  EXPECT_EQ(row(faker, "SHOW BRANCH 'other'"), std::tuple("other", false, 0, false));
  EXPECT_EQ(row(faker, "SHOW BRANCH other"), std::tuple("other", false, 0, false));
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("SHOW BRANCH 'nope'"); }), "Branch 'nope' does not exist.");
}

// A session whose database was dropped from under it has none left: every branch statement is refused like any other
// query, none aborts the server.
TEST_F(VersioningGatedTest, BranchStatementsWithoutACurrentDatabaseAreRefused) {
  faker.interpreter.ResetDB();
  for (const auto *query : {"SHOW BRANCH", "SHOW BRANCH 'b'", "SHOW BRANCHES", "SHOW BRANCHES FOR DATABASE memgraph"}) {
    EXPECT_EQ(ThrownMessage([&] { faker.Interpret(query); }), "Database required for query execution.") << query;
  }
}

// A session whose branch is gone gets the dead-branch error for branch queries too, until it leaves.
TEST_F(VersioningGatedTest, ShowOnABranchThatIsGoneFails) {
  Fork("gone");
  ASSERT_EQ(db->version_store()->ForceDropBranch("gone"), memgraph::versioning::DropResult::kDropped);
  ExpectShowRefusedWith(faker, "Branch 'gone' no longer exists. Run USE BRANCH or USE DATABASE to continue.");
  faker.interpreter.current_db_.LeaveBranch();
  EXPECT_EQ(faker.Interpret("SHOW BRANCH").GetResults().at(0)[0].ValueString(), "main");
}

TEST_F(VersioningGatedTest, BranchQueriesAreRefusedInExplicitTransactions) {
  faker.Interpret("BEGIN");
  EXPECT_THROW(faker.Interpret("SHOW BRANCHES"), memgraph::query::VersioningQueryInMulticommandTxException);
  faker.Interpret("ROLLBACK");
}

TEST_F(VersioningGatedTest, BranchQueriesReportBranchPrivileges) {
  using memgraph::query::AuthQuery;
  const auto privileges_of = [&](const std::string &query) {
    auto prepared =
        faker.interpreter.Prepare(query, [](auto *) { return memgraph::storage::ExternalPropertyValue::map_t{}; }, {});
    faker.Abort();
    return prepared.privileges;
  };
  for (const auto *query : {"SHOW BRANCH", "SHOW BRANCH main", "SHOW BRANCHES"}) {
    EXPECT_THAT(privileges_of(query), ::testing::ElementsAre(AuthQuery::Privilege::BRANCH_USE)) << query;
  }
}

// A name that is not a plain identifier fails as a grammar error; for branch statements the message says to quote it.
TEST_F(VersioningGatedTest, BranchNameThatIsNotAnIdentifierGetsAQuotingHint) {
  for (const auto *query : {"SHOW BRANCH a-b", "show branch a.b", "SHOW BRANCH 007"}) {
    EXPECT_THAT(ThrownMessage([&] { faker.Interpret(query); }),
                ::testing::HasSubstr("quote it, for example 'my-branch' or `my-branch`."))
        << query;
  }
  for (const auto *query : {"MATCH (n RETURN n", "SHOW BRANCH x garbage", "SHOW DATABASE a-b"}) {
    EXPECT_THAT(ThrownMessage([&] { faker.Interpret(query); }), ::testing::Not(::testing::HasSubstr("quote it")))
        << query;
  }
  for (const auto *query : {"SHOW BRANCH 'a-b'", "SHOW BRANCH `007`"}) {
    EXPECT_NO_THROW(memgraph::query::frontend::opencypher::Parser{query}) << query;
  }
}

// SHOW BRANCHES FOR DATABASE is authorized against the listed database, before the database is looked up.
TEST_F(VersioningGatedTest, ShowBranchesForDatabaseIsAuthorizedAgainstTheListedDatabase) {
  auto user = std::make_shared<ScopedDatabaseUser>(std::set<std::string, std::less<>>{"memgraph"});
  faker.interpreter.SetUser(user);
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("SHOW BRANCHES FOR DATABASE other_tenant"); }),
            "You are not authorized to show branches of the database \"other_tenant\"! Please contact your database "
            "administrator.");
  EXPECT_THAT(*user->asked_,
              ::testing::Contains(::testing::Pair(
                  "other_tenant", ::testing::ElementsAre(memgraph::query::AuthQuery::Privilege::BRANCH_USE))));
}
