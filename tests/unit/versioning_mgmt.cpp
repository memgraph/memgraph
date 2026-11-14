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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "query/exceptions.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/query_user.hpp"
#include "utils/temporal.hpp"
#include "versioning/gate.hpp"
#include "versioning_fixture.hpp"

using memgraph::tests::versioning_fixture::IntRows;
using memgraph::tests::versioning_fixture::Rows;
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
                                              "main_unreleased_deltas",
                                              "status",
                                              "status_reason"};

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

struct LackingPrivilegesUser final : memgraph::query::QueryUserOrRole {
  explicit LackingPrivilegesUser(std::set<memgraph::query::AuthQuery::Privilege> lacking)
      : memgraph::query::QueryUserOrRole{"lacking", {}}, lacking_{std::move(lacking)} {}

  bool IsAuthorized(const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
                    std::optional<std::string_view> /*db_name*/,
                    memgraph::query::UserPolicy * /*policy*/) const override {
    return std::ranges::none_of(privileges, [&](auto privilege) { return lacking_.contains(privilege); });
  }

  std::shared_ptr<QueryUserOrRole> clone() const override { return std::make_shared<LackingPrivilegesUser>(*this); }

  std::vector<std::string> GetRolenames(std::optional<std::string> /*db_name*/) const override { return {}; }

  bool CanImpersonate(const std::string & /*target*/, memgraph::query::UserPolicy * /*policy*/,
                      std::optional<std::string_view> /*db_name*/) const override {
    return false;
  }

  std::string GetDefaultDB() const override { return "memgraph"; }

  std::set<memgraph::query::AuthQuery::Privilege> lacking_;
};

// Durable branches need the WAL, so this database could never create one.
class VersioningNoWalTest : public VersioningFixture<true, false> {
 protected:
  void SetUp() override {
    VersioningFixture<true, false>::SetUp();
    SetVersioningFlag(true);
  }
};

// The fixture's Attach, for another database.
void AttachTo(memgraph::dbms::DatabaseAccess &db, InterpreterFaker &session, const std::string &name) {
  auto *version_store = db->version_store();
  auto info = version_store->TryAcquireCheckout(name);
  ASSERT_TRUE(info) << info.error();
  auto *main = static_cast<memgraph::storage::InMemoryStorage *>(db->storage());
  auto core = version_store->GetOrBuildCore(
      info->number, [&]() -> std::expected<std::shared_ptr<memgraph::versioning::BranchCore>, std::string> {
        auto built = memgraph::versioning::BranchCore::BuildForCheckout(*main, *info);
        if (!built) return std::unexpected(std::move(built.error().message));
        (*built)->SetGidReserver(version_store->GidReserverOf(info->number));
        return std::move(*built);
      });
  if (!core) version_store->ReleaseCheckout(info->number);
  ASSERT_TRUE(core) << core.error();
  auto recorded = version_store->RecordGidFloors(info->number, (*core)->vertex_gid_floor(), (*core)->edge_gid_floor());
  if (!recorded) version_store->ReleaseCheckout(info->number);
  ASSERT_TRUE(recorded) << recorded.error();
  session.interpreter.current_db_.SetBranchContext(
      std::make_unique<memgraph::versioning::BranchContext>(std::move(*core), name), version_store, info->number);
}

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

// A branch whose files could not be recovered at startup is listed with status 'unavailable' and the reason; main and
// healthy branches are 'available' with no reason. DROP removes it without FORCE, and its name stays taken until then.
TEST_F(VersioningGatedTest, AnUnavailableBranchIsListedWithItsReasonAndDropsWithoutForce) {
  auto *version_store = db->version_store();
  ASSERT_TRUE(version_store->CreateBranch("good"));
  const auto bad = version_store->CreateBranch("bad");
  ASSERT_TRUE(bad);
  version_store->MarkUnavailable(bad->number, "the write-ahead log directory is missing");

  const auto is_null = [](const auto &value) {
    return value.type() == memgraph::communication::bolt::Value::Type::Null;
  };
  for (const std::string query : {"SHOW BRANCHES", "SHOW BRANCH bad"}) {
    auto stream = faker.Interpret(query);
    ASSERT_EQ(stream.GetHeader(), kBranchColumns) << query;
    bool seen = false;
    for (const auto &row : stream.GetResults()) {
      ASSERT_EQ(row.size(), kBranchColumns.size()) << query;
      if (row[0].ValueString() == "bad") {
        seen = true;
        EXPECT_EQ(row[8].ValueString(), "unavailable") << query;
        EXPECT_EQ(row[9].ValueString(), "the write-ahead log directory is missing") << query;
      } else {
        EXPECT_EQ(row[8].ValueString(), "available") << query << " " << row[0].ValueString();
        EXPECT_TRUE(is_null(row[9])) << query << " " << row[0].ValueString();
      }
    }
    EXPECT_TRUE(seen) << query;
  }

  EXPECT_THAT(ThrownMessage([&] { faker.Interpret("CREATE BRANCH bad"); }), ::testing::HasSubstr("already exists"));
  faker.Interpret("DROP BRANCH bad");
  EXPECT_FALSE(version_store->Exists("bad"));
  EXPECT_EQ(faker.Interpret("SHOW BRANCHES").GetResults().size(), 2U);
  faker.Interpret("CREATE BRANCH bad");
}

// An unreadable ledger must not look like zero branches: every branch statement refuses with its message.
TEST_F(VersioningGatedTest, AnUnreadableLedgerRefusesBranchStatementsWithItsMessage) {
  db->version_store()->RestoreLedgerUnreadable("The branch ledger cannot be read; restore it.");
  for (const char *query : {"SHOW BRANCHES", "SHOW BRANCH", "CREATE BRANCH x", "DROP BRANCH x"}) {
    EXPECT_EQ(ThrownMessage([&] { faker.Interpret(query); }), "The branch ledger cannot be read; restore it.") << query;
  }
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
  for (const auto *query : {"SHOW BRANCH",
                            "SHOW BRANCH 'b'",
                            "SHOW BRANCHES",
                            "SHOW BRANCHES FOR DATABASE memgraph",
                            "CREATE BRANCH x",
                            "DROP BRANCH x",
                            "DROP BRANCH x FORCE"}) {
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
  for (const auto *query : {"SHOW BRANCHES", "CREATE BRANCH 'a'", "DROP BRANCH 'a'"}) {
    EXPECT_THROW(faker.Interpret(query), memgraph::query::VersioningQueryInMulticommandTxException) << query;
  }
  faker.Interpret("ROLLBACK");
  EXPECT_TRUE(db->version_store()->Empty());
}

TEST_F(VersioningGatedTest, BranchQueriesReportBranchPrivileges) {
  using memgraph::query::AuthQuery;
  ASSERT_TRUE(db->version_store()->CreateBranch("feature"));
  const auto privileges_of = [&](const std::string &query) {
    auto prepared =
        faker.interpreter.Prepare(query, [](auto *) { return memgraph::storage::ExternalPropertyValue::map_t{}; }, {});
    faker.Abort();
    return prepared.privileges;
  };
  for (const auto *query : {"SHOW BRANCH", "SHOW BRANCH main", "SHOW BRANCHES"}) {
    EXPECT_THAT(privileges_of(query), ::testing::ElementsAre(AuthQuery::Privilege::BRANCH_USE)) << query;
  }
  EXPECT_THAT(privileges_of("CREATE BRANCH 'other'"), ::testing::ElementsAre(AuthQuery::Privilege::BRANCH_EDIT));
  EXPECT_THAT(privileges_of("DROP BRANCH 'feature'"), ::testing::ElementsAre(AuthQuery::Privilege::BRANCH_EDIT));
  EXPECT_THAT(
      privileges_of("DROP BRANCH 'feature' FORCE"),
      ::testing::UnorderedElementsAre(AuthQuery::Privilege::BRANCH_EDIT, AuthQuery::Privilege::TRANSACTION_MANAGEMENT));
}

TEST_F(VersioningGatedTest, AuthorizationRefusalNamesTheMissingBranchPrivilege) {
  using memgraph::query::AuthQuery;
  using Privilege = AuthQuery::Privilege;
  const auto refusal =
      [&](std::set<Privilege> lacking, std::vector<Privilege> privileges, std::optional<std::string> db = {}) {
        faker.interpreter.SetUser(std::make_shared<LackingPrivilegesUser>(std::move(lacking)));
        return ThrownMessage([&] { faker.interpreter.CheckAuthorized(privileges, std::move(db)); });
      };
  const std::set<Privilege> lacks_edit{Privilege::BRANCH_EDIT, Privilege::MATCH};
  const std::string_view edit_message =
      "You are not authorized to execute this query! Missing privilege: BRANCH_EDIT. Ask your database administrator "
      "to grant it.";
  EXPECT_EQ(refusal(lacks_edit, {Privilege::BRANCH_EDIT}), edit_message);
  EXPECT_EQ(refusal(lacks_edit, {Privilege::BRANCH_USE, Privilege::BRANCH_EDIT}), edit_message);
  EXPECT_EQ(refusal(lacks_edit, {Privilege::BRANCH_EDIT, Privilege::BRANCH_EDIT}), edit_message) << "named once";
  EXPECT_EQ(
      refusal(lacks_edit, {Privilege::BRANCH_EDIT}, std::optional<std::string>{"memgraph"}),
      "You are not authorized to execute this query on database \"memgraph\"! Missing privilege: BRANCH_EDIT. Ask "
      "your database administrator to grant it.");
  EXPECT_THAT(refusal(lacks_edit, {Privilege::MATCH}),
              ::testing::HasSubstr("You are not authorized to execute this query"));
  EXPECT_THAT(refusal(lacks_edit, {Privilege::MATCH}), ::testing::Not(::testing::HasSubstr("Missing privilege")));
  EXPECT_THAT(refusal(lacks_edit, {Privilege::MATCH, Privilege::BRANCH_EDIT}),
              ::testing::HasSubstr("Missing privilege: BRANCH_EDIT."));
  EXPECT_THAT(refusal({Privilege::BRANCH_USE, Privilege::BRANCH_EDIT}, {Privilege::BRANCH_USE, Privilege::BRANCH_EDIT}),
              ::testing::HasSubstr("Missing privilege: BRANCH_USE, BRANCH_EDIT."));
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

TEST_F(VersioningNoWalTest, CreateRefused) {
  memgraph::license::global_license_checker.EnableTesting();
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("CREATE BRANCH 'a'"); }), GateMessage(GateError::kNotDurable));
  EXPECT_TRUE(db->version_store()->Empty());
}

// CREATE BRANCH checks the flag, the license, the storage mode and durability, then that the instance is MAIN, and
// reports the first failure.
TEST_F(VersioningInterpreterTest, CreateGateOrder) {
  faker.Interpret("STORAGE MODE IN_MEMORY_ANALYTICAL");
  db->storage()->config_.durability.recover_on_startup = false;
  const auto expect_refused_with = [&](std::string_view message) {
    EXPECT_EQ(ThrownMessage([&] { faker.Interpret("CREATE BRANCH 'a'"); }), message);
  };

  expect_refused_with(GateMessage(GateError::kDisabled));
  SetVersioningFlag(true);
  expect_refused_with(GateMessage(GateError::kNoLicense));
  memgraph::license::global_license_checker.EnableTesting();
  expect_refused_with(GateMessage(GateError::kUnsupportedStorageMode));
  faker.Interpret("STORAGE MODE IN_MEMORY_TRANSACTIONAL");
  expect_refused_with(GateMessage(GateError::kNotDurable));
  db->storage()->config_.durability.recover_on_startup = true;
  ASSERT_TRUE(repl_state.Lock()->SetReplicationRoleReplica(
      memgraph::replication::ReplicationServerConfig{.repl_server =
                                                         memgraph::io::network::Endpoint("127.0.0.1", 30246)},
      /*maybe_main_uuid=*/std::nullopt));
  expect_refused_with(kReplicaError);
  for (const auto *query : {"DROP BRANCH 'a'", "DROP BRANCH 'a' FORCE"}) {
    EXPECT_EQ(ThrownMessage([&] { faker.Interpret(query); }), kReplicaError) << query;
  }
  EXPECT_TRUE(db->version_store()->Empty());
}

TEST_F(VersioningGatedTest, CreateAndDropBranch) {
  faker.Interpret("CREATE (:A)");
  {
    auto stream = faker.Interpret("CREATE BRANCH 'feature'");
    EXPECT_TRUE(stream.GetHeader().empty());
    EXPECT_TRUE(stream.GetResults().empty());
  }
  // The quoted name is a stripped literal: the cached AST must not keep the first one.
  faker.Interpret("CREATE BRANCH 'other'");
  faker.Interpret("CREATE BRANCH bare");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value()) << "CREATE BRANCH does not switch";
  faker.Interpret("CREATE (:A)");
  EXPECT_EQ(IntRows(faker, "MATCH (n:A) RETURN count(n)"), (Rows{{2}}));
  EXPECT_EQ(faker.Interpret("SHOW BRANCHES").GetResults().size(), 4U);
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("CREATE BRANCH 'feature'"); }), "Branch 'feature' already exists.");

  // Issued from a branch, CREATE BRANCH still forks from main.
  Attach(faker, "feature");
  faker.Interpret("CREATE (:A)");
  faker.Interpret("CREATE BRANCH 'from_branch'");
  faker.interpreter.current_db_.LeaveBranch();
  InterpreterFaker other{&interpreter_context, db};
  Attach(other, "from_branch");
  EXPECT_EQ(IntRows(other, "MATCH (n:A) RETURN count(n)"), (Rows{{2}}));
  other.interpreter.current_db_.LeaveBranch();

  for (const auto *query :
       {"DROP BRANCH 'feature'", "DROP BRANCH 'other'", "DROP BRANCH bare", "DROP BRANCH from_branch"}) {
    EXPECT_TRUE(faker.Interpret(query).GetResults().empty()) << query;
  }
  EXPECT_TRUE(db->version_store()->Empty());
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("DROP BRANCH 'feature'"); }), "Branch 'feature' does not exist.");
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("DROP BRANCH main"); }), "Branch 'main' does not exist.");
}

TEST_F(VersioningGatedTest, InvalidBranchNameRejected) {
  constexpr auto kAt = "Branch name must not contain '@': it names a branch as <database>@<branch>.";
  auto const too_long = fmt::format("CREATE BRANCH '{}'", std::string(64, 'a'));
  for (const auto &[query, message] :
       {std::pair{std::string{"CREATE BRANCH main"}, "'main' is a reserved branch name."},
        std::pair{std::string{"CREATE BRANCH ''"}, "Branch name must not be empty."},
        std::pair{std::string{"CREATE BRANCH 'a@b'"}, kAt},
        std::pair{too_long, "Branch name must be at most 63 characters."}}) {
    EXPECT_EQ(ThrownMessage([&] { faker.Interpret(query); }), message) << query;
  }
  EXPECT_TRUE(db->version_store()->Empty());

  // Boundary values are accepted.
  for (auto const &name : {std::string{"a"}, std::string(63, 'b'), std::string{"Rel-1.2_x"}}) {
    faker.Interpret(fmt::format("CREATE BRANCH '{}'", name));
    EXPECT_TRUE(db->version_store()->Exists(name)) << name;
  }
}

// Without a valid license branches stay readable and droppable, so a session on one is never stuck.
TEST_F(VersioningGatedTest, WithoutLicenseBranchesAreReadOnlyAndDroppable) {
  faker.Interpret("CREATE (:V {id: 1})");
  faker.Interpret("CREATE BRANCH 'keep'");
  faker.Interpret("CREATE BRANCH 'force_me'");
  Fork("b");
  faker.Interpret("CREATE (:V {id: 2})");
  InterpreterFaker on_force_me{&interpreter_context, db};
  Attach(on_force_me, "force_me");

  memgraph::license::global_license_checker.DisableTesting();
  const auto no_license = GateMessage(GateError::kNoLicense);
  EXPECT_EQ(IntRows(faker, "MATCH (n:V) RETURN count(n)"), (Rows{{2}}));
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("CREATE (:V {id: 3})"); }), no_license);
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("MATCH (n:V) SET n.x = 1"); }), no_license);
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("CREATE BRANCH 'c'"); }), no_license);
  EXPECT_FALSE(db->version_store()->Exists("c"));
  EXPECT_EQ(IntRows(faker, "MATCH (n:V) WHERE n.x IS NULL RETURN count(n)"), (Rows{{2}}))
      << "a refused write changes nothing";
  EXPECT_EQ(faker.Interpret("SHOW BRANCHES").GetResults().size(), 4U);

  faker.interpreter.current_db_.LeaveBranch();
  faker.Interpret("CREATE (:V {id: 4})");
  EXPECT_EQ(IntRows(faker, "MATCH (n:V) RETURN count(n)"), (Rows{{2}})) << "main's writes are not the branch's";

  Attach(faker, "b");
  faker.Interpret("DROP BRANCH 'b'");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  faker.Interpret("DROP BRANCH 'force_me' FORCE");
  faker.Interpret("DROP BRANCH 'keep'");
  EXPECT_TRUE(db->version_store()->Empty());
}

// Storage-global operations refused while branches exist hold exclusive access to main from Prepare to commit, so a
// CREATE BRANCH issued in between waits: no branch can appear between their check and their effect.
TEST_F(VersioningGatedTest, CreateBranchWaitsForPreparedEnumChange) {
  faker.Interpret("CREATE ENUM Status VALUES { Good, Bad }");
  for (const auto *query : {"CREATE ENUM Mood VALUES { Happy, Sad }",
                            "ALTER ENUM Status ADD VALUE Unknown",
                            "ALTER ENUM Status UPDATE VALUE Bad TO Poor"}) {
    auto [stream, qid] = faker.Prepare(query);
    InterpreterFaker creator{&interpreter_context, db};
    std::atomic<bool> done{false};
    std::string failure;
    std::thread worker{[&] {
      failure = ThrownMessage([&] { creator.Interpret("CREATE BRANCH 'raced'"); });
      done = true;
    }};
    std::this_thread::sleep_for(std::chrono::milliseconds{200});
    EXPECT_FALSE(done) << query;
    EXPECT_TRUE(db->version_store()->Empty()) << query;

    faker.Pull(&stream, {}, qid);
    worker.join();
    EXPECT_EQ(failure, "<no exception>") << query;
    EXPECT_TRUE(db->version_store()->Exists("raced")) << query;
    ASSERT_EQ(db->version_store()->DropBranch("raced"), memgraph::versioning::DropResult::kDropped);
  }
}

// The gate runs at Prepare, and the switch to another storage mode takes exclusive access to main: one that lands
// before the statement runs must refuse it, since no statement can drop a branch of a non-transactional main.
TEST_F(VersioningGatedTest, CreateBranchRefusedWhenStorageModeChangedAfterPrepare) {
  auto [stream, qid] = faker.Prepare("CREATE BRANCH 'late'");
  InterpreterFaker switcher{&interpreter_context, db};
  switcher.Interpret("STORAGE MODE IN_MEMORY_ANALYTICAL");

  EXPECT_EQ(ThrownMessage([&] { faker.Pull(&stream, {}, qid); }), GateMessage(GateError::kUnsupportedStorageMode));
  EXPECT_TRUE(db->version_store()->Empty());
}

// DROP BRANCH ... FORCE drops a branch other sessions are on and terminates their transactions; those sessions then get
// the dead-branch error until they leave.
TEST_F(VersioningGatedTest, DropBranchForceTerminatesOtherSessions) {
  faker.Interpret("CREATE (:K {v: 10})");
  ASSERT_TRUE(db->version_store()->CreateBranch("b"));
  InterpreterFaker other{&interpreter_context, db};
  Attach(other, "b");
  other.Interpret("BEGIN");
  other.Interpret("MATCH (n:K) SET n.v = 11");
  InterpreterFaker on_main{&interpreter_context, db};
  on_main.Interpret("BEGIN");
  on_main.Interpret("MATCH (n:K) SET n.v = 12");

  memgraph::storage::Config other_config{};
  other_config.durability.storage_directory = data_directory / "other_db";
  other_config.durability.recover_on_startup = true;
  other_config.durability.snapshot_wal_mode =
      memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
  other_config.disk.main_storage_directory = data_directory / "other_db" / "disk";
  other_config.experimental_versioning = true;
  other_config.salient.name = "other_db";
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> other_gk{other_config};
  auto other_acc = other_gk.access();
  ASSERT_TRUE(other_acc);
  ASSERT_EQ((*other_acc)->name(), "other_db");
  ASSERT_TRUE((*other_acc)->version_store()->CreateBranch("b"));
  InterpreterFaker in_other_db{&interpreter_context, *other_acc};
  interpreter_context.auth_checker = &faker.auth_checker;
  AttachTo(*other_acc, in_other_db, "b");
  ASSERT_EQ(in_other_db.interpreter.current_db_.foreign_branch_number(),
            other.interpreter.current_db_.foreign_branch_number());
  in_other_db.Interpret("BEGIN");
  in_other_db.Interpret("MATCH (n) RETURN count(n)");

  faker.Interpret("DROP BRANCH 'b' FORCE");
  EXPECT_FALSE(db->version_store()->Exists("b"));
  EXPECT_NE(in_other_db.interpreter.transaction_status_.load(), memgraph::query::TransactionStatus::TERMINATED)
      << "a branch of the same number in another database is untouched";
  in_other_db.Interpret("ROLLBACK");
  in_other_db.interpreter.current_db_.LeaveBranch();
  EXPECT_EQ(other.interpreter.transaction_status_.load(), memgraph::query::TransactionStatus::TERMINATED);
  EXPECT_NE(on_main.interpreter.transaction_status_.load(), memgraph::query::TransactionStatus::TERMINATED)
      << "only the branch's transactions are terminated";
  on_main.Interpret("ROLLBACK");
  other.Interpret("ROLLBACK");
  EXPECT_EQ(ThrownMessage([&] { other.Interpret("MATCH (n:K) RETURN n.v"); }),
            "Branch 'b' no longer exists. Run USE BRANCH or USE DATABASE to continue.");
  EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN n.v"), (Rows{{10}}));

  Fork("solo");
  faker.Interpret("DROP BRANCH solo FORCE");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value()) << "the dropping session moves to main";
  EXPECT_EQ(faker.interpreter.current_db_.branch_context(), nullptr);
}

// DROP BRANCH ... FORCE also stops a statement that is executing on the branch: the session gets an error, never an
// acknowledgement for writes that die with the branch.
TEST_F(VersioningGatedTest, DropBranchForceStopsAStatementRunningOnTheBranch) {
  constexpr auto kWrite = "UNWIND range(1, 1000) AS i CREATE (:Big {i: i}) RETURN i";
  ASSERT_TRUE(db->version_store()->CreateBranch("b"));
  InterpreterFaker other{&interpreter_context, db};
  Attach(other, "b");
  auto [stream, qid] = other.Prepare(kWrite);
  other.Pull(&stream, 10, qid);
  ASSERT_EQ(other.interpreter.transaction_status_.load(), memgraph::query::TransactionStatus::ACTIVE);

  faker.Interpret("DROP BRANCH 'b' FORCE");
  EXPECT_EQ(other.interpreter.transaction_status_.load(), memgraph::query::TransactionStatus::TERMINATED);
  EXPECT_THROW(other.Pull(&stream, {}, qid), memgraph::query::HintedAbortError);
  other.Abort();
}

// A refused DROP (to a session on the branch or not) changes nothing: both attached sessions keep the branch data and
// see each other's commits; the drop succeeds once the other session has left.
TEST_F(VersioningGatedTest, RefusedDropLeavesTheBranchDataAndBothSessionsIntact) {
  faker.Interpret("CREATE (:K {v: 1})");
  Fork();
  faker.Interpret("CREATE (:B {v: 2})");
  auto sessions = SessionsOnBranch(1);
  auto &other = *sessions[0];
  auto const in_use = "Cannot drop branch 'b': it is in use by another session. Use DROP BRANCH b FORCE.";
  constexpr auto kBranchValues = "MATCH (n:B) RETURN n.v ORDER BY n.v";

  other.Interpret("BEGIN");
  EXPECT_EQ(IntRows(other, kBranchValues), (Rows{{2}}));
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("DROP BRANCH b"); }), in_use);
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("DROP BRANCH 'b'"); }), in_use) << "a second refusal is the same";
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  EXPECT_EQ(IntRows(other, kBranchValues), (Rows{{2}})) << "the open transaction is undisturbed";
  other.Interpret("COMMIT");

  faker.Interpret("CREATE (:B {v: 3})");
  EXPECT_EQ(IntRows(other, kBranchValues), (Rows{{2}, {3}}));
  EXPECT_EQ(IntRows(faker, kBranchValues), (Rows{{2}, {3}}));
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 2U);
  InterpreterFaker third{&interpreter_context, db};
  EXPECT_EQ(ThrownMessage([&] { third.Interpret("DROP BRANCH b"); }), in_use) << "a session on main is refused too";

  other.interpreter.current_db_.LeaveBranch();
  faker.Interpret("DROP BRANCH b");
  EXPECT_FALSE(db->version_store()->Exists("b"));
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 0U);
  EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN count(n)"), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:B) RETURN count(n)"), (Rows{{0}}));
}

TEST_F(VersioningGatedTest, DroppingTheOwnBranchLeavesTheSessionOnMainAndARecreationStartsFresh) {
  faker.Interpret("CREATE (:K {v: 1})");
  Fork();
  faker.Interpret("CREATE (:B {v: 2})");
  faker.Interpret("DROP BRANCH b");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  EXPECT_EQ(faker.interpreter.current_db_.branch_context(), nullptr);
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 0U);
  faker.Interpret("CREATE (:K {v: 4})");
  EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN n.v ORDER BY n.v"), (Rows{{1}, {4}})) << "writes now land on main";

  faker.Interpret("CREATE BRANCH b");
  Attach(faker, "b");
  EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN n.v ORDER BY n.v"), (Rows{{1}, {4}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:B) RETURN count(n)"), (Rows{{0}})) << "the dropped branch's data must not return";
}

// Bounded stress: while branches are created and dropped and main commits, readers on another branch keep stable
// snapshots in explicit transactions, keep exactly the branch's data, and never see main's writes.
TEST_F(VersioningGatedTest, CreatingAndDroppingBranchesWhileReadersAreOnAnotherBranch) {
  constexpr int kRounds = 40;
  faker.Interpret("CREATE (:K {v: 1}), (:K {v: 2})");
  ASSERT_TRUE(db->version_store()->CreateBranch("b"));
  auto sessions = SessionsOnBranch(2);
  sessions[0]->Interpret("CREATE (:B {v: 7})");
  std::atomic<bool> done{false};

  {
    std::vector<std::jthread> threads;
    for (auto &session : sessions) {
      threads.emplace_back([&session = *session, &done] {
        do {
          session.Interpret("BEGIN");
          auto const k_first = IntRows(session, "MATCH (n:K) RETURN count(n)");
          auto const b_first = IntRows(session, "MATCH (n:B) RETURN n.v");
          auto const k_again = IntRows(session, "MATCH (n:K) RETURN count(n)");
          auto const b_again = IntRows(session, "MATCH (n:B) RETURN n.v");
          session.Interpret("COMMIT");
          ASSERT_EQ(k_first, (Rows{{2}})) << "main's later commits must not reach the branch";
          ASSERT_EQ(k_again, k_first);
          ASSERT_EQ(b_first, (Rows{{7}}));
          ASSERT_EQ(b_again, b_first);
        } while (!done.load());
      });
    }
    threads.emplace_back([&] {
      for (int i = 0; i < kRounds; ++i) {
        auto const name = fmt::format("t{}", i);
        faker.Interpret(fmt::format("CREATE BRANCH {}", name));
        faker.Interpret(fmt::format("CREATE (:M {{i: {}}})", i));
        faker.Interpret(fmt::format("DROP BRANCH {}", name));
      }
      done = true;
    });
  }

  EXPECT_EQ(db->version_store()->List().size(), 1U);
  EXPECT_EQ(IntRows(faker, "MATCH (n:M) RETURN count(n)"), (Rows{{kRounds}}));
  Attach(faker, "b");
  EXPECT_EQ(IntRows(faker, "MATCH (n:M) RETURN count(n)"), (Rows{{0}}));
}
