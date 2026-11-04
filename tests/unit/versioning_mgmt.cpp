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
#include <type_traits>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "query/exceptions.hpp"
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
    "Branch operations are forbidden on a REPLICA. Run them on the MAIN instance.";

std::string GateMessage(GateError err) { return std::string{memgraph::versioning::GateErrorMessage(err)}; }

const std::vector<std::string> kBranchColumns{"name", "created_at", "sessions", "current"};

void ExpectShowRefusedWith(InterpreterFaker &session, std::string_view message) {
  for (const auto *query : {"SHOW BRANCHES", "SHOW BRANCH"}) {
    EXPECT_EQ(ThrownMessage([&] { session.Interpret(query); }), message) << query;
  }
}

// Authorizes every privilege, but only on the databases it was built with.
struct ScopedDatabaseUser final : memgraph::query::QueryUserOrRole {
  explicit ScopedDatabaseUser(std::set<std::string, std::less<>> dbs)
      : memgraph::query::QueryUserOrRole{"scoped", {}}, dbs_{std::move(dbs)} {}

  bool IsAuthorized(const std::vector<memgraph::query::AuthQuery::Privilege> & /*privileges*/,
                    std::optional<std::string_view> db_name, memgraph::query::UserPolicy * /*policy*/) const override {
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
};

// Branches must survive a restart: without the WAL (NoWal) or, with it, without recovery on startup (NoRecovery) these
// databases cannot create one.
template <bool kWal, bool kRecover>
class VersioningNotDurableFixture : public VersioningFixture<true, kWal, kRecover> {
 protected:
  void SetUp() override {
    VersioningFixture<true, kWal, kRecover>::SetUp();
    this->SetVersioningFlag(true);
  }
};

using VersioningNoWalTest = VersioningNotDurableFixture<false, true>;
using VersioningNoRecoveryTest = VersioningNotDurableFixture<true, false>;

template <typename Fixture>
class VersioningNotDurableTest : public Fixture {};

struct NotDurableFixtureName {
  template <typename Fixture>
  static std::string GetName(int /*index*/) {
    return std::is_same_v<Fixture, VersioningNoWalTest> ? "NoWal" : "NoRecovery";
  }
};

using NotDurableFixtures = ::testing::Types<VersioningNoWalTest, VersioningNoRecoveryTest>;
TYPED_TEST_SUITE(VersioningNotDurableTest, NotDurableFixtures, NotDurableFixtureName);

}  // namespace

// Branch queries other than CREATE BRANCH check the flag, the storage mode and commit-lock narrowing, then that the
// instance is MAIN, and report the first failure.
TEST_F(VersioningInterpreterTest, ShowGateOrder) {
  memgraph::license::global_license_checker.EnableTesting();
  faker.Interpret("STORAGE MODE IN_MEMORY_ANALYTICAL");
  db->storage()->config_.experimental_commit_lock_narrowing = true;

  ExpectShowRefusedWith(faker, GateMessage(GateError::kDisabled));
  SetVersioningFlag(true);
  ExpectShowRefusedWith(faker, GateMessage(GateError::kUnsupportedStorageMode));
  faker.Interpret("STORAGE MODE IN_MEMORY_TRANSACTIONAL");
  ExpectShowRefusedWith(faker, GateMessage(GateError::kCommitLockNarrowing));
  ASSERT_TRUE(repl_state.Lock()->SetReplicationRoleReplica(
      memgraph::replication::ReplicationServerConfig{.repl_server =
                                                         memgraph::io::network::Endpoint("127.0.0.1", 30246)},
      /*maybe_main_uuid=*/std::nullopt));
  ExpectShowRefusedWith(faker, GateMessage(GateError::kCommitLockNarrowing));
  db->storage()->config_.experimental_commit_lock_narrowing = false;
  ExpectShowRefusedWith(faker, kReplicaError);
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
  ASSERT_TRUE(version_store->CreateBranch("feature", "main"));
  ASSERT_TRUE(version_store->CreateBranch("other", "main"));
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

// SHOW BRANCH without a name reports the session's branch; a quoted name is a stripped literal, so the cached AST must
// not keep the first name it was parsed with.
TEST_F(VersioningGatedTest, ShowBranchReportsOneBranch) {
  ASSERT_TRUE(db->version_store()->CreateBranch("feature", "main"));
  ASSERT_TRUE(db->version_store()->CreateBranch("other", "main"));
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

// A session whose branch is gone gets the dead-branch error for branch queries too, until it leaves.
TEST_F(VersioningGatedTest, ShowOnABranchThatIsGoneFails) {
  Fork("gone");
  ASSERT_EQ(db->version_store()->ForceDropBranch("gone"), true);
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

// SHOW BRANCHES FOR DATABASE is authorized against the listed database, before the database is looked up.
TEST_F(VersioningGatedTest, ShowBranchesForDatabaseIsAuthorizedAgainstTheListedDatabase) {
  faker.interpreter.SetUser(std::make_shared<ScopedDatabaseUser>(std::set<std::string, std::less<>>{"memgraph"}));
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("SHOW BRANCHES FOR DATABASE other_tenant"); }),
            "You are not authorized to show branches of the database \"other_tenant\"! Please contact your database "
            "administrator.");
}

TYPED_TEST(VersioningNotDurableTest, CreateRefused) {
  memgraph::license::global_license_checker.EnableTesting();
  EXPECT_EQ(ThrownMessage([&] { this->faker.Interpret("CREATE BRANCH 'a'"); }), GateMessage(GateError::kNotDurable));
  EXPECT_TRUE(this->db->version_store()->Empty());
}

// CREATE BRANCH checks the flag, the license, the storage mode, durability and commit-lock narrowing, then that the
// instance is MAIN, and reports the first failure.
TEST_F(VersioningInterpreterTest, CreateGateOrder) {
  faker.Interpret("STORAGE MODE IN_MEMORY_ANALYTICAL");
  db->storage()->config_.durability.recover_on_startup = false;
  db->storage()->config_.experimental_commit_lock_narrowing = true;
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
  expect_refused_with(GateMessage(GateError::kCommitLockNarrowing));
  ASSERT_TRUE(repl_state.Lock()->SetReplicationRoleReplica(
      memgraph::replication::ReplicationServerConfig{.repl_server =
                                                         memgraph::io::network::Endpoint("127.0.0.1", 30246)},
      /*maybe_main_uuid=*/std::nullopt));
  db->storage()->config_.experimental_commit_lock_narrowing = false;
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
  for (const auto &[query, message] :
       {std::pair{"CREATE BRANCH main", "'main' is a reserved branch name."},
        std::pair{"CREATE BRANCH '.hidden'", "Branch name must not start with '.'."},
        std::pair{"CREATE BRANCH 'a/b'", "Branch name must not contain '/', '\\' or '@'."},
        std::pair{"CREATE BRANCH 'a@b'", "Branch name must not contain '/', '\\' or '@'."},
        std::pair{"CREATE BRANCH ''", "Branch name must not be empty."}}) {
    EXPECT_EQ(ThrownMessage([&] { faker.Interpret(query); }), message) << query;
  }
  EXPECT_TRUE(db->version_store()->Empty());
}

TEST_F(VersioningGatedTest, ManagementQueriesAreRefusedInExplicitTransactions) {
  faker.Interpret("BEGIN");
  for (const auto *query : {"CREATE BRANCH 'a'", "DROP BRANCH 'a'"}) {
    EXPECT_THROW(faker.Interpret(query), memgraph::query::VersioningQueryInMulticommandTxException) << query;
  }
  faker.Interpret("ROLLBACK");
  EXPECT_TRUE(db->version_store()->Empty());
}

TEST_F(VersioningGatedTest, ManagementQueriesReportBranchEdit) {
  using memgraph::query::AuthQuery;
  ASSERT_TRUE(db->version_store()->CreateBranch("feature", "main"));
  const auto privileges_of = [&](const std::string &query) {
    auto prepared =
        faker.interpreter.Prepare(query, [](auto *) { return memgraph::storage::ExternalPropertyValue::map_t{}; }, {});
    faker.Abort();
    return prepared.privileges;
  };
  EXPECT_THAT(privileges_of("CREATE BRANCH 'other'"), ::testing::ElementsAre(AuthQuery::Privilege::BRANCH_EDIT));
  EXPECT_THAT(privileges_of("DROP BRANCH 'feature'"), ::testing::ElementsAre(AuthQuery::Privilege::BRANCH_EDIT));
  EXPECT_THAT(
      privileges_of("DROP BRANCH 'feature' FORCE"),
      ::testing::UnorderedElementsAre(AuthQuery::Privilege::BRANCH_EDIT, AuthQuery::Privilege::TRANSACTION_MANAGEMENT));
}

// Without a valid license branches are read-only and droppable: a session on a branch is never stuck, and main's
// retention can be released. Nothing is lost.
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

// CREATE BRANCH holds shared access to main from before the fork until the branch is registered, so the operations
// refused while branches exist, which check under exclusive access, cannot interleave with it.
TEST_F(VersioningGatedTest, CreateBranchWaitsForExclusiveAccessToMain) {
  InterpreterFaker creator{&interpreter_context, db};
  std::atomic<bool> started{false};
  std::atomic<bool> done{false};
  std::string failure;
  std::thread worker;
  {
    std::unique_lock exclusive{db->storage()->main_lock_};
    worker = std::thread{[&] {
      started = true;
      failure = ThrownMessage([&] { creator.Interpret("CREATE BRANCH 'raced'"); });
      done = true;
    }};
    while (!started) std::this_thread::yield();
    std::this_thread::sleep_for(std::chrono::milliseconds{200});
    EXPECT_FALSE(done) << "CREATE BRANCH must wait while an exclusive operation holds main";
    EXPECT_TRUE(db->version_store()->Empty());
  }
  worker.join();
  EXPECT_EQ(failure, "<no exception>");
  EXPECT_TRUE(db->version_store()->Exists("raced"));
}

// DROP BRANCH is refused while another session is attached; the dropping session itself moves to main.
TEST_F(VersioningGatedTest, DropBranchIsRefusedWhileAnotherSessionIsAttached) {
  ASSERT_TRUE(db->version_store()->CreateBranch("b", "main"));
  InterpreterFaker other{&interpreter_context, db};
  Attach(other, "b");
  const auto in_use = "Cannot drop branch 'b': it is in use by another session. Use DROP BRANCH b FORCE.";
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("DROP BRANCH 'b'"); }), in_use);

  Attach(faker, "b");
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("DROP BRANCH 'b'"); }), in_use);
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b") << "a refused DROP leaves the session where it was";
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 2U);

  other.interpreter.current_db_.LeaveBranch();
  faker.Interpret("DROP BRANCH 'b'");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  EXPECT_FALSE(db->version_store()->Exists("b"));
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 0U);
}

// DROP BRANCH ... FORCE drops a branch other sessions are on and terminates their transactions; those sessions then get
// the dead-branch error until they leave.
TEST_F(VersioningGatedTest, DropBranchForceTerminatesOtherSessions) {
  faker.Interpret("CREATE (:K {v: 10})");
  ASSERT_TRUE(db->version_store()->CreateBranch("b", "main"));
  InterpreterFaker other{&interpreter_context, db};
  Attach(other, "b");
  other.Interpret("BEGIN");
  other.Interpret("MATCH (n:K) SET n.v = 11");
  InterpreterFaker on_main{&interpreter_context, db};
  on_main.Interpret("BEGIN");
  on_main.Interpret("MATCH (n:K) SET n.v = 12");

  faker.Interpret("DROP BRANCH 'b' FORCE");
  EXPECT_FALSE(db->version_store()->Exists("b"));
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
