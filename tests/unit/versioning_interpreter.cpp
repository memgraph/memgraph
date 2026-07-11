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

// Graph Versioning (branches) v1, CHUNK 7a: interpreter-level tests for the management-query
// dispatch wired in query/interpreter.cpp's PrepareVersioningQuery. Chosen over a full e2e suite
// (flagged in the chunk's own audit) because everything under test -- the gate check, autocommit
// guard, VersionStore CRUD, MergeBranch dispatch, and per-connection current_version_ -- is
// reachable and observable purely through Interpreter::Prepare/Pull against a real, in-process
// Database, mirroring tests/unit/interpreter.cpp's own InterpreterFaker harness.
//
// NOT covered here (flagged, not silently skipped):
//   - SHOW BRANCHES FOR DATABASE (and its MEDIUM-fix re-gate-against-the-target-db ordering):
//     needs a real DbmsHandler with a second database; this fixture's InterpreterContext
//     deliberately mirrors tests/unit/interpreter.cpp's own (dbms_handler = nullptr) since
//     standing up a full DbmsHandler is unrelated scope for a dispatch-only test.
//   - A genuine MERGE D3 conflict: chunk 7a's MERGE_BRANCH always replays an EMPTY change-log
//     (real branch writes aren't captured until chunks 7b/7c -- see PrepareVersioningQuery's own
//     top-of-case comment), so a real modify-conflict cannot be produced end-to-end yet. The
//     conflict-detection MECHANISM itself is already covered directly against MergeBranch in
//     tests/unit/versioning_merge.cpp (chunk 6).
//
// The HIGH-2 fix (VersionStore::BeginMerge/FinishMerge/AbortMerge closing the double-merge TOCTOU
// window) is unit-tested directly at the VersionStore level in
// tests/unit/versioning_version_store.cpp -- that is where the invariant actually lives; this
// file's MergeHappyPathFastForwardsEmptyChangelog/MergeAndDropRejectedWhenBranchHasChildren below
// only re-confirm the interpreter still dispatches through it correctly end-to-end.

#include <filesystem>

#include "gtest/gtest.h"

#include "flags/general.hpp"
#include "interpreter_faker.hpp"
#include "license/license.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/exceptions.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/logging.hpp"
#include "utils/synchronized.hpp"

namespace {
constexpr auto kNoHandler = nullptr;
}  // namespace

class VersioningInterpreterTest : public ::testing::Test {
 protected:
  std::filesystem::path data_directory =
      std::filesystem::temp_directory_path() / "MG_tests_unit_versioning_interpreter";

  memgraph::storage::Config config{[&] {
    memgraph::storage::Config config{};
    config.durability.storage_directory = data_directory;
    // Chunk-0 gate requires PERIODIC_SNAPSHOT_WITH_WAL (D9) -- see WalGateDisabled below for the
    // default-config (WAL-off) rejection path instead.
    config.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    config.disk.main_storage_directory = data_directory / "disk";
    return config;
  }()};

  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config};
  memgraph::dbms::DatabaseAccess db{[&] {
    auto db_acc_opt = db_gk.access();
    MG_ASSERT(db_acc_opt, "Failed to access db");
    auto &db_acc = *db_acc_opt;
    MG_ASSERT(db_acc->GetStorageMode() == memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL,
              "Wrong storage mode!");
    return db_acc;
  }()};

  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context{{},
                                                          nullptr,
                                                          nullptr,
                                                          kNoHandler,
                                                          &repl_state,
                                                          system_state,
                                                          nullptr
#ifdef MG_ENTERPRISE
                                                          ,
                                                          nullptr,
                                                          nullptr
#endif
  };

  InterpreterFaker faker{&interpreter_context, db};

  void SetUp() override { FLAGS_versioning_enabled = false; }

  void TearDown() override {
    FLAGS_versioning_enabled = false;
    memgraph::license::global_license_checker.DisableTesting();
    std::filesystem::remove_all(data_directory);
  }

  // Flips on every chunk-0 gate precondition (flag + license); storage mode + WAL are already
  // satisfied by `config` above.
  void SatisfyGate() {
    FLAGS_versioning_enabled = true;
    memgraph::license::global_license_checker.EnableTesting();
  }
};

TEST_F(VersioningInterpreterTest, GateRejectsWhenFlagOff) {
  // FLAGS_versioning_enabled left false by SetUp -- even with a license this must be rejected.
  memgraph::license::global_license_checker.EnableTesting();
  ASSERT_THROW(faker.Interpret("CREATE BRANCH 'feature' FROM 'main'"), memgraph::query::QueryRuntimeException);
}

TEST_F(VersioningInterpreterTest, GateRejectsWhenNoLicense) {
  FLAGS_versioning_enabled = true;
  // No EnableTesting() call -- license stays invalid/community.
  ASSERT_THROW(faker.Interpret("CREATE BRANCH 'feature' FROM 'main'"), memgraph::query::QueryRuntimeException);
}

TEST_F(VersioningInterpreterTest, GateRejectsWhenWalDisabled) {
  // A second, WAL-less database -- everything else about the gate (flag+license) satisfied.
  memgraph::storage::Config no_wal_config{};
  no_wal_config.durability.storage_directory = data_directory / "no_wal";
  no_wal_config.disk.main_storage_directory = data_directory / "no_wal" / "disk";
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> no_wal_gk{no_wal_config};
  auto no_wal_db_acc_opt = no_wal_gk.access();
  ASSERT_TRUE(no_wal_db_acc_opt);
  auto no_wal_db = *no_wal_db_acc_opt;
  ASSERT_EQ(no_wal_db->GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);

  InterpreterFaker no_wal_faker{&interpreter_context, no_wal_db};
  SatisfyGate();
  ASSERT_THROW(no_wal_faker.Interpret("CREATE BRANCH 'feature' FROM 'main'"), memgraph::query::QueryRuntimeException);
}

TEST_F(VersioningInterpreterTest, ManagementQueryRejectedInExplicitTransaction) {
  SatisfyGate();
  faker.Interpret("BEGIN");
  ASSERT_THROW(faker.Interpret("CREATE BRANCH 'feature' FROM 'main'"), memgraph::query::MulticommandTxException);
  faker.Interpret("ROLLBACK");
}

TEST_F(VersioningInterpreterTest, InvalidBranchNameRejected) {
  SatisfyGate();
  // 'main' is reserved (versioning::ValidateBranchName) -- checked at dispatch time (deferred from
  // parse time per ast.hpp's own doc-comment).
  ASSERT_THROW(faker.Interpret("CREATE BRANCH 'main' FROM 'main'"), memgraph::query::QueryRuntimeException);
  ASSERT_THROW(faker.Interpret("CREATE BRANCH '.hidden' FROM 'main'"), memgraph::query::QueryRuntimeException);
  ASSERT_THROW(faker.Interpret("CREATE BRANCH 'a/b' FROM 'main'"), memgraph::query::QueryRuntimeException);
}

TEST_F(VersioningInterpreterTest, CreateShowCheckoutDropHappyPath) {
  SatisfyGate();

  {
    auto stream = faker.Interpret("CREATE BRANCH 'feature' FROM 'main'");
    ASSERT_EQ(stream.GetHeader(), (std::vector<std::string>{"version", "number", "description", "parent"}));
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "feature");
    EXPECT_EQ(stream.GetResults()[0][1].ValueInt(), 2);
    EXPECT_EQ(stream.GetResults()[0][2].type(), memgraph::communication::bolt::Value::Type::Null);
    EXPECT_EQ(stream.GetResults()[0][3].ValueString(), "main");
  }

  {
    // New name already exists.
    ASSERT_THROW(faker.Interpret("CREATE BRANCH 'feature' FROM 'main'"), memgraph::query::QueryRuntimeException);
  }

  {
    auto stream = faker.Interpret("SHOW BRANCHES");
    ASSERT_EQ(stream.GetHeader(), (std::vector<std::string>{"number", "version", "description"}));
    ASSERT_EQ(stream.GetResults().size(), 2U);
    EXPECT_EQ(stream.GetResults()[0][1].ValueString(), "main");
    EXPECT_EQ(stream.GetResults()[1][1].ValueString(), "feature");
  }

  {
    // Session starts on main.
    auto stream = faker.Interpret("SHOW BRANCH");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "main");
  }

  faker.Interpret("CHECKOUT BRANCH 'feature'");
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "feature");

  {
    auto stream = faker.Interpret("SHOW BRANCH");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "feature");
  }

  // Switching to a non-existent branch fails and does not move the session.
  ASSERT_THROW(faker.Interpret("CHECKOUT BRANCH 'nope'"), memgraph::query::QueryRuntimeException);
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "feature");

  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());

  {
    auto stream = faker.Interpret("DROP BRANCH 'feature'");
    EXPECT_TRUE(stream.GetResults().empty());
  }

  {
    auto stream = faker.Interpret("SHOW BRANCHES");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][1].ValueString(), "main");
  }

  ASSERT_THROW(faker.Interpret("DROP BRANCH 'feature'"), memgraph::query::QueryRuntimeException);
}

TEST_F(VersioningInterpreterTest, CheckoutCreateAndSwitchClearsOnDrop) {
  SatisfyGate();

  auto stream = faker.Interpret("CHECKOUT BRANCH 'feature' WITH DESCRIPTION 'desc' FROM 'main'");
  ASSERT_EQ(stream.GetHeader(), (std::vector<std::string>{"version", "number", "description", "parent"}));
  ASSERT_EQ(stream.GetResults().size(), 1U);
  EXPECT_EQ(stream.GetResults()[0][2].ValueString(), "desc");
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "feature");

  // Dropping the branch the session is checked out on clears the session pointer (mirrors MERGE's
  // own post-merge clearing, spec §4.2).
  faker.Interpret("DROP BRANCH 'feature'");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
}

TEST_F(VersioningInterpreterTest, MergeHappyPathFastForwardsEmptyChangelog) {
  SatisfyGate();

  faker.Interpret("CREATE BRANCH 'feature' FROM 'main'");
  auto stream = faker.Interpret("MERGE BRANCH 'feature'");
  ASSERT_EQ(stream.GetHeader(), (std::vector<std::string>{"merged", "into"}));
  ASSERT_EQ(stream.GetResults().size(), 1U);
  EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "feature");
  EXPECT_EQ(stream.GetResults()[0][1].ValueString(), "main");

  // Merged branches are dropped (spec §4.2).
  ASSERT_THROW(faker.Interpret("MERGE BRANCH 'feature'"), memgraph::query::QueryRuntimeException);
}

TEST_F(VersioningInterpreterTest, MergeNonExistentBranchRejected) {
  SatisfyGate();
  ASSERT_THROW(faker.Interpret("MERGE BRANCH 'nope'"), memgraph::query::QueryRuntimeException);
}

TEST_F(VersioningInterpreterTest, MergeAndDropRejectedWhenBranchHasChildren) {
  SatisfyGate();
  faker.Interpret("CREATE BRANCH 'parent' FROM 'main'");
  faker.Interpret("CREATE BRANCH 'child' FROM 'parent'");

  ASSERT_THROW(faker.Interpret("MERGE BRANCH 'parent'"), memgraph::query::QueryRuntimeException);
  ASSERT_THROW(faker.Interpret("DROP BRANCH 'parent'"), memgraph::query::QueryRuntimeException);

  // Children first, then the parent -- both should now succeed.
  faker.Interpret("MERGE BRANCH 'child'");
  faker.Interpret("DROP BRANCH 'parent'");
}

TEST_F(VersioningInterpreterTest, ShowBranchDiffValidatesThenFlagsNotYetImplemented) {
  SatisfyGate();

  // No name, no active checkout.
  ASSERT_THROW(faker.Interpret("SHOW BRANCH DIFF"), memgraph::query::QueryRuntimeException);

  // 'main' is rejected outright.
  ASSERT_THROW(faker.Interpret("SHOW BRANCH DIFF 'main'"), memgraph::query::QueryRuntimeException);

  // Non-existent branch.
  ASSERT_THROW(faker.Interpret("SHOW BRANCH DIFF 'nope'"), memgraph::query::QueryRuntimeException);

  // A real, existing branch -- validation passes, then the (flagged, chunk 7b/7c) data gap fires.
  faker.Interpret("CREATE BRANCH 'feature' FROM 'main'");
  ASSERT_THROW(faker.Interpret("SHOW BRANCH DIFF 'feature'"), memgraph::utils::NotYetImplemented);
}

// HIGH-1 fix: glue/SessionHL.cpp's RuntimeConfig::Configure re-invokes
// Interpreter::SetCurrentDB(same_db_name, ...) whenever ANY field of the Bolt RUN "extra" map
// differs from the previous query ("mode"/"tx_timeout"/"tx_metadata" -- ToQueryExtras), which
// ordinary routing-aware drivers vary between successive autocommit queries even against the SAME
// database. Before this fix, CurrentDB::SetCurrentDB unconditionally reset current_version_ on
// every call, silently reverting a CHECKOUT BRANCH to main on the very next query under a real
// driver -- with no error. This test drives CurrentDB directly (a direct SetCurrentDB
// same-db-vs-different-db comparison) rather than through the full SessionHL/RuntimeConfig glue
// layer, per the fix's own request ("if not, a direct test is acceptable").
TEST_F(VersioningInterpreterTest, SetCurrentDBOnlyResetsCurrentVersionOnADifferentDatabase) {
  memgraph::query::CurrentDB current_db{db};
  current_db.SetCurrentVersion(std::string{"feature"});
  ASSERT_EQ(current_db.CurrentVersion(), "feature");

  // A second DatabaseAccess handle to the SAME underlying database -- mirrors what a
  // metadata-only RuntimeConfig::Configure re-resolution actually produces (a fresh Accessor via
  // DbmsHandler::Get(), pointing at the identical Database).
  auto same_db_acc_opt = db_gk.access();
  ASSERT_TRUE(same_db_acc_opt);
  current_db.SetCurrentDB(*same_db_acc_opt, /*in_explicit_db=*/false);
  EXPECT_EQ(current_db.CurrentVersion(), "feature") << "same-database SetCurrentDB must NOT reset the branch";

  // A genuinely different database must still reset it -- branches are per-database.
  memgraph::storage::Config other_config{};
  other_config.durability.storage_directory = data_directory / "other_db";
  other_config.disk.main_storage_directory = data_directory / "other_db" / "disk";
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> other_gk{other_config};
  auto other_db_acc_opt = other_gk.access();
  ASSERT_TRUE(other_db_acc_opt);
  current_db.SetCurrentDB(*other_db_acc_opt, false);
  EXPECT_FALSE(current_db.CurrentVersion().has_value()) << "a genuine database switch must reset the branch";

  // Deadlock trap (see the project's own "unit-test deadlock trap" note): `current_db` now holds
  // a live DatabaseAccess into `other_gk`'s Database. Locals destruct in REVERSE declaration
  // order, so `other_gk` (declared AFTER current_db) would otherwise destruct BEFORE current_db
  // does -- and ~Gatekeeper() blocks until its accessor count reaches 0, which can only happen
  // once current_db itself is destroyed. That destruction can't happen until ~Gatekeeper()
  // returns, so the test would hang forever at scope exit. Explicitly release current_db's held
  // accessor here, while other_gk is still alive, to break the cycle.
  current_db.ResetDB();
}

// Same fix, driven through a real, checked-out Interpreter session rather than a bare CurrentDB:
// a fresh same-database DatabaseAccess re-applied to the REAL interpreter's current_db_ (mirroring
// what RuntimeConfig::Configure's re-resolution actually produces on a metadata-only RUN extras
// change) must not disturb an active CHECKOUT. (Interpreter::SetCurrentDB(name) itself needs a
// real DbmsHandler this fixture deliberately omits -- see the file-level comment -- so this drives
// CurrentDB::SetCurrentDB directly on the real interpreter's current_db_, one layer below the
// DbmsHandler lookup RuntimeConfig::Configure performs before calling it.)
TEST_F(VersioningInterpreterTest, ReApplyingSameDatabaseAccessPreservesCheckout) {
  SatisfyGate();
  faker.Interpret("CREATE BRANCH 'feature' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'feature'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "feature");

  auto same_db_acc_opt = db_gk.access();
  ASSERT_TRUE(same_db_acc_opt);
  faker.interpreter.current_db_.SetCurrentDB(*same_db_acc_opt, /*in_explicit_db=*/false);
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "feature");

  // The session is still genuinely checked out: SHOW BRANCH still reports it.
  auto stream = faker.Interpret("SHOW BRANCH");
  ASSERT_EQ(stream.GetResults().size(), 1U);
  EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "feature");
}
