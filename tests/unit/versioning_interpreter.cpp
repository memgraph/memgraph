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

// Graph Versioning v1 (lazy diff-context, slice E-1): end-to-end regression for the unit this
// file's CHECKOUT_BRANCH handler now wires up -- ordinary Cypher issued while checked out on a
// branch resolves through that branch's own BranchContext (diff-engine-first, falling back to the
// historical fork-state base), with the session reading back its own writes. Checking back out
// onto 'main' must route straight back to main's real storage, untouched by anything the branch
// session wrote -- R35 (main is never written by branch activity).
//
// VERTEX-ONLY (this slice's own scope, superseding the predecessor BranchEngine's full-copy
// version of this same test): no edges. MATCH (n) here (rather than MATCH (n:A)) purely because
// this test only ever has one vertex to find either way; see
// BranchLabelScanIsUnionAwareNotJustDiffEngineIndex below for the dedicated label-indexed-scan
// regression (DbAccessor::Vertices(view, label) is branch-aware too, as of the HIGH-3(b)
// adversarial-review fix).
TEST_F(VersioningInterpreterTest, BranchWriteIsIsolatedAndReadYourWrites) {
  SatisfyGate();

  faker.Interpret("CREATE (:A {x:1})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");

  faker.Interpret("MATCH (n) SET n.x = 2");

  // Read-your-writes against the branch's own diff engine.
  {
    auto stream = faker.Interpret("MATCH (n) RETURN n.x");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2);
  }

  // Releases the branch context and routes back to main's real storage.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());

  {
    auto stream = faker.Interpret("MATCH (n) RETURN n.x");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 1);
  }
}

// Graph Versioning v1 (lazy diff-context, slice E-1): the SELECTIVE-COW regression the redesign
// exists for -- a branch touching ONE of two main vertices must copy-on-write ONLY that one; the
// untouched vertex must keep resolving through the historical fork-state base, never materialized
// into the diff engine. Also verifies the CHECKOUT-away isolation contract (R35) one more time
// against a two-vertex graph (BranchWriteIsIsolatedAndReadYourWrites, above, only has one).
//
// VERTEX-ONLY: `WHERE 'A' IN labels(n)` (ScanAll + a labels() filter) stands in for `MATCH (n:A)`
// here purely to keep this test's focus on selective-COW, independent of whether a label index
// happens to exist -- see BranchLabelScanIsUnionAwareNotJustDiffEngineIndex below for the
// dedicated MATCH (n:A) / label-indexed-scan regression.
TEST_F(VersioningInterpreterTest, BranchVertexWriteIsLazyDiffAndIsolated) {
  SatisfyGate();

  faker.Interpret("CREATE (:A {x:1}), (:B {y:9})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");

  faker.Interpret("MATCH (n) WHERE 'A' IN labels(n) SET n.x = 2");

  // Lower-level check straight against BranchContext (branch_context() is public on CurrentDB):
  // exactly ONE vertex (A) should have been copy-on-write'd into the diff engine -- B must still
  // be un-materialized there.
  {
    auto *ctx = faker.interpreter.current_db_.branch_context();
    ASSERT_NE(ctx, nullptr);
    auto acc = ctx->diff_engine().Access(memgraph::storage::StorageAccessType::READ);
    size_t diff_vertex_count = 0;
    for (auto v : acc->Vertices(memgraph::storage::View::OLD)) {
      (void)v;
      ++diff_vertex_count;
    }
    EXPECT_EQ(diff_vertex_count, 1U) << "only the touched vertex (A) should have been COW'd";
  }

  // ScanAll union read: A resolves to the diff-engine's COW'd copy (x=2); B resolves to the
  // historical base, untouched (y=9).
  {
    auto stream = faker.Interpret("MATCH (n) RETURN n.x, n.y");
    ASSERT_EQ(stream.GetResults().size(), 2U);
    bool found_a = false;
    bool found_b = false;
    for (auto &row : stream.GetResults()) {
      if (row[0].type() != memgraph::communication::bolt::Value::Type::Null) {
        EXPECT_EQ(row[0].ValueInt(), 2) << "A must resolve to the diff-engine's COW'd copy";
        found_a = true;
      } else {
        ASSERT_NE(row[1].type(), memgraph::communication::bolt::Value::Type::Null);
        EXPECT_EQ(row[1].ValueInt(), 9) << "B must resolve to historical_, untouched";
        found_b = true;
      }
    }
    EXPECT_TRUE(found_a);
    EXPECT_TRUE(found_b);
  }

  // Checking back onto main: A must read back its ORIGINAL value (1) -- R35, main is never
  // written by branch activity.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    auto stream = faker.Interpret("MATCH (n) WHERE 'A' IN labels(n) RETURN n.x");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 1);
  }
}

// Graph Versioning v1 (lazy diff-context, slice E-1) HIGH-1 regression (adversarial-review fix):
// query::VertexAccessor::UpdateProperties (the `SET n += {...}` operator, plan/operator.cpp's
// SetPropertiesOnRecord) was missing the COW guard SetProperty/AddLabel already had -- on a branch
// it wrote straight through the READ-ONLY historical accessor, tripping transaction.hpp's
// `is_historical_` MG_ASSERT. This must not crash, and the write must still be isolated from main
// (R35).
TEST_F(VersioningInterpreterTest, BranchUpdatePropertiesIsCowedAndIsolated) {
  SatisfyGate();

  faker.Interpret("CREATE (:A {x:1})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // Must not crash -- HIGH-1's whole point (previously an MG_ASSERT abort against the historical,
  // read-only accessor).
  faker.Interpret("MATCH (n) SET n += {y: 2}");

  {
    auto stream = faker.Interpret("MATCH (n) RETURN n.y");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2);
  }

  // Main was never touched -- R35.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    auto stream = faker.Interpret("MATCH (n) RETURN n.y");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].type(), memgraph::communication::bolt::Value::Type::Null);
  }
}

// Graph Versioning v1 (lazy diff-context, slice E-1) HIGH-2 regression (adversarial-review fix):
// single-STATEMENT read-your-write. `SET n.x=2` COWs the vertex and redirects `impl_` on the
// mutator's OWN (transient, expression-evaluated) VertexAccessor -- but the Frame's own copy of
// symbol `n` (the one `RETURN n.x`, further down the SAME operator tree, actually reads) is a
// SEPARATE value that keeps pointing at the stale, pre-COW historical object unless every read is
// made self-correcting (VertexAccessor::GetProperty/Labels/etc's own doc-comment). Must see the NEW
// value, not the historical one, all within one autocommit statement/transaction -- no AdvanceCommand
// or second statement involved.
TEST_F(VersioningInterpreterTest, BranchSingleStatementSetThenReturnSeesNewValue) {
  SatisfyGate();

  faker.Interpret("CREATE (:A {x:1})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  auto stream = faker.Interpret("MATCH (n) SET n.x = 2 RETURN n.x");
  ASSERT_EQ(stream.GetResults().size(), 1U);
  EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2) << "must see the post-SET value, not the stale historical one";
}

// Graph Versioning v1 (lazy diff-context, slice E-1) HIGH-3(b) regression (adversarial-review fix):
// a checked-out branch's OWN label index (created directly against the diff engine -- CREATE INDEX
// is branch-aware via the same query::DbAccessor/accessor_ redirection as everything else, see
// db_accessor.hpp's CreateIndex) only knows about vertices PHYSICALLY resident in the diff engine
// (COW'd, or branch-native) -- it has no reconciliation with historical_'s fork-state vertices.
// Once that index exists, the planner lowers `MATCH (n:A)` to a real ScanAllByLabel
// (plan/rewrite/index_lookup.hpp gates on LabelIndexReady, which the diff engine's freshly-created
// index now satisfies) -- DbAccessor::Vertices(view, label) must fall back to the filtered union
// scan (MaterializeFilteredBranchScan) rather than trust the diff engine's own, structurally
// incomplete index, or every still-historical (non-COW'd) A-labeled vertex would silently go
// missing from the result.
TEST_F(VersioningInterpreterTest, BranchLabelScanIsUnionAwareNotJustDiffEngineIndex) {
  SatisfyGate();

  faker.Interpret("CREATE (:A {x:1}), (:A {x:5})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // Creates the index against the DIFF ENGINE only -- main's (and hence historical_'s) two
  // A-vertices are not, and never will be, reconciled into it.
  faker.Interpret("CREATE INDEX ON :A");

  // Touches (COWs) only the x=1 vertex -- the x=5 one stays purely historical, absent from the
  // diff engine's own index.
  faker.Interpret("MATCH (n:A) WHERE n.x = 1 SET n.x = 2");

  auto stream = faker.Interpret("MATCH (n:A) RETURN n.x");
  ASSERT_EQ(stream.GetResults().size(), 2U) << "must find BOTH the COW'd vertex and the still-historical one";
  bool found_cowed = false;
  bool found_historical = false;
  for (auto &row : stream.GetResults()) {
    const auto value = row[0].ValueInt();
    if (value == 2) {
      found_cowed = true;
    } else if (value == 5) {
      found_historical = true;
    }
  }
  EXPECT_TRUE(found_cowed) << "the COW'd vertex (diff engine's own index DOES know about it) must still be found";
  EXPECT_TRUE(found_historical)
      << "the still-historical vertex (diff engine's own index does NOT know about it) must not be silently missed";
}

// Graph Versioning v1 (lazy diff-context, slice E-1) HIGH-4 regression (adversarial-review fix):
// DELETE has no defined semantics yet on a branch (this slice is VERTEX-ONLY -- no tombstone
// concept exists in BranchContext::UnionVerticesIterable, see its own doc-comment) -- must be
// rejected cleanly (NotYetImplemented) rather than crashing or silently misbehaving (e.g. deleting
// a not-yet-COW'd historical object, or a resurrection once the union scan next runs).
TEST_F(VersioningInterpreterTest, BranchDeleteThrowsNotYetImplemented) {
  SatisfyGate();

  faker.Interpret("CREATE (:A {x:1})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  ASSERT_THROW(faker.Interpret("MATCH (n) DELETE n"), memgraph::query::NotYetImplemented);

  // Must not have crashed or left the session/transaction unusable -- an ordinary read still works.
  auto stream = faker.Interpret("MATCH (n) RETURN n.x");
  ASSERT_EQ(stream.GetResults().size(), 1U);
  EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 1);
}
