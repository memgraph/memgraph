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

// Durable-capture slice (design slices A+B+C) THE PAYOFF: a branch's committed vertex write is now
// captured into its own durable, per-checkout-session BranchLog (Interpreter::Commit's capture
// hook) and, once the checkout is released (Finalize()'d in BranchContext's destructor), MERGE
// BRANCH discovers and replays that REAL change-log onto main (CollectBranchChangelog +
// versioning::MergeBranch) -- superseding the old CHUNK 7a placeholder that always merged an empty
// vector (see MergeHappyPathFastForwardsEmptyChangelog above, still valid for a branch that
// genuinely recorded no writes).
TEST_F(VersioningInterpreterTest, BranchMergeAppliesCapturedVertexToMain) {
  SatisfyGate();

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");

  faker.Interpret("CREATE (:Foo {x:1})");

  // Checking back onto main releases (and Finalize()s) this session's BranchLog -- but the write
  // has NOT been merged yet, so main must still show nothing.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  {
    auto stream = faker.Interpret("MATCH (n:Foo) RETURN n.x");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "not yet merged -- main must not see the branch's write";
  }

  auto merge_stream = faker.Interpret("MERGE BRANCH 'b'");
  ASSERT_EQ(merge_stream.GetResults().size(), 1U);
  EXPECT_EQ(merge_stream.GetResults()[0][0].ValueString(), "b");
  EXPECT_EQ(merge_stream.GetResults()[0][1].ValueString(), "main");

  // Merged -- main now HAS the branch's data.
  {
    auto stream = faker.Interpret("MATCH (n:Foo) RETURN n.x");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 1);
  }
}

// Edge-side analogue of BranchMergeAppliesCapturedVertexToMain above: a branch-native CREATE of
// two vertices AND the edge (with a property) between them must all be captured (CaptureBranchCommit
// walks EVERY delta the transaction produced, vertex- and edge-owned alike) and replayed by MERGE.
TEST_F(VersioningInterpreterTest, BranchMergeAppliesCapturedEdgeToMain) {
  SatisfyGate();

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");

  faker.Interpret("CREATE (:A)-[:R {w:5}]->(:B)");

  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    auto stream = faker.Interpret("MATCH (:A)-[r:R]->(:B) RETURN r.w");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "not yet merged -- main must not see the branch's edge";
  }

  faker.Interpret("MERGE BRANCH 'b'");

  {
    auto stream = faker.Interpret("MATCH (:A)-[r:R]->(:B) RETURN r.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 5);
  }
}

// Multi-checkout-cycle regression (adversarial review, 2026-07-12, TEST-2; UPDATED 2026-07-12 for
// slice D): a branch checked out TWICE (write, checkout away, checkout back, write again) produces
// TWO separate per-session BranchLog files (BranchContext::BuildFromFork mints a fresh session
// subdirectory every call -- branch_engine.hpp). MERGE BRANCH must discover and concatenate BOTH
// via CollectBranchChangelog, not just the most recent one.
//
// Originally this test also exercised (and locked in a MergeBranch R11 remap for) a cross-session
// gid COLLISION: each fresh checkout session used to get its own brand-new diff engine that
// reserved the SAME kBranchNativeGidWatermark and restarted its own branch-native gid counter from
// there, so Foo1 (session 1's first branch-native create) and Foo2 (session 2's first branch-native
// create) ended up with the IDENTICAL numeric gid -- and concatenating both sessions' records into
// one MergeBranch replay actually CRASHED (storage.cpp's append_deltas hit a NULL_PTR delta on the
// resulting corrupt/aliased vertex; R11's remap logic never even got a chance to run, because slice
// D wasn't in place yet to keep the two diff engines' gid spaces disjoint in the first place).
//
// Slice D (replay-on-checkout, BranchContext::ReplayChangelogIntoDiffEngine, branch_engine.cpp)
// fixes this at the SOURCE rather than relying on MERGE-time remap: session 2's CHECKOUT now
// replays session 1's already-captured Foo1 record into its OWN diff engine BEFORE any new write
// happens, which -- via CreateVertexEx's own atomic_fetch_max on the explicit gid -- advances
// session 2's gid counter past Foo1's gid. Foo2 (session 2's first branch-native create) therefore
// gets a NUMERICALLY DIFFERENT gid than Foo1 from the start, so by the time MERGE concatenates both
// sessions' files there is no collision left for R11 to even need to resolve. This ALSO fixes the
// "re-checkout shows fork-state" limitation the original version of this test documented: Foo1 is
// now visible immediately after the second CHECKOUT (proving replay actually ran), not just after
// MERGE.
TEST_F(VersioningInterpreterTest, BranchMergeConcatenatesMultipleCheckoutSessions) {
  SatisfyGate();

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");

  // Session 1: write Foo1, then leave (Finalize()s session 1's BranchLog).
  faker.Interpret("CHECKOUT BRANCH 'b'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  faker.Interpret("CREATE (:Foo1)");
  faker.Interpret("CHECKOUT BRANCH 'main'");

  // Session 2: re-checkout the SAME branch -- slice D replays session 1's captured Foo1 into this
  // FRESH diff engine before any query runs against it, so Foo1 IS visible here now.
  faker.Interpret("CHECKOUT BRANCH 'b'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  {
    auto stream = faker.Interpret("MATCH (n:Foo1) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 1U) << "slice D: re-checkout must replay the branch's own prior writes";
  }
  faker.Interpret("CREATE (:Foo2)");
  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());

  // Not yet merged -- main must see neither.
  {
    auto stream = faker.Interpret("MATCH (n) WHERE n:Foo1 OR n:Foo2 RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U);
  }

  // The real regression check: this must not crash (it used to, pre-slice-D).
  faker.Interpret("MERGE BRANCH 'b'");

  // Both sessions' writes were captured (two separate BranchLog files) and concatenated by
  // CollectBranchChangelog into one changelog -- main now has BOTH.
  {
    auto stream = faker.Interpret("MATCH (n:Foo1) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 1U) << "session 1's write must have been merged";
  }
  {
    auto stream = faker.Interpret("MATCH (n:Foo2) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 1U) << "session 2's write must have been merged too";
  }
  {
    // Both as genuinely DISTINCT nodes (not one silently overwritten/collided into the other).
    auto stream = faker.Interpret("MATCH (n) WHERE n:Foo1 OR n:Foo2 RETURN count(n)");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2) << "both must survive as distinct, uncollided nodes";
  }
}

// Dedicated, minimal slice-D regression: a SINGLE write, checkout away, checkout back must show
// the write immediately -- no MERGE involved, isolating replay-on-checkout from the multi-session
// gid-disjointness scenario above.
TEST_F(VersioningInterpreterTest, BranchReCheckoutReplaysPriorWrites) {
  SatisfyGate();

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  faker.Interpret("CREATE (:P {v:1})");

  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());

  faker.Interpret("CHECKOUT BRANCH 'b'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  {
    auto stream = faker.Interpret("MATCH (p:P) RETURN p.v");
    ASSERT_EQ(stream.GetResults().size(), 1U)
        << "was fork-state/empty before slice D -- re-checkout must now replay the branch's own prior write";
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 1);
  }
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

// Graph Versioning v1, slice E-4: SUPERSEDES the original E-1-era version of this test (which
// asserted ALL delete on a branch threw NotYetImplemented -- true back when this slice was
// VERTEX-ONLY and no tombstone concept existed in BranchContext::UnionVerticesIterable). Plain
// (non-cascading) DELETE is now fully supported (see the Branch*Delete* tests below) -- only
// DETACH DELETE's automatic incident-edge CASCADE remains deferred: a COW'd fork vertex's
// diff-engine adjacency is EMPTY (CowVertex copies props+labels only, not adjacency, see its own
// doc-comment, branch_engine.cpp), so the diff engine's own native cascade
// (storage.cpp's ClearEdgesOnVertices) would silently miss every fork-resident incident edge --
// reject cleanly (DbAccessor::DetachDelete/DetachRemoveVertex, db_accessor.hpp) rather than ship
// that gap.
TEST_F(VersioningInterpreterTest, BranchDetachDeleteThrowsNotYetImplemented) {
  SatisfyGate();

  faker.Interpret("CREATE (:A {x:1})-[:R]->(:B)");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  ASSERT_THROW(faker.Interpret("MATCH (n:A) DETACH DELETE n"), memgraph::query::NotYetImplemented);

  // Must not have crashed or left the session/transaction unusable -- an ordinary read still works.
  auto stream = faker.Interpret("MATCH (n:A) RETURN n.x");
  ASSERT_EQ(stream.GetResults().size(), 1U);
  EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 1);
}

// Graph Versioning v1, slice E-2a (branch edge create + basic single-hop expansion): covers two
// things at once --
//   (1) a branch-native `CREATE (x)-[:R]->(y)` against two EXISTING (not-yet-COW'd historical)
//       endpoints must COW both endpoints AND be visible from BOTH directions afterwards (the
//       DbAccessor::InsertEdge COW-both fix -- without it, either the storage-layer same-transaction
//       MG_ASSERT fires, or only one direction resolves correctly).
//   (2) ENDPOINT SHADOWING: an edge created/resolved while one endpoint is still historical must
//       re-Resolve that endpoint by gid on every subsequent access -- if the endpoint is COW'd by a
//       LATER, unrelated statement, expanding across the (unmodified, historical) edge must still
//       see the endpoint's latest (COW'd) value, not a stale historical snapshot pinned at
//       expansion time (EdgeAccessor::To()'s own re-Resolve fix, edge_accessor.cpp).
// Finally re-confirms R35 (main untouched) on the edge axis, mirroring BranchVertexWriteIsLazyDiff-
// AndIsolated's own vertex-side confirmation.
TEST_F(VersioningInterpreterTest, BranchEdgeCreateAndEndpointShadowingIsUnionAware) {
  SatisfyGate();

  // On main: A/B with no edge between them; S--[:E]-->A(name:a2) exists BEFORE the fork.
  faker.Interpret("CREATE (:A {name:'a'}), (:B {name:'b'})");
  faker.Interpret("CREATE (:S {name:'s'})-[:E]->(:A {name:'a2'})");

  faker.Interpret("CREATE BRANCH 'br' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'br'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "br");

  // Branch-native edge create between two still-historical endpoints (x=A/a, y=B/b) -- exercises
  // DbAccessor::InsertEdge's COW-both-endpoints fix.
  faker.Interpret("MATCH (x:A {name:'a'}), (y:B {name:'b'}) CREATE (x)-[:R]->(y)");

  // (1) Visible from BOTH directions -- COW-both symmetry (neither endpoint was "the one" that
  // happened to get COW'd first; both must resolve consistently either way the edge is traversed).
  {
    auto stream = faker.Interpret("MATCH (x:A {name:'a'})-[:R]->(y) RETURN y.name");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "b");
  }
  {
    auto stream = faker.Interpret("MATCH (y:B {name:'b'})<-[:R]-(x) RETURN x.name");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "a");
  }

  // (2) Endpoint shadowing: COW a2 via an unrelated statement, then expand across the historical
  // (never touched) S--[:E]-->a2 edge from S (still purely historical) -- a2 must resolve to its
  // POST-SET (diff-engine) value, not the historical snapshot the edge's endpoint pointer was
  // fixed at.
  faker.Interpret("MATCH (a2:A {name:'a2'}) SET a2.name = 'a2_mod'");
  {
    auto stream = faker.Interpret("MATCH (:S)-[:E]->(a2) RETURN a2.name");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "a2_mod") << "endpoint must be re-Resolved, not stale";
  }

  // R35: main was never touched by any of the above.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    auto stream = faker.Interpret("MATCH (x:A {name:'a'})-[:R]->(y) RETURN y.name");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "the branch-native edge must be invisible on main";
  }
  {
    auto stream = faker.Interpret("MATCH (:S)-[:E]->(a2) RETURN a2.name");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "a2") << "main's a2 must be untouched by the branch's SET";
  }
}

// Graph Versioning v1, slice E-2a HIGH regression (adversarial-review, ROUND 2): `DbAccessor::
// FindEdge` (both overloads) was UNGUARDED for branch mode -- it called straight into
// `accessor_->FindEdge`, i.e. the diff engine ONLY, so any pre-fork (historical) edge would silently
// "not exist" on a branch. Reachable via `MATCH ()-[e]-() WHERE id(e) = <gid>`, which
// `rewrite/edge_index_lookup.hpp` rewrites to a `ScanAllByEdgeId` operator UNCONDITIONALLY (no index
// needed, see its own `IdFilters` rewrite) -- so this is a plain, always-on path, not a corner case
// gated behind an index.
//
// ROUND 1 tried a real diff-vs-historical `ResolveEdge` fix here and it was adversarially found
// WRONG (the historical half doesn't reliably find the edge -- see `BranchContext::ResolveEdge`'s
// own doc-comment, branch_engine.hpp, for why). Pivoted to fail-loud: `FindEdge` on a branch now
// unconditionally throws `NotYetImplemented`, mirroring the edge-mutator and edge-index-scan guards
// elsewhere in this slice -- REAL edge-by-id resolution is deferred to E-2d. This test now checks
// exactly that: the query throws cleanly (not silently wrong, not a crash), and the session/
// transaction stays usable afterward (ordinary expansion still works).
TEST_F(VersioningInterpreterTest, BranchFindEdgeByIdThrowsNotYetImplemented) {
  SatisfyGate();

  // Pre-fork edge on main.
  faker.Interpret("CREATE (:S {name:'s'})-[:E]->(:A {name:'a2'})");

  faker.Interpret("CREATE BRANCH 'br' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'br'");

  // Directed pattern (not `()-[e]-()`) and an arbitrary id -- ScanAllByEdgeId's rewrite fires on the
  // presence of an `id(e) = ...` filter alone, regardless of whether that id actually resolves to
  // anything, so the guard must fire before `FindEdge` ever gets a chance to look the gid up either
  // way.
  ASSERT_THROW(faker.Interpret("MATCH ()-[e]->() WHERE id(e) = 0 RETURN type(e)"), memgraph::query::NotYetImplemented);

  // Must not have crashed or left the session/transaction unusable -- ordinary (non-id) expansion
  // across the pre-fork edge still works.
  auto stream = faker.Interpret("MATCH (:S)-[e:E]->(:A) RETURN type(e)");
  ASSERT_EQ(stream.GetResults().size(), 1U);
  EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "E");
}

// Graph Versioning v1, slice E-2a MED regression (adversarial-review): `DbAccessor::Edges(...)`
// (every edge-type/property INDEX scan overload) was unguarded -- dead today only because a fresh
// branch's diff engine has no edge index of its own, but silently wrong (diff-engine-only contents,
// missing every still-historical edge) the moment one exists. `CREATE EDGE INDEX` is unguarded and
// routes to the diff engine only (mirrors `BranchLabelScanIsUnionAwareNotJustDiffEngineIndex`'s own
// vertex-side comment -- same accepted design, not a new bug: `EdgeTypeIndexReady`/
// `EdgeTypePropertyIndexReady`/`EdgesCount` are ALL unguarded too, consulting the diff engine's own
// index/count directly), which is enough to make `*IndexReady` report true and make the planner
// consider an indexed edge scan.
//
// DIAGNOSIS (adversarial-review, ROUND 2): a first attempt at this test used a PLAIN edge-type index
// (`CREATE EDGE INDEX ON :R`, no property) with no additional filter (`MATCH ()-[e:R]->() RETURN e`)
// -- the guard did NOT fire, because `VariableStartPlanner`'s cost-based plan selection didn't pick
// the edge-indexed start on that tiny graph (both endpoints fully anonymous, one edge total -- a
// plain vertex-scan-then-expand plan was evidently cheaper by its estimate). This is a PLAN-SELECTION
// artifact of that specific query/graph shape, not evidence the indexed path can never be reached on
// a branch (nothing in CreateIndex/EdgeTypeIndexReady/EdgesCount blocks it, mirroring the vertex
// label-index case that IS reachable). Rewritten below to mirror
// `tests/unit/interpreter.cpp`'s own `EdgePropertyInListIndexedEquivalence` recipe exactly (edge-TYPE
// -AND-PROPERTY index + an equality filter on the indexed property) -- that is an EXISTING,
// already-relied-upon case in this codebase where the property-indexed path IS the one under test on
// an equally small graph, giving much higher confidence the planner actually selects it here too.
// NOT independently re-verified by a build (none was run for this fix) -- if this still doesn't
// reach the guard, the correct fallback (per the review) is to assert the query returns the correct
// result via the (uncontested) Expand fallback instead of asserting a throw.
TEST_F(VersioningInterpreterTest, BranchEdgeTypePropertyIndexScanThrowsNotYetImplemented) {
  SatisfyGate();

  faker.Interpret("CREATE (:X {name:'x'})-[:R {prop: 1}]->(:Y {name:'y'})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("CREATE EDGE INDEX ON :R(prop)");

  ASSERT_THROW(faker.Interpret("MATCH ()-[e:R]->() WHERE e.prop = 1 RETURN e"), memgraph::query::NotYetImplemented);

  // Must not have crashed or left the session/transaction unusable -- an ordinary (non-indexed)
  // read still works.
  auto stream = faker.Interpret("MATCH (:X)-[e:R]->(:Y) RETURN type(e)");
  ASSERT_EQ(stream.GetResults().size(), 1U);
  EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "R");
}

// Graph Versioning v1, slice E-2c (branch edge-property SET/REMOVE): the edge-side mirror of
// BranchSingleStatementSetThenReturnSeesNewValue (E-1, vertices) and
// BranchVertexWriteIsLazyDiffAndIsolated's own R35 confirmation -- covers three things at once:
//   (1) SET on a still-historical (never-touched) edge must COW it into the diff engine rather than
//       crash writing through historical_'s read-only accessor (BranchContext::CowEdge,
//       EdgeAccessor::CowEdgeIfNeeded).
//   (2) single-STATEMENT read-your-write: the trailing RETURN (same statement as the SET) must see
//       the post-SET value, not a stale pre-COW copy -- EdgeAccessor::GetProperty/Properties' own
//       diff-side self-correcting-read fix (FindDiffEdge), mirroring HIGH-2's vertex-side fix.
//   (3) R35: main's own copy of the edge must never be touched by the branch's SET.
TEST_F(VersioningInterpreterTest, BranchEdgePropertyModifyIsCowedAndIsolated) {
  SatisfyGate();

  faker.Interpret("CREATE (:S)-[:E {w:1}]->(:A)");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");

  // (1)+(2): must not crash (HIGH-1-equivalent regression class), and the RETURN in the SAME
  // statement must already see w=2.
  {
    auto stream = faker.Interpret("MATCH (:S)-[e:E]->(:A) SET e.w = 2 RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2) << "must see the post-SET value, not the stale historical one";
  }

  // Persists across a LATER, separate statement within the same branch/session.
  {
    auto stream = faker.Interpret("MATCH (:S)-[e:E]->(:A) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2);
  }

  // (3) R35: main was never touched by the branch's SET.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    auto stream = faker.Interpret("MATCH (:S)-[e:E]->(:A) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 1) << "main must be untouched by the branch's SET";
  }
}

// Graph Versioning v1, slice E-2c: REMOVE (== SetProperty(key, null) internally, see
// EdgeAccessor::RemoveProperty) exercises the exact same CowEdgeIfNeeded path as SET above --
// cheap, separate regression in case a future refactor special-cases REMOVE away from SetProperty.
TEST_F(VersioningInterpreterTest, BranchEdgePropertyRemoveIsCowedAndIsolated) {
  SatisfyGate();

  faker.Interpret("CREATE (:S)-[:E {w:1}]->(:A)");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("MATCH (:S)-[e:E]->(:A) REMOVE e.w");

  {
    auto stream = faker.Interpret("MATCH (:S)-[e:E]->(:A) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].type(), memgraph::communication::bolt::Value::Type::Null)
        << "e.w must be removed on the branch";
  }

  // R35: main still has w=1 -- REMOVE on the branch must not touch it.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    auto stream = faker.Interpret("MATCH (:S)-[e:E]->(:A) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 1);
  }
}

// Graph Versioning v1, slice E-4, increment 2: a branch-NATIVE vertex (created and deleted entirely
// within the SAME checkout session -- no fork counterpart at all) must be hidden from the branch's
// own read path immediately after its own DELETE, and MERGE must have nothing to apply for it: pass
// 1's classify already excludes a branch-local gid from D3 entirely, and CowVertex/CreateVertexEx/
// DeleteVertex all happened purely inside the diff engine -- no fork object was ever touched, so
// this exercises the "works WITHOUT the Q6 merge fix" case (contrast
// BranchModifyForkVertexPropagatesToMainOnMerge/BranchDeleteForkVertexRemovesFromMainOnMerge below,
// which specifically need it).
TEST_F(VersioningInterpreterTest, BranchDeleteBranchNativeVertexHiddenAndMergesAbsent) {
  SatisfyGate();

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("CREATE (:X {id:1})");
  faker.Interpret("MATCH (n:X) DELETE n");

  {
    auto stream = faker.Interpret("MATCH (n:X) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "the branch-native vertex must be hidden after its own DELETE";
  }

  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("MERGE BRANCH 'b'");

  auto stream = faker.Interpret("MATCH (n:X) RETURN n");
  EXPECT_EQ(stream.GetResults().size(), 0U) << "main must never have seen the created-then-deleted vertex";
}

// MULTI-COMMIT fix regression (2026-07-12): direct proof that a checkout SESSION making SEVERAL
// separate query-commits (not just the CREATE-then-DELETE pair above) has EVERY one of them
// captured and replayed onto main in the right order, not just the first. Before the fix, a single
// BranchLog spanning the whole session accumulated all three commits' deltas into ONE file --
// `storage::durability::ReadWalInfo`'s per-transaction scan (wal.cpp) stops counting the instant it
// sees a SECOND transaction's differing timestamp, so `CollectBranchChangelog` would have silently
// recovered ONLY the first commit (CREATE :M1) -- MERGE would then leave main with just :M1, never
// :M2, and :M1 would never get its `v:2` property update either. Three separate `faker.Interpret`
// calls below are three separate autocommitted transactions (three separate per-commit BranchLog
// files, per `BranchContext::CreateCommitLog`) -- proving all three round-trip and apply in order.
TEST_F(VersioningInterpreterTest, BranchMultipleCommitsAllCapturedAndMergedInOrder) {
  SatisfyGate();

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // Commit 1: create :M1.
  faker.Interpret("CREATE (:M1 {v:1})");
  // Commit 2: create :M2 (a second, independent branch-native vertex).
  faker.Interpret("CREATE (:M2 {v:1})");
  // Commit 3: modify :M1 -- only correct if commit 1's vertex is visible to this later commit,
  // proving the diff engine (not just the eventual merged main) sees every earlier commit too.
  faker.Interpret("MATCH (n:M1) SET n.v = 2");

  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("MERGE BRANCH 'b'");

  {
    auto stream = faker.Interpret("MATCH (n:M1) RETURN n.v");
    ASSERT_EQ(stream.GetResults().size(), 1U) << "commit 1 (create) must have been merged";
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2) << "commit 3 (the SET) must have been merged too";
  }
  {
    auto stream = faker.Interpret("MATCH (n:M2) RETURN n.v");
    ASSERT_EQ(stream.GetResults().size(), 1U) << "commit 2 (create) must have been merged";
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 1);
  }
}

// Graph Versioning v1, slice E-4, increment 3 (+ increment 4's Q6 edge-side fix, merge.cpp): a
// FORK edge (pre-existing on main before the branch was even created) that the branch DELETEs must
// be hidden on the branch immediately, and gone from main after MERGE. WITHOUT the Q6 edge-side
// classify fix (FindHistoricalEdgeByEndpoint, merge.cpp), this test's final assertion would FAIL:
// the COW'd fork edge's WalEdgeCreate echo gets misclassified branch-local, so MERGE's WalEdgeCreate
// apply recreates a DUPLICATE edge on main (since the original gid already exists there, R11 remaps
// it to a fresh one) instead of no-oping, and the following WalEdgeDelete then deletes THAT
// duplicate -- net leaving the ORIGINAL fork edge on main completely untouched (silently WRONG).
TEST_F(VersioningInterpreterTest, BranchDeleteEdgeHiddenAndMerged) {
  SatisfyGate();

  faker.Interpret("CREATE (:S)-[:E]->(:A)");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("MATCH (:S)-[e:E]->(:A) DELETE e");

  {
    auto stream = faker.Interpret("MATCH (:S)-[e:E]->(:A) RETURN e");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "the edge must be hidden on the branch immediately after DELETE";
  }

  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    auto stream = faker.Interpret("MATCH (:S)-[e:E]->(:A) RETURN e");
    ASSERT_EQ(stream.GetResults().size(), 1U) << "main must still have ITS OWN edge -- not yet merged";
  }

  faker.Interpret("MERGE BRANCH 'b'");

  auto stream = faker.Interpret("MATCH (:S)-[e:E]->(:A) RETURN e");
  EXPECT_EQ(stream.GetResults().size(), 0U) << "the branch's delete must have removed the fork edge from main too";
}

// Graph Versioning v1, slice E-4, increment 4 (Q6, HEADLINE fix -- PROVES a LATENT bug in the
// already-shipped merge, chunk 6): a fork vertex the branch only MODIFIES (never deletes) still
// gets COW'd (BranchContext::CowVertex recreates it at its OWN gid via CreateVertexEx), which
// produces a WalVertexCreate record for it too -- indistinguishable from a genuine new-vertex create
// by the record alone. BEFORE the Q6 classify fix (merge.cpp), that record was unconditionally
// treated as branch-local, so MERGE silently created a DUPLICATE :P on main and applied the SET to
// the duplicate, never touching main's REAL :P -- this test's assertions (exactly one :P, showing
// v=2) would FAIL against the pre-fix code (either 2 rows, or the original untouched at v=1).
TEST_F(VersioningInterpreterTest, BranchModifyForkVertexPropagatesToMainOnMerge) {
  SatisfyGate();

  faker.Interpret("CREATE (:P {v:1})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("MATCH (n:P) SET n.v = 2");

  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("MERGE BRANCH 'b'");

  auto stream = faker.Interpret("MATCH (n:P) RETURN n.v");
  ASSERT_EQ(stream.GetResults().size(), 1U)
      << "must be exactly ONE :P vertex on main -- the pre-fix classify bug would have created a duplicate";
  EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2)
      << "main's REAL :P must show the branch's SET, not v=1 on an untouched original";
}

// Graph Versioning v1, slice E-4, increment 4: an ISOLATED fork vertex (no incident edges, so the
// plain non-cascading delete path applies) the branch DELETEs must be gone from main after MERGE --
// exercises the Q6 classify fix's WalVertexDelete-after-WalVertexCreate-echo interplay: the
// WalVertexCreate echo is correctly no-op'd (fork-existing, not branch-local), so the following
// WalVertexDelete actually deletes main's REAL vertex, not a bogus duplicate that would otherwise
// have been left behind untouched.
TEST_F(VersioningInterpreterTest, BranchDeleteForkVertexRemovesFromMainOnMerge) {
  SatisfyGate();

  faker.Interpret("CREATE (:Q {id:1})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("MATCH (n:Q) DELETE n");

  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    auto stream = faker.Interpret("MATCH (n:Q) RETURN n");
    ASSERT_EQ(stream.GetResults().size(), 1U) << "main must still have :Q -- not yet merged";
  }

  faker.Interpret("MERGE BRANCH 'b'");

  auto stream = faker.Interpret("MATCH (n:Q) RETURN n");
  EXPECT_EQ(stream.GetResults().size(), 0U) << "the branch's delete of the fork vertex must be applied to main";
}

// Graph Versioning v1, slice E-4, increment 4 (Q6, EDGE-side analog of
// BranchModifyForkVertexPropagatesToMainOnMerge -- the vertex analog is what caught the real HIGH
// bug; this locks in the symmetric edge-side fix, merge.cpp's FindHistoricalEdgeByEndpoint). A
// FORK edge the branch only MODIFIES (never deletes) still gets COW'd (BranchContext::CowEdge
// recreates it at its OWN gid via CreateEdgeEx), which produces a WalEdgeCreate record for it too --
// indistinguishable from a genuine new-edge create by the record alone. WITHOUT the Q6 edge-side
// classify fix (FindHistoricalEdgeByEndpoint walking the FROM endpoint's own OutEdges(View::OLD)
// instead of a bare, unreliable historical->FindEdge(gid) scan), this record would be misclassified
// branch-local: pass 2's WalEdgeCreate apply would then either collide (EdgeGidExists true, since
// main's original edge already occupies that gid) and remap to a FRESH duplicate edge gid, or --
// worse -- silently duplicate the edge outright if the gid check somehow didn't fire, with the
// following WalEdgeSetProperty misapplied to the WRONG (duplicate) copy, never reaching main's REAL
// edge at all. This is the one case in the whole E-4 slice that had NO executed coverage before this
// test (BranchDeleteEdgeHiddenAndMerged exercises the same classify path but via a DELETE, not a
// bare property SET-and-merge).
TEST_F(VersioningInterpreterTest, BranchModifyForkEdgePropertyPropagatesToMainOnMerge) {
  SatisfyGate();

  faker.Interpret("CREATE (:S)-[:R {w:1}]->(:A)");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("MATCH (:S)-[e:R]->(:A) SET e.w = 2");

  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("MERGE BRANCH 'b'");

  auto stream = faker.Interpret("MATCH (:S)-[e:R]->(:A) RETURN e.w");
  ASSERT_EQ(stream.GetResults().size(), 1U)
      << "must be exactly ONE :S-[:R]->:A edge on main -- the pre-fix classify bug would have created a duplicate";
  EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2)
      << "main's REAL edge must show the branch's SET, not w=1 on an untouched original";
}

// REMOVE variant of the test above -- same Q6 classify path (a property mutator always COWs the
// edge first, REMOVE included: EdgeAccessor::RemoveProperty is SetProperty(key, null) internally),
// cheap to lock in separately in case a future refactor special-cases REMOVE away from SetProperty.
TEST_F(VersioningInterpreterTest, BranchRemoveForkEdgePropertyPropagatesToMainOnMerge) {
  SatisfyGate();

  faker.Interpret("CREATE (:S)-[:R {w:1}]->(:A)");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("MATCH (:S)-[e:R]->(:A) REMOVE e.w");

  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("MERGE BRANCH 'b'");

  auto stream = faker.Interpret("MATCH (:S)-[e:R]->(:A) RETURN e.w");
  ASSERT_EQ(stream.GetResults().size(), 1U)
      << "must be exactly ONE :S-[:R]->:A edge on main -- the pre-fix classify bug would have created a duplicate";
  EXPECT_EQ(stream.GetResults()[0][0].type(), memgraph::communication::bolt::Value::Type::Null)
      << "main's REAL edge must show the branch's REMOVE, not w=1 on an untouched original";
}
