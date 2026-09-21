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

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <span>
#include <stdexcept>
#include <thread>
#include <unordered_set>

#include <gmock/gmock.h>
#include "gtest/gtest.h"

#include "flags/general.hpp"
#include "interpreter_faker.hpp"
#include "io/network/endpoint.hpp"
#include "license/license.hpp"
#include "metrics/prometheus_metrics.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/query/auth_query.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "query/query_user.hpp"
#include "replication/config.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/exceptions.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/logging.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/synchronized.hpp"
#include "versioning/version_store.hpp"

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

// Gate precondition: missing either the feature flag or the enterprise license must individually
// reject the query. SetUp() leaves FLAGS_versioning_enabled=false and no license active; each
// case enables exactly one of the two while leaving the other disabled -- one satisfied
// precondition is not sufficient on its own.
// (Collapses GateRejectsWhenFlagOff + GateRejectsWhenNoLicense; param: which gate is disabled.)
TEST_F(VersioningInterpreterTest, GateRejectsWhenSinglePreconditionMissing) {
  struct Case {
    bool enable_flag;
    bool enable_license;
    const char *label;
  };

  static constexpr Case cases[] = {
      // Original: GateRejectsWhenFlagOff
      {false, true, "flag=OFF license=ON"},
      // Original: GateRejectsWhenNoLicense
      {true, false, "flag=ON  license=OFF"},
  };
  for (const auto &c : cases) {
    FLAGS_versioning_enabled = c.enable_flag;
    if (c.enable_license) memgraph::license::global_license_checker.EnableTesting();
    ASSERT_THROW(faker.Interpret("CREATE BRANCH 'feature' FROM 'main'"), memgraph::query::QueryRuntimeException)
        << c.label;
    // Reset to SetUp's postcondition before the next case.
    FLAGS_versioning_enabled = false;
    memgraph::license::global_license_checker.DisableTesting();
  }
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

// Graph Versioning v1, Fix #3 (instance-role guard, MED-HIGH): PrepareVersioningQuery had no
// replica guard, so CREATE/CHECKOUT/DROP BRANCH executed against a REPLICA's own local
// VersionStore -- silently diverging from MAIN's registry (only MERGE would eventually fail, and
// only at the storage commit layer). `EnsureMainInstance` (the same helper PrepareTtlQuery already
// uses) now runs at the top of PrepareVersioningQuery's dispatch, before the gate/registry are ever
// touched. Flips this fixture's `repl_state` to REPLICA via the real
// `ReplicationState::SetReplicationRoleReplica` (mirrors tests/unit/replication_state_writing.cpp's
// own pattern) rather than a hand-rolled stub, so this exercises the genuine `IsReplica()` codepath
// EnsureMainInstance reads.
TEST_F(VersioningInterpreterTest, ManagementQueryRejectedOnReplica) {
  SatisfyGate();

  ASSERT_TRUE(repl_state.Lock()->SetReplicationRoleReplica(
      memgraph::replication::ReplicationServerConfig{.repl_server =
                                                         memgraph::io::network::Endpoint("127.0.0.1", 30246)},
      /*maybe_main_uuid=*/std::nullopt));
  ASSERT_TRUE(repl_state.ReadLock()->IsReplica());

  ASSERT_THROW(faker.Interpret("CREATE BRANCH 'feature' FROM 'main'"), memgraph::query::QueryException);
  ASSERT_THROW(faker.Interpret("CHECKOUT BRANCH 'feature'"), memgraph::query::QueryException);
  ASSERT_THROW(faker.Interpret("DROP BRANCH 'feature'"), memgraph::query::QueryException);
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

// Graph Versioning v1, chunk 7d (D10): strict write-routing rail, updated for the current-version
// model. The session target is simply CurrentDB::current_version_ -- the last CHECKOUT wins,
// exactly like USE DATABASE. CHECKOUT BRANCH '<name>' engages the rail and routes writes to the
// branch; CHECKOUT BRANCH 'main' un-engages it again (CurrentDB::SetCurrentVersion(std::nullopt)
// now also clears versioning_engaged_, interpreter.hpp) and routes writes straight back to main --
// there is no stickiness across a return to main. This test is the regression lock for that: a bare
// data-plane write after an explicit CHECKOUT BRANCH 'main' must SUCCEED, not throw
// WriteWithoutResolvedVersionException.
TEST_F(VersioningInterpreterTest, EngagedConnectionCannotSilentlyWriteMain) {
  SatisfyGate();

  // Pre-engage: a normal write to main works -- the rail is off, this connection never checked out
  // a branch yet.
  faker.Interpret("CREATE (:Seed {v:1})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");

  // Branch write: allowed (the session is genuinely ON a branch).
  faker.Interpret("CREATE (:OnBranch)");

  // Back to main -- un-engages (no longer sticky).
  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  EXPECT_FALSE(faker.interpreter.current_db_.VersioningEngaged());

  // A bare data-plane write to main now SUCCEEDS directly -- this is the fix: CHECKOUT BRANCH 'main'
  // must make main writable again on the very same connection, with no reconnect required.
  faker.Interpret("CREATE (:NoLongerBlockedMainWrite {v:2})");
  {
    auto stream = faker.Interpret("MATCH (n:Seed) RETURN n.v");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 1);
  }
  {
    auto stream = faker.Interpret("MATCH (n:NoLongerBlockedMainWrite) RETURN n.v");
    ASSERT_EQ(stream.GetResults().size(), 1U) << "CHECKOUT BRANCH 'main' must re-enable main writes";
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2);
  }
  {
    // The earlier branch write must not have leaked onto main.
    auto stream = faker.Interpret("MATCH (n:OnBranch) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "the branch-only write must not be visible on main";
  }

  // Re-checking out a branch still makes writes route to the branch, not main.
  faker.Interpret("CHECKOUT BRANCH 'b'");
  faker.Interpret("CREATE (:AnotherBranchWrite)");
  {
    auto stream = faker.Interpret("MATCH (n:AnotherBranchWrite) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 1U);
  }
}

// Adversarial-review follow-up to EngagedConnectionCannotSilentlyWriteMain, updated for the
// current-version model. PROFILE wraps an inner Cypher query and PrepareProfileQuery takes the
// PreparedQuery's rw_type from that INNER plan, but the rail's discriminator classifies the OUTER
// AST node -- a ProfileQuery, not a CypherQuery (they are siblings, not sub/super-class). This test
// now confirms a PROFILE'd write on main behaves exactly like a bare write after CHECKOUT BRANCH
// 'main': it SUCCEEDS (no longer rejected), same as the un-PROFILE'd case.
TEST_F(VersioningInterpreterTest, EngagedConnectionProfileWriteOnMainRejected) {
  SatisfyGate();

  faker.Interpret("CREATE (:Seed)");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  faker.Interpret("CHECKOUT BRANCH 'main'");  // un-engages; back on main, writable again
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  EXPECT_FALSE(faker.interpreter.current_db_.VersioningEngaged());

  // PROFILE of a write on main now succeeds, matching bare CREATE's post-fix behavior.
  faker.Interpret("PROFILE CREATE (:NoLongerBlockedProfiledWrite)");

  // No-throw is the assertion: the rail no longer rejects a PROFILE'd write on an un-engaged
  // connection. We intentionally do NOT assert the vertex is visible on main -- PrepareProfileQuery
  // aborts its transaction by design (PROFILE never commits its wrapped write, independent of
  // versioning), so a follow-up MATCH would legitimately see nothing.

  // Sanity: PROFILE of a READ on main is fine too -- unaffected either way.
  faker.Interpret("PROFILE MATCH (n:Seed) RETURN n");
}

// Counterpart to EngagedConnectionCannotSilentlyWriteMain: a connection that never genuinely checks
// out a branch (versioning_engaged_ stays false -- CREATE BRANCH alone never calls SetBranchContext,
// only CHECKOUT_BRANCH's CheckoutBranchEngine does, interpreter.cpp) must keep writing main exactly
// as if versioning didn't exist -- the rail must be a strict no-op for such a connection, including
// right after issuing a (never-checked-out) CREATE BRANCH management query.
TEST_F(VersioningInterpreterTest, NeverEngagedConnectionWritesMainNormally) {
  SatisfyGate();

  faker.Interpret("CREATE (:A {v:1})");
  faker.Interpret("MATCH (n:A) SET n.v = 2");
  {
    auto stream = faker.Interpret("MATCH (n:A) RETURN n.v");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2);
  }

  // CREATE BRANCH is management (VersioningQuery), not a CypherQuery, and never calls
  // SetBranchContext -- it must not engage the rail for this still-never-checked-out connection.
  faker.Interpret("CREATE BRANCH 'x' FROM 'main'");
  faker.Interpret("CREATE (:B)");
  {
    auto stream = faker.Interpret("MATCH (n:B) RETURN n");
    ASSERT_EQ(stream.GetResults().size(), 1U) << "CREATE BRANCH alone must not engage the write-routing rail";
  }
}

// Regression test for the write-routing sticky bug this fix closes, capturing the user's exact
// model on a SINGLE session/interpreter: the session target is just current_version_ -- the last
// CHECKOUT wins, exactly like USE DATABASE. CHECKOUT BRANCH 'x' routes writes to x, then CHECKOUT
// BRANCH 'y' routes to y, then CHECKOUT BRANCH 'main' routes back to main and main stays WRITABLE --
// all on the one connection, with no reconnect. Before the fix, the second CHECKOUT (to 'y', or
// back to 'main') would leave versioning_engaged_ stuck true from the FIRST branch checkout, and a
// bare write after the final CHECKOUT BRANCH 'main' would be rejected with
// WriteWithoutResolvedVersionException instead of landing on main.
TEST_F(VersioningInterpreterTest, CheckoutSwitchesBetweenBranchesThenBackToMainLikeUseDatabase) {
  SatisfyGate();

  // Base data on main.
  faker.Interpret("CREATE (:Base {v:0})");

  // CHECKOUT BRANCH x -- a write now lands on x, not main.
  faker.Interpret("CREATE BRANCH 'x' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'x'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "x");
  faker.Interpret("CREATE (:OnX {v:1})");
  {
    // Read-back on x: sees the fork base plus x's own write.
    auto stream = faker.Interpret("MATCH (n) RETURN count(n) AS c");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2) << "x must see :Base (forked) + :OnX (its own write)";
  }

  // CHECKOUT BRANCH y -- a write now lands on y, not x and not main.
  faker.Interpret("CREATE BRANCH 'y' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'y'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "y");
  faker.Interpret("CREATE (:OnY {v:2})");
  {
    // Read-back on y: sees the fork base plus y's own write, but NOT x's write -- branches forked
    // from main are isolated from each other.
    auto stream = faker.Interpret("MATCH (n:OnX) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "y must not see x's branch-native write";
  }
  {
    auto stream = faker.Interpret("MATCH (n:OnY) RETURN n.v");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2);
  }

  // CHECKOUT BRANCH main -- this is the regression this fix closes: a bare write must land on MAIN
  // and SUCCEED, on the very same connection that just came from two branch checkouts.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  EXPECT_FALSE(faker.interpreter.current_db_.VersioningEngaged())
      << "returning to main must un-engage the rail, not leave it stuck from the earlier checkouts";
  faker.Interpret("CREATE (:OnMain {v:3})");

  // Read-back on main: sees :Base and :OnMain, but neither branch-native write -- writes really
  // landed in the right place at every step.
  {
    auto stream = faker.Interpret("MATCH (n:Base) RETURN n.v");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 0);
  }
  {
    auto stream = faker.Interpret("MATCH (n:OnMain) RETURN n.v");
    ASSERT_EQ(stream.GetResults().size(), 1U) << "the post-CHECKOUT-main write must have landed on main";
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 3);
  }
  {
    auto stream = faker.Interpret("MATCH (n) WHERE n:OnX OR n:OnY RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "neither branch's native write may leak onto main";
  }

  // Cross-check via USING VERSION: re-checking out x and using the explicit escape hatch to peek at
  // main confirms main's state independently of the CHECKOUT-based read-back above.
  faker.Interpret("CHECKOUT BRANCH 'x'");
  {
    auto stream = faker.Interpret("USING VERSION 'main' MATCH (n:OnMain) RETURN n.v");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 3);
  }
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

// Durable-capture slice (design slices A+B+C): a branch's committed write (vertex or edge) is
// captured into its own durable BranchLog (Interpreter::Commit's capture hook) and replayed onto
// main by MERGE BRANCH (CollectBranchChangelog + versioning::MergeBranch). Parametrized over
// object kind (vertex / edge); the edge case carries a pre-merge isolation check on the edge
// property, preserved from BranchMergeAppliesCapturedEdgeToMain. MERGE drops the branch (spec
// §4.2) so each iteration starts clean for the next.
// (Collapses BranchMergeAppliesCapturedVertexToMain + BranchMergeAppliesCapturedEdgeToMain.)
TEST_F(VersioningInterpreterTest, BranchMergeAppliesCapturedObjectToMain) {
  SatisfyGate();

  struct Case {
    const char *name;
    const char *branch_write;
    const char *read_query;
    int64_t expected_value;
    const char *isolation_msg;
  };

  const Case cases[] = {
      {
          "vertex",
          "CREATE (:Foo {x:1})",
          "MATCH (n:Foo) RETURN n.x",
          1,
          "not yet merged -- main must not see the branch's write",
      },
      {
          "edge",
          "CREATE (:A)-[:R {w:5}]->(:B)",
          "MATCH (:A)-[r:R]->(:B) RETURN r.w",
          5,
          "not yet merged -- main must not see the branch's edge",
      },
  };

  for (const auto &c : cases) {
    SCOPED_TRACE(c.name);
    // MERGE (spec §4.2) drops the previous iteration's branch automatically, so no cleanup needed.
    faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
    faker.Interpret("CHECKOUT BRANCH 'b'");
    ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");

    faker.Interpret(c.branch_write);

    faker.Interpret("CHECKOUT BRANCH 'main'");
    EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
    {
      auto stream = faker.Interpret(c.read_query);
      EXPECT_EQ(stream.GetResults().size(), 0U) << c.isolation_msg;
    }

    auto merge_stream = faker.Interpret("MERGE BRANCH 'b'");
    ASSERT_EQ(merge_stream.GetResults().size(), 1U);
    EXPECT_EQ(merge_stream.GetResults()[0][0].ValueString(), "b");
    EXPECT_EQ(merge_stream.GetResults()[0][1].ValueString(), "main");

    {
      auto stream = faker.Interpret(c.read_query);
      ASSERT_EQ(stream.GetResults().size(), 1U) << "main must have the branch's " << c.name << " after merge";
      EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), c.expected_value);
    }
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

// Bug fix (change-log replay dropped `REMOVE :Label`, data loss on re-checkout): companion
// regression to BranchReCheckoutReplaysPriorWrites above, but for a label REMOVE instead of a
// vertex CREATE. `ReplayChangelogIntoDiffEngine` (branch_engine.cpp) pattern-matches every
// WalDeltaData variant CaptureBranchCommit can ever produce -- WalVertexCreate, WalVertexAddLabel,
// WalVertexSetProperty, WalEdgeCreate, WalEdgeSetProperty, WalVertexDelete, WalEdgeDelete -- to
// replay a branch's own prior writes into the freshly-rebuilt diff engine every time BuildFromFork
// re-derives the BranchContext (e.g. on CHECKOUT away and back). It had NO case for
// `WalVertexRemoveLabel`, so that variant silently fell through to the generic `[&](auto const &)
// {}` catch-all no-op: the label removal was captured into the change-log just fine (proven by
// versioning::MergeBranch, merge.cpp, which DOES have a WalVertexRemoveLabel case and applies the
// removal to main correctly on MERGE) but was then silently DROPPED on replay -- so after tearing
// down and rebuilding the BranchContext (CHECKOUT BRANCH 'main' then back to 'b'), the removed
// label reappeared on the branch, even though the very same session had already observed it gone
// pre-replay. Now fixed: ReplayChangelogIntoDiffEngine gained a WalVertexRemoveLabel case
// (mirroring WalVertexAddLabel's) that calls VertexAccessor::RemoveLabel during replay.
TEST_F(VersioningInterpreterTest, BranchRemoveLabelSurvivesRecheckoutReplay) {
  SatisfyGate();

  faker.Interpret("CREATE (:Person:Employee {id: 1})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("MATCH (n {id: 1}) REMOVE n:Employee");

  // In-session (no replay yet): this already worked pre-fix -- establishes the removal was
  // genuinely applied to the branch's live diff-engine state, not merely attempted.
  {
    auto stream = faker.Interpret("MATCH (n {id: 1}) RETURN labels(n) AS l");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    const auto &label_list = stream.GetResults()[0][0].ValueList();
    bool has_person = false;
    bool has_employee = false;
    for (const auto &label : label_list) {
      const auto &name = label.ValueString();
      if (name == "Person") has_person = true;
      if (name == "Employee") has_employee = true;
    }
    EXPECT_TRUE(has_person) << "the untouched :Person label must survive the REMOVE";
    EXPECT_FALSE(has_employee) << "in-session, before any replay, the REMOVE must already be visible";
    EXPECT_EQ(label_list.size(), 1U) << "no stray labels beyond :Person";
  }

  // Force change-log replay: tear down and rebuild the BranchContext via BuildFromFork by
  // checking out away from 'b' and back onto it.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");

  // THE REGRESSION: pre-fix, this returned :Person AND :Employee -- the freshly-rebuilt diff
  // engine replayed the change-log, silently skipped the WalVertexRemoveLabel entry, and the
  // removed label reappeared.
  {
    auto stream = faker.Interpret("MATCH (n {id: 1}) RETURN labels(n) AS l");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    const auto &label_list = stream.GetResults()[0][0].ValueList();
    bool has_person = false;
    bool has_employee = false;
    for (const auto &label : label_list) {
      const auto &name = label.ValueString();
      if (name == "Person") has_person = true;
      if (name == "Employee") has_employee = true;
    }
    EXPECT_TRUE(has_person) << "the untouched :Person label must still be present after replay";
    EXPECT_FALSE(has_employee)
        << "the REMOVE'd :Employee label must NOT reappear after the change-log replay -- this is "
           "the data-loss bug: ReplayChangelogIntoDiffEngine lacked a WalVertexRemoveLabel case";
    EXPECT_EQ(label_list.size(), 1U) << "no stray labels beyond :Person after replay";
  }

  // The branch-local removal must not leak onto main -- R35 isolation, unaffected by this fix.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  {
    auto stream = faker.Interpret("MATCH (n {id: 1}) RETURN labels(n) AS l");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    const auto &label_list = stream.GetResults()[0][0].ValueList();
    bool has_person = false;
    bool has_employee = false;
    for (const auto &label : label_list) {
      const auto &name = label.ValueString();
      if (name == "Person") has_person = true;
      if (name == "Employee") has_employee = true;
    }
    EXPECT_TRUE(has_person) << "main's vertex must still carry :Person";
    EXPECT_TRUE(has_employee) << "main must be untouched by the branch's REMOVE -- isolation must hold both ways";
    EXPECT_EQ(label_list.size(), 2U) << "main must still have both labels, exactly as originally created";
  }
}

// Branch-side change filters (branch_change_filter.hpp): a read of a KIND the branch never changed
// must return the fork-state value via the direct (no-resolve) fast path, while a read of a kind the
// branch DID change must reflect the change. Exercises per-kind independence: a property SET must not
// make label reads resolve, and a label ADD must not make property reads resolve -- yet both changed
// values are still observed. (Correctness is invariant to whether the filter resolves or reads fork;
// this test pins the observable VALUES, which must be identical either way.)
TEST_F(VersioningInterpreterTest, BranchChangeFilterKindIsolation) {
  SatisfyGate();

  faker.Interpret("CREATE (:User {id: 1, age: 40, name: 'a'})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // Property-only change: the LABEL filter stays clear (label read must be fork-correct via the
  // direct path), the PROPERTY filter is set (property read must reflect the new value).
  faker.Interpret("MATCH (n {id: 1}) SET n.age = 99");
  {
    auto stream = faker.Interpret("MATCH (n {id: 1}) RETURN n.age AS age, labels(n) AS l, n.name AS name");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 99) << "the changed property must be observed";
    const auto &labels = stream.GetResults()[0][1].ValueList();
    ASSERT_EQ(labels.size(), 1U);
    EXPECT_EQ(labels[0].ValueString(), "User") << "label filter clear -> fork label read directly, still correct";
    EXPECT_EQ(stream.GetResults()[0][2].ValueString(), "a") << "an unchanged property must read fork-state";
  }

  // Label-only change on a different vertex: PROPERTY filter stays clear for it (property reads must
  // be fork-correct via the direct path), the LABEL filter is set (label read reflects the add).
  faker.Interpret("CREATE (:User {id: 2, age: 50})");  // branch-native, but read via a fresh match below
  faker.Interpret("MATCH (n {id: 1}) SET n:VIP");
  {
    auto stream = faker.Interpret("MATCH (n {id: 1}) RETURN n.age AS age, ('VIP' IN labels(n)) AS vip");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 99) << "property read must stay correct after a label-only change";
    EXPECT_TRUE(stream.GetResults()[0][1].ValueBool()) << "the added label must be observed";
  }
}

// Branch-side change filters: cross-branch isolation and the fork-pin. The global branched() bit is
// set by ANY branch and never cleared, so a vertex changed by branch b1 has branched()==true even
// when read on branch b2 that never touched it. b2's OWN change filters are clear for that gid, so it
// must read the FORK value via the direct path -- never b1's change (which lives in b1's private diff
// engine) and never main's post-fork mutation (b2 is pinned at its own fork timestamp).
TEST_F(VersioningInterpreterTest, BranchChangeFilterCrossBranchIsolation) {
  SatisfyGate();

  faker.Interpret("CREATE (:User {id: 1, age: 10})");
  faker.Interpret("CREATE (:User {id: 2, age: 20})");

  // b1 mutates id=1 (sets its global branched() bit) but is otherwise independent.
  faker.Interpret("CREATE BRANCH 'b1' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b1'");
  faker.Interpret("MATCH (n {id: 1}) SET n.age = 111, n:Changed");
  faker.Interpret("CHECKOUT BRANCH 'main'");

  // main mutates id=2 AFTER b2 will fork -- do it before forking so b2's fork-state has age=20, then
  // change it on main to prove b2 stays pinned. (Order: fork b2 first, then mutate main.)
  faker.Interpret("CREATE BRANCH 'b2' FROM 'main'");
  faker.Interpret("MATCH (n {id: 2}) SET n.age = 222");  // main post-fork write; b2 must NOT see it
  faker.Interpret("CHECKOUT BRANCH 'b2'");

  {
    // id=1: changed by b1 (branched() set globally), never touched by b2 -> b2 reads FORK (age=10,
    // no :Changed). This is the cross-branch fast-exit the filter delivers.
    auto stream = faker.Interpret("MATCH (n {id: 1}) RETURN n.age AS age, ('Changed' IN labels(n)) AS c");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 10) << "b2 must not see b1's property change (cross-branch leak)";
    EXPECT_FALSE(stream.GetResults()[0][1].ValueBool()) << "b2 must not see b1's label change";
  }
  {
    // id=2: mutated on main AFTER b2 forked -> b2 is fork-pinned, must read age=20 (its fork-state),
    // not main's post-fork 222. b2 never touched id=2, so this is the direct fork-read path.
    auto stream = faker.Interpret("MATCH (n {id: 2}) RETURN n.age AS age");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 20) << "b2 must read fork-state, not main's post-fork write (C1)";
  }
}

// Fine-grained (gid, property_id) filter: a point read GetProperty(pid) gates on the fine filter
// ALONE, so on the change-log replay path (re-checkout) that filter MUST be re-seeded from
// WalVertexSetProperty, or a genuinely-changed property reaches a reader through main's copy and reads
// stale fork-state. This is a correctness requirement (not just precision): reaching the changed
// vertex via an EDGE TRAVERSAL lands on main's (main-resident, branched()) copy -- the exact case the
// fine filter guards -- rather than the diff-resident copy a scan would yield. A missing replay-seed
// returns the fork value here.
TEST_F(VersioningInterpreterTest, BranchChangeFilterFinePropertySurvivesRecheckout) {
  SatisfyGate();

  faker.Interpret("CREATE (n:User {id: 1})-[:E]->(v:User {id: 2, age: 40, keep: 7})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  faker.Interpret("MATCH (v {id: 2}) SET v.age = 99");  // property change; v is COW'd, fine-seeded live

  // In-session, via a traversal (reaches v through main's pointer -> fine filter consulted).
  {
    auto stream = faker.Interpret("MATCH (n {id: 1})-[:E]->(v) RETURN v.age AS age, v.keep AS keep");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 99) << "changed property must resolve to the new value";
    EXPECT_EQ(stream.GetResults()[0][1].ValueInt(), 7)
        << "an UNCHANGED property on the same vertex must read fork-state via the fine fast path";
  }

  // Force change-log replay (re-checkout), then read again VIA THE TRAVERSAL.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  {
    auto stream = faker.Interpret("MATCH (n {id: 1})-[:E]->(v) RETURN v.age AS age, v.keep AS keep");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 99)
        << "post-recheckout the fine property_field filter must be re-seeded from WalVertexSetProperty, "
           "else the changed property reads stale fork-state through main's copy";
    EXPECT_EQ(stream.GetResults()[0][1].ValueInt(), 7) << "the unchanged property must still read fork-state";
  }

  // Isolation: main is untouched by the branch's property change.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    auto stream = faker.Interpret("MATCH (v {id: 2}) RETURN v.age AS age");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 40) << "main must not see the branch's property change";
  }
}

TEST_F(VersioningInterpreterTest, MergeNonExistentBranchRejected) {
  SatisfyGate();
  ASSERT_THROW(faker.Interpret("MERGE BRANCH 'nope'"), memgraph::query::QueryRuntimeException);
}

TEST_F(VersioningInterpreterTest, MergeAndDropRejectedWhenBranchHasChildren) {
  SatisfyGate();
  faker.Interpret("CREATE BRANCH 'parent' FROM 'main'");
  // The interpreter's B2 guard rejects nested branches (FROM a non-main parent) at query time,
  // so the child is registered via the VersionStore API directly. The MERGE/DROP "has children"
  // rejection lives in VersionStore::HasChildren (called by the interpreter's MERGE_BRANCH and
  // DROP_BRANCH handlers), not in the interpreter path that creates the branch, so the invariant
  // under test is still fully exercised end-to-end through the interpreter.
  auto created = db->version_store()->CreateBranch("child", "parent", std::nullopt);
  ASSERT_TRUE(created.has_value()) << "VersionStore must accept a nested branch (the B2 guard is interpreter-only)";

  ASSERT_THROW(faker.Interpret("MERGE BRANCH 'parent'"), memgraph::query::QueryRuntimeException);
  ASSERT_THROW(faker.Interpret("DROP BRANCH 'parent'"), memgraph::query::QueryRuntimeException);

  // Children first, then the parent -- both should now succeed.
  faker.Interpret("MERGE BRANCH 'child'");
  faker.Interpret("DROP BRANCH 'parent'");
}

// Graph Versioning v1, B2 fix: CREATE BRANCH FROM a non-main parent is rejected at the
// interpreter layer. MERGE BRANCH replays the changelog onto MAIN (not onto the declared parent),
// so permitting nested creation would silently misroute branch writes. The rejection must happen
// before any VersionStore state mutation: 'child' must not exist after the throw.
TEST_F(VersioningInterpreterTest, CreateBranchFromNonMainParentIsRejected) {
  SatisfyGate();

  // A first-level branch from main is the permitted case.
  faker.Interpret("CREATE BRANCH 'feature' FROM 'main'");
  ASSERT_TRUE(db->version_store()->Exists("feature"));

  // A nested branch (FROM a non-main parent) must be rejected by the interpreter with a
  // QueryRuntimeException; the VersionStore API itself (unchanged in v1) would accept it.
  ASSERT_THROW(faker.Interpret("CREATE BRANCH 'child' FROM 'feature'"), memgraph::query::QueryRuntimeException);

  // The rejection must be pre-mutation: 'child' must not have been registered in the VersionStore.
  EXPECT_FALSE(db->version_store()->Exists("child"))
      << "a rejected CREATE BRANCH must leave the VersionStore unchanged";
}

// Observability: the six global versioning Prometheus metrics (metrics/prometheus_metrics.hpp)
// must move as the branch lifecycle progresses -- create, checkout, a captured branch commit,
// merge (which also drops the merged branch, per spec §4.2), and a plain drop.
//   - The four counters are process-global monotonic singletons shared across every test in this
//     binary, so assert DELTAS captured at the start of this test, never absolute values (mirrors
//     tests/unit/hot_cold_resume.cpp's own SuspendResumeMoveObservabilityMetrics precedent).
//   - versioning_active_branches/versioning_active_checkouts are gauges that other tests in this
//     binary may have left residue on too; assert with EXPECT_GE against their post-op floor rather
//     than an exact value.
TEST_F(VersioningInterpreterTest, VersioningMetricsIncrementOnLifecycle) {
  SatisfyGate();
  auto &g = memgraph::metrics::Metrics().global;
  const double created0 = g.versioning_branches_created->Value();
  const double merged0 = g.versioning_branches_merged->Value();
  const double dropped0 = g.versioning_branches_dropped->Value();
  const double captured0 = g.versioning_branch_commits_captured->Value();

  faker.Interpret("CREATE (:A {x:1})");
  faker.Interpret("CREATE BRANCH 'b1' FROM 'main'");
  faker.Interpret("CREATE BRANCH 'b2' FROM 'main'");
  EXPECT_DOUBLE_EQ(g.versioning_branches_created->Value() - created0, 2.0)
      << "two CREATE BRANCH statements must increment the created counter by exactly 2";
  // main + b1 + b2 now exist.
  EXPECT_GE(g.versioning_active_branches->Value(), 2.0);

  // Checkout b1, write (-> a captured branch commit on release), then back to main.
  faker.Interpret("CHECKOUT BRANCH 'b1'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b1");
  EXPECT_GE(g.versioning_active_checkouts->Value(), 1.0);
  faker.Interpret("CREATE (:OnBranch {y:1})");
  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_GE(g.versioning_branch_commits_captured->Value() - captured0, 1.0)
      << "releasing the checkout must capture at least one branch commit";

  // Merge b1 into main -- MERGE also drops the merged branch (spec §4.2), so this alone must NOT
  // move the dropped counter.
  faker.Interpret("MERGE BRANCH 'b1'");
  EXPECT_DOUBLE_EQ(g.versioning_branches_merged->Value() - merged0, 1.0)
      << "a successful MERGE BRANCH increments the merged counter by exactly 1";

  // Drop b2 directly (never checked out, never merged).
  faker.Interpret("DROP BRANCH 'b2'");
  EXPECT_DOUBLE_EQ(g.versioning_branches_dropped->Value() - dropped0, 1.0)
      << "a plain DROP BRANCH increments the dropped counter by exactly 1 (independent of MERGE's "
         "own internal drop of 'b1', which must not double-count here)";
}

namespace {
// Batch 4 ("MERGE = normal commit") test helpers -----------------------------------------------

// Busy-waits for the database's after-commit-trigger thread pool to drain -- RunTriggersAfterCommit
// is dispatched via `db->AddTask` (dbms/database.hpp), a single-thread pool, so a test that just
// issued a triggering MERGE must wait for the async task to actually finish before asserting on its
// effects. Mirrors tests/unit/utils_thread_pool.cpp's own UnfinishedTasksNum() polling idiom.
void WaitForAfterCommitTriggers(memgraph::dbms::DatabaseAccess &db) {
  auto *pool = db->thread_pool();
  for (int i = 0; i < 500 && pool->UnfinishedTasksNum() != 0; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_EQ(pool->UnfinishedTasksNum(), 0U) << "after-commit trigger dispatch never completed";
}

// M5 test double: an authenticated user whose IsAuthorized() denies a configurable set of coarse
// privileges (everything else -- MULTI_DATABASE_EDIT included -- is granted), so
// MergePrivilegeGateRequiresWritePrivileges below can prove required_privileges.cpp's action-scoped
// CREATE/SET/DELETE/REMOVE addition (M5, REMOVE added under M1) is actually enforced, not just
// computed and ignored.
class PrivilegeDenyingUser final : public memgraph::query::QueryUserOrRole {
 public:
  explicit PrivilegeDenyingUser(std::unordered_set<memgraph::query::AuthQuery::Privilege> denied)
      : memgraph::query::QueryUserOrRole(std::string{"restricted"}, {}), denied_(std::move(denied)) {}

  bool IsAuthorized(const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
                    std::optional<std::string_view> /*db_name*/,
                    memgraph::query::UserPolicy * /*policy*/) const override {
    for (auto privilege : privileges) {
      if (denied_.contains(privilege)) return false;
    }
    return true;
  }

  std::shared_ptr<memgraph::query::QueryUserOrRole> clone() const override {
    return std::make_shared<PrivilegeDenyingUser>(*this);
  }

  std::vector<std::string> GetRolenames(std::optional<std::string> /*db_name*/) const override { return {}; }

#ifdef MG_ENTERPRISE
  bool CanImpersonate(const std::string & /*target*/, memgraph::query::UserPolicy * /*policy*/,
                      std::optional<std::string_view> /*db_name*/ = std::nullopt) const override {
    return true;
  }

  std::string GetDefaultDB() const override { return std::string{"memgraph"}; }
#endif

 private:
  std::unordered_set<memgraph::query::AuthQuery::Privilege> denied_;
};

// FGA test double: a FineGrainedAuthChecker that denies every check touching one configured label,
// allows everything else (edges, other labels, properties). Mirrors
// AllowEverythingFineGrainedAuthChecker's shape (query/auth_checker.hpp) except
// NeedsFineGrainedAuthChecker() is true (so the fail-closed pre-pass in
// CheckMergeFineGrainedAuth/interpreter.cpp actually runs its per-delta checks instead of being
// short-circuited as a no-op) and Has(...) on the denied label returns false.
class DenyLabelFineGrainedAuthChecker final : public memgraph::query::FineGrainedAuthChecker {
 public:
  explicit DenyLabelFineGrainedAuthChecker(memgraph::storage::LabelId denied_label) : denied_label_(denied_label) {}

  bool Has(const memgraph::query::VertexAccessor &vertex, memgraph::storage::View view,
           memgraph::query::AuthQuery::FineGrainedPrivilege /*fine_grained_privilege*/) const override {
    auto labels = vertex.Labels(view);
    if (!labels.has_value()) return true;
    return std::find(labels->begin(), labels->end(), denied_label_) == labels->end();
  }

  bool Has(const memgraph::query::EdgeAccessor & /*edge*/,
           memgraph::query::AuthQuery::FineGrainedPrivilege /*fine_grained_privilege*/) const override {
    return true;
  }

  bool Has(std::span<memgraph::storage::LabelId const> labels,
           memgraph::query::AuthQuery::FineGrainedPrivilege /*fine_grained_privilege*/) const override {
    return std::find(labels.begin(), labels.end(), denied_label_) == labels.end();
  }

  bool Has(const memgraph::storage::EdgeTypeId & /*edge_type*/,
           memgraph::query::AuthQuery::FineGrainedPrivilege /*fine_grained_privilege*/) const override {
    return true;
  }

  bool HasGlobalPrivilegeOnVertices(memgraph::query::AuthQuery::FineGrainedPrivilege /*fgp*/) const override {
    return true;
  }

  bool HasGlobalPrivilegeOnEdges(memgraph::query::AuthQuery::FineGrainedPrivilege /*fgp*/) const override {
    return true;
  }

  bool HasAllGlobalPrivilegesOnVertices() const override { return false; }

  bool HasAllGlobalPrivilegesOnEdges() const override { return true; }

  bool HasUnrestrictedAccessToVertices() const override { return false; }

  bool HasUnrestrictedAccessToEdges() const override { return true; }

  bool HasUnrestrictedAccessToVertexProperties() const override { return true; }

  bool HasUnrestrictedAccessToEdgeTypeProperties() const override { return true; }

  bool NeedsFineGrainedAuthChecker() const override { return true; }

  bool HasPropertyPermission(std::span<memgraph::storage::LabelId const> /*labels*/,
                             memgraph::storage::PropertyId /*property*/,
                             memgraph::query::AuthQuery::PropertyPermissionType /*type*/) const override {
    return true;
  }

  bool HasPropertyPermission(memgraph::storage::EdgeTypeId const & /*edge_type*/,
                             memgraph::storage::PropertyId /*property*/,
                             memgraph::query::AuthQuery::PropertyPermissionType /*type*/) const override {
    return true;
  }

  void MakeThreadSafe() const override {}

  bool IsThreadSafe() const override { return true; }

 private:
  memgraph::storage::LabelId denied_label_;
};

class DenyLabelAuthChecker final : public memgraph::query::AuthChecker {
 public:
  explicit DenyLabelAuthChecker(memgraph::storage::LabelId denied_label) : denied_label_(denied_label) {}

  std::shared_ptr<memgraph::query::QueryUserOrRole> GenQueryUser(
      const std::optional<std::string> &name, const std::vector<std::string> & /*rolenames*/) const override {
    return std::make_shared<memgraph::query::AllowEverythingAuthChecker::User>(name.value_or("fga_tester"));
  }

  std::shared_ptr<memgraph::query::QueryUserOrRole> GenEmptyUser() const override {
    return std::make_shared<memgraph::query::AllowEverythingAuthChecker::User>();
  }

  std::unique_ptr<memgraph::query::FineGrainedAuthChecker> GetFineGrainedAuthChecker(
      const memgraph::query::QueryUserOrRole & /*user*/,
      const memgraph::query::DbAccessor * /*db_accessor*/) const override {
    return std::make_unique<DenyLabelFineGrainedAuthChecker>(denied_label_);
  }

 private:
  memgraph::storage::LabelId denied_label_;
};

// M2 fix regression scaffold: an AuthChecker double whose GetFineGrainedAuthChecker call itself
// throws -- simulating an unexpected internal failure at exactly the call site (interpreter.cpp's
// MERGE_BRANCH handler) that used to sit BETWEEN BeginMerge and the (previously too-narrow) inner
// try/catch that only ever wrapped CheckMergeFineGrainedAuth itself. Proves the M2 fix's widened
// try/catch now clears the "merging" marker for ANY throw in that whole span, not just
// CheckMergeFineGrainedAuth's own denial.
class ThrowingAuthChecker final : public memgraph::query::AuthChecker {
 public:
  std::shared_ptr<memgraph::query::QueryUserOrRole> GenQueryUser(
      const std::optional<std::string> &name, const std::vector<std::string> & /*rolenames*/) const override {
    return std::make_shared<memgraph::query::AllowEverythingAuthChecker::User>(name.value_or("throwing_tester"));
  }

  std::shared_ptr<memgraph::query::QueryUserOrRole> GenEmptyUser() const override {
    return std::make_shared<memgraph::query::AllowEverythingAuthChecker::User>();
  }

  std::unique_ptr<memgraph::query::FineGrainedAuthChecker> GetFineGrainedAuthChecker(
      const memgraph::query::QueryUserOrRole & /*user*/,
      const memgraph::query::DbAccessor * /*db_accessor*/) const override {
    throw std::runtime_error("simulated internal failure resolving the fine-grained auth checker");
  }
};
}  // namespace

// Batch 4 ("MERGE = normal commit", #4): MERGE now fires the database's AFTER-commit triggers over
// the merge's own change-set. The trigger is deliberately created AFTER the branch write (and after
// checking back onto main) so it can only ever fire for the merge itself, never for the branch
// checkout session's own (separately captured, not-yet-merged) commit.
TEST_F(VersioningInterpreterTest, MergeFiresAfterCommitTriggerWithCreatedChangeSet) {
  SatisfyGate();

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  faker.Interpret("CREATE (:Foo {x: 42})");
  faker.Interpret("CHECKOUT BRANCH 'main'");

  faker.Interpret(
      "CREATE TRIGGER merge_audit ON CREATE AFTER COMMIT EXECUTE "
      "UNWIND createdVertices AS cv CREATE (:Audit {x: cv.x})");

  faker.Interpret("MERGE BRANCH 'b'");
  WaitForAfterCommitTriggers(db);

  auto stream = faker.Interpret("MATCH (a:Audit) RETURN a.x");
  ASSERT_EQ(stream.GetResults().size(), 1U)
      << "the after-commit trigger must have fired exactly once for the merge's created vertex";
  EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 42);
}

// R4 (supersedes the original M3 test, which asserted the OPPOSITE of the now-chosen design): a
// MERGE whose branch deleted a fork-existing object must (a) not crash/hang -- MergeBranch's
// `merge` accessor finalizes normally inside MergeBranch itself, nothing is retained past its
// return -- (b) still fire after-commit triggers for the created/updated objects the SAME merge
// also produced, and (c) NOT report the deleted object to any trigger: AssembleMergeTriggerContext
// never populates a deletedVertices row for a merge (see its own doc-comment in interpreter.cpp),
// so a `() DELETE AFTER COMMIT` trigger's own event-type gate (TriggerContext::ShouldEventTrigger)
// never even fires it -- documenting the v1 limitation rather than crashing on it.
TEST_F(VersioningInterpreterTest, MergeWithDeleteFiresCreatedUpdatedTriggersButNotDeleted) {
  SatisfyGate();

  // Seed main BEFORE branching, so the branch's delete targets a fork-existing (not branch-local)
  // vertex -- the interesting merge.cpp DeleteVertex apply-site case.
  faker.Interpret("CREATE (:ToDelete {tag: 7})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  faker.Interpret("MATCH (n:ToDelete) DELETE n");
  // The SAME merge also creates a vertex, so this test can prove (b): a delete elsewhere in the
  // change-set must not suppress the created-object after-commit trigger.
  faker.Interpret("CREATE (:Kept {tag: 42})");
  faker.Interpret("CHECKOUT BRANCH 'main'");

  faker.Interpret(
      "CREATE TRIGGER merge_created_audit ON CREATE AFTER COMMIT EXECUTE "
      "UNWIND createdVertices AS cv CREATE (:CreatedAudit {x: cv.tag})");
  faker.Interpret(
      "CREATE TRIGGER merge_delete_audit ON () DELETE AFTER COMMIT EXECUTE "
      "UNWIND deletedVertices AS dv CREATE (:DeletedAudit {tag: dv.tag})");

  faker.Interpret("MERGE BRANCH 'b'");
  WaitForAfterCommitTriggers(db);

  // (b) the created-object after-commit trigger still fires for this merge.
  auto created_stream = faker.Interpret("MATCH (a:CreatedAudit) RETURN a.x");
  ASSERT_EQ(created_stream.GetResults().size(), 1U)
      << "the after-commit CREATE trigger must still fire for the merge's created vertex, even "
         "though the SAME merge also deleted an object";
  EXPECT_EQ(created_stream.GetResults()[0][0].ValueInt(), 42);

  // (c) R4: the deleted object is NOT reported -- the () DELETE trigger never fires at all (its
  // own ShouldEventTrigger gate sees an always-empty deleted_vertices_ for a merge), so no
  // DeletedAudit row is ever created.
  auto deleted_stream = faker.Interpret("MATCH (a:DeletedAudit) RETURN a.tag");
  EXPECT_EQ(deleted_stream.GetResults().size(), 0U)
      << "R4 (documented v1 limitation): MERGE after-commit triggers do not report deleted "
         "objects -- see AssembleMergeTriggerContext's own doc-comment (query/interpreter.cpp)";

  // (a) no crash/hang, and the vertex is genuinely gone from main.
  auto gone_stream = faker.Interpret("MATCH (n:ToDelete) RETURN n");
  EXPECT_EQ(gone_stream.GetResults().size(), 0U) << "the vertex must be genuinely gone from main";
}

// M3 fix (row-collapse): unlike an ordinary commit, whose TriggerContextCollector collapses every
// repeated write to the same (gid, property) into a single net row before handing rows to triggers
// (trigger_context.hpp/.cpp), AssembleMergeTriggerContext used to emit ONE phantom row per WAL
// delta -- so a branch that SET the same property twice produced TWO setVertexProperties rows on
// merge instead of one. Proves the fix: only ONE row surfaces, carrying the fork-snapshot old value
// and the LATEST (not intermediate) new value.
TEST_F(VersioningInterpreterTest, MergeCollapsesRepeatedPropertySetIntoSingleRow) {
  SatisfyGate();

  // Seed main BEFORE branching so `val` has a fork-snapshot value (1) distinct from either branch-side write.
  faker.Interpret("CREATE (:Item {val: 1})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  // Two SETs of the SAME (gid, property) within one branch changelog -- before the fix this became
  // two WalVertexSetProperty deltas and thus two phantom rows on merge.
  faker.Interpret("MATCH (n:Item) SET n.val = 10");
  faker.Interpret("MATCH (n:Item) SET n.val = 20");
  faker.Interpret("CHECKOUT BRANCH 'main'");

  faker.Interpret(
      "CREATE TRIGGER merge_set_audit ON () UPDATE AFTER COMMIT EXECUTE "
      "UNWIND setVertexProperties AS sp CREATE (:SetAudit {new_val: sp.new, old_val: sp.old})");

  faker.Interpret("MERGE BRANCH 'b'");
  WaitForAfterCommitTriggers(db);

  auto stream = faker.Interpret("MATCH (a:SetAudit) RETURN a.new_val, a.old_val");
  ASSERT_EQ(stream.GetResults().size(), 1U)
      << "M3 fix: two SETs of the same (gid, property) within one branch's changelog must collapse "
         "into a single net row on merge, exactly like an ordinary commit's TriggerContextCollector "
         "-- not one phantom row per WAL delta";
  EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 20)
      << "the collapsed row's new value must be the LATEST value written on the branch, not an "
         "intermediate one";
  EXPECT_EQ(stream.GetResults()[0][1].ValueInt(), 1)
      << "the collapsed row's old value must be the fork-snapshot value (main's value at fork_ts), "
         "identical no matter how many branch-side deltas touched this property";
}

// M5: required_privileges.cpp now adds CREATE/SET/DELETE/REMOVE to MERGE_BRANCH's own required
// privileges (action-scoped -- NOT added for CREATE/CHECKOUT/DROP/SHOW). A user with
// MULTI_DATABASE_EDIT but lacking those write privileges must be rejected by
// Interpreter::CheckAuthorized (the actual enforcement point -- glue/SessionHL.cpp's own call
// site); a user with full rights must succeed.
//
// InterpreterFaker::Interpret/Prepare never call CheckAuthorized (only the Bolt session layer
// does), so this test calls Interpreter::Prepare + CheckAuthorized directly, mirroring
// glue/SessionHL.cpp's own InterpretPrepare usage.
TEST_F(VersioningInterpreterTest, MergePrivilegeGateRequiresWritePrivileges) {
  SatisfyGate();
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");

  const std::unordered_set<memgraph::query::AuthQuery::Privilege> denied{memgraph::query::AuthQuery::Privilege::CREATE,
                                                                         memgraph::query::AuthQuery::Privilege::SET,
                                                                         memgraph::query::AuthQuery::Privilege::DELETE,
                                                                         memgraph::query::AuthQuery::Privilege::REMOVE};
  faker.interpreter.SetUser(std::make_shared<PrivilegeDenyingUser>(denied));

  memgraph::storage::ExternalPropertyValue::map_t no_params;
  {
    auto prepare_result =
        faker.interpreter.Prepare("MERGE BRANCH 'b'", [&](auto * /*storage*/) { return no_params; }, {});
    ASSERT_THROW(faker.interpreter.CheckAuthorized(prepare_result.privileges, prepare_result.db),
                 memgraph::query::QueryException)
        << "MULTI_DATABASE_EDIT alone (the pre-M5 privilege set) must no longer be sufficient to MERGE";
  }

  // M1: pin REMOVE specifically -- a branch can carry RemoveLabel/RemoveProperty deltas (e.g. from
  // a branch-side `REMOVE n:L` / `REMOVE n.p`), so denying ONLY Privilege::REMOVE (while granting
  // CREATE/SET/DELETE) must still reject the merge. This isolates the M1 fix from the pre-existing
  // CREATE/SET/DELETE requirement asserted above.
  {
    const std::unordered_set<memgraph::query::AuthQuery::Privilege> denied_remove_only{
        memgraph::query::AuthQuery::Privilege::REMOVE};
    faker.interpreter.SetUser(std::make_shared<PrivilegeDenyingUser>(denied_remove_only));
    auto prepare_result =
        faker.interpreter.Prepare("MERGE BRANCH 'b'", [&](auto * /*storage*/) { return no_params; }, {});
    ASSERT_THROW(faker.interpreter.CheckAuthorized(prepare_result.privileges, prepare_result.db),
                 memgraph::query::QueryException)
        << "MERGE_BRANCH must require Privilege::REMOVE on its own (M1) -- a branch can carry "
           "RemoveLabel/RemoveProperty deltas just like a direct REMOVE n:L / REMOVE n.p would";
  }

  // Restore full rights (the faker's own AllowEverythingAuthChecker user) -- the SAME query now
  // passes the privilege gate and can run to completion (and the branch is gone -- merged).
  faker.interpreter.SetUser(faker.auth_checker.GenQueryUser(std::nullopt, {}));
  faker.Interpret("MERGE BRANCH 'b'");
  {
    auto stream = faker.Interpret("SHOW BRANCHES");
    ASSERT_EQ(stream.GetResults().size(), 1U)
        << "branch 'b' was merged and dropped; only the synthetic 'main' row remains (SHOW BRANCHES "
           "always emits a 'main' row, interpreter.cpp)";
    EXPECT_EQ(stream.GetResults()[0][1].ValueString(), "main") << "the only remaining branch row is 'main'";
  }
}

// #5 / FGA: a fail-closed pre-pass now runs the MERGING USER's own FineGrainedAuthChecker over the
// branch's change-log before MergeBranch ever touches main (CheckMergeFineGrainedAuth,
// interpreter.cpp). A user denied a label the branch's writes touch must have the merge rejected
// (main untouched, branch still mergeable/retryable -- the AbortMerge fix); a user with full FGA
// rights must succeed. (Full role/policy-backed FGA setup -- real Auth roles with GRANT/DENY -- is
// an e2e-level concern; this test exercises the interpreter-level wiring with a hand-rolled
// FineGrainedAuthChecker double, which is what's practical to construct at the unit level.)
TEST_F(VersioningInterpreterTest, MergeFgaDeniedLabelRejectsMergeMainUntouched) {
  SatisfyGate();

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  faker.Interpret("CREATE (:Secret {x: 1})");
  faker.Interpret("CHECKOUT BRANCH 'main'");

  const auto secret_label = [&] {
    auto acc = db->storage()->Access(memgraph::storage::StorageAccessType::READ);
    return acc->NameToLabel("Secret");
  }();

  DenyLabelAuthChecker deny_checker(secret_label);
  auto *original_auth_checker = interpreter_context.auth_checker;
  interpreter_context.auth_checker = &deny_checker;
  faker.interpreter.SetUser(deny_checker.GenQueryUser("denied_tester", {}));

  ASSERT_THROW(faker.Interpret("MERGE BRANCH 'b'"), memgraph::query::QueryRuntimeException)
      << "the merging user is denied all fine-grained access to :Secret -- the merge must be "
         "rejected before touching main";

  // Restore full FGA rights BEFORE verifying main's state: DenyLabelFineGrainedAuthChecker::Has
  // does not discriminate by privilege type, so a :Secret READ while it is still active would
  // ALSO be silently filtered by the query engine's own FGA-read-gating (operator.cpp) -- that
  // would make "0 results" ambiguous between "FGA is hiding it" and "main is genuinely untouched".
  // Restoring first makes the read trustworthy, and doubles as proof the branch is still
  // mergeable/retryable (the try/catch + AbortMerge fix around the FGA pre-pass) -- not stuck in
  // BeginMerge's "merging" marker forever.
  interpreter_context.auth_checker = original_auth_checker;
  faker.interpreter.SetUser(faker.auth_checker.GenQueryUser(std::nullopt, {}));
  {
    auto stream = faker.Interpret("MATCH (n:Secret) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "main must be untouched by the rejected merge";
  }

  // The SAME branch now merges successfully with full rights.
  faker.Interpret("MERGE BRANCH 'b'");
  {
    auto stream = faker.Interpret("MATCH (n:Secret) RETURN n");
    ASSERT_EQ(stream.GetResults().size(), 1U);
  }
}

// M2 fix: a throw from GetFineGrainedAuthChecker itself (BEFORE CheckMergeFineGrainedAuth is ever
// called, hence outside the OLD inner try/catch that only wrapped that one call) used to leave
// `name` permanently stuck in BeginMerge's "merging" marker -- un-mergeable AND un-droppable until
// restart. The M2 fix widens the try/catch (interpreter.cpp, MERGE_BRANCH handler) to cover this
// whole span, so this now aborts cleanly and the branch stays retryable.
TEST_F(VersioningInterpreterTest, MergeAbortsMarkerWhenFgaCheckerResolutionThrows) {
  SatisfyGate();

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  faker.Interpret("CREATE (:Secret {x: 1})");
  faker.Interpret("CHECKOUT BRANCH 'main'");

  ThrowingAuthChecker throwing_checker;
  auto *original_auth_checker = interpreter_context.auth_checker;
  interpreter_context.auth_checker = &throwing_checker;
  faker.interpreter.SetUser(throwing_checker.GenQueryUser("throwing_tester", {}));

  ASSERT_THROW(faker.Interpret("MERGE BRANCH 'b'"), std::exception)
      << "GetFineGrainedAuthChecker throwing must propagate, not be swallowed";

  // Restore a working auth checker BEFORE checking retryability.
  interpreter_context.auth_checker = original_auth_checker;
  faker.interpreter.SetUser(faker.auth_checker.GenQueryUser(std::nullopt, {}));

  // Without the M2 fix, `b` would still be stuck in BeginMerge's "merging" marker here --
  // BeginMerge refuses a branch already marked "merging", so a retry would throw
  // "Version 'b' is in use by another session." instead of actually merging.
  faker.Interpret("MERGE BRANCH 'b'");
  {
    auto stream = faker.Interpret("MATCH (n:Secret) RETURN n");
    ASSERT_EQ(stream.GetResults().size(), 1U)
        << "the branch merged successfully on retry -- the 'merging' marker was cleared, not left stuck";
  }
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
// BranchLabelScanUnionAwareWithForkEraIndex below for the dedicated label-indexed-scan
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
// happens to exist -- see BranchLabelScanUnionAwareWithForkEraIndex below for the
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

// Graph Versioning v1 (lazy diff-context, slice E-1) HIGH-3(b) regression (adversarial-review fix)
// -- REWORKED for chunk 8 (R14): this test used to create the label index directly against the
// branch's own diff engine (`CREATE INDEX ON :A` issued AFTER `CHECKOUT BRANCH 'b'`, exploiting the
// same query::DbAccessor/accessor_ redirection as everything else, see db_accessor.hpp's
// CreateIndex) to force the planner into a real ScanAllByLabel and exercise
// DbAccessor::Vertices(view, label)'s union-aware fallback. Chunk 8 now REJECTS schema-plane DDL
// (including `CREATE INDEX`) while a versioning branch is checked out -- see the `is_schema_ddl`
// guard in `interpreter.cpp`'s `Prepare` -- so that branch-side `CREATE INDEX` would now throw
// instead of building the index, invalidating the old recipe outright.
//
// The index is created on MAIN instead, BEFORE the fork -- a fork-era index is a legitimate part of
// the historical base a branch reads through, and chunk 8 does not (and must not) reject schema DDL
// issued while checked out on main. Because chunk 8 also forbids a SECOND, branch-side index, the
// branch's own diff engine never acquires a label index of its own, so the planner may now fall back
// to a plain ScanAll for `MATCH (n:A)` rather than an indexed ScanAllByLabel. That is fine: this test
// asserts the RESULT (union-aware -- both the COW'd vertex and the still-historical one are
// returned), which holds regardless of which operator the planner picks.
// `DbAccessor::Vertices`'s union-materialize path (`MaterializeFilteredBranchScan`) remains the
// correctness guarantee for the day fork-era-catalog reconciliation (deferred) makes an indexed
// branch scan planner-reachable again.
TEST_F(VersioningInterpreterTest, BranchLabelScanUnionAwareWithForkEraIndex) {
  SatisfyGate();

  faker.Interpret("CREATE (:A {x:1}), (:A {x:5})");
  // Index on MAIN, pre-fork -- schema DDL on main is unaffected by the branch guard.
  faker.Interpret("CREATE INDEX ON :A");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // Touches (COWs) only the x=1 vertex -- the x=5 one stays purely historical. No branch-side
  // index exists -- chunk 8 forbids creating one here.
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
  EXPECT_TRUE(found_cowed) << "the COW'd vertex must still be found regardless of the scan operator chosen";
  EXPECT_TRUE(found_historical)
      << "the still-historical vertex (absent from any branch-side index) must not be silently missed";
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

// Graph Versioning v1, slice E-2d (edge-by-id resolution): supersedes the ROUND-2 fail-loud guard
// that used to live here (`BranchFindEdgeByIdThrowsNotYetImplemented` -- `DbAccessor::FindEdge` on a
// branch unconditionally threw `NotYetImplemented`, deferring real resolution to E-2d; see that
// history for why a real diff-vs-historical `ResolveEdge` fix was adversarially rejected in ROUND 1).
// `MATCH ()-[e]->() WHERE id(e) = <gid>` still rewrites to a `ScanAllByEdgeId` operator UNCONDITIONALLY
// (rewrite/edge_index_lookup.hpp's own `IdFilters` rewrite, no index needed), which calls straight
// into `DbAccessor::FindEdge` -- this now resolves BOTH halves of the union:
//   (1) a still-historical (pre-fork) edge, via the bare-gid full-scan fallback (the edge is not
//       resident in the diff engine at all -- `FindDiffEdge` alone would miss it).
//   (2) a branch-native edge, via the `FindDiffEdge` fast path (looked up directly in the diff
//       engine, no full scan needed).
// Also confirms a non-existent gid returns no rows rather than throwing or crashing.
TEST_F(VersioningInterpreterTest, BranchFindEdgeByIdResolvesForkAndBranchNativeEdges) {
  SatisfyGate();

  // Pre-fork (fork) edge on main.
  faker.Interpret("CREATE (:S)-[:E]->(:A)");

  int64_t fork_edge_gid;
  {
    auto s = faker.Interpret("MATCH ()-[e:E]->() RETURN id(e)");
    ASSERT_EQ(s.GetResults().size(), 1U);
    fork_edge_gid = s.GetResults()[0][0].ValueInt();
  }

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // Resolves the FORK edge by id on the branch -- exercises the bare-gid full-scan fallback.
  {
    auto s = faker.Interpret("MATCH ()-[e]->() WHERE id(e) = " + std::to_string(fork_edge_gid) + " RETURN type(e)");
    ASSERT_EQ(s.GetResults().size(), 1U) << "the pre-fork edge must resolve by id via the full-scan fallback";
    EXPECT_EQ(s.GetResults()[0][0].ValueString(), "E");
  }

  // Branch-native edge -- exercises the FindDiffEdge fast path instead.
  faker.Interpret("MATCH (a:A) CREATE (a)-[:F]->(:B)");
  int64_t native_gid;
  {
    auto s = faker.Interpret("MATCH ()-[e:F]->() RETURN id(e)");
    ASSERT_EQ(s.GetResults().size(), 1U);
    native_gid = s.GetResults()[0][0].ValueInt();
  }
  {
    auto s = faker.Interpret("MATCH ()-[e]->() WHERE id(e) = " + std::to_string(native_gid) + " RETURN type(e)");
    ASSERT_EQ(s.GetResults().size(), 1U) << "the branch-native edge must resolve by id via FindDiffEdge";
    EXPECT_EQ(s.GetResults()[0][0].ValueString(), "F");
  }

  // Non-existent id: neither the historical nor the diff-engine lookup finds anything -- no rows,
  // no throw.
  {
    auto s = faker.Interpret("MATCH ()-[e]->() WHERE id(e) = 999999 RETURN type(e)");
    EXPECT_EQ(s.GetResults().size(), 0U) << "a non-existent gid must return no rows, not throw";
  }
}

// Graph Versioning v1, slice E-2d (edge-type+property indexed scan) -- REWORKED for chunk 8 (R14):
// this test used to create the edge-type-and-property index ON THE BRANCH itself (mirroring
// `tests/unit/interpreter.cpp`'s own `EdgePropertyInListIndexedEquivalence` recipe) to reliably force
// the planner into an indexed scan. Chunk 8 now REJECTS `CREATE EDGE INDEX` (and every other
// schema-plane DDL) while a versioning branch is checked out -- see the `is_schema_ddl` guard in
// `interpreter.cpp`'s `Prepare` -- so that branch-side `CREATE EDGE INDEX` would now throw instead of
// building the index, invalidating the old recipe outright.
//
// The index is created on MAIN instead, BEFORE the fork -- a fork-era index is a legitimate part of
// the historical base a branch reads through, and chunk 8 does not (and must not) reject schema DDL
// issued while checked out on main. Because chunk 8 also forbids a SECOND, branch-side index, the
// branch's own diff engine never acquires an edge index of its own, so the planner may now fall back
// to a plain Expand for the branch-native edges rather than an indexed scan. That is fine: this test
// asserts the RESULT (union-aware -- the historical fork-era edge and the branch-native match are both
// returned, the non-match excluded), which holds regardless of which operator the planner picks.
// `DbAccessor::Edges`'s union-materialize path (`MaterializeFilteredBranchEdgeScan`) remains the
// correctness guarantee for the day fork-era-catalog reconciliation (deferred) makes an indexed branch
// scan planner-reachable again.
TEST_F(VersioningInterpreterTest, BranchEdgeScanUnionAwareWithForkEraEdgeIndex) {
  SatisfyGate();

  // Fork edge, prop=1, created on MAIN before the branch even exists.
  faker.Interpret("CREATE (:X {name:'x'})-[:R {prop:1}]->(:Y {name:'y'})");
  // Index on MAIN, pre-fork -- schema DDL on main is unaffected by the branch guard.
  faker.Interpret("CREATE EDGE INDEX ON :R(prop)");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // Branch-native edges: one matching (prop=1), one that must be excluded (prop=2). No branch-side
  // index exists -- chunk 8 forbids creating one here.
  faker.Interpret("CREATE (:X)-[:R {prop:1}]->(:Y)");
  faker.Interpret("CREATE (:X)-[:R {prop:2}]->(:Y)");

  auto stream = faker.Interpret("MATCH ()-[e:R]->() WHERE e.prop = 1 RETURN e.prop");
  ASSERT_EQ(stream.GetResults().size(), 2U)
      << "branch edge scan must be union-aware: the historical fork edge (prop=1) plus the "
         "branch-native prop=1 edge, excluding prop=2 -- regardless of whether the planner picks an "
         "indexed scan or Expand, both are union-aware on a branch";
  for (auto &row : stream.GetResults()) {
    EXPECT_EQ(row[0].ValueInt(), 1);
  }

  // Isolation: main still has exactly ONE :R edge -- the branch's native edges never leaked across.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  auto s2 = faker.Interpret("MATCH ()-[e:R]->() RETURN e");
  ASSERT_EQ(s2.GetResults().size(), 1U) << "branch-native edges must not leak to main";
}

// Graph Versioning v1, chunk 8 (R14): the schema-plane DDL rejection guard itself. Every
// index/constraint/ANALYZE-GRAPH-shaped DDL must throw QueryRuntimeException while checked out on a
// branch (BEFORE it ever reaches storage), a data-plane write and an ordinary read must still work
// afterward on that SAME session (the throw must not leave the interpreter wedged), and the identical
// DDL must succeed once checked back out onto main -- the guard is branch-scoped, not global.
//
// Widened (adversarial-review follow-up, see the guard's own comment in interpreter.cpp's Prepare):
// the same rejection also covers storage-global operations -- TTL and STORAGE MODE are asserted here
// with simple, path-free syntax; RECOVER SNAPSHOT is deliberately NOT exercised in this test (it
// needs a real snapshot file on disk) but is covered by the identical guard/exception path.
TEST_F(VersioningInterpreterTest, BranchSchemaDdlRejected) {
  SatisfyGate();

  faker.Interpret("CREATE (:L {p:1})-[:R {p:1}]->(:M)");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // Every schema-plane DDL is rejected on a branch:
  ASSERT_THROW(faker.Interpret("CREATE INDEX ON :L(p)"), memgraph::query::QueryRuntimeException);
  ASSERT_THROW(faker.Interpret("CREATE EDGE INDEX ON :R(p)"), memgraph::query::QueryRuntimeException);
  ASSERT_THROW(faker.Interpret("CREATE CONSTRAINT ON (n:L) ASSERT EXISTS (n.p)"),
               memgraph::query::QueryRuntimeException);
  ASSERT_THROW(faker.Interpret("ANALYZE GRAPH"), memgraph::query::QueryRuntimeException);

  // Storage-global operations are rejected too (same guard, widened set):
  ASSERT_THROW(faker.Interpret("STORAGE MODE IN_MEMORY_ANALYTICAL"), memgraph::query::QueryRuntimeException);
  ASSERT_THROW(faker.Interpret("ENABLE TTL"), memgraph::query::QueryRuntimeException);

  // Session still usable after the rejections -- an ordinary data read works.
  {
    auto s = faker.Interpret("MATCH (n:L) RETURN n.p");
    ASSERT_EQ(s.GetResults().size(), 1U);
    EXPECT_EQ(s.GetResults()[0][0].ValueInt(), 1);
  }

  // Data-plane write still allowed on the branch.
  faker.Interpret("CREATE (:L {p:2})");

  // The SAME DDL succeeds on main -- the guard is branch-scoped.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("CREATE INDEX ON :L(p)");  // must NOT throw
  {
    auto s = faker.Interpret("SHOW INDEX INFO");
    EXPECT_GE(s.GetResults().size(), 1U);
  }
}

// Graph Versioning v1, Fix #7: TriggerQuery/StreamQuery mutate main's global, durable trigger/
// stream registries (TriggerStore/Streams live on the Database, not the diff engine) -- the same
// class of storage-global side effect BranchSchemaDdlRejected above already covers for TTL/
// STORAGE MODE/indexes. Before this fix, TtlQuery was in the on-branch blocklist but
// TriggerQuery/StreamQuery were not, so a branch session could silently mutate main's trigger/
// stream registries. Mirrors BranchSchemaDdlRejected's own shape (ASSERT_THROW on the branch, then
// the identical query succeeds on main, proving the guard is branch-scoped not global).
TEST_F(VersioningInterpreterTest, BranchTriggerAndStreamDdlRejected) {
  SatisfyGate();

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  ASSERT_THROW(faker.Interpret("CREATE TRIGGER my_trigger AFTER COMMIT EXECUTE RETURN 1"),
               memgraph::query::QueryRuntimeException);
  // Must be parse+semantic-valid (TOPICS/TRANSFORM are required) so it reaches the on-branch guard
  // in Prepare rather than failing semantic validation first; the guard rejects it by query type
  // before the stream handler ever loads the transform.
  ASSERT_THROW(faker.Interpret("CREATE KAFKA STREAM my_stream TOPICS my_topic TRANSFORM my_module.my_transform"),
               memgraph::query::QueryRuntimeException);

  // Session still usable after the rejections.
  faker.Interpret("CREATE (:OnBranch {x:1})");

  // The SAME DDL succeeds on main -- the guard is branch-scoped.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("CREATE TRIGGER my_trigger AFTER COMMIT EXECUTE RETURN 1");  // must NOT throw
  {
    auto s = faker.Interpret("SHOW TRIGGERS");
    EXPECT_EQ(s.GetResults().size(), 1U);
  }
}

// Graph Versioning v1, slice E-2c (branch edge-property SET/REMOVE): both exercise
// BranchContext::CowEdgeIfNeeded on the same still-historical edge. The SET variant embeds a
// RETURN for a single-statement read-your-write check (HIGH-2 analogue); REMOVE does not (it is
// SetProperty(key, null) internally -- same CowEdgeIfNeeded path). Both must be isolated from
// main (R35). Parametrized over mutation kind and expected post-mutation branch value.
// (Collapses BranchEdgePropertyModifyIsCowedAndIsolated + BranchEdgePropertyRemoveIsCowedAndIsolated.)
TEST_F(VersioningInterpreterTest, BranchEdgePropertyMutationIsCowedAndIsolated) {
  SatisfyGate();

  struct Case {
    const char *name;
    // When non-null: mutation query includes RETURN for a read-your-write assertion.
    const char *mutation_with_return;
    int ryw_expected_int;
    // When non-null: mutation query with no RETURN.
    const char *mutation_no_return;
    bool expect_null;
    int expected_int;  // valid when !expect_null
  };

  static const Case cases[] = {
      // Original: BranchEdgePropertyModifyIsCowedAndIsolated
      {"SET", "MATCH (:S)-[e:E]->(:A) SET e.w = 2 RETURN e.w", 2, nullptr, false, 2},
      // Original: BranchEdgePropertyRemoveIsCowedAndIsolated
      {"REMOVE", nullptr, 0, "MATCH (:S)-[e:E]->(:A) REMOVE e.w", true, 0},
  };

  int iter = 0;
  for (const auto &c : cases) {
    SCOPED_TRACE(c.name);
    if (iter++ > 0) {
      // SET iteration ended on main with branch 'b' still alive; clear for REMOVE.
      faker.Interpret("MATCH (n) DETACH DELETE n");
      faker.Interpret("DROP BRANCH 'b'");
    }

    faker.Interpret("CREATE (:S)-[:E {w:1}]->(:A)");
    faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
    faker.Interpret("CHECKOUT BRANCH 'b'");
    ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");

    if (c.mutation_with_return) {
      // (1)+(2): must not crash (HIGH-1-equivalent), and the RETURN in the SAME statement must
      // already see the post-mutation value.
      auto stream = faker.Interpret(c.mutation_with_return);
      ASSERT_EQ(stream.GetResults().size(), 1U);
      EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), c.ryw_expected_int)
          << "must see the post-SET value, not the stale historical one";
    } else {
      faker.Interpret(c.mutation_no_return);
    }

    // Persists in a later, separate statement within the same branch session.
    {
      auto stream = faker.Interpret("MATCH (:S)-[e:E]->(:A) RETURN e.w");
      ASSERT_EQ(stream.GetResults().size(), 1U);
      if (c.expect_null) {
        EXPECT_EQ(stream.GetResults()[0][0].type(), memgraph::communication::bolt::Value::Type::Null)
            << "e.w must be removed on the branch";
      } else {
        EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), c.expected_int);
      }
    }

    // R35: main must be untouched by the branch's mutation.
    faker.Interpret("CHECKOUT BRANCH 'main'");
    {
      auto stream = faker.Interpret("MATCH (:S)-[e:E]->(:A) RETURN e.w");
      ASSERT_EQ(stream.GetResults().size(), 1U);
      EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 1) << "main must be untouched by the branch's " << c.name;
    }
  }
}

// Graph Versioning v1, perf/versioning-branch-read D1: edge VALUE-read fast path
// (query::EdgeAccessor::Properties/GetProperty/GetPropertySize, edge_accessor.cpp). Symmetric to
// BranchChangeFilterKindIsolation's vertex coverage above, but exercising storage::Edge::branched()
// (edge.hpp) directly: a changed edge must resolve through FindDiffEdge, while an UNCHANGED edge --
// even one hanging off a vertex the branch DID COW (S below, via e1's SET) -- must read fork-state
// via the new `!MaybeBranchedHint()` gate, never touching FindDiffEdge. Three edges pin the whole
// matrix in one test:
//   e1 (:S)-[:E1]->(:A)  -- SET on the branch: THE CHANGED CASE (1).
//   e3 (:S)-[:E3]->(:C)  -- same FROM-vertex as e1 (so S gets vertex-COW'd/branched by e1's SET),
//                           but e3 itself is never touched: THE "unchanged edge of a branched
//                           vertex" CASE (2) -- exactly the fast path's win (S's own branched() bit
//                           is set, but e3's OWN edge-object bit stays clear).
//   e2 (:S2)-[:E2]->(:B) -- both endpoints and the edge itself are completely untouched: THE
//                           "fully-unbranched vertex" CASE (3).
TEST_F(VersioningInterpreterTest, BranchEdgeBitFastPathSkipsResolveForUnchangedEdges) {
  SatisfyGate();

  faker.Interpret("CREATE (s:S)-[:E1 {w:1}]->(:A), (s)-[:E3 {w:3}]->(:C)");
  faker.Interpret("CREATE (:S2)-[:E2 {w:2}]->(:B)");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");

  faker.Interpret("MATCH (:S)-[e:E1]->(:A) SET e.w = 100");

  // (1) Changed edge: must reflect the new value.
  {
    auto stream = faker.Interpret("MATCH (:S)-[e:E1]->(:A) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 100) << "the changed edge must reflect the new value";
  }
  // (2) Unchanged sibling edge of the now-branched S vertex: must still read fork-state (3), not
  // resolve via the diff engine.
  {
    auto stream = faker.Interpret("MATCH (:S)-[e:E3]->(:C) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 3)
        << "an edge untouched by the branch must read fork-state via the branched()-bit fast path, "
           "even though its FROM vertex was COW'd by an unrelated SET on a sibling edge";
  }
  // (3) Fully-unbranched vertex+edge: must read fork-state (2).
  {
    auto stream = faker.Interpret("MATCH (:S2)-[e:E2]->(:B) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2) << "a completely untouched edge must read fork-state";
  }

  // R35: main is untouched by any of the above.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    auto stream = faker.Interpret("MATCH (:S)-[e:E1]->(:A) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 1) << "main must be untouched by the branch's SET";
  }
}

// D1 correctness case (4): the coarse "any-branch" bit is monotonic and never cleared, so branch X
// setting main's Edge::branched() bit must NOT leak X's diff-engine value to a DIFFERENT branch Y
// that never touched the edge -- Y's own FindDiffEdge lookup misses (its diff engine is private), so
// it must fall through to its OWN fork-state read. Mirrors BranchChangeFilterCrossBranchIsolation's
// vertex-level coverage above, but for the edge-bit gate specifically.
TEST_F(VersioningInterpreterTest, BranchEdgeBitFastPathCrossBranchIsolation) {
  SatisfyGate();

  faker.Interpret("CREATE (:S)-[:E {w:1}]->(:A)");

  // b1 touches the edge -- sets main's Edge::branched() bit GLOBALLY (monotonic, never cleared).
  faker.Interpret("CREATE BRANCH 'b1' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b1'");
  faker.Interpret("MATCH (:S)-[e:E]->(:A) SET e.w = 999");
  faker.Interpret("CHECKOUT BRANCH 'main'");

  // b2 forks AFTER b1's change (main itself is untouched, R35), so b2's fork-state is still w=1.
  faker.Interpret("CREATE BRANCH 'b2' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b2'");

  {
    auto stream = faker.Interpret("MATCH (:S)-[e:E]->(:A) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 1)
        << "b2 must read its OWN fork value, not b1's diff-engine change -- the bit is a coarse "
           "any-branch hint, and a bit-set-but-FindDiffEdge-miss must fall back to fork-state, never "
           "leak across branches";
  }
}

// D1 restart-correctness case (7): BranchContext::MarkMainEdgeBranched re-seeds main's own
// Edge::branched() bit during BuildFromFork's change-log replay (branch_engine.cpp) -- without it, a
// freshly-rebuilt diff engine (post re-checkout, or equivalently post-restart) would start with the
// bit CLEARED again on main's Edge object (the diff engine that originally set it is gone), so the
// fast path would incorrectly treat a genuinely-changed edge as untouched and read stale fork-state.
// Mirrors BranchChangeFilterFinePropertySurvivesRecheckout's vertex-level coverage above.
TEST_F(VersioningInterpreterTest, BranchEdgeBitFastPathSurvivesRecheckoutReplay) {
  SatisfyGate();

  faker.Interpret("CREATE (s:S)-[:E1 {w:1}]->(:A), (s)-[:E2 {w:2}]->(:B)");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  faker.Interpret("MATCH (:S)-[e:E1]->(:A) SET e.w = 99");

  // In-session (no replay yet): the change is already visible, the sibling edge stays fork-correct.
  {
    auto stream = faker.Interpret("MATCH (:S)-[e:E1]->(:A) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 99);
  }
  {
    auto stream = faker.Interpret("MATCH (:S)-[e:E2]->(:B) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2);
  }

  // Force change-log replay: tear down and rebuild the BranchContext via BuildFromFork by checking
  // out away from 'b' and back onto it -- the diff engine (and with it, whatever it once set) is
  // gone; only the re-seed during replay can restore correct resolution.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");

  {
    auto stream = faker.Interpret("MATCH (:S)-[e:E1]->(:A) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 99)
        << "post-recheckout, MarkMainEdgeBranched must have re-seeded main's Edge::branched() bit so "
           "the changed edge resolves via FindDiffEdge again -- a missing re-seed would incorrectly "
           "take the fast path and read stale fork-state (1)";
  }
  {
    auto stream = faker.Interpret("MATCH (:S)-[e:E2]->(:B) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2)
        << "the untouched sibling edge must still read fork-state correctly after replay";
  }

  // R35: main is untouched throughout.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    auto stream = faker.Interpret("MATCH (:S)-[e:E1]->(:A) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 1) << "main must be untouched by the branch's SET";
  }
}

// D1 correctness case (8): the branched() bit lives on the EDGE object, not the vertex -- a
// self-loop (from==to) and two parallel edges (same endpoints, distinct gid) must each resolve
// independently. The parallel-edge half is the sharper case: P1 and P2 share BOTH endpoints, so
// SETting P1 vertex-COWs (branches) both S2 and T2 -- if the fast path incorrectly kept per-VERTEX
// rather than per-EDGE state, P2 (untouched, but hanging off two now-"branched" vertices) would
// wrongly resolve too.
TEST_F(VersioningInterpreterTest, BranchEdgeBitFastPathSelfLoopAndParallelEdgesPerEdgeObject) {
  SatisfyGate();

  faker.Interpret("CREATE (s:S)-[:SELF {w:1}]->(s)");
  faker.Interpret("CREATE (:S2)-[:P1 {w:10}]->(:T2), (:S2)-[:P2 {w:20}]->(:T2)");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("MATCH (:S)-[e:SELF]->(:S) SET e.w = 999");
  faker.Interpret("MATCH (:S2)-[e:P1]->(:T2) SET e.w = 111");

  {
    auto stream = faker.Interpret("MATCH (:S)-[e:SELF]->(:S) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 999) << "the changed self-loop edge must reflect the new value";
  }
  {
    auto stream = faker.Interpret("MATCH (:S2)-[e:P1]->(:T2) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 111) << "the changed parallel edge P1 must reflect the new value";
  }
  {
    auto stream = faker.Interpret("MATCH (:S2)-[e:P2]->(:T2) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 20)
        << "the untouched parallel edge P2 must read fork-state -- the bit is per-edge-object, not "
           "per-vertex, even though P2 shares BOTH endpoints with the now-branched P1";
  }

  // R35: main is untouched.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    auto stream = faker.Interpret("MATCH (:S)-[e:SELF]->(:S) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 1);
  }
  {
    auto stream = faker.Interpret("MATCH (:S2)-[e:P1]->(:T2) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 10);
  }
}

// D1 delete-safety hardening (case 5, NEW-view half): a branch-deleted edge went through CowEdge
// (which unconditionally sets main's Edge::branched() bit, branch_engine.cpp) BEFORE the delete, so
// it must never take the `!MaybeBranchedHint()` fast path -- it must still fall through to
// FindDiffEdge (a miss, since the diff copy is itself deleted-and-thus-invisible at View::NEW) and
// then the existing S2 tombstone guard, exactly as BranchDeleteEdgeThenReadPropertyOnNewViewRaises
// above already covers for a never-touched fork edge. This variant additionally SETs the edge first
// (so it is already diff-resident, exercising the diff-resident short-circuit's OWN delete-safety --
// step (0) must not skip the delete either) before deleting it in a LATER statement.
TEST_F(VersioningInterpreterTest, BranchEdgeBitFastPathDeleteAfterModifyStillRaisesOnNewView) {
  SatisfyGate();

  faker.Interpret("CREATE (:S)-[:E {w:1}]->(:A)");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("MATCH (:S)-[e:E]->(:A) SET e.w = 42");
  ASSERT_THROW(faker.Interpret("MATCH (:S)-[e:E]->(:A) DELETE e RETURN e.w AS w"),
               memgraph::query::QueryRuntimeException)
      << "reading a property of an already-modified-then-deleted edge in the SAME query must still "
         "raise DELETED_OBJECT, exactly as for a never-touched fork edge -- D1's diff-resident "
         "short-circuit and branched()-bit gate must both defer to the existing tombstone guard";

  {
    auto stream = faker.Interpret("MATCH (:S)-[e:E]->(:A) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U)
        << "the DELETE statement threw (DELETED_OBJECT), so its autocommit transaction aborted and "
           "rolled back -- the edge survives, matching main's ACID semantics (H1: a pending tombstone "
           "is discarded on abort, not left hiding live data)";
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 42)
        << "the edge retains the last committed value from 'SET e.w = 42'; the aborted DELETE never took effect";
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

// Graph Versioning v1, Fix #2 (durable-capture, HIGH -- data loss): `USING PERIODIC COMMIT` /
// `CALL {} IN TRANSACTIONS` flush the diff-engine transaction mid-query via
// `query::DbAccessor::PeriodicCommit` (db_accessor.hpp), a DIFFERENT code path than
// `Interpreter::Commit()`'s own end-of-query commit boundary. Before this fix, `PeriodicCommit`
// forwarded straight to the storage layer with no branch-capture call at all, so every
// periodically-committed batch on a branch was flushed from the diff engine but never written to
// the BranchLog -- silently lost on re-checkout/MERGE/restart (in-session reads still looked fine,
// masking it). This test drives 100 rows through a periodic-commit-every-10 boundary (10 separate
// `PeriodicCommit` calls, each of which must now capture via the shared
// `BranchContext::CaptureCommitIfBranch` helper -- see db_accessor.hpp's `PeriodicCommit` and
// interpreter.cpp's `Interpreter::Commit`), then forces a re-checkout (which rebuilds
// `BranchContext` from the persisted changelog via `ReplayChangelogIntoDiffEngine`) and a MERGE --
// WITHOUT the fix, only whatever happened to still be in the live diff engine before this test's
// re-checkout would survive (at best the batches present at the moment of re-checkout; in the
// pre-fix code, NONE of them, since nothing was ever captured).
TEST_F(VersioningInterpreterTest, BranchPeriodicCommitCapturedAndSurvivesRecheckoutAndMerge) {
  SatisfyGate();

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("USING PERIODIC COMMIT 10 UNWIND range(1, 100) AS x CREATE (:Batch {id: x})");

  {
    auto s = faker.Interpret("MATCH (n:Batch) RETURN count(n)");
    ASSERT_EQ(s.GetResults().size(), 1U);
    EXPECT_EQ(s.GetResults()[0][0].ValueInt(), 100) << "all periodically-committed rows must be visible in-session";
  }

  // Re-checkout the SAME branch: BranchContext is torn down and rebuilt from the branch's
  // persisted changelog (BuildFromFork -> ReplayChangelogIntoDiffEngine). If periodic commits were
  // never captured to the BranchLog, this is exactly where the data would be lost.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  {
    auto s = faker.Interpret("MATCH (n:Batch) RETURN count(n)");
    ASSERT_EQ(s.GetResults().size(), 1U);
    EXPECT_EQ(s.GetResults()[0][0].ValueInt(), 100)
        << "periodic-commit batches must survive a re-checkout replay -- Fix #2 (durable-capture at "
           "the periodic-commit boundary)";
  }

  // MERGE onto main must also bring in every periodically-committed row.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("MERGE BRANCH 'b'");
  {
    auto s = faker.Interpret("MATCH (n:Batch) RETURN count(n)");
    ASSERT_EQ(s.GetResults().size(), 1U);
    EXPECT_EQ(s.GetResults()[0][0].ValueInt(), 100) << "MERGE must bring in every periodic-commit batch";
  }
}

// Graph Versioning v1, Fix #2 follow-up: `CALL {} IN TRANSACTIONS` is a SEPARATE cursor
// (PeriodicSubquery, operator.cpp) from `USING PERIODIC COMMIT`'s (PeriodicCommitCursor), but both
// ultimately call the SAME `query::DbAccessor::PeriodicCommit` -- confirm the capture fix covers
// this call site too, not just the USING-PERIODIC-COMMIT one exercised above.
TEST_F(VersioningInterpreterTest, BranchCallInTransactionsCapturedAndSurvivesRecheckout) {
  SatisfyGate();

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("UNWIND range(1, 20) AS x CALL { WITH x CREATE (:Batch2 {id: x}) } IN TRANSACTIONS OF 5 ROWS");

  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  auto s = faker.Interpret("MATCH (n:Batch2) RETURN count(n)");
  ASSERT_EQ(s.GetResults().size(), 1U);
  EXPECT_EQ(s.GetResults()[0][0].ValueInt(), 20)
      << "CALL {} IN TRANSACTIONS batches must survive a re-checkout replay too (same PeriodicCommit "
         "call site as USING PERIODIC COMMIT)";
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

// Graph Versioning v1, slice E-4, increment 5 (DETACH DELETE cascade, SUPERSEDES
// BranchDetachDeleteThrowsNotYetImplemented): a FORK vertex's incident edge is now pre-COW'd before
// the native diff-engine cascade runs, so the fork-resident edge is no longer invisible to it (the
// old NotYetImplemented reject is gone). Both the vertex and its fork-resident :R edge must be hidden
// on the branch immediately, untouched on main until MERGE, and BOTH gone from main afterward.
// Crucially, the edge's OTHER endpoint (:S) is only the FROM side, not detach-deleted itself, and
// must survive the merge -- before the pre-COW fix, the cascade had no way to reach a fork edge at
// all, so this whole scenario would have thrown NotYetImplemented instead of cascading.
TEST_F(VersioningInterpreterTest, BranchDetachDeleteForkVertexWithForkEdgesCascadesAndMergesRemoved) {
  SatisfyGate();

  faker.Interpret("CREATE (:S)-[:R]->(:A)");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("MATCH (n:A) DETACH DELETE n");

  {
    auto stream = faker.Interpret("MATCH (n:A) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "the detach-deleted vertex must be hidden on the branch immediately";
  }
  {
    auto stream = faker.Interpret("MATCH ()-[e:R]->() RETURN e");
    EXPECT_EQ(stream.GetResults().size(), 0U)
        << "the vertex's fork-resident incident edge must be cascaded-hidden on the branch too";
  }

  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    auto stream = faker.Interpret("MATCH (n:A) RETURN n");
    ASSERT_EQ(stream.GetResults().size(), 1U) << "main must still have :A -- not yet merged";
  }
  {
    auto stream = faker.Interpret("MATCH ()-[e:R]->() RETURN e");
    ASSERT_EQ(stream.GetResults().size(), 1U) << "main must still have its :R edge -- not yet merged";
  }

  faker.Interpret("MERGE BRANCH 'b'");

  {
    auto stream = faker.Interpret("MATCH (n:A) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "the branch's cascade must have removed :A from main too";
  }
  {
    auto stream = faker.Interpret("MATCH ()-[e:R]->() RETURN e");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "the branch's cascade must have removed the :R edge from main too";
  }
  {
    auto stream = faker.Interpret("MATCH (n:S) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 1U)
        << ":S is only the edge's OTHER endpoint, not itself detach-deleted -- it must survive the cascade";
  }
}

// Graph Versioning v1, slice E-4, increment 5 (DETACH DELETE cascade, branch-native variant): both
// the vertex AND its incident edge are created and detach-deleted ENTIRELY within the branch, never
// having existed on main at all. Exercises the pure diff-engine-only cascade path (no fork/historical
// data involved), and confirms MERGE correctly no-ops the create+delete pair rather than leaving a
// dangling half-applied create or edge on main.
TEST_F(VersioningInterpreterTest, BranchDetachDeleteBranchNativeVertexWithEdgesCascades) {
  SatisfyGate();

  faker.Interpret("CREATE (:S)");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("MATCH (s:S) CREATE (s)-[:R]->(:N {tag:1})");
  faker.Interpret("MATCH (n:N) DETACH DELETE n");

  {
    auto stream = faker.Interpret("MATCH (n:N) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "the branch-native detach-deleted vertex must be hidden";
  }
  {
    auto stream = faker.Interpret("MATCH ()-[e:R]->() RETURN e");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "its branch-native incident edge must be cascaded-hidden too";
  }

  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("MERGE BRANCH 'b'");

  {
    auto stream = faker.Interpret("MATCH (n:N) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "the create+delete pair must cancel out -- no :N on main";
  }
  {
    auto stream = faker.Interpret("MATCH ()-[e:R]->() RETURN e");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "the create+delete pair must cancel out -- no :R edge on main";
  }
  {
    auto stream = faker.Interpret("MATCH (n:S) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 1U) << ":S was never touched by the cascade and must survive";
  }
}

// Graph Versioning v1, slice E-4, increment 5 (DETACH DELETE cascade, shared-edge variant): BOTH
// endpoints of the same edge are detach-deleted in a SINGLE statement. The edge must be pre-COW'd and
// removed exactly once -- if the pre-COW path naively re-processed the edge from each endpoint's own
// incident-edge walk without idempotency, this would double-delete (an assertion failure / crash in
// the diff engine) rather than cleanly no-op the second time.
TEST_F(VersioningInterpreterTest, BranchDetachDeleteSharedEdgeBetweenTwoDeletedNodesDeletesEdgeOnce) {
  SatisfyGate();

  faker.Interpret("CREATE (:A)-[:R]->(:B)");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("MATCH (a:A), (b:B) DETACH DELETE a, b");

  {
    auto stream = faker.Interpret("MATCH (n) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "both detach-deleted endpoints must be hidden on the branch";
  }
  {
    auto stream = faker.Interpret("MATCH ()-[e]->() RETURN e");
    EXPECT_EQ(stream.GetResults().size(), 0U)
        << "the shared edge must be hidden exactly once, not double-deleted (no crash, no leftover)";
  }

  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("MERGE BRANCH 'b'");

  {
    auto stream = faker.Interpret("MATCH (n) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "both vertices must be gone from main after merge";
  }
  {
    auto stream = faker.Interpret("MATCH ()-[e]->() RETURN e");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "the shared edge must be gone from main after merge";
  }
}

// Graph Versioning v1, slice E-4, increment 5 (DETACH DELETE cascade, self-loop variant): a
// vertex with a SELF-loop edge -- the same edge is simultaneously A's OUT-edge and A's IN-edge, so
// the pre-COW-before-cascade logic will walk it from BOTH sides of A's own adjacency. Exercises the
// idempotent double-COW path directly (unlike the two-distinct-vertices shared-edge test above, here
// it's the SAME vertex's own adjacency listing the same edge twice) -- would double-COW / double-
// delete-assert without idempotency.
TEST_F(VersioningInterpreterTest, BranchDetachDeleteSelfLoopCascades) {
  SatisfyGate();

  faker.Interpret("CREATE (:A)");
  faker.Interpret("MATCH (a:A) CREATE (a)-[:R]->(a)");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("MATCH (n:A) DETACH DELETE n");

  {
    auto stream = faker.Interpret("MATCH (n:A) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "the self-looped vertex must be hidden on the branch";
  }
  {
    auto stream = faker.Interpret("MATCH ()-[e]->() RETURN e");
    EXPECT_EQ(stream.GetResults().size(), 0U)
        << "the self-loop edge must be hidden exactly once, not double-deleted (no crash)";
  }

  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("MERGE BRANCH 'b'");

  {
    auto stream = faker.Interpret("MATCH (n:A) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "main must have no :A left after merge";
  }
  {
    auto stream = faker.Interpret("MATCH ()-[e]->() RETURN e");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "main must have no edges left after merge";
  }
}

// Graph Versioning v1, slice E-4, increment 4 (Q6 edge classify, SET vs REMOVE): a fork edge
// COW'd by either a property SET or REMOVE still produces a WalEdgeCreate echo that is
// indistinguishable from a branch-local create by the record alone. The Q6 classify fix
// (FindHistoricalEdgeByEndpoint, merge.cpp) resolves it as fork-existing so the subsequent
// WalEdgeSetProperty / WalEdgeSetProperty(null) is applied to main's REAL edge rather than a
// misclassified duplicate. REMOVE is SetProperty(key, null) internally -- same classify path as SET.
// MERGE drops each branch (spec §4.2); graph data is cleared between iterations to ensure a
// single :S-[:R]->:A edge is present for each case's assertions.
// (Collapses BranchModifyForkEdgePropertyPropagatesToMainOnMerge +
// BranchRemoveForkEdgePropertyPropagatesToMainOnMerge.)
TEST_F(VersioningInterpreterTest, BranchForkEdgePropertyMutationPropagatesToMainOnMerge) {
  SatisfyGate();

  struct Case {
    const char *name;
    const char *branch_mutation;
    bool expect_null;
    int64_t expected_int;  // valid when !expect_null
  };

  static const Case cases[] = {
      // Original: BranchModifyForkEdgePropertyPropagatesToMainOnMerge
      {"SET", "MATCH (:S)-[e:R]->(:A) SET e.w = 2", false, 2},
      // Original: BranchRemoveForkEdgePropertyPropagatesToMainOnMerge
      {"REMOVE", "MATCH (:S)-[e:R]->(:A) REMOVE e.w", true, 0},
  };

  int iter = 0;
  for (const auto &c : cases) {
    SCOPED_TRACE(c.name);
    if (iter++ > 0) {
      // Previous MERGE left us on main with the merged edge; clear before the next case.
      faker.Interpret("MATCH (n) DETACH DELETE n");
    }

    faker.Interpret("CREATE (:S)-[:R {w:1}]->(:A)");
    faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
    faker.Interpret("CHECKOUT BRANCH 'b'");

    faker.Interpret(c.branch_mutation);

    faker.Interpret("CHECKOUT BRANCH 'main'");
    faker.Interpret("MERGE BRANCH 'b'");

    auto stream = faker.Interpret("MATCH (:S)-[e:R]->(:A) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U)
        << "must be exactly ONE :S-[:R]->:A edge -- the pre-fix classify bug would have created a duplicate";
    if (c.expect_null) {
      EXPECT_EQ(stream.GetResults()[0][0].type(), memgraph::communication::bolt::Value::Type::Null)
          << "main's REAL edge must show the branch's REMOVE, not w=1 on an untouched original";
    } else {
      EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), c.expected_int)
          << "main's REAL edge must show the branch's SET, not w=1 on an untouched original";
    }
  }
}

// Graph Versioning v1, chunk 9b (R17): locks in the BRANCHES-EXIST guard on STORAGE MODE, RECOVER
// SNAPSHOT, and DROP GRAPH -- these are DB-global, destructive/wholesale-replacing operations that
// would strand every branch's fork-point base (the historical_ storage a branch's diff engine reads
// through) if allowed to run. Chunk 8's guard (BranchSchemaDdlRejected, above) only fires while THIS
// session is checked out on a branch; R17 is strictly BROADER -- PrepareStorageModeQuery /
// PrepareDropGraphQuery / PrepareRecoverSnapshotQuery (interpreter.cpp) check
// `db_acc_->get()->version_store()->Empty()` directly, with no dependence on the calling session's
// own current_version_. This test deliberately stays on MAIN throughout (CREATE BRANCH does not
// auto-checkout -- confirmed against CreateShowCheckoutDropHappyPath above, where SHOW BRANCH still
// reports "main" immediately after CREATE BRANCH and only the later, explicit CHECKOUT BRANCH moves
// the session) to prove the rejection is driven by branch 'b' merely EXISTING, not by this session
// being on it.
//
// RECOVER SNAPSHOT is deliberately NOT exercised here (needs a real snapshot path/file on disk, same
// caveat as BranchSchemaDdlRejected's own comment) -- it shares the identical
// `version_store()->Empty()` guard/exception path as the two ops asserted below, so no separate
// coverage gap is introduced.
TEST_F(VersioningInterpreterTest, BranchExistenceRejectsStorageModeAndDropGraph) {
  SatisfyGate();

  faker.Interpret("CREATE (:A {x:1})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  // Deliberately DO NOT checkout 'b' -- CREATE BRANCH does not auto-checkout (verified above), so
  // the session is already on main here. The branch merely EXISTS.
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());

  // R17: rejected because branch 'b' EXISTS, even though this session is on main.
  ASSERT_THROW(faker.Interpret("STORAGE MODE IN_MEMORY_ANALYTICAL"), memgraph::query::QueryRuntimeException);
  ASSERT_THROW(faker.Interpret("DROP GRAPH"), memgraph::query::QueryRuntimeException);

  // Session still usable, and main's data untouched, after both rejections.
  {
    auto s = faker.Interpret("MATCH (n:A) RETURN n.x");
    ASSERT_EQ(s.GetResults().size(), 1U);
    EXPECT_EQ(s.GetResults()[0][0].ValueInt(), 1);
  }

  // Once the last branch is gone, the guard clears -- same op, now allowed (branches-exist-scoped,
  // not a permanent lockout).
  faker.Interpret("DROP BRANCH 'b'");
  faker.Interpret("STORAGE MODE IN_MEMORY_ANALYTICAL");  // must NOT throw now that no branch exists
}

// Graph Versioning v1, chunk 10 (D5/R13): `FLAGS_versioning_max_changelog_length` bounds a branch's
// cumulative captured change-log record count. `Interpreter::Commit` (interpreter.cpp, the
// "RETENTION-CAP fix" comment block right above the `FLAGS_versioning_max_changelog_length` check)
// computes the prospective total (`bctx->ChangelogLength() + records`) BEFORE ever calling
// `commit_log->Finalize()` / `AddCapturedRecords`, and throws `QueryRuntimeException` if it would
// exceed the cap -- strictly before `PrepareForCommitPhase`, so the diff-engine transaction is never
// prepared/committed. That throw propagates out of `Interpreter::Pull`'s autocommit path (the
// `!in_explicit_transaction_` branch calls `Commit()` inside the same try/catch that runs
// `AbortCommand` on any `utils::BasicException` before rethrowing -- see interpreter.cpp's
// `Pull<TStream>` body), so the rejected write must be invisible on the branch afterward and the
// session must stay usable for further queries. Only live capture is gated here (this test never
// exercises re-checkout replay).
TEST_F(VersioningInterpreterTest, BranchChangelogRetentionCapRejectsOversizeCommit) {
  SatisfyGate();

  // Save/restore the gflag so this test cannot leak its tiny cap into sibling tests, even if an
  // assertion below fails and returns early out of the test body.
  const auto saved_max_changelog_length = FLAGS_versioning_max_changelog_length;
  memgraph::utils::OnScopeExit const restore_flag(
      [saved_max_changelog_length] { FLAGS_versioning_max_changelog_length = saved_max_changelog_length; });

  faker.Interpret("CREATE (:Seed)");  // on main, pre-fork -- must be completely unaffected by the branch cap.
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // Cap of 1: the branch's changelog starts at 0, so even a single-vertex CREATE's captured record
  // count (a handful of records) already pushes the prospective total over the cap -- rejected on
  // the very first branch write, regardless of the exact per-commit record count.
  FLAGS_versioning_max_changelog_length = 1;
  ASSERT_THROW(faker.Interpret("CREATE (:OnBranch {x:1})"), memgraph::query::QueryRuntimeException);

  // Rolled back cleanly: the rejected vertex must NOT be visible on the branch, and the session
  // (including main's untouched data) stays usable.
  {
    auto s = faker.Interpret("MATCH (n:OnBranch) RETURN n");
    EXPECT_EQ(s.GetResults().size(), 0U) << "rejected commit must not leave a visible write on the branch";
  }
  {
    auto s = faker.Interpret("MATCH (n:Seed) RETURN n");
    EXPECT_EQ(s.GetResults().size(), 1U) << "main's pre-fork data must be unaffected by the branch's cap rejection";
  }

  // Positive control: raise the cap back up and show the identical write now succeeds.
  FLAGS_versioning_max_changelog_length = saved_max_changelog_length;
  faker.Interpret("CREATE (:OnBranch {x:1})");  // must NOT throw now.
  {
    auto s = faker.Interpret("MATCH (n:OnBranch) RETURN n.x");
    ASSERT_EQ(s.GetResults().size(), 1U);
    EXPECT_EQ(s.GetResults()[0][0].ValueInt(), 1);
  }
}

// Graph Versioning v1 (USING VERSION per-query override, Step 1): `USING VERSION 'main'` is a
// per-query prefix directive (interpreter.cpp's Prepare, force_main_this_query /
// force_main_override plumbed into CurrentDB::SetupDatabaseTransaction) that routes THAT ONE query
// to main even while the session is genuinely checked out on a branch -- and leaves the session's
// own checkout (current_version_/branch_context_) completely untouched afterward.
//
// The branch was forked from main AFTER main's MainOnly write, so a plain (session-routed) read on
// the branch sees the union of both: MainOnly (inherited from the fork-era historical base) plus
// BranchOnly (branch-native). `USING VERSION 'main'` must instead resolve straight against main's
// own real storage -- seeing MainOnly only, NOT the branch-native BranchOnly -- and must not move
// the session off the branch for the query that follows.
TEST_F(VersioningInterpreterTest, UsingVersionMainReadsMainWhileOnBranch) {
  SatisfyGate();

  faker.Interpret("CREATE (:MainOnly {v:1})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  faker.Interpret("CREATE (:BranchOnly {v:2})");  // branch-native

  // Plain read: session-routed to branch 'b' -- sees the union (MainOnly inherited + BranchOnly).
  {
    auto stream = faker.Interpret("MATCH (n) RETURN count(n) AS c");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2);
  }

  // USING VERSION 'main': routes THIS query to main's own real storage -- MainOnly only, the
  // branch-native BranchOnly must not be visible.
  {
    auto stream = faker.Interpret("USING VERSION 'main' MATCH (n) RETURN count(n) AS c");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 1);
  }

  // The override was per-query only -- the session is still checked out on 'b', and an unqualified
  // read goes right back to seeing both.
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  {
    auto stream = faker.Interpret("MATCH (n) RETURN count(n) AS c");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2);
  }
}

// Write-side counterpart: `USING VERSION 'main' CREATE ...` while checked out on a branch commits
// straight to main's real storage, not the branch's diff engine -- R35-style isolation, just
// deliberately aimed at main by an explicit per-query directive instead of a plain session-routed
// write.
TEST_F(VersioningInterpreterTest, UsingVersionMainWriteWhileOnBranchGoesToMain) {
  SatisfyGate();

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");

  faker.Interpret("USING VERSION 'main' CREATE (:WroteToMain {v:9})");

  // Not visible on the branch (session-routed read stays on 'b').
  {
    auto stream = faker.Interpret("MATCH (n:WroteToMain) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "a USING VERSION 'main' write must not land on the branch";
  }

  // Visible on main.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    auto stream = faker.Interpret("MATCH (n:WroteToMain) RETURN n.v");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 9);
  }
}

// Graph Versioning v1, chunk 7d (D10) interaction, updated for the current-version model: after an
// explicit CHECKOUT BRANCH 'main' the session is un-engaged and writable again (see
// EngagedConnectionCannotSilentlyWriteMain above), so `USING VERSION 'main'` is no longer needed as
// an escape hatch in this scenario -- it becomes a harmless, redundant no-op that must keep working
// (naming a target explicitly must never behave differently from, or break, plain routing once both
// resolve to the same place). This is a regression guard on the `!has_explicit_version` plumbing
// itself (interpreter.cpp's rail exemption): the fix to SetCurrentVersion must not have broken
// USING VERSION's resolution path for the 'main' target while genuinely on main. The still-real
// escape-hatch scenario -- USING VERSION 'main' writing straight to main while the session is
// actually checked out on a branch -- is covered separately by
// UsingVersionMainWriteWhileOnBranchIsNotCapturedIntoBranchLog below.
TEST_F(VersioningInterpreterTest, UsingVersionMainIsEscapeHatchOnEngagedMain) {
  SatisfyGate();

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  faker.Interpret("CHECKOUT BRANCH 'main'");  // un-engages; back on main, writable again
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  EXPECT_FALSE(faker.interpreter.current_db_.VersioningEngaged());

  // Plain write: succeeds directly (no rail to bypass anymore).
  faker.Interpret("CREATE (:PlainWrite {v:6})");

  // USING VERSION 'main': still allowed, still commits to main -- redundant but not broken.
  faker.Interpret("USING VERSION 'main' CREATE (:Allowed {v:7})");
  {
    auto stream = faker.Interpret("MATCH (n:Allowed) RETURN n.v");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 7);
  }
  {
    auto stream = faker.Interpret("MATCH (n:PlainWrite) RETURN n.v");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 6);
  }
}

// `USING VERSION '<the session's own currently-checked-out branch>'` is a no-op: it names exactly
// where ordinary session routing already sends the query, so the write lands on the branch (not
// main) and stays isolated from main exactly as an unqualified branch write would.
TEST_F(VersioningInterpreterTest, UsingVersionSessionBranchIsNoOp) {
  SatisfyGate();

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("USING VERSION 'b' CREATE (:OnB {v:5})");
  {
    auto stream = faker.Interpret("MATCH (n:OnB) RETURN n.v");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 5);
  }

  // Isolation: not on main.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    auto stream = faker.Interpret("MATCH (n:OnB) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "USING VERSION naming the session's own branch must not leak to main";
  }
}

// Step 2 boundary: naming any branch OTHER than 'main' or the session's own currently-checked-out
// branch is not yet supported and must throw utils::NotYetImplemented (interpreter.cpp's Prepare,
// the `else` arm of the USING VERSION resolution -- same exception type as the pre-existing
// ShowBranchDiffValidatesThenFlagsNotYetImplemented precedent above, not query::NotYetImplemented).
// The throwing query must not disturb the session, which stays perfectly usable on its own branch
// afterward.
TEST_F(VersioningInterpreterTest, UsingVersionOtherBranchNotYetImplemented) {
  SatisfyGate();

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CREATE BRANCH 'c' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  ASSERT_THROW(faker.Interpret("USING VERSION 'c' MATCH (n) RETURN n"), memgraph::utils::NotYetImplemented);

  // Session still usable on 'b' afterward.
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  faker.Interpret("MATCH (n) RETURN n");
}

// Regression lock for the force-main spurious-branch-capture bug: `Interpreter::Commit`'s
// branch-capture block used to guard only on `current_db_.branch_context() != nullptr`, which is
// SESSION state -- still non-null while checked out on 'b' even for a `USING VERSION 'main'` query
// that routes THIS transaction at main's own storage (force_main_override). That made the block
// fire for a force-main write too, capturing MAIN's own deltas into branch 'b's BranchLog; replaying
// that log on MERGE BRANCH would then duplicate the force-main write onto main a second time. The
// fix keys the guard off `bctx->current_diff_txn() != nullptr` instead -- set only when this
// transaction's own SetupDatabaseTransaction actually ran against the branch's diff engine, which a
// force-main query never does. This test creates the fork vertex :M via `USING VERSION 'main'`
// while checked out on 'b', makes a REAL branch write (:BranchNode) in the same session, then merges
// 'b' into main and asserts :M is still exactly one vertex (not duplicated by a spurious replay) and
// :BranchNode was correctly captured/merged exactly once.
TEST_F(VersioningInterpreterTest, UsingVersionMainWriteWhileOnBranchIsNotCapturedIntoBranchLog) {
  SatisfyGate();

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("USING VERSION 'main' CREATE (:M {v:1})");  // force-main write while on branch b
  faker.Interpret("CREATE (:BranchNode {v:2})");              // a REAL branch write (goes to b)

  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    auto stream = faker.Interpret("MATCH (n:M) RETURN n.v");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 1);
  }
  {
    auto stream = faker.Interpret("MATCH (n:BranchNode) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "the branch write must not be visible on main before MERGE";
  }

  faker.Interpret("MERGE BRANCH 'b'");

  {
    auto stream = faker.Interpret("MATCH (n:M) RETURN n.v");
    ASSERT_EQ(stream.GetResults().size(), 1U)
        << "force-main write must not be duplicated by a spurious branch-log capture+replay";
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 1);
  }
  {
    auto stream = faker.Interpret("MATCH (n:BranchNode) RETURN n.v");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2);
  }
}

// Graph Versioning v1 (branch-local plan cache) regression lock: the plan cache used to be forced
// to nullptr for the whole session whenever a branch was checked out (`current_db.branch_context()
// == nullptr` in the old plan-cache selection), specifically because a plan built and cached while
// on MAIN (e.g. a ScanAllByLabel against main's populated label index) must never be reused
// verbatim on a branch, where the exact same operator would scan the diff engine's own
// always-empty index instead -- silently returning 0 rows. The fix routes a checked-out session to
// its own private, per-checkout `CurrentDB::branch_plan_cache()` instead of disabling caching
// outright, so branches get real plan reuse while remaining structurally unable to read/pollute
// main's cache (see the plan-cache selection comment at PrepareCypherQuery, interpreter.cpp). This
// test locks the two things that must both hold for that fix to be correct rather than merely
// "not obviously broken": (1) a query run on the branch must see fork-state data (the very
// 0-rows hazard the old guard was protecting against) on both the first (cold) and second
// (cache-hit) run, and (2) since the cache holds a PLAN, not a materialized result, a later run of
// the identically-cached query must still reflect newly-written branch data, not a frozen
// snapshot from the first run.
TEST_F(VersioningInterpreterTest, BranchQueryPlanCacheServesCorrectResultsAcrossRepeatedRuns) {
  SatisfyGate();

  faker.Interpret("CREATE INDEX ON :User(id)");
  faker.Interpret("CREATE (:User {id: 1, tag: 'main'})");
  faker.Interpret("CREATE (:User {id: 2, tag: 'main'})");
  faker.Interpret("CREATE (:User {id: 3, tag: 'main'})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // First (cold) run: fork-state row must be visible through the branch's diff engine -- this is
  // exactly the hazard the old branch-context guard existed to prevent (a main-shaped plan reused
  // on a branch would instead scan the diff engine's own empty index and return 0 rows here).
  {
    auto stream = faker.Interpret("MATCH (n:User {id: 1}) RETURN n.tag");
    ASSERT_EQ(stream.GetResults().size(), 1U)
        << "fork-state row must be visible on the branch -- must NOT be the old 0-rows hazard";
    EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "main");
  }

  // Second, identical run: now served from the branch-local cache. A cache hit must reproduce the
  // exact same (correct) result, not corrupt it or silently fall back to something stale/wrong.
  {
    auto stream = faker.Interpret("MATCH (n:User {id: 1}) RETURN n.tag");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "main");
  }

  // A fresh branch-native row under the SAME id -- proves the cache holds a reusable PLAN, not a
  // memoized result set: re-running the cached plan must pick up this brand-new branch write.
  faker.Interpret("CREATE (:User {id: 1, tag: 'branch'})");
  {
    auto stream = faker.Interpret("MATCH (n:User {id: 1}) RETURN n.tag");
    ASSERT_EQ(stream.GetResults().size(), 2U)
        << "reused plan must reflect fresh branch data, not a stale cached result";
    const auto &tag0 = stream.GetResults()[0][0].ValueString();
    const auto &tag1 = stream.GetResults()[1][0].ValueString();
    EXPECT_TRUE((tag0 == "main" && tag1 == "branch") || (tag0 == "branch" && tag1 == "main"))
        << "expected the set {main, branch}, got {" << tag0 << ", " << tag1 << "}";
  }

  // Same stripped query text (literal id stripped into a parameter), different id -- the cache is
  // keyed on the stripped query, so this must still resolve against the correct id=2 row, not
  // whatever id=1 last produced.
  {
    auto stream = faker.Interpret("MATCH (n:User {id: 2}) RETURN n.tag");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "main");
  }
}

// Graph Versioning v1 (branch-local plan cache) hardening regression: the branch-local cache
// selection must key off the per-TRANSACTION signal, not session state. `current_db.branch_context()`
// alone stays non-null across a `USING VERSION 'main'` force-main query even though that query
// routes THIS transaction at main's own storage -- so a naive `on_branch` computed from
// branch_context() alone would file the main-shaped plan it builds under the branch's cache slot
// (or, symmetrically, let a branch-shaped plan answer a force-main query from that same
// mis-keyed slot). The fix keys `on_branch` off `bctx->current_diff_txn() != nullptr` instead --
// set only when this transaction's own SetupDatabaseTransaction actually ran against the branch's
// diff engine, which a force-main query never does -- mirroring the identical guard idiom
// `Interpreter::Commit` uses to gate branch-changelog capture (interpreter.cpp:~11938), and its
// sibling regression test above, UsingVersionMainWriteWhileOnBranchIsNotCapturedIntoBranchLog
// (~line 1881), which locks the same guard for commit-time capture rather than plan-cache
// selection.
TEST_F(VersioningInterpreterTest, UsingVersionMainDoesNotPolluteBranchPlanCache) {
  SatisfyGate();

  faker.Interpret("CREATE (:User {id: 1, tag: 'main'})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // Branch-only row: main has no id=100 vertex.
  faker.Interpret("CREATE (:User {id: 100, tag: 'branch'})");

  // Force-main query while checked out on 'b': routes at main (current_diff_txn() stays null for
  // this transaction), so main correctly has no id=100 row. This primes whatever cache slot a
  // buggy `on_branch` computed from branch_context() alone would have used.
  {
    auto stream = faker.Interpret("USING VERSION 'main' MATCH (n:User {id: 100}) RETURN n.tag");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "main has no id=100 row";
  }

  // Ordinary (session-routed) branch query, same stripped text: if the force-main plan above had
  // been cached under the branch's slot, this would wrongly reuse it and return 0 rows instead of
  // the real branch-only vertex.
  {
    auto stream = faker.Interpret("MATCH (n:User {id: 100}) RETURN n.tag");
    ASSERT_EQ(stream.GetResults().size(), 1U)
        << "branch plan cache must not have been polluted by the force-main query above";
    EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "branch");
  }

  // Reverse direction: a second force-main query must not be answered by the branch-cached plan
  // either -- main must still correctly report 0 rows.
  {
    auto stream = faker.Interpret("USING VERSION 'main' MATCH (n:User {id: 100}) RETURN n.tag");
    EXPECT_EQ(stream.GetResults().size(), 0U)
        << "force-main path must not be polluted by the branch-cached plan either";
  }
}

// Bug fix (double-delete-on-a-branch crash): on a checked-out branch, `MATCH (:A)-[r:R]->(:B) DETACH
// DELETE r DELETE r` used to ABORT the process (MG_ASSERT "The edge must be inserted here!",
// storage.cpp) -- two separate Delete operators each call query::DbAccessor::DetachDelete once; the
// first tombstones r, but the second's `r` EdgeAccessor still points at the same gid, and
// BranchContext::CowEdge's idempotency check (a diff-engine miss at View::NEW) cannot tell "already
// deleted" apart from "never COW'd", so it fell through to CreateEdgeEx at a gid the diff engine's
// skiplist still physically occupied. On `main`, re-deleting an already-deleted edge in the same
// query is a no-op (single delete's worth of side effects) -- the branch must match, not crash.
TEST_F(VersioningInterpreterTest, BranchDoubleDeleteAlreadyTombstonedEdgeIsNoOpNotCrash) {
  SatisfyGate();

  faker.Interpret("CREATE (:A)-[:R]->(:B)");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // Two DELETE clauses targeting the SAME named edge `r` -- must not crash, and must behave as a
  // single delete (the edge is gone, endpoints survive). No RETURN clause, so the interesting
  // assertion is simply that this call returns at all (does not abort the process) -- state is
  // checked via the follow-up MATCH queries below, mirroring every other delete test in this file.
  faker.Interpret("MATCH (:A)-[r:R]->(:B) DETACH DELETE r DELETE r");

  {
    auto edges = faker.Interpret("MATCH ()-[e:R]->() RETURN e");
    EXPECT_EQ(edges.GetResults().size(), 0U) << "the double-deleted edge must be gone, exactly as a single delete";
  }
  {
    auto vertices = faker.Interpret("MATCH (n) RETURN n");
    EXPECT_EQ(vertices.GetResults().size(), 2U) << "both endpoints (:A and :B) must survive -- edge-only delete";
  }
}

// Bug fix (double-delete-on-a-branch crash), vertex analogue: `CowVertex` has the identical latent
// idempotency-check gap as `CowEdge` (FindVertex miss at View::NEW cannot distinguish "already
// tombstoned" from "never COW'd") -- `MATCH (n) DETACH DELETE n DELETE n` on a branch used to hit the
// same MG_ASSERT("gid collided") crash via CreateVertexEx. Must be a no-op on the second DELETE, same
// as on `main`.
TEST_F(VersioningInterpreterTest, BranchDoubleDeleteAlreadyTombstonedVertexIsNoOpNotCrash) {
  SatisfyGate();

  faker.Interpret("CREATE (:Iso {tag:1})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // No RETURN clause -- the interesting assertion is simply that this call returns at all (does not
  // abort the process); state is checked via the follow-up MATCH query below.
  faker.Interpret("MATCH (n:Iso) DETACH DELETE n DELETE n");

  auto vertices = faker.Interpret("MATCH (n:Iso) RETURN n");
  EXPECT_EQ(vertices.GetResults().size(), 0U) << "the double-deleted vertex must be gone, exactly as a single delete";
}

// Bug fix S2 (stale read of an entity deleted earlier in the SAME query on a branch): the branch
// read methods (VertexAccessor::GetProperty/Labels/etc.) call BranchContext::ResolveVertex(gid,
// view), which returns nullopt for a tombstoned gid and used to fall through unconditionally to
// `impl_` -- the STALE historical/fork copy, still alive in memory -- silently returning the
// pre-delete value instead of erroring. On `main`, `MATCH (n) DETACH DELETE n RETURN n.prop AS
// prop` raises ("Delete node and return property throws an error", gql_behave delete.feature) --
// by the time the result is serialized, the vertex really has been deleted, so reading it errors.
// The branch must match: the fix gates the tombstone check on `view == storage::View::NEW` (the
// query's current-command state, where the object genuinely is gone), returning
// Error::DELETED_OBJECT instead of falling through to the stale `impl_` value.
TEST_F(VersioningInterpreterTest, BranchDeleteThenReadPropertyOnNewViewRaises) {
  SatisfyGate();

  faker.Interpret("CREATE (:P {prop: 1})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  ASSERT_THROW(faker.Interpret("MATCH (n:P) DETACH DELETE n RETURN n.prop AS prop"),
               memgraph::query::QueryRuntimeException)
      << "reading a property of a vertex deleted earlier in the SAME query must raise, exactly as on main -- "
         "the pre-fix bug silently returned the stale prop=1 instead";
}

// Label-read analogue of BranchDeleteThenReadPropertyOnNewViewRaises above -- exercises
// VertexAccessor::Labels' own view-gated guard (a separate call site from GetProperty's).
TEST_F(VersioningInterpreterTest, BranchDeleteThenReadLabelsOnNewViewRaises) {
  SatisfyGate();

  faker.Interpret("CREATE (:P:Q)");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  ASSERT_THROW(faker.Interpret("MATCH (n:P) DETACH DELETE n RETURN labels(n)"), memgraph::query::QueryRuntimeException)
      << "reading labels of a vertex deleted earlier in the SAME query must raise, exactly as on main";
}

// Edge analogue of BranchDeleteThenReadPropertyOnNewViewRaises -- exercises EdgeAccessor::
// GetProperty's own view-gated guard (FindDiffEdge miss + IsEdgeTombstoned at View::NEW).
TEST_F(VersioningInterpreterTest, BranchDeleteEdgeThenReadPropertyOnNewViewRaises) {
  SatisfyGate();

  faker.Interpret("CREATE (:S)-[:E {w: 1}]->(:A)");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  ASSERT_THROW(faker.Interpret("MATCH (:S)-[e:E]->(:A) DELETE e RETURN e.w AS w"),
               memgraph::query::QueryRuntimeException)
      << "reading a property of an edge deleted earlier in the SAME query must raise, exactly as on main -- "
         "the pre-fix bug silently returned the stale w=1 instead";
}

// Write-path regression guard: SET/REMOVE following a DETACH DELETE in the SAME query (no RETURN)
// must remain a silent no-op -- exactly as on main (gql_behave delete.feature, "the result should
// be empty", NO exception for the no-RETURN form). The write path (CowVertex/CowIfNeeded) was not
// touched by the S2 read-path fix; this pins down that the new view-gated read guards did not
// accidentally change write behavior. Param: the write operation (SET or REMOVE).
// (Collapses BranchDeleteThenSetPropertyIsNoOpNotError + BranchDeleteThenRemovePropertyIsNoOpNotError.)
TEST_F(VersioningInterpreterTest, BranchDeleteThenWritePropertyIsNoOpNotError) {
  SatisfyGate();

  struct Case {
    const char *name;
    const char *delete_and_write;
  };

  static constexpr Case cases[] = {
      // Original: BranchDeleteThenSetPropertyIsNoOpNotError
      {"SET", "MATCH (n:P) DETACH DELETE n SET n.prop = 2"},
      // Original: BranchDeleteThenRemovePropertyIsNoOpNotError
      {"REMOVE", "MATCH (n:P) DETACH DELETE n REMOVE n.prop"},
  };

  int iter = 0;
  for (const auto &c : cases) {
    SCOPED_TRACE(c.name);
    if (iter++ > 0) {
      // SET iteration left us on branch 'b' with :P deleted; return to main and clean up.
      faker.Interpret("CHECKOUT BRANCH 'main'");
      faker.Interpret("MATCH (n) DETACH DELETE n");
      faker.Interpret("DROP BRANCH 'b'");
    }

    faker.Interpret("CREATE (:P {prop: 1})");
    faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
    faker.Interpret("CHECKOUT BRANCH 'b'");

    auto stream = faker.Interpret(c.delete_and_write);
    EXPECT_EQ(stream.GetResults().size(), 0U) << "must be a silent no-op, matching main -- no exception, no rows";

    auto vertices = faker.Interpret("MATCH (n:P) RETURN n");
    EXPECT_EQ(vertices.GetResults().size(), 0U)
        << "the vertex must still be gone -- the " << c.name << " must not have resurrected it";
  }
}

// S2b family: sibling of S2 (commit 7a012aa8b). S2 made branch read methods return
// storage::Error::DELETED_OBJECT when a query reads an entity it deleted earlier in the same
// query, gated on view == storage::View::NEW. That guard also closes DELETE ... SET/REMOVE/SET-LABEL
// ... RETURN queries: the trailing RETURN reads the just-deleted entity at View::NEW and must raise,
// exactly as on main -- unlike the plain write-with-no-RETURN no-op cases above. Parametrized over
// the write operation (SET property / REMOVE property / SET label) following the delete.
// (Collapses BranchDeleteThenSetThenReturnRaises + BranchDeleteThenRemoveThenReturnRaises +
// BranchDeleteThenSetLabelThenReturnRaises.)
TEST_F(VersioningInterpreterTest, BranchDeleteThenWriteThenReturnRaises) {
  SatisfyGate();

  static constexpr const char *queries[] = {
      // Original: BranchDeleteThenSetThenReturnRaises
      "MATCH (n:P) DETACH DELETE n SET n.prop = 2 RETURN n.prop AS prop",
      // Original: BranchDeleteThenRemoveThenReturnRaises
      "MATCH (n:P) DETACH DELETE n REMOVE n.prop RETURN n.prop AS prop",
      // Original: BranchDeleteThenSetLabelThenReturnRaises
      "MATCH (n:P) DETACH DELETE n SET n:X RETURN labels(n)",
  };

  int iter = 0;
  for (const char *query : queries) {
    SCOPED_TRACE(query);
    if (iter++ > 0) {
      // Previous throw aborted its transaction; we are still on branch 'b'. Return to main and
      // prepare a fresh :P for the next case.
      faker.Interpret("CHECKOUT BRANCH 'main'");
      faker.Interpret("MATCH (n) DETACH DELETE n");
      faker.Interpret("DROP BRANCH 'b'");
    }

    faker.Interpret("CREATE (:P {prop: 1})");
    faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
    faker.Interpret("CHECKOUT BRANCH 'b'");

    ASSERT_THROW(faker.Interpret(query), memgraph::query::QueryRuntimeException)
        << "S2b: reading (via RETURN) an entity deleted earlier in the same query must raise, "
           "matching main -- the pre-S2-fix bug returned a stale value";
  }
}

// -----------------------------------------------------------------------------------------------
// Graph Versioning v1, Slice 2 (branch-aware label-property index scan) regression tests.
//
// Part A (branch_engine.cpp, MirrorMainIndexDefinitionsIntoDiffEngine): a single simple-property
// label index main held at fork_ts is mirrored onto the branch's diff engine, so the planner
// selects a real ScanAllByLabelProperties on a checked-out branch instead of falling back to a
// plain ScanAll.
// Part B (db_accessor.hpp, DbAccessor::Vertices(view, label, properties, property_ranges, order)):
// the branch-aware overload merges (a) the diff engine's own mirrored-index matches with (b)
// main@fork's historical index matches (tombstone/COW-gated), then explicitly re-sorts by the
// indexed property -- because the planner, trusting index order, has ELIDED an explicit Sort/
// OrderBy operator for this query shape.
//
// These tests lock the OBSERVABLE union + order behavior end-to-end through the real interpreter
// (CREATE INDEX + ORDER BY exercise the real planner and the real merge) -- they do not reach into
// BranchContext/DbAccessor internals directly.
// -----------------------------------------------------------------------------------------------

// Branch-aware label-property index scan (Slice 2, Part B): ORDER BY ASC vs DESC. Identical
// setup and mutations; parametrized over the ORDER direction and expected age vector.
// The :Person(age) index is created once and reused for the DESC case; Person vertices and
// branch 'b' are cleared between iterations via CHECKOUT + DELETE + DROP so each case
// starts from a clean state.
// (Collapses BranchIndexScanOrderByAscReflectsBranchMutations + BranchIndexScanOrderByDescReflectsBranchMutations.)
TEST_F(VersioningInterpreterTest, BranchIndexScanOrderByReflectsBranchMutations) {
  SatisfyGate();

  struct Case {
    const char *name;
    const char *order_clause;
    std::vector<int64_t> expected_ages;
    const char *order_msg;
  };

  const Case cases[] = {
      {
          "ASC",
          "ORDER BY n.age",
          {10, 25, 30, 50, 99},
          "expected union ASC-ordered by age: A=10, F=25, C=30, E=50, B COW'd to 99 -- D tombstoned; "
          "a gid/creation-ordered result would indicate the merge returns rows in the wrong order",
      },
      {
          "DESC",
          "ORDER BY n.age DESC",
          {99, 50, 30, 25, 10},
          "same union but DESC: IndexOrder::DESC must flip the explicit re-sort comparator too, not "
          "just the ASC one -- a stale ASC-only comparator would produce the ascending order here",
      },
  };

  int iter = 0;
  for (const auto &c : cases) {
    SCOPED_TRACE(c.name);
    if (iter++ > 0) {
      // ASC iteration left us on branch 'b'; clear vertices and branch before DESC case.
      faker.Interpret("CHECKOUT BRANCH 'main'");
      faker.Interpret("DROP BRANCH 'b'");
      faker.Interpret("MATCH (n) DETACH DELETE n");
      // The :Person(age) index persists across iterations -- no need to recreate.
    } else {
      faker.Interpret("CREATE INDEX ON :Person(age)");
    }

    // Creation order deliberately does NOT match age order, so a gid-ordered (unsorted) result
    // would be trivially distinguishable from a property-ordered one.
    faker.Interpret("CREATE (:Person {age: 50, name: 'E'})");
    faker.Interpret("CREATE (:Person {age: 20, name: 'B'})");
    faker.Interpret("CREATE (:Person {age: 40, name: 'D'})");
    faker.Interpret("CREATE (:Person {age: 10, name: 'A'})");
    faker.Interpret("CREATE (:Person {age: 30, name: 'C'})");

    faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
    faker.Interpret("CHECKOUT BRANCH 'b'");

    faker.Interpret("MATCH (n:Person {name: 'B'}) SET n.age = 99");   // COW: moves B out of its old slot
    faker.Interpret("MATCH (n:Person {name: 'D'}) DETACH DELETE n");  // tombstone: D must vanish
    faker.Interpret("CREATE (:Person {age: 25, name: 'F'})");         // branch-native: F is diff-engine-only

    const auto query = std::string("MATCH (n:Person) WHERE n.age > 0 RETURN n.age AS age ") + c.order_clause;
    auto stream = faker.Interpret(query);
    const auto &rows = stream.GetResults();
    std::vector<int64_t> ages;
    ages.reserve(rows.size());
    for (const auto &row : rows) {
      ages.push_back(row[0].ValueInt());
    }
    EXPECT_EQ(ages, c.expected_ages) << c.order_msg;
  }
}

TEST_F(VersioningInterpreterTest, BranchIndexScanEqualKeyDifferentVerticesBothReturned) {
  SatisfyGate();

  faker.Interpret("CREATE INDEX ON :Person(age)");

  faker.Interpret("CREATE (:Person {age: 30, name: 'C'})");
  faker.Interpret("CREATE (:Person {age: 30, name: 'C2'})");
  faker.Interpret("CREATE (:Person {age: 10, name: 'A'})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  auto stream = faker.Interpret("MATCH (n:Person) WHERE n.age = 30 RETURN n.name AS name ORDER BY n.name");
  const auto &rows = stream.GetResults();
  ASSERT_EQ(rows.size(), 2U) << "an equal-key tie between two distinct historical vertices must not drop a row";
  std::vector<std::string> names;
  names.reserve(rows.size());
  for (const auto &row : rows) {
    names.push_back(row[0].ValueString());
  }

  EXPECT_EQ(names, (std::vector<std::string>{"C", "C2"}))
      << "both same-age vertices (C, C2) must survive the merge -- a keying bug that collapses "
         "equal property values onto one slot would silently lose one of them";
}

// Locks the multi-index mirror fix: MirrorMainIndexDefinitionsIntoDiffEngine gives each mirrored
// index its OWN fresh ReadOnlyAccess (branch_engine.cpp's `commit_one` helper) precisely because
// InMemoryStorage::InMemoryAccessor::CreateIndex calls DowngradeToReadIfValid() internally, which
// downgrades the access from READ_ONLY to READ after the FIRST index -- a second CreateIndex
// reusing that same (now-downgraded) access used to trip CreateIndex's own
// `MG_ASSERT(type() == UNIQUE || type() == READ_ONLY)` and abort the process. Two single-property
// label indexes at fork_ts is exactly the case that used to hit that abort.
TEST_F(VersioningInterpreterTest, BranchTwoLabelPropertyIndexesOrExpressionDoesNotAbort) {
  SatisfyGate();

  faker.Interpret("CREATE INDEX ON :Label1(id)");
  faker.Interpret("CREATE INDEX ON :Label2(id)");

  faker.Interpret("CREATE (:Label1 {id: 1})");
  faker.Interpret("CREATE (:Label2 {id: 2})");
  faker.Interpret("CREATE (:Label1:Label2 {id: 1})");

  // This is where the abort used to fire -- BuildFromFork mirrors BOTH indexes while constructing
  // the branch's diff engine.
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  auto stream = faker.Interpret("MATCH (n:Label1|Label2) WHERE n.id < 2 RETURN n");
  EXPECT_EQ(stream.GetResults().size(), 2U) << "must return both id=1 vertices (:Label1 and :Label1:Label2) "
                                               "without throwing/aborting on the two-index mirror";
}

TEST_F(VersioningInterpreterTest, BranchEdgeTypeIndexScanReflectsBranchMutations) {
  SatisfyGate();

  faker.Interpret("CREATE EDGE INDEX ON :KNOWS");

  faker.Interpret("CREATE (a:N {n:'a'}),(b:N {n:'b'}),(c:N {n:'c'})");
  faker.Interpret("MATCH (a:N {n:'a'}),(b:N {n:'b'}) CREATE (a)-[:KNOWS]->(b)");
  faker.Interpret("MATCH (b:N {n:'b'}),(c:N {n:'c'}) CREATE (b)-[:KNOWS]->(c)");
  faker.Interpret("MATCH (a:N {n:'a'}),(c:N {n:'c'}) CREATE (a)-[:KNOWS]->(c)");
  faker.Interpret("MATCH (a:N {n:'a'}),(c:N {n:'c'}) CREATE (a)-[:LIKES]->(c)");  // distractor type

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // Branch mutations: tombstone the fork's a->b KNOWS edge, add a branch-native c->a KNOWS edge.
  faker.Interpret("MATCH (:N {n:'a'})-[r:KNOWS]->(:N {n:'b'}) DELETE r");
  faker.Interpret("MATCH (a:N {n:'c'}),(b:N {n:'a'}) CREATE (a)-[:KNOWS]->(b)");

  // The ORDER BY here is just to make the vector comparison deterministic -- the KNOWS scan itself
  // carries no property order.
  auto stream = faker.Interpret("MATCH (x)-[r:KNOWS]->(y) RETURN x.n + '->' + y.n AS e ORDER BY e");
  const auto &rows = stream.GetResults();
  std::vector<std::string> edges;
  edges.reserve(rows.size());
  for (const auto &row : rows) {
    edges.push_back(row[0].ValueString());
  }

  EXPECT_EQ(edges, (std::vector<std::string>{"a->c", "b->c", "c->a"}))
      << "expected the union of main@fork's historical KNOWS index (a->b, b->c, a->c) with the "
         "branch's own diff-engine KNOWS mutations (a->b tombstoned away, c->a created "
         "branch-native), correctly restricted to the KNOWS type -- a missing/extra edge means the "
         "branch edge-type merge (tombstone skip / diff-union / type filter) is wrong, and the LIKES "
         "distractor edge must never appear.";
}

TEST_F(VersioningInterpreterTest, BranchEdgeTypeIndexSelectedInPlanOnBranch) {
  SatisfyGate();

  faker.Interpret("CREATE EDGE INDEX ON :KNOWS");
  faker.Interpret("CREATE (:N {n:'a'})-[:KNOWS]->(:N {n:'b'})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  auto stream = faker.Interpret("EXPLAIN MATCH ()-[r:KNOWS]->() RETURN r");
  std::string plan;
  for (const auto &row : stream.GetResults()) {
    plan += row[0].ValueString();
    plan += '\n';
  }

  // Locks both that MirrorMainIndexDefinitionsIntoDiffEngine registers the edge-type index on the
  // branch's diff engine AND that its early-return guard accounts for edge_type indexes -- a bug
  // where main had ONLY an edge-type index (no label/label-property index) made the mirror bail out
  // early and left the branch planning ScanAll+Expand instead of the mirrored ScanAllByEdgeType.
  EXPECT_THAT(plan, ::testing::HasSubstr("ScanAllByEdgeType"))
      << "branch plan did not select the mirrored edge-type index; full plan:\n"
      << plan;
}

TEST_F(VersioningInterpreterTest, BranchEdgeTypePropertyIndexScanOrderedReflectsBranchMutations) {
  SatisfyGate();

  faker.Interpret("CREATE EDGE INDEX ON :KNOWS(w)");

  faker.Interpret("CREATE (:N {n:'a'}),(:N {n:'b'}),(:N {n:'c'}),(:N {n:'e'})");
  // Creation order deliberately does NOT match w order, so a gid-ordered (unsorted) result would be
  // trivially distinguishable from a property-ordered one.
  faker.Interpret("MATCH (a:N {n:'a'}),(b:N {n:'b'}) CREATE (a)-[:KNOWS {w: 50}]->(b)");
  faker.Interpret("MATCH (b:N {n:'b'}),(c:N {n:'c'}) CREATE (b)-[:KNOWS {w: 20}]->(c)");
  faker.Interpret("MATCH (a:N {n:'a'}),(c:N {n:'c'}) CREATE (a)-[:KNOWS {w: 40}]->(c)");
  faker.Interpret("MATCH (c:N {n:'c'}),(e:N {n:'e'}) CREATE (c)-[:KNOWS {w: 10}]->(e)");
  faker.Interpret("MATCH (a:N {n:'a'}),(e:N {n:'e'}) CREATE (a)-[:LIKES {w: 99}]->(e)");  // distractor type

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // Branch mutations: COW b->c's property, delete a->c, add a branch-native e->a edge.
  faker.Interpret("MATCH (:N {n:'b'})-[r:KNOWS]->(:N {n:'c'}) SET r.w = 99");
  faker.Interpret("MATCH (:N {n:'a'})-[r:KNOWS]->(:N {n:'c'}) DELETE r");
  faker.Interpret("MATCH (a:N {n:'e'}),(b:N {n:'a'}) CREATE (a)-[:KNOWS {w: 25}]->(b)");

  auto stream = faker.Interpret("MATCH ()-[r:KNOWS]->() WHERE r.w > 0 RETURN r.w AS w ORDER BY r.w");
  const auto &rows = stream.GetResults();
  std::vector<int64_t> ws;
  ws.reserve(rows.size());
  for (const auto &row : rows) {
    ws.push_back(row[0].ValueInt());
  }

  EXPECT_EQ(ws, (std::vector<int64_t>{10, 25, 50, 99}))
      << "expected the union of main@fork's historical KNOWS(w) index (a->b=50, b->c=20, a->c=40, "
         "c->e=10) with the branch's own diff-engine mutations (b->c COW'd 20->99, a->c tombstoned "
         "away, e->a created branch-native at 25), correctly ASC-ordered by w -- fork survivors "
         "{50, 10} plus branch survivors {99, 25} sorted is {10, 25, 50, 99}; a gid/creation-ordered "
         "or unsorted result would indicate the branch edge-type+property merge (sort / tombstone "
         "skip / COW dedup) is wrong, and the LIKES distractor edge (w=99) must never appear. Note "
         "the planner elides the explicit ORDER BY (Sort) when it selects the property index, so "
         "order correctness here rides entirely on the merge itself.";
}

TEST_F(VersioningInterpreterTest, BranchEdgeTypePropertyIndexSelectedInPlanOnBranch) {
  SatisfyGate();

  faker.Interpret("CREATE EDGE INDEX ON :KNOWS(w)");
  faker.Interpret("CREATE (:N {n:'a'})-[:KNOWS {w: 1}]->(:N {n:'b'})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  auto stream = faker.Interpret("EXPLAIN MATCH ()-[r:KNOWS]->() WHERE r.w > 0 RETURN r.w ORDER BY r.w");
  std::string plan;
  for (const auto &row : stream.GetResults()) {
    plan += row[0].ValueString();
    plan += '\n';
  }

  // Locks that the edge-type+property index is mirrored via its own diff-engine registration (a
  // per-index loop over main's index definitions), NOT a fragile up-front emptiness guard -- a prior
  // guard bug made a main holding ONLY an edge-property index bail out before mirroring anything,
  // leaving the branch to full-scan instead of selecting the mirrored index.
  EXPECT_THAT(plan, ::testing::HasSubstr("ScanAllByEdgeTypeProperty"))
      << "branch plan did not select the mirrored edge-type+property index; full plan:\n"
      << plan;
  EXPECT_THAT(plan, ::testing::Not(::testing::HasSubstr("OrderBy")))
      << "branch plan should elide the explicit Sort once the property index scan already produces "
         "ASC order; full plan:\n"
      << plan;
}

TEST_F(VersioningInterpreterTest, BranchGlobalEdgePropertyIndexScanOrderedReflectsBranchMutations) {
  SatisfyGate();

  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");

  faker.Interpret("CREATE (:N {n:'a'}),(:N {n:'b'}),(:N {n:'c'})");
  // Creation order deliberately does NOT match w order, and edge types deliberately differ, so a
  // gid-ordered or type-filtered result would be trivially distinguishable from a correct
  // property-ordered, type-agnostic one.
  faker.Interpret("MATCH (a:N {n:'a'}),(b:N {n:'b'}) CREATE (a)-[:KNOWS {w: 50}]->(b)");
  faker.Interpret("MATCH (b:N {n:'b'}),(c:N {n:'c'}) CREATE (b)-[:LIKES {w: 20}]->(c)");
  faker.Interpret("MATCH (a:N {n:'a'}),(c:N {n:'c'}) CREATE (a)-[:KNOWS {w: 40}]->(c)");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // Branch mutations: COW a cross-type (LIKES) edge, delete a KNOWS edge, add a branch-native edge
  // of a third type (FOLLOWS) to prove the global index has no type filter.
  faker.Interpret("MATCH (:N {n:'b'})-[r:LIKES]->(:N {n:'c'}) SET r.w = 99");
  faker.Interpret("MATCH (:N {n:'a'})-[r:KNOWS]->(:N {n:'c'}) DELETE r");
  faker.Interpret("MATCH (a:N {n:'c'}),(b:N {n:'a'}) CREATE (a)-[:FOLLOWS {w: 25}]->(b)");

  auto stream = faker.Interpret("MATCH ()-[r]->() WHERE r.w > 0 RETURN r.w AS w ORDER BY r.w");
  const auto &rows = stream.GetResults();
  std::vector<int64_t> ws;
  ws.reserve(rows.size());
  for (const auto &row : rows) {
    ws.push_back(row[0].ValueInt());
  }

  EXPECT_EQ(ws, (std::vector<int64_t>{25, 50, 99}))
      << "expected the union of main@fork's historical global edge-property(w) index (a-[:KNOWS]->b=50, "
         "b-[:LIKES]->c=20, a-[:KNOWS]->c=40) with the branch's own diff-engine mutations "
         "(b-[:LIKES]->c COW'd 20->99, a-[:KNOWS]->c tombstoned away, c-[:FOLLOWS]->a created "
         "branch-native at 25), correctly ASC-ordered by w and spanning three distinct edge types -- "
         "fork survivor {50} plus branch survivors {99, 25} sorted is {25, 50, 99}; a gid/creation-"
         "ordered, unsorted, or edge-type-filtered result would indicate the branch GLOBAL "
         "edge-property merge (sort / tombstone skip / COW dedup / no-type-filter) is wrong. Note "
         "the planner elides the explicit ORDER BY (Sort) when it selects the property index, so "
         "order correctness here rides entirely on the merge itself.";
}

TEST_F(VersioningInterpreterTest, BranchGlobalEdgePropertyIndexSelectedInPlanOnBranch) {
  SatisfyGate();

  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret("CREATE (:N {n:'a'})-[:KNOWS {w: 1}]->(:N {n:'b'})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  auto stream = faker.Interpret("EXPLAIN MATCH ()-[r]->() WHERE r.w > 0 RETURN r.w ORDER BY r.w");
  std::string plan;
  for (const auto &row : stream.GetResults()) {
    plan += row[0].ValueString();
    plan += '\n';
  }

  // Locks that the GLOBAL edge-property index is mirrored via its own diff-engine registration, and
  // that the planner selects it for an untyped edge pattern.
  EXPECT_THAT(plan, ::testing::HasSubstr("ScanAllByEdgeProperty"))
      << "branch plan did not select the mirrored global edge-property index; full plan:\n"
      << plan;
  EXPECT_THAT(plan, ::testing::Not(::testing::HasSubstr("OrderBy")))
      << "branch plan should elide the explicit Sort once the property index scan already produces "
         "ASC order; full plan:\n"
      << plan;
}

// -----------------------------------------------------------------------------------------------
// Graph Versioning v1, Slice 3b (branch-aware GLOBAL VERTEX PROPERTY index scan) tests.
//
// Mirrors the global edge-property block above but for vertices: `CREATE GLOBAL INDEX ON :(prop)`
// creates a label-agnostic vertex-property index, exposing the `ScanAllByVertexProperty` operator.
// These tests lock the branch-side observable union + planner selection behaviour end-to-end
// through the real interpreter -- they drive the implementation in db_accessor.hpp
// (Vertices(view, property, lower_bound, ...) branch-aware overload) and branch_engine.cpp
// (MirrorMainIndexDefinitionsIntoDiffEngine, global-vertex-property registration path).
// -----------------------------------------------------------------------------------------------

TEST_F(VersioningInterpreterTest, BranchGlobalVertexPropertyIndexScanOrderedReflectsBranchMutations) {
  SatisfyGate();

  faker.Interpret("CREATE GLOBAL INDEX ON :(w)");

  // Creation order deliberately does NOT match w order, so a gid-ordered (unsorted) result would
  // be trivially distinguishable from a correct property-ordered one.
  faker.Interpret("CREATE (:V {n:'a', w: 50})");
  faker.Interpret("CREATE (:V {n:'b', w: 20})");
  faker.Interpret("CREATE (:V {n:'c', w: 40})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // Branch mutations: COW one fork vertex, tombstone another, create a branch-native vertex.
  faker.Interpret("MATCH (n:V {n:'b'}) SET n.w = 99");     // COW: moves b out of its old w=20 slot
  faker.Interpret("MATCH (n:V {n:'c'}) DETACH DELETE n");  // tombstone: c(w=40) must be absent
  faker.Interpret("CREATE (:V {n:'d', w: 25})");           // branch-native: w=25

  auto stream = faker.Interpret("MATCH (n) WHERE n.w > 0 RETURN n.w AS w ORDER BY n.w");
  const auto &rows = stream.GetResults();
  std::vector<int64_t> ws;
  ws.reserve(rows.size());
  for (const auto &row : rows) {
    ws.push_back(row[0].ValueInt());
  }

  EXPECT_EQ(ws, (std::vector<int64_t>{25, 50, 99}))
      << "expected the union of main@fork's historical global vertex-property(w) index "
         "(a=50, b=20, c=40) with the branch's own diff-engine mutations "
         "(b COW'd 20->99, c tombstoned away, d created branch-native at 25), correctly ASC-ordered "
         "by w and spanning a label-agnostic scan -- fork survivor {50} plus branch survivors {99, 25} "
         "sorted is {25, 50, 99}; a gid/creation-ordered, unsorted, or stale-value result would "
         "indicate the branch GLOBAL vertex-property merge (sort / tombstone skip / COW dedup / "
         "no-label-filter) is wrong. Note the planner elides the explicit ORDER BY (Sort) when it "
         "selects the property index, so order correctness here rides entirely on the merge itself.";
}

TEST_F(VersioningInterpreterTest, BranchVertexPropertyIndexSelectedInPlanOnBranch) {
  SatisfyGate();

  faker.Interpret("CREATE GLOBAL INDEX ON :(w)");
  faker.Interpret("CREATE (:V {w: 1})");
  faker.Interpret("CREATE (:V {w: 2})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  auto stream = faker.Interpret("EXPLAIN MATCH (n) WHERE n.w > 0 RETURN n.w ORDER BY n.w");
  std::string plan;
  for (const auto &row : stream.GetResults()) {
    plan += row[0].ValueString();
    plan += '\n';
  }

  // Locks that MirrorMainIndexDefinitionsIntoDiffEngine registers the global vertex-property index
  // on the branch's diff engine and that the planner selects ScanAllByVertexProperty for an
  // unlabelled vertex pattern, eliding the explicit Sort because the index already yields ASC order.
  EXPECT_THAT(plan, ::testing::HasSubstr("ScanAllByVertexProperty"))
      << "branch plan did not select the mirrored global vertex-property index; full plan:\n"
      << plan;
  EXPECT_THAT(plan, ::testing::Not(::testing::HasSubstr("OrderBy")))
      << "branch plan should elide the explicit Sort once the global vertex-property index scan "
         "already produces ASC order; full plan:\n"
      << plan;
}

TEST_F(VersioningInterpreterTest, BranchLabelOnlyIndexScanReflectsBranchMutations) {
  SatisfyGate();

  faker.Interpret("CREATE INDEX ON :L");

  faker.Interpret("CREATE (:L {n:'a'})");
  faker.Interpret("CREATE (:L {n:'b'})");
  faker.Interpret("CREATE (:L {n:'c'})");
  faker.Interpret("CREATE (:Other {n:'x'})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // Branch mutations: delete a fork L vertex, COW-remove the label off another fork L vertex,
  // COW-add the label onto a fork non-L vertex, and create a branch-native L vertex.
  faker.Interpret("MATCH (n:L {n:'a'}) DETACH DELETE n");
  faker.Interpret("MATCH (n:L {n:'b'}) REMOVE n:L");
  faker.Interpret("MATCH (n:Other {n:'x'}) SET n:L");
  faker.Interpret("CREATE (:L {n:'d'})");

  // The ORDER BY here is just to make the vector comparison deterministic -- the label scan itself
  // carries no property order (label-only indexes have no property order).
  auto stream = faker.Interpret("MATCH (n:L) RETURN n.n AS name ORDER BY n.n");
  const auto &rows = stream.GetResults();
  std::vector<std::string> names;
  names.reserve(rows.size());
  for (const auto &row : rows) {
    names.push_back(row[0].ValueString());
  }

  EXPECT_EQ(names, (std::vector<std::string>{"c", "d", "x"}))
      << "locks the label-only set-union merge (diff-engine label index union main@fork's label "
         "index, skipping tombstoned/COW'd) including the COW label add/remove cases: 'a' was "
         "deleted on the branch and must be gone; 'b' lost the :L label via COW and must be excluded "
         "even though it is still sitting in main@fork's historical label index -- the "
         "COW-membership skip must catch it; 'x' gained the :L label via COW on a previously "
         "non-L fork vertex and must be included via the diff-engine's own label index; 'd' is "
         "branch-native and must be included; 'c' was never touched on the branch and must still "
         "surface from main@fork's index untouched.";
}

TEST_F(VersioningInterpreterTest, BranchLabelOnlyIndexSelectedInPlanOnBranch) {
  SatisfyGate();

  faker.Interpret("CREATE INDEX ON :L");
  faker.Interpret("CREATE (:L {n:'a'})");
  faker.Interpret("CREATE (:L {n:'b'})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  auto stream = faker.Interpret("EXPLAIN MATCH (n:L) RETURN n");
  std::string plan;
  for (const auto &row : stream.GetResults()) {
    plan += row[0].ValueString();
    plan += '\n';
  }

  // Locks that MirrorMainIndexDefinitionsIntoDiffEngine registers the label-only index on the
  // branch's diff engine and that the planner selects the mirrored ScanAllByLabel instead of
  // falling back to a plain ScanAll+Filter. Label-only indexes carry no property order, so there is
  // no accompanying OrderBy-elision assertion here (unlike the label-property index tests above).
  EXPECT_THAT(plan, ::testing::HasSubstr("ScanAllByLabel"))
      << "branch plan did not select the mirrored label-only index; full plan:\n"
      << plan;
}

// Locks the composite (multi single-level property) label-property index mirror + the branch
// read path's LEXICOGRAPHIC tuple-key merge (diff ∪ main@fork, tombstone/COW skip). Unlike the
// single-property index above, the planner keeps an explicit OrderBy for a composite index (the
// Sort is NOT elided), so ordering correctness here rides on that explicit OrderBy -- what this
// test actually locks down is that the composite INDEX SELECTION and the merged SET (with a
// COW-reposition inside a composite key, a delete, and a native create) are correct.
TEST_F(VersioningInterpreterTest, BranchCompositeLabelPropertyIndexReflectsBranchMutations) {
  SatisfyGate();

  faker.Interpret("CREATE INDEX ON :P(a, b)");

  // Creation order deliberately does NOT match (a,b) lexicographic order.
  faker.Interpret("CREATE (:P {n:'v1', a:2, b:1})");
  faker.Interpret("CREATE (:P {n:'v2', a:1, b:9})");
  faker.Interpret("CREATE (:P {n:'v3', a:1, b:2})");
  faker.Interpret("CREATE (:P {n:'v4', a:3, b:0})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("MATCH (n:P {n:'v2'}) SET n.b = 0");      // COW: repositions (1,9) -> (1,0)
  faker.Interpret("MATCH (n:P {n:'v4'}) DETACH DELETE n");  // tombstone: (3,0) must vanish
  faker.Interpret("CREATE (:P {n:'v5', a:2, b:5})");        // branch-native: (2,5)

  auto stream = faker.Interpret("MATCH (n:P) WHERE n.a > 0 RETURN n.a AS a, n.b AS b ORDER BY n.a, n.b");
  const auto &rows = stream.GetResults();
  std::vector<std::pair<int64_t, int64_t>> ab;
  ab.reserve(rows.size());
  for (const auto &row : rows) {
    ab.emplace_back(row[0].ValueInt(), row[1].ValueInt());
  }

  EXPECT_EQ(ab, (std::vector<std::pair<int64_t, int64_t>>{{1, 0}, {1, 2}, {2, 1}, {2, 5}}))
      << "expected the union of main@fork's historical composite index (v1=(2,1), v3=(1,2) -- v4's "
         "(3,0) tombstoned away) with the branch's own diff-engine composite index (v2 COW'd from "
         "(1,9) to (1,0), v5 created at (2,5)), correctly ordered by the explicit (a, b) OrderBy -- "
         "a keying bug that treats 'a' and 'b' as independent single-property slots instead of one "
         "composite tuple key would either collapse the COW'd v2 onto the wrong slot or fail to "
         "distinguish it from v1/v3.";
}

TEST_F(VersioningInterpreterTest, BranchCompositeLabelPropertyIndexSelectedInPlanOnBranch) {
  SatisfyGate();

  faker.Interpret("CREATE INDEX ON :P(a, b)");
  faker.Interpret("CREATE (:P {a:1, b:1})");
  faker.Interpret("CREATE (:P {a:2, b:2})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  auto stream = faker.Interpret("EXPLAIN MATCH (n:P) WHERE n.a > 0 RETURN n ORDER BY n.a, n.b");
  std::string plan;
  for (const auto &row : stream.GetResults()) {
    plan += row[0].ValueString();
    plan += '\n';
  }

  // Locks that MirrorMainIndexDefinitionsIntoDiffEngine registers the composite label-property
  // index on the branch's diff engine and that the planner selects the mirrored
  // ScanAllByLabelProperties instead of falling back to a plain ScanAll+Filter. Unlike the
  // single-property index test above, composite indexes keep the explicit OrderBy (the Sort is
  // NOT elided), so there is no accompanying OrderBy-elision assertion here.
  EXPECT_THAT(plan, ::testing::HasSubstr("ScanAllByLabelProperties"))
      << "branch plan did not select the mirrored composite label-property index; full plan:\n"
      << plan;
}

TEST_F(VersioningInterpreterTest, BranchNestedLabelPropertyIndexReflectsBranchMutations) {
  SatisfyGate();

  faker.Interpret("CREATE INDEX ON :P(a.b)");

  // Creation order deliberately does NOT match b-value order.
  faker.Interpret("CREATE (:P {n:'v1', a:{b:50}})");
  faker.Interpret("CREATE (:P {n:'v2', a:{b:20}})");
  faker.Interpret("CREATE (:P {n:'v3', a:{b:40}})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("MATCH (n:P {n:'v2'}) SET n.a = {b: 99}");  // nested COW: (b:20) -> (b:99)
  faker.Interpret("MATCH (n:P {n:'v3'}) DETACH DELETE n");    // tombstone: (b:40) must vanish
  faker.Interpret("CREATE (:P {n:'v4', a:{b:25}})");          // branch-native: (b:25)

  auto stream = faker.Interpret("MATCH (n:P) WHERE n.a.b > 0 RETURN n.a.b AS v ORDER BY n.a.b");
  const auto &rows = stream.GetResults();
  std::vector<int64_t> v;
  v.reserve(rows.size());
  for (const auto &row : rows) {
    v.emplace_back(row[0].ValueInt());
  }

  EXPECT_EQ(v, (std::vector<int64_t>{25, 50, 99}))
      << "expected the union of main@fork's historical nested-path index (v1=50, v3=40 -- "
         "tombstoned away) with the branch's own diff-engine nested-path index (v2 COW'd from "
         "b:20 to b:99 via a whole-map SET n.a = {b: 99}, v4 created natively at b:25), correctly "
         "ordered by n.a.b -- a bug in the nested-path key extraction (ReadNestedPropertyValue) "
         "through the branch merge, or in propagating a nested-map COW, would either drop a row, "
         "keep a stale b-value, or fail to order by the extracted leaf.";
}

TEST_F(VersioningInterpreterTest, BranchNestedLabelPropertyIndexSelectedInPlanOnBranch) {
  SatisfyGate();

  faker.Interpret("CREATE INDEX ON :P(a.b)");
  faker.Interpret("CREATE (:P {a:{b:1}})");
  faker.Interpret("CREATE (:P {a:{b:1}})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  auto stream = faker.Interpret("EXPLAIN MATCH (n:P) WHERE n.a.b > 0 RETURN n ORDER BY n.a.b");
  std::string plan;
  for (const auto &row : stream.GetResults()) {
    plan += row[0].ValueString();
    plan += '\n';
  }

  // Locks that MirrorMainIndexDefinitionsIntoDiffEngine registers the nested-path label-property
  // index on the branch's diff engine and that the planner selects the mirrored
  // ScanAllByLabelProperties instead of falling back to a plain ScanAll+Filter.
  EXPECT_THAT(plan, ::testing::HasSubstr("ScanAllByLabelProperties"))
      << "branch plan did not select the mirrored nested-path label-property index; full plan:\n"
      << plan;
}

TEST_F(VersioningInterpreterTest, BranchHopsLimitEnforcedOnExpand) {
  SatisfyGate();

  // 100 disconnected 2-node edges -- an expansion that ignores the hops budget will happily
  // traverse all 100 of them.
  faker.Interpret("UNWIND range(1, 100) AS x CREATE ()-[:NEXT]->()");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  auto stream = faker.Interpret("USING HOPS LIMIT 5 MATCH p=()-[:NEXT]->() RETURN count(p) AS c");
  const auto &rows = stream.GetResults();
  ASSERT_EQ(rows.size(), 1U);
  const auto c = rows[0][0].ValueInt();

  // Locks that USING HOPS LIMIT is enforced on a branch: before the fix the branch InEdges/OutEdges
  // path ignored the hops budget entirely and this returned 100 (the full disconnected edge set).
  EXPECT_EQ(c, 5) << "USING HOPS LIMIT 5 did not truncate expansion on a checked-out branch (got " << c
                  << " instead of 5) -- the branch edge-iteration path is not threading the hops budget "
                     "through incident edges.";
}

TEST_F(VersioningInterpreterTest, BranchGetHopsCounterTracksTraversal) {
  SatisfyGate();

  faker.Interpret("UNWIND range(1, 100) AS x CREATE ()-[:NEXT]->()");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // Exact shape exercised by hops_limit.feature ("Test retrieving hops limit counter", ~line 206):
  // a CALL subquery traverses 50 of the 100 available edges, well within the 100-hop budget.
  auto stream = faker.Interpret(
      "USING HOPS LIMIT 100 CALL { MATCH (a)-[r]->(b) WITH a, r, b LIMIT 50 RETURN count(*) AS cnt } "
      "RETURN getHopsCounter() AS hc");
  const auto &rows = stream.GetResults();
  ASSERT_EQ(rows.size(), 1U);
  const auto hc = rows[0][0].ValueInt();

  // Locks that getHopsCounter tracks branch traversal: before the fix the branch edge-iteration path
  // never charged hops against the counter, so this read back 0 instead of 50.
  EXPECT_EQ(hc, 50) << "getHopsCounter() did not reflect the branch traversal (got " << hc
                    << " instead of 50) -- hops are not being counted on the branch InEdges/OutEdges path.";
}

// Graph Versioning v1 (Gid-based std::hash<VertexAccessor>, vertex_accessor.hpp): regression lock for
// the identity fix that makes EdgeAccessor::To()/From()'s DEFERRED endpoint resolution (edge_accessor.cpp)
// safe. To()/From() used to eagerly ResolveVertex() every branched endpoint to canonicalize it to ONE
// physical copy, purely so std::hash<VertexAccessor> (keyed on the impl_ pointer) would stay consistent
// with operator== (keyed on Gid() once a branch is checked out) -- two containers disagreeing on identity
// for the "same" vertex. Hashing by Gid() instead (when branch_ctx_ != nullptr) removes the need for that
// canonicalization, so To()/From() can now hand back the RAW endpoint. This test builds a vertex that gets
// COW'd (historical copy -> diff-engine copy, same gid, distinct impl_) mid-query, then drives it through
// STShortestPathCursor::VertexEdgeMapT (plan/operator.cpp) -- a bidirectional-BFS map keyed directly on
// VertexAccessor via the plain (no custom-hash-functor) std::hash<VertexAccessor> specialization. Without
// the Gid-based hash, the historical and COW'd copies of the middle vertex would land in different hash
// buckets despite comparing ==, mis-keying the map and corrupting/truncating the reconstructed path.
TEST_F(VersioningInterpreterTest, BranchShortestPathAcrossCowdVertexHasConsistentIdentity) {
  SatisfyGate();

  // A simple directed chain on main: 1 -> 2 -> 3 -> 4.
  faker.Interpret("CREATE (:Chain {id: 1})-[:R]->(:Chain {id: 2})-[:R]->(:Chain {id: 3})-[:R]->(:Chain {id: 4})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // COW the MIDDLE vertex (id=2) mid-query via a branch-local SET -- this splits it into a historical
  // copy (pre-SET, still resident in `historical_`) and a diff-engine copy (post-SET), sharing one gid.
  faker.Interpret("MATCH (n:Chain {id: 2}) SET n.tag = 'cowd'");

  // Pre-binding BOTH endpoints in a separate MATCH forces ExpandVariable::MakeCursor to pick
  // STShortestPathCursor (existing_node on both sides, bidirectional BFS) over
  // SingleSourceShortestPathCursor -- see that cursor's VertexEdgeMapT (operator.cpp) this test locks.
  auto stream = faker.Interpret(
      "MATCH (a:Chain {id: 1}), (d:Chain {id: 4}) MATCH p=(a)-[*bfs..10]->(d) "
      "RETURN [x IN nodes(p) | x.id] AS path");
  const auto &rows = stream.GetResults();
  ASSERT_EQ(rows.size(), 1U);

  const auto &path_list = rows[0][0].ValueList();
  std::vector<int64_t> ids;
  ids.reserve(path_list.size());
  for (const auto &v : path_list) ids.push_back(v.ValueInt());

  EXPECT_EQ(ids, (std::vector<int64_t>{1, 2, 3, 4}))
      << "shortest path through a mid-query COW'd vertex must be intact and pass through id=2 exactly "
         "once -- a broken hash/equality contract on VertexAccessor (impl_-pointer hash vs Gid-based "
         "operator==) would mis-key STShortestPathCursor's VertexEdgeMapT, corrupting or truncating the "
         "reconstructed path.";
}

// Graph Versioning v1 (Gid-based std::hash<VertexAccessor>, vertex_accessor.hpp): companion regression to
// BranchShortestPathAcrossCowdVertexHasConsistentIdentity above, exercising a DIFFERENT direct consumer of
// the same std::hash<VertexAccessor> specialization: SingleSourceShortestPathCursor::processed_
// (plan/operator.cpp), selected instead of STShortestPathCursor when the destination is introduced fresh
// in the SAME MATCH pattern as the traversal (not pre-bound in an earlier clause), i.e. existing_node ==
// false. Deliberately NOT written as a TypedValue-level DISTINCT/equality check: TypedValue::Hash's
// Type::Vertex branch (typed_value.cpp) independently re-derives Gid().AsUint() and never calls
// std::hash<VertexAccessor> at all, so a DISTINCT-based test would pass even without this fix and would
// not actually lock it -- this test's map is keyed DIRECTLY on VertexAccessor via the specialization.
TEST_F(VersioningInterpreterTest, BranchSingleSourceShortestPathAcrossCowdVertexHasConsistentIdentity) {
  SatisfyGate();

  // Same chain as above: 1 -> 2 -> 3 -> 4.
  faker.Interpret("CREATE (:Chain {id: 1})-[:R]->(:Chain {id: 2})-[:R]->(:Chain {id: 3})-[:R]->(:Chain {id: 4})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // COW the middle vertex (id=2) mid-query -- identical setup to the test above.
  faker.Interpret("MATCH (n:Chain {id: 2}) SET n.tag = 'cowd'");

  // Both endpoints introduced fresh in ONE MATCH pattern -- existing_node == false on the destination,
  // so this selects SingleSourceShortestPathCursor (its own `processed_` map) instead of
  // STShortestPathCursor.
  auto stream =
      faker.Interpret("MATCH p=(a:Chain {id: 1})-[*bfs..10]->(d:Chain {id: 4}) RETURN [x IN nodes(p) | x.id] AS path");
  const auto &rows = stream.GetResults();
  ASSERT_EQ(rows.size(), 1U);

  const auto &path_list = rows[0][0].ValueList();
  std::vector<int64_t> ids;
  ids.reserve(path_list.size());
  for (const auto &v : path_list) ids.push_back(v.ValueInt());

  EXPECT_EQ(ids, (std::vector<int64_t>{1, 2, 3, 4}))
      << "shortest path through a mid-query COW'd vertex must be intact and pass through id=2 exactly "
         "once -- a broken hash/equality contract on VertexAccessor would mis-key "
         "SingleSourceShortestPathCursor::processed_, corrupting or truncating the reconstructed path.";
}

// Graph Versioning v1: branch-local DROP INDEX. Unlike CREATE INDEX (schema-plane DDL, rejected on
// a branch -- see BranchSchemaDdlRejected above), DROP INDEX on a checked-out branch is allowed and
// applies ONLY to the branch's own diff engine: the branch stops using the index (planner falls back
// to ScanAll+Filter, and results must still be correct through that fallback) while MAIN keeps the
// index untouched.
TEST_F(VersioningInterpreterTest, BranchLocalDropIndexIsQueryEffectiveAndMainUnaffected) {
  SatisfyGate();

  faker.Interpret("CREATE INDEX ON :Person(age)");
  faker.Interpret("CREATE (:Person {age:10})");
  faker.Interpret("CREATE (:Person {age:20})");
  faker.Interpret("CREATE (:Person {age:30})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // Branch-local drop must NOT throw -- this is the newly-allowed path.
  faker.Interpret("DROP INDEX ON :Person(age)");

  {
    auto s = faker.Interpret("EXPLAIN MATCH (n:Person) WHERE n.age > 0 RETURN n");
    std::string plan;
    for (const auto &row : s.GetResults()) {
      plan += row[0].ValueString();
      plan += '\n';
    }
    EXPECT_THAT(plan, ::testing::Not(::testing::HasSubstr("ScanAllByLabelProperties")))
        << "branch plan still selected the branch-dropped label-property index instead of falling "
           "back to ScanAll+Filter; full plan:\n"
        << plan;
  }

  {
    auto s = faker.Interpret("MATCH (n:Person) WHERE n.age > 0 RETURN n.age AS a ORDER BY n.age");
    const auto &rows = s.GetResults();
    std::vector<int64_t> ages;
    ages.reserve(rows.size());
    for (const auto &row : rows) {
      ages.emplace_back(row[0].ValueInt());
    }
    EXPECT_EQ(ages, (std::vector<int64_t>{10, 20, 30}))
        << "branch-local DROP INDEX must still produce correct results via the ScanAll+Filter "
           "fallback, not lose or corrupt rows.";
  }

  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    auto s = faker.Interpret("EXPLAIN MATCH (n:Person) WHERE n.age > 0 RETURN n");
    std::string plan;
    for (const auto &row : s.GetResults()) {
      plan += row[0].ValueString();
      plan += '\n';
    }
    EXPECT_THAT(plan, ::testing::HasSubstr("ScanAllByLabelProperties"))
        << "main's :Person(age) index must be intact -- a branch-local DROP INDEX must not touch "
           "main's own index set; full plan:\n"
        << plan;
  }
}

// Mirrors the test above at the SHOW INDEX INFO level: a branch-local DROP INDEX must remove the
// dropped index from the branch's own index listing (read via the diff engine's GetActiveIndices,
// see PrepareDatabaseInfoQuery's INDEX case) while leaving an untouched sibling index -- and main's
// full index set -- alone.
TEST_F(VersioningInterpreterTest, BranchLocalDropIndexReflectedInShowIndexInfo) {
  SatisfyGate();

  faker.Interpret("CREATE INDEX ON :Person(age)");
  faker.Interpret("CREATE INDEX ON :Person");
  faker.Interpret("CREATE (:Person {age:1})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("DROP INDEX ON :Person(age)");

  {
    // header = {"index type", "label", "property", "count"} -- row[0] is the type string.
    auto s = faker.Interpret("SHOW INDEX INFO");
    const auto &rows = s.GetResults();
    std::vector<std::string> types;
    types.reserve(rows.size());
    for (const auto &row : rows) {
      types.emplace_back(row[0].ValueString());
    }

    EXPECT_THAT(types, ::testing::Contains(std::string("label")))
        << "the :Person label-only index must still be listed on the branch after dropping the "
           "unrelated :Person(age) index.";
    EXPECT_THAT(types, ::testing::Not(::testing::Contains(std::string("label+property"))))
        << "the branch-dropped :Person(age) index must no longer be listed by SHOW INDEX INFO on "
           "the branch.";
  }

  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    auto s = faker.Interpret("SHOW INDEX INFO");
    const auto &rows = s.GetResults();
    std::vector<std::string> types;
    types.reserve(rows.size());
    for (const auto &row : rows) {
      types.emplace_back(row[0].ValueString());
    }

    EXPECT_THAT(types, ::testing::Contains(std::string("label+property")))
        << "main's :Person(age) index must reappear intact -- the branch-local drop must not have "
           "touched main's index set.";
  }
}

// Graph Versioning v1, bug #6 fix regression (enum crash): an enum defined on MAIN before a fork
// must be usable from a checked-out branch, both by a branch-native CREATE and by a SET that COWs
// a still-historical fork vertex. Before the fix, `BranchContext::BuildFromFork` gave the diff
// engine its OWN, empty `EnumStore` (only the `NameIdMapper` was shared) -- so `EnumValueAccess`'s
// evaluation (`dba_->GetEnumValue`, query/interpret/eval.hpp), which resolves against the checked-
// out branch's own diff-engine accessor, could never resolve `Status::...` on a branch at all,
// regardless of whether it was already registered on main. `MirrorMainEnumsIntoDiffEngine`
// (branch_engine.cpp) now mirrors main's enum definitions into the diff engine's own EnumStore at
// checkout time -- with IDENTICAL type/value ids, since ids are assigned by insertion position (see
// that function's own doc-comment) -- so this must now work, and read back as the exact enum value
// written, not a crash or a different/garbled one.
TEST_F(VersioningInterpreterTest, BranchEnumMirroredFromMainSupportsNativeCreateAndCowSet) {
  SatisfyGate();

  faker.Interpret("CREATE ENUM Status VALUES { Good, Bad };");
  // Fork vertex deliberately has NO enum property yet: `BranchContext::CowVertex` unconditionally
  // rejects COW'ing a vertex whose PRE-EXISTING (pre-COW) properties already contain an Enum (a
  // separate, already-covered restriction, not what this test targets) -- so the fork vertex must
  // start enum-free for the later branch-side SET (which ADDS a brand-new enum property) to reach
  // the code path under test instead of bailing out on that unrelated guard.
  faker.Interpret("CREATE (:X {tag: 'fork'})");

  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  // Branch-native CREATE with an inline enum literal -- must not crash/throw, and must resolve
  // through the diff engine's own (now-mirrored) EnumStore.
  faker.Interpret("CREATE (:X {tag: 'native', status: Status::Good})");

  // SET on the still-historical fork vertex: triggers CowVertex (the pre-existing `tag` property
  // has no enum, so the COW itself succeeds), and the enum-literal SET must then resolve the same
  // way against the diff engine's mirrored EnumStore.
  faker.Interpret("MATCH (n:X {tag: 'fork'}) SET n.status = Status::Bad");

  auto stream = faker.Interpret("MATCH (n:X) RETURN n.tag, n.status ORDER BY n.tag");
  ASSERT_EQ(stream.GetResults().size(), 2U);

  auto get_enum_value_str = [](const memgraph::communication::bolt::Value &val) -> std::string {
    if (!val.IsMap()) {
      ADD_FAILURE() << "expected an enum property to be encoded as a bolt Map (glue/communication.cpp), got type "
                    << static_cast<int>(val.type());
      return {};
    }
    const auto &map = val.ValueMap();
    EXPECT_EQ(map.at("__type").ValueString(), "mg_enum");
    return map.at("__value").ValueString();
  };

  // ORDER BY n.tag ASC: "fork" < "native" lexicographically.
  const auto &fork_row = stream.GetResults()[0];
  EXPECT_EQ(fork_row[0].ValueString(), "fork");
  EXPECT_EQ(get_enum_value_str(fork_row[1]), "Status::Bad")
      << "SET on a COW'd fork vertex must write and read back the exact enum value, not garble it";

  const auto &native_row = stream.GetResults()[1];
  EXPECT_EQ(native_row[0].ValueString(), "native");
  EXPECT_EQ(get_enum_value_str(native_row[1]), "Status::Good")
      << "a branch-native CREATE with an inline enum literal must read back the exact enum value, "
         "not crash or garble it";
}

// Graph Versioning v1, perf/versioning-branch-read D1 -- THE BLOCKER GUARD: `properties_on_edges =
// false` REFERENCE edges have no `storage::Edge` object at all (`EdgeRef` aliases a bare `Gid`
// instead of a pointer, edge_ref.hpp). `query::EdgeAccessor::MaybeBranchedHint`
// (storage/v2/edge_accessor.cpp) MUST guard `properties_on_edges` BEFORE dereferencing `edge_.ptr`,
// exactly like `Gid()` immediately above it -- a naive implementation that skipped the guard would
// alias the bare gid as a pointer and deref it, UB (the same crash class already fixed for edge-type
// indexes, commit 0c78ebee0). This fixture mirrors VersioningInterpreterTest exactly, except the
// storage Config is built with `properties_on_edges = false` from the start (VersioningInterpreterTest
// itself cannot be reused with an override -- its `config` member is fully constructed, including the
// dependent Database/Gatekeeper members, before a derived class's constructor could ever touch it).
class VersioningInterpreterReferenceEdgeTest : public ::testing::Test {
 protected:
  std::filesystem::path data_directory =
      std::filesystem::temp_directory_path() / "MG_tests_unit_versioning_interpreter_reference_edge";

  memgraph::storage::Config config{[&] {
    memgraph::storage::Config config{};
    config.durability.storage_directory = data_directory;
    config.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    config.disk.main_storage_directory = data_directory / "disk";
    // THE point of this fixture: reference (light, properties-off) edges -- no storage::Edge object,
    // no per-edge branched() bit to read.
    config.salient.items.properties_on_edges = false;
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

  void SatisfyGate() {
    FLAGS_versioning_enabled = true;
    memgraph::license::global_license_checker.EnableTesting();
  }
};

TEST_F(VersioningInterpreterReferenceEdgeTest, BranchReferenceEdgePropertyReadDoesNotCrashAndReturnsDefault) {
  SatisfyGate();

  faker.Interpret("CREATE (:S)-[:E]->(:A)");  // reference edge -- no properties possible at creation.
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");

  // MUST NOT CRASH: this is the actual blocker guard. A naive MaybeBranchedHint that skipped the
  // properties_on_edges gate would alias edge_.ptr (a bare Gid under this config) as a storage::Edge*
  // and dereference it -- UB, likely a SIGSEGV in a debug/ASan run.
  auto stream = faker.Interpret("MATCH (:S)-[e:E]->(:A) RETURN e.w AS w");
  ASSERT_EQ(stream.GetResults().size(), 1U);
  EXPECT_EQ(stream.GetResults()[0][0].type(), memgraph::communication::bolt::Value::Type::Null)
      << "a reference edge (properties_on_edges=false) has no properties -- GetProperty returns a "
         "default (Null) PropertyValue as a SUCCESS, not an error, and MUST NOT crash reaching it";

  // Same guard reached via a branch-native reference edge (created after checkout, not just a
  // fork-state one) -- both are plausible call sites for the same UB if the guard were missing.
  auto native_stream = faker.Interpret("MATCH (a:A) CREATE (a)-[e2:E2]->(:B) RETURN e2.w AS w");
  ASSERT_EQ(native_stream.GetResults().size(), 1U);
  EXPECT_EQ(native_stream.GetResults()[0][0].type(), memgraph::communication::bolt::Value::Type::Null)
      << "a branch-native reference edge must be readable the same way, without crashing";
}
