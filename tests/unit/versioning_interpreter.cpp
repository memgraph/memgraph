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
// file's MergeHappyPathFastForwardsEmptyChangelog below
// only re-confirm the interpreter still dispatches through it correctly end-to-end.

#include <algorithm>
#include <array>
#include <cctype>
#include <filesystem>
#include <limits>
#include <set>
#include <string>
#include <string_view>
#include <typeinfo>
#include <vector>

#include <gmock/gmock.h>
#include "gtest/gtest.h"

#include "auth/models.hpp"
#include "flags/general.hpp"
#include "glue/auth_checker.hpp"
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
constexpr int64_t kNullInt = std::numeric_limits<int64_t>::min();

using Rows = std::vector<std::vector<int64_t>>;

// Every row of `query` as integers (Null -> kNullInt).
Rows IntRows(InterpreterFaker &session, const std::string &query) {
  auto stream = session.Interpret(query);
  Rows rows;
  for (const auto &row : stream.GetResults()) {
    auto &out = rows.emplace_back();
    for (const auto &value : row) {
      out.push_back(value.type() == memgraph::communication::bolt::Value::Type::Null ? kNullInt : value.ValueInt());
    }
  }
  return rows;
}

std::vector<std::string> SortedNames(const memgraph::communication::bolt::Value &list) {
  std::vector<std::string> out;
  for (const auto &elem : list.ValueList()) out.emplace_back(elem.ValueString());
  std::ranges::sort(out);
  return out;
}

// The EXPLAIN output of `query`, one operator per line.
std::string ExplainText(InterpreterFaker &session, const std::string &query) {
  auto stream = session.Interpret("EXPLAIN " + query);
  std::string plan;
  for (const auto &row : stream.GetResults()) {
    plan += row[0].ValueString();
    plan += '\n';
  }
  return plan;
}

// True when `plan` contains the operator named exactly `op`, not merely as the prefix of a longer operator name
// (ScanAllByLabel inside ScanAllByLabelProperties).
bool PlanHasOperator(std::string_view plan, std::string_view op) {
  auto const is_identifier_char = [](char c) { return std::isalnum(static_cast<unsigned char>(c)) || c == '_'; };
  for (auto pos = plan.find(op); pos != std::string_view::npos; pos = plan.find(op, pos + 1)) {
    auto const end = pos + op.size();
    if ((pos == 0 || !is_identifier_char(plan[pos - 1])) && (end == plan.size() || !is_identifier_char(plan[end]))) {
      return true;
    }
  }
  return false;
}

// The value assertions of the index-overlay tests only pin the branch merge if the planner really scans the index
// they are named after; otherwise ScanAll + Filter would satisfy them without touching the overlay.
void ExpectPlanUses(InterpreterFaker &session, const std::string &query, std::string_view op) {
  const auto plan = ExplainText(session, query);
  EXPECT_TRUE(PlanHasOperator(plan, op)) << "plan of `" << query << "` did not select " << op << "; full plan:\n"
                                         << plan;
}
}  // namespace

// `kPropertiesOnEdges = false` builds the storage with reference (light) edges.
template <bool kPropertiesOnEdges>
class VersioningFixture : public ::testing::Test {
 protected:
  std::filesystem::path data_directory = std::filesystem::temp_directory_path() /
                                         (kPropertiesOnEdges ? "MG_tests_unit_versioning_interpreter"
                                                             : "MG_tests_unit_versioning_interpreter_reference_edge");

  memgraph::storage::Config config{[&] {
    memgraph::storage::Config config{};
    config.durability.storage_directory = data_directory;
    config.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    config.disk.main_storage_directory = data_directory / "disk";
    config.salient.items.properties_on_edges = kPropertiesOnEdges;
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

  // Flips on every chunk-0 gate precondition (flag + license); the storage mode is already
  // satisfied by `config` above.
  void SatisfyGate() {
    FLAGS_versioning_enabled = true;
    memgraph::license::global_license_checker.EnableTesting();
  }

  // CREATE BRANCH `name` FROM 'main' and CHECKOUT it on `session`.
  static void Fork(InterpreterFaker &session, const std::string &name = "b") {
    session.Interpret("CREATE BRANCH '" + name + "' FROM 'main'");
    session.Interpret("CHECKOUT BRANCH '" + name + "'");
    ASSERT_EQ(session.interpreter.current_db_.CurrentVersion(), name);
    ASSERT_NE(session.interpreter.current_db_.branch_context(), nullptr);
  }

  void Fork(const std::string &name = "b") { Fork(faker, name); }
};

using VersioningInterpreterTest = VersioningFixture<true>;

// Every test but the gate-precondition one starts with the gate satisfied.
class VersioningGatedTest : public VersioningInterpreterTest {
 protected:
  void SetUp() override {
    VersioningInterpreterTest::SetUp();
    SatisfyGate();
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

// Graph Versioning v1, Fix #3 (instance-role guard, MED-HIGH): PrepareVersioningQuery had no
// replica guard, so CREATE/CHECKOUT/DROP BRANCH executed against a REPLICA's own local
// VersionStore -- silently diverging from MAIN's registry (only MERGE would eventually fail, and
// only at the storage commit layer). `EnsureMainInstance` (the same helper PrepareTtlQuery already
// uses) now runs at the top of PrepareVersioningQuery's dispatch, before the gate/registry are ever
// touched. Flips this fixture's `repl_state` to REPLICA via the real
// `ReplicationState::SetReplicationRoleReplica` (mirrors tests/unit/replication_state_writing.cpp's
// own pattern) rather than a hand-rolled stub, so this exercises the genuine `IsReplica()` codepath
// EnsureMainInstance reads.
TEST_F(VersioningGatedTest, ManagementQueryRejectedOnReplica) {
  faker.Interpret("CREATE BRANCH 'feature' FROM 'main'");
  ASSERT_TRUE(repl_state.Lock()->SetReplicationRoleReplica(
      memgraph::replication::ReplicationServerConfig{.repl_server =
                                                         memgraph::io::network::Endpoint("127.0.0.1", 30246)},
      /*maybe_main_uuid=*/std::nullopt));
  ASSERT_TRUE(repl_state.ReadLock()->IsReplica());

  for (const auto *q : {"CREATE BRANCH 'other' FROM 'main'", "CHECKOUT BRANCH 'feature'", "DROP BRANCH 'feature'"}) {
    try {
      faker.Interpret(q);
      FAIL() << "expected the replica guard to reject: " << q;
    } catch (const memgraph::query::QueryException &e) {
      EXPECT_THAT(e.what(), ::testing::HasSubstr("forbidden on REPLICA")) << q;
    }
  }
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
}

TEST_F(VersioningGatedTest, ManagementQueryRejectedInExplicitTransaction) {
  faker.Interpret("BEGIN");
  ASSERT_THROW(faker.Interpret("CREATE BRANCH 'feature' FROM 'main'"), memgraph::query::MulticommandTxException);
  faker.Interpret("ROLLBACK");
}

TEST_F(VersioningGatedTest, InvalidBranchNameRejected) {
  // 'main' is reserved (versioning::ValidateBranchName) -- checked at dispatch time (deferred from
  // parse time per ast.hpp's own doc-comment).
  ASSERT_THROW(faker.Interpret("CREATE BRANCH 'main' FROM 'main'"), memgraph::query::QueryRuntimeException);
  ASSERT_THROW(faker.Interpret("CREATE BRANCH '.hidden' FROM 'main'"), memgraph::query::QueryRuntimeException);
  ASSERT_THROW(faker.Interpret("CREATE BRANCH 'a/b' FROM 'main'"), memgraph::query::QueryRuntimeException);
}

TEST_F(VersioningGatedTest, CreateShowCheckoutDropHappyPath) {
  // A connection that never checked out a branch writes main exactly as if versioning did not exist.
  faker.Interpret("CREATE (:A {v:1})");
  faker.Interpret("MATCH (n:A) SET n.v = 2");
  EXPECT_EQ(IntRows(faker, "MATCH (n:A) RETURN n.v"), (Rows{{2}}));

  {
    auto stream = faker.Interpret("CREATE BRANCH 'feature' FROM 'main'");
    ASSERT_EQ(stream.GetHeader(), (std::vector<std::string>{"version", "number", "description", "parent"}));
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "feature");
    EXPECT_EQ(stream.GetResults()[0][1].ValueInt(), 2);
    EXPECT_EQ(stream.GetResults()[0][2].type(), memgraph::communication::bolt::Value::Type::Null);
    EXPECT_EQ(stream.GetResults()[0][3].ValueString(), "main");
  }

  // CREATE BRANCH alone never moves the session: the next write still lands on main.
  faker.Interpret("CREATE (:B)");
  EXPECT_EQ(IntRows(faker, "MATCH (n:B) RETURN count(n)"), (Rows{{1}}))
      << "CREATE BRANCH alone must not engage the write-routing rail";

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

  // CHECKOUT ... WITH DESCRIPTION ... FROM creates and switches in one statement.
  {
    auto stream = faker.Interpret("CHECKOUT BRANCH 'feature' WITH DESCRIPTION 'desc' FROM 'main'");
    ASSERT_EQ(stream.GetHeader(), (std::vector<std::string>{"version", "number", "description", "parent"}));
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][2].ValueString(), "desc");
  }
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "feature");
  {
    auto stream = faker.Interpret("SHOW BRANCHES");
    ASSERT_EQ(stream.GetResults().size(), 2U);
    EXPECT_EQ(stream.GetResults()[1][2].ValueString(), "desc");
  }

  // Dropping the branch the session is checked out on clears the session pointer.
  faker.Interpret("DROP BRANCH 'feature'");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
}

// PROFILE wraps an inner Cypher query and PrepareProfileQuery takes the
// PreparedQuery's rw_type from that INNER plan, but the rail's discriminator classifies the OUTER
// AST node -- a ProfileQuery, not a CypherQuery (they are siblings, not sub/super-class). This test
// now confirms a PROFILE'd write on main behaves exactly like a bare write after CHECKOUT BRANCH
// 'main': it SUCCEEDS (no longer rejected), same as the un-PROFILE'd case.
TEST_F(VersioningGatedTest, ProfileWriteOnMainAfterCheckoutMainSucceeds) {
  faker.Interpret("CREATE (:Seed)");
  Fork();
  faker.Interpret("CHECKOUT BRANCH 'main'");  // un-engages; back on main, writable again
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());

  // PROFILE of a write on main now succeeds, matching bare CREATE's post-fix behavior.
  auto profile = faker.Interpret("PROFILE CREATE (:NoLongerBlockedProfiledWrite)");
  EXPECT_FALSE(profile.GetResults().empty()) << "PROFILE must return its plan table";

  // PrepareProfileQuery aborts its transaction by design (QueryHandlerResult::ABORT), so the PROFILE'd write leaves
  // nothing behind -- the same as on a main without versioning.
  EXPECT_EQ(IntRows(faker, "MATCH (n:NoLongerBlockedProfiledWrite) RETURN count(n)"), (Rows{{0}}));
  // A bare write on the same un-engaged session commits to main.
  faker.Interpret("CREATE (:BareWrite)");
  EXPECT_EQ(IntRows(faker, "MATCH (n:BareWrite) RETURN count(n)"), (Rows{{1}}));

  // Sanity: PROFILE of a READ on main is fine too -- unaffected either way.
  EXPECT_FALSE(faker.Interpret("PROFILE MATCH (n:Seed) RETURN n").GetResults().empty());
}

// Regression test for the write-routing sticky bug this fix closes, capturing the user's exact
// model on a SINGLE session/interpreter: the session target is just current_version_ -- the last
// CHECKOUT wins, exactly like USE DATABASE. CHECKOUT BRANCH 'x' routes writes to x, then CHECKOUT
// BRANCH 'y' routes to y, then CHECKOUT BRANCH 'main' routes back to main and main stays WRITABLE --
// all on the one connection, with no reconnect.
TEST_F(VersioningGatedTest, CheckoutSwitchesBetweenBranchesThenBackToMainLikeUseDatabase) {
  // Base data on main.
  faker.Interpret("CREATE (:Base {v:0})");

  // CHECKOUT BRANCH x -- a write now lands on x, not main.
  Fork("x");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "x");
  faker.Interpret("CREATE (:OnX {v:1})");
  {
    // Read-back on x: sees the fork base plus x's own write.
    EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n) AS c"), (Rows{{2}}))
        << "x must see :Base (forked) + :OnX (its own write)";
  }

  // CHECKOUT BRANCH y -- a write now lands on y, not x and not main.
  Fork("y");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "y");
  faker.Interpret("CREATE (:OnY {v:2})");
  {
    // Read-back on y: sees the fork base plus y's own write, but NOT x's write -- branches forked
    // from main are isolated from each other.
    auto stream = faker.Interpret("MATCH (n:OnX) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "y must not see x's branch-native write";
  }
  {
    EXPECT_EQ(IntRows(faker, "MATCH (n:OnY) RETURN n.v"), (Rows{{2}}));
  }

  // CHECKOUT BRANCH main -- this is the regression this fix closes: a bare write must land on MAIN
  // and SUCCEED, on the very same connection that just came from two branch checkouts.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
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
    EXPECT_EQ(IntRows(faker, "USING VERSION 'main' MATCH (n:OnMain) RETURN n.v"), (Rows{{3}}));
  }

  // Back on a branch after main writes, writes route to the branch again and stay off main.
  faker.Interpret("CREATE (:AnotherBranchWrite)");
  EXPECT_EQ(IntRows(faker, "MATCH (n:AnotherBranchWrite) RETURN count(n)"), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, "USING VERSION 'main' MATCH (n:AnotherBranchWrite) RETURN count(n)"), (Rows{{0}}));
}

// Branch-side change filters (branch_change_filter.hpp): a read of a KIND the branch never changed
// must return the fork-state value via the direct (no-resolve) fast path, while a read of a kind the
// branch DID change must reflect the change. Exercises per-kind independence: a property SET must not
// make label reads resolve, and a label ADD must not make property reads resolve -- yet both changed
// values are still observed. (Correctness is invariant to whether the filter resolves or reads fork;
// this test pins the observable VALUES, which must be identical either way.)
TEST_F(VersioningGatedTest, BranchChangeFilterKindIsolation) {
  faker.Interpret("CREATE (:User {id: 1, age: 40, name: 'a'})");
  Fork();

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

// Branch-side change filters: cross-branch isolation and the fork-pin. The global branched() bit (vertex) and
// Edge::branched() bit are set by ANY branch and never cleared, so an entity changed by branch b1 reads as branched
// when read on branch b2 that never touched it. b2's OWN change filters are clear for it, so it must read the FORK
// value via the direct path -- never b1's change (which lives in b1's private diff engine) and never main's post-fork
// mutation (b2 is pinned at its own fork timestamp).
TEST_F(VersioningGatedTest, BranchCrossBranchIsolationReadsOwnFork) {
  faker.Interpret("CREATE (:User {id: 1, age: 10})");
  faker.Interpret("CREATE (:User {id: 2, age: 20})");
  faker.Interpret("CREATE (:S)-[:E {w:1}]->(:A)");

  // b1 mutates id=1 and the edge (setting their branched() bits globally) but is otherwise independent.
  Fork("b1");
  faker.Interpret("MATCH (n {id: 1}) SET n.age = 111, n:Changed");
  faker.Interpret("MATCH (:S)-[e:E]->(:A) SET e.w = 999");
  EXPECT_EQ(IntRows(faker, "MATCH (n {id: 1}) RETURN n.age"), (Rows{{111}})) << "b1 must read its own write";
  EXPECT_EQ(IntRows(faker, "MATCH (:S)-[e:E]->(:A) RETURN e.w"), (Rows{{999}})) << "b1 must read its own edge write";
  faker.Interpret("CHECKOUT BRANCH 'main'");

  // b2 forks AFTER b1's changes (main itself is untouched), then main changes id=2: b2 must stay pinned.
  faker.Interpret("CREATE BRANCH 'b2' FROM 'main'");
  faker.Interpret("MATCH (n {id: 2}) SET n.age = 222");
  EXPECT_EQ(IntRows(faker, "MATCH (n {id: 2}) RETURN n.age"), (Rows{{222}})) << "main must read its own post-fork write";
  faker.Interpret("CHECKOUT BRANCH 'b2'");

  {
    // id=1: changed by b1, never touched by b2 -> b2 reads FORK (age=10, no :Changed).
    auto stream = faker.Interpret("MATCH (n {id: 1}) RETURN n.age AS age, ('Changed' IN labels(n)) AS c");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 10) << "b2 must not see b1's property change (cross-branch leak)";
    EXPECT_FALSE(stream.GetResults()[0][1].ValueBool()) << "b2 must not see b1's label change";
  }
  EXPECT_EQ(IntRows(faker, "MATCH (n {id: 2}) RETURN n.age"), (Rows{{20}}))
      << "b2 must read fork-state, not main's post-fork write";
  EXPECT_EQ(IntRows(faker, "MATCH (:S)-[e:E]->(:A) RETURN e.w"), (Rows{{1}}))
      << "b2 must read its OWN fork edge value, not b1's diff-engine change -- a bit-set-but-FindDiffEdge-miss must "
         "fall back to fork-state, never leak across branches";
}

// Fine-grained (gid, property_id) filter: a point read GetProperty(pid) gates on the fine filter
// ALONE, so on the change-log replay path (re-checkout) that filter MUST be re-seeded from
// WalVertexSetProperty, or a genuinely-changed property reaches a reader through main's copy and reads
// stale fork-state. This is a correctness requirement (not just precision): reaching the changed
// vertex via an EDGE TRAVERSAL lands on main's (main-resident, branched()) copy -- the exact case the
// fine filter guards -- rather than the diff-resident copy a scan would yield. A missing replay-seed
// returns the fork value here.
TEST_F(VersioningGatedTest, BranchFinePropertyChangeReadViaTraversal) {
  faker.Interpret("CREATE (n:User {id: 1})-[:E]->(v:User {id: 2, age: 40, keep: 7})");
  Fork();
  faker.Interpret("MATCH (v {id: 2}) SET v.age = 99");  // property change; v is COW'd, fine-seeded live

  // In-session, via a traversal (reaches v through main's pointer -> fine filter consulted).
  {
    auto stream = faker.Interpret("MATCH (n {id: 1})-[:E]->(v) RETURN v.age AS age, v.keep AS keep");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 99) << "changed property must resolve to the new value";
    EXPECT_EQ(stream.GetResults()[0][1].ValueInt(), 7)
        << "an UNCHANGED property on the same vertex must read fork-state via the fine fast path";
  }

  // Isolation: main is untouched by the branch's property change.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    EXPECT_EQ(IntRows(faker, "MATCH (v {id: 2}) RETURN v.age AS age"), (Rows{{40}}))
        << "main must not see the branch's property change";
  }
}

// Graph Versioning v1, B2 fix: CREATE BRANCH FROM a non-main parent is rejected at the
// interpreter layer. MERGE BRANCH replays the changelog onto MAIN (not onto the declared parent),
// so permitting nested creation would silently misroute branch writes. The rejection must happen
// before any VersionStore state mutation: 'child' must not exist after the throw.
TEST_F(VersioningGatedTest, CreateBranchFromNonMainParentIsRejected) {
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
// must move as the branch lifecycle progresses -- create, checkout and drop (merge is unsupported).
//   - The four counters are process-global monotonic singletons shared across every test in this
//     binary, so assert DELTAS captured at the start of this test, never absolute values (mirrors
//     tests/unit/hot_cold_resume.cpp's own SuspendResumeMoveObservabilityMetrics precedent).
//   - versioning_active_branches/versioning_active_checkouts are gauges Set() from this database's VersionStore
//     on every change, so after each operation they hold exact values regardless of earlier tests.
TEST_F(VersioningGatedTest, VersioningMetricsIncrementOnLifecycle) {
  auto &g = memgraph::metrics::Metrics().global;
  const double created0 = g.versioning_branches_created->Value();
  const double dropped0 = g.versioning_branches_dropped->Value();

  faker.Interpret("CREATE (:A {x:1})");
  faker.Interpret("CREATE BRANCH 'b1' FROM 'main'");
  faker.Interpret("CREATE BRANCH 'b2' FROM 'main'");
  EXPECT_DOUBLE_EQ(g.versioning_branches_created->Value() - created0, 2.0)
      << "two CREATE BRANCH statements must increment the created counter by exactly 2";
  // VersionStore::CreateBranch Set()s the gauge to branches_.size() (b1 + b2; main is not registered).
  EXPECT_DOUBLE_EQ(g.versioning_active_branches->Value(), 2.0);

  // Checkout b1, write, then back to main.
  faker.Interpret("CHECKOUT BRANCH 'b1'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b1");
  EXPECT_DOUBLE_EQ(g.versioning_active_checkouts->Value(), 1.0);
  faker.Interpret("CREATE (:OnBranch {y:1})");
  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_DOUBLE_EQ(g.versioning_active_checkouts->Value(), 0.0) << "ReleaseCheckout must Set() the gauge back to 0";

  // Drop b1 (checked out earlier, now detached).
  faker.Interpret("DROP BRANCH 'b1'");
  EXPECT_DOUBLE_EQ(g.versioning_branches_dropped->Value() - dropped0, 1.0);
  EXPECT_DOUBLE_EQ(g.versioning_active_branches->Value(), 1.0);

  // Drop b2 directly (never checked out).
  faker.Interpret("DROP BRANCH 'b2'");
  EXPECT_DOUBLE_EQ(g.versioning_branches_dropped->Value() - dropped0, 2.0)
      << "each DROP BRANCH increments the dropped counter by exactly 1";
  EXPECT_DOUBLE_EQ(g.versioning_active_branches->Value(), 0.0);
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
TEST_F(VersioningGatedTest, ReApplyingSameDatabaseAccessPreservesCheckout) {
  Fork("feature");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "feature");

  auto same_db_acc_opt = db_gk.access();
  ASSERT_TRUE(same_db_acc_opt);
  faker.interpreter.current_db_.SetCurrentDB(*same_db_acc_opt, /*in_explicit_db=*/false);
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "feature");
  // Unlike the bare-CurrentDB test above, a real checkout also holds a BranchContext and an exclusive checkout slot:
  // neither may be dropped by a same-database re-apply (ClearBranchContext is only for a genuine switch).
  EXPECT_NE(faker.interpreter.current_db_.branch_context(), nullptr);
  EXPECT_EQ(db->version_store()->CheckoutCount("feature"), 1U);

  // The session is still genuinely checked out: SHOW BRANCH still reports it.
  auto stream = faker.Interpret("SHOW BRANCH");
  ASSERT_EQ(stream.GetResults().size(), 1U);
  EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "feature");
}

// Graph Versioning v1 (lazy diff-context, slice E-1): the SELECTIVE-COW regression the redesign
// exists for -- a branch touching ONE of two main vertices must copy-on-write ONLY that one; the
// untouched vertex must keep resolving through the historical fork-state base, never materialized
// into the diff engine. Also verifies the CHECKOUT-away isolation contract (R35) one more time
// against a two-vertex graph.
//
// VERTEX-ONLY: `WHERE 'A' IN labels(n)` (ScanAll + a labels() filter) stands in for `MATCH (n:A)`
// here purely to keep this test's focus on selective-COW, independent of whether a label index
// happens to exist -- BranchLabelOnlyIndexScanReflectsBranchMutations covers the
// dedicated MATCH (n:A) / label-indexed-scan regression.
TEST_F(VersioningGatedTest, BranchWriteStubsOnlyTheTouchedVertex) {
  faker.Interpret("CREATE (:A {x:1}), (:B {y:9})");
  Fork();
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
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  {
    EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE 'A' IN labels(n) RETURN n.x"), (Rows{{1}}));
  }
}

// Whole-map property writes on a fork vertex: `SET n += {...}` (query::VertexAccessor::UpdateProperties, which lacked
// the COW guard SetProperty/AddLabel had and wrote straight through the READ-ONLY historical accessor, tripping the
// `is_historical_` MG_ASSERT) and `SET n = {...}` (which keeps only the new properties). Both must be isolated from
// main.
TEST_F(VersioningGatedTest, BranchWholeMapPropertyWriteOnForkVertex) {
  faker.Interpret("CREATE (:A {x:1})");
  faker.Interpret("CREATE (:P {id: 1, p: 7, q: 8})");
  Fork();

  faker.Interpret("MATCH (n:A) SET n += {y: 2}");
  EXPECT_EQ(IntRows(faker, "MATCH (n:A) RETURN n.y"), (Rows{{2}}));

  faker.Interpret("MATCH (n:P) SET n = {x: 1}");
  {
    auto stream = faker.Interpret("MATCH (n:P) RETURN keys(n), n.x, n.p IS NULL, n.id IS NULL");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    const auto &row = stream.GetResults()[0];
    EXPECT_EQ(SortedNames(row[0]), (std::vector<std::string>{"x"}));
    EXPECT_EQ(row[1].ValueInt(), 1);
    EXPECT_TRUE(row[2].ValueBool());
    EXPECT_TRUE(row[3].ValueBool());
  }

  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_EQ(IntRows(faker, "MATCH (n:A) RETURN n.y"), (Rows{{kNullInt}})) << "main must not see the branch's SET +=";
  EXPECT_EQ(IntRows(faker, "MATCH (n:P) RETURN n.id, n.p, n.q, n.x"), (Rows{{1, 7, 8, kNullInt}}))
      << "main must not see the branch's SET =";
}

// Graph Versioning v1 (lazy diff-context, slice E-1) HIGH-2 regression (adversarial-review fix):
// single-STATEMENT read-your-write. `SET n.x=2` COWs the vertex and redirects `impl_` on the
// mutator's OWN (transient, expression-evaluated) VertexAccessor -- but the Frame's own copy of
// symbol `n` (the one `RETURN n.x`, further down the SAME operator tree, actually reads) is a
// SEPARATE value that keeps pointing at the stale, pre-COW historical object unless every read is
// made self-correcting (VertexAccessor::GetProperty/Labels/etc's own doc-comment). Must see the NEW
// value, not the historical one, all within one autocommit statement/transaction -- no AdvanceCommand
// or second statement involved.
TEST_F(VersioningGatedTest, BranchSingleStatementSetThenReturnSeesNewValue) {
  faker.Interpret("CREATE (:A {x:1})");
  Fork();

  EXPECT_EQ(IntRows(faker, "MATCH (n) SET n.x = 2 RETURN n.x"), (Rows{{2}}))
      << "must see the post-SET value, not the stale historical one";
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
TEST_F(VersioningGatedTest, BranchEdgeCreateAndEndpointShadowingIsUnionAware) {
  // On main: A/B with no edge between them; S--[:E]-->A(name:a2) exists BEFORE the fork.
  faker.Interpret("CREATE (:A {name:'a'}), (:B {name:'b'})");
  faker.Interpret("CREATE (:S {name:'s'})-[:E]->(:A {name:'a2'})");

  Fork("br");
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
TEST_F(VersioningGatedTest, BranchFindEdgeByIdResolvesForkAndBranchNativeEdges) {
  // Pre-fork (fork) edge on main.
  faker.Interpret("CREATE (:S)-[:E]->(:A)");

  int64_t fork_edge_gid;
  {
    auto s = faker.Interpret("MATCH ()-[e:E]->() RETURN id(e)");
    ASSERT_EQ(s.GetResults().size(), 1U);
    fork_edge_gid = s.GetResults()[0][0].ValueInt();
  }

  Fork();

  ExpectPlanUses(faker, "MATCH ()-[e]->() WHERE id(e) = " + std::to_string(fork_edge_gid) + " RETURN type(e)", "ScanAllByEdgeId");

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

// Graph Versioning v2: indexes, constraints, descriptions, triggers, streams and storage-global operations belong to
// main, so each is rejected with a QueryRuntimeException while checked out on a branch (before it ever reaches
// storage). The session stays usable afterwards, SHOW INDEX INFO on the branch still equals main's and the planner
// still uses the mirrored index, and the identical statements succeed on main -- the guard is branch-scoped.
TEST_F(VersioningGatedTest, StatementsRejectedOnBranch) {
  faker.Interpret("CREATE INDEX ON :Person(age)");
  faker.Interpret("CREATE INDEX ON :Person");
  faker.Interpret("CREATE EDGE INDEX ON :KNOWS");
  faker.Interpret("CREATE (:Person {age:10})");
  faker.Interpret("CREATE (:L {p:1})-[:R {p:1}]->(:M)");

  auto index_rows = [&]() {
    auto s = faker.Interpret("SHOW INDEX INFO");
    std::vector<std::string> rows;
    for (const auto &row : s.GetResults()) {
      std::string r;
      for (size_t i = 0; i + 1 < row.size(); ++i) {  // skip the trailing count column
        r += row[i].IsString() ? std::string(row[i].ValueString()) : std::string("-");
        r += '|';
      }
      rows.push_back(std::move(r));
    }
    std::sort(rows.begin(), rows.end());
    return rows;
  };
  const auto main_rows = index_rows();
  ASSERT_GE(main_rows.size(), 3U);

  Fork();

  const std::string kGuardMessage = "not allowed while a versioning branch is checked out";
  static constexpr const char *rejected[] = {
      "CREATE INDEX ON :L(p)",
      "CREATE EDGE INDEX ON :R(p)",
      "CREATE POINT INDEX ON :L(p)",
      "CREATE VECTOR INDEX vi ON :L(p) WITH CONFIG {\"dimension\": 2, \"capacity\": 10}",
      "CREATE TEXT INDEX ti ON :L",
      "DROP INDEX ON :Person(age)",
      "DROP INDEX ON :Person",
      "DROP EDGE INDEX ON :KNOWS",
      "DROP ALL INDEXES",
      "CREATE CONSTRAINT ON (n:L) ASSERT EXISTS (n.p)",
      "DROP ALL CONSTRAINTS",
      "CREATE ENUM Status VALUES { Good, Bad }",
      "ANALYZE GRAPH",
      "DROP GRAPH",
      "STORAGE MODE IN_MEMORY_ANALYTICAL",
      "RECOVER SNAPSHOT \"/nonexistent/snapshot\"",
      "ENABLE TTL",
      "CREATE TRIGGER my_trigger AFTER COMMIT EXECUTE RETURN 1",
      // Must be parse+semantic-valid (TOPICS/TRANSFORM are required) so it reaches the on-branch guard in Prepare
      // instead of failing semantic validation first.
      "CREATE KAFKA STREAM my_stream TOPICS my_topic TRANSFORM my_module.my_transform",
      "SET DESCRIPTION ON LABEL :Person \"d\"",
      "DELETE DESCRIPTION ON LABEL :Person",
      "SHOW DESCRIPTIONS",
  };
  for (const auto *query : rejected) {
    SCOPED_TRACE(query);
    try {
      faker.Interpret(query);
      ADD_FAILURE() << "must be rejected on a branch";
    } catch (const memgraph::query::QueryRuntimeException &e) {
      EXPECT_THAT(e.what(), ::testing::HasSubstr(kGuardMessage));
    }
  }

  // The session is not wedged: reads and data-plane writes still work.
  EXPECT_EQ(IntRows(faker, "MATCH (n:L) RETURN n.p"), (Rows{{1}}));
  faker.Interpret("CREATE (:L {p:2})");

  EXPECT_EQ(index_rows(), main_rows);
  EXPECT_TRUE(
      PlanHasOperator(ExplainText(faker, "MATCH (n:Person) WHERE n.age > 0 RETURN n"), "ScanAllByLabelProperties"));

  // The same statements succeed on main.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_EQ(index_rows(), main_rows);
  faker.Interpret("CREATE INDEX ON :L(p)");
  EXPECT_EQ(index_rows().size(), main_rows.size() + 1);
  faker.Interpret("CREATE TRIGGER my_trigger AFTER COMMIT EXECUTE RETURN 1");
  {
    auto s = faker.Interpret("SHOW TRIGGERS");
    EXPECT_EQ(s.GetResults().size(), 1U);
  }
  EXPECT_NO_THROW(faker.Interpret("SET DESCRIPTION ON LABEL :Person \"d\""));
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
TEST_F(VersioningGatedTest, BranchEdgeBitFastPathSkipsResolveForUnchangedEdges) {
  faker.Interpret("CREATE (s:S)-[:E1 {w:1}]->(:A), (s)-[:E3 {w:3}]->(:C)");
  faker.Interpret("CREATE (:S2)-[:E2 {w:2}]->(:B)");
  Fork();
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");

  faker.Interpret("MATCH (:S)-[e:E1]->(:A) SET e.w = 100");

  // (1) Changed edge: must reflect the new value.
  {
    EXPECT_EQ(IntRows(faker, "MATCH (:S)-[e:E1]->(:A) RETURN e.w"), (Rows{{100}}))
        << "the changed edge must reflect the new value";
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
    EXPECT_EQ(IntRows(faker, "MATCH (:S2)-[e:E2]->(:B) RETURN e.w"), (Rows{{2}}))
        << "a completely untouched edge must read fork-state";
  }

  // R35: main is untouched by any of the above.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    EXPECT_EQ(IntRows(faker, "MATCH (:S)-[e:E1]->(:A) RETURN e.w"), (Rows{{1}}))
        << "main must be untouched by the branch's SET";
  }
}

// D1 correctness case (8): the branched() bit lives on the EDGE object, not the vertex -- a
// self-loop (from==to) and two parallel edges (same endpoints, distinct gid) must each resolve
// independently. The parallel-edge half is the sharper case: P1 and P2 share BOTH endpoints, so
// SETting P1 vertex-COWs (branches) both S2 and T2 -- if the fast path incorrectly kept per-VERTEX
// rather than per-EDGE state, P2 (untouched, but hanging off two now-"branched" vertices) would
// wrongly resolve too.
TEST_F(VersioningGatedTest, BranchEdgeBitFastPathSelfLoopAndParallelEdgesPerEdgeObject) {
  faker.Interpret("CREATE (s:S)-[:SELF {w:1}]->(s)");
  faker.Interpret("CREATE (:S2)-[:P1 {w:10}]->(:T2), (:S2)-[:P2 {w:20}]->(:T2)");
  Fork();

  faker.Interpret("MATCH (:S)-[e:SELF]->(:S) SET e.w = 999");
  faker.Interpret("MATCH (:S2)-[e:P1]->(:T2) SET e.w = 111");

  {
    EXPECT_EQ(IntRows(faker, "MATCH (:S)-[e:SELF]->(:S) RETURN e.w"), (Rows{{999}}))
        << "the changed self-loop edge must reflect the new value";
  }
  {
    EXPECT_EQ(IntRows(faker, "MATCH (:S2)-[e:P1]->(:T2) RETURN e.w"), (Rows{{111}}))
        << "the changed parallel edge P1 must reflect the new value";
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
    EXPECT_EQ(IntRows(faker, "MATCH (:S)-[e:SELF]->(:S) RETURN e.w"), (Rows{{1}}));
  }
  {
    EXPECT_EQ(IntRows(faker, "MATCH (:S2)-[e:P1]->(:T2) RETURN e.w"), (Rows{{10}}));
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
TEST_F(VersioningGatedTest, BranchDeleteBranchNativeVertexHiddenAndMainNeverSawIt) {
  Fork();

  faker.Interpret("CREATE (:X {id:1})");
  EXPECT_EQ(IntRows(faker, "MATCH (n:X) RETURN count(n)"), (Rows{{1}})) << "the native vertex must be visible before its DELETE";
  faker.Interpret("MATCH (n:X) DELETE n");

  {
    auto stream = faker.Interpret("MATCH (n:X) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "the branch-native vertex must be hidden after its own DELETE";
  }

  faker.Interpret("CHECKOUT BRANCH 'main'");

  auto stream = faker.Interpret("MATCH (n:X) RETURN n");
  EXPECT_EQ(stream.GetResults().size(), 0U) << "main must never have seen the created-then-deleted vertex";
}

// A branch's own writes live in the diff engine and are rebuilt from the per-commit change-log every time a session
// attaches, so every kind of write must read back identically after CHECKOUT away and back, and main must stay
// untouched. The separate autocommitted statements are separate captured commits: a later commit (the SET of :M1) is
// only correct if every earlier one (its CREATE) was captured and replayed in order. Replay must also re-seed the
// change filters: the fine property filter (reached through an edge traversal, main's copy of the vertex) and main's
// Edge::branched() bit, else the changed value reads stale fork-state.
TEST_F(VersioningGatedTest, BranchStateSurvivesDetachReattach) {
  faker.Interpret("CREATE (:Person:Employee {id: 1})");
  faker.Interpret("CREATE (:User {id: 1})-[:E]->(:User {id: 2, age: 40, keep: 7})");
  faker.Interpret("CREATE (s:S)-[:E1 {w: 1}]->(:A), (s)-[:E2 {w: 2}]->(:B)");
  faker.Interpret("CREATE (:RP {id: 1, p: 7, q: 8})");
  Fork();

  faker.Interpret("CREATE (:M1 {v:1})");
  faker.Interpret("CREATE (:M2 {v:1})");
  faker.Interpret("MATCH (n:M1) SET n.v = 2");
  faker.Interpret("MATCH (n:Person {id: 1}) REMOVE n:Employee");
  faker.Interpret("MATCH (v:User {id: 2}) SET v.age = 99");
  faker.Interpret("MATCH (:S)-[e:E1]->(:A) SET e.w = 99");
  faker.Interpret("MATCH (n:RP) REMOVE n.p");
  faker.Interpret("MATCH (n:RP) SET n.q = null");

  auto const check_branch = [&] {
    auto stream = faker.Interpret("MATCH (n:M1) RETURN n.v");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2) << "the create and the later SET must both be visible";
    EXPECT_EQ(IntRows(faker, "MATCH (n:M2) RETURN n.v"), (Rows{{1}}));
    {
      auto stream = faker.Interpret("MATCH (n:Person {id: 1}) RETURN labels(n)");
      ASSERT_EQ(stream.GetResults().size(), 1U);
      EXPECT_EQ(SortedNames(stream.GetResults()[0][0]), (std::vector<std::string>{"Person"}))
          << "the REMOVE'd :Employee must stay gone and the untouched :Person must stay";
    }
    EXPECT_EQ(IntRows(faker, "MATCH (:User {id: 1})-[:E]->(v) RETURN v.age, v.keep"), (Rows{{99, 7}}))
        << "the changed property resolves through the traversal, the unchanged one reads fork-state";
    EXPECT_EQ(IntRows(faker, "MATCH (:S)-[e:E1]->(:A) RETURN e.w"), (Rows{{99}})) << "the changed edge";
    EXPECT_EQ(IntRows(faker, "MATCH (:S)-[e:E2]->(:B) RETURN e.w"), (Rows{{2}})) << "the untouched sibling edge";
    {
      auto stream = faker.Interpret("MATCH (n:RP) RETURN n.p IS NULL, n.q IS NULL, keys(n), n.id");
      ASSERT_EQ(stream.GetResults().size(), 1U);
      const auto &row = stream.GetResults()[0];
      EXPECT_TRUE(row[0].ValueBool());
      EXPECT_TRUE(row[1].ValueBool());
      EXPECT_EQ(SortedNames(row[2]), (std::vector<std::string>{"id"}));
      EXPECT_EQ(row[3].ValueInt(), 1);
    }
  };

  check_branch();

  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  check_branch();

  faker.Interpret("CHECKOUT BRANCH 'main'");
  auto stream = faker.Interpret("MATCH (n:M1) RETURN count(n)");
  ASSERT_EQ(stream.GetResults().size(), 1U);
  EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 0);
  EXPECT_EQ(IntRows(faker, "MATCH (n:M2) RETURN count(n)"), (Rows{{0}}));
  {
    auto stream = faker.Interpret("MATCH (n:Person {id: 1}) RETURN labels(n)");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(SortedNames(stream.GetResults()[0][0]), (std::vector<std::string>{"Employee", "Person"}))
        << "main must keep both labels";
  }
  EXPECT_EQ(IntRows(faker, "MATCH (v:User {id: 2}) RETURN v.age"), (Rows{{40}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:S)-[e:E1]->(:A) RETURN e.w"), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:RP) RETURN n.p, n.q"), (Rows{{7, 8}}));
}

// `USING PERIODIC COMMIT` and `CALL {} IN TRANSACTIONS` flush the diff-engine transaction mid-query through
// query::DbAccessor::PeriodicCommit, a different path from Interpreter::Commit's end-of-query boundary (and two
// different cursors reaching it). Every flushed batch must be captured into the branch's change-log, else it is
// silently lost on re-checkout/restart while in-session reads still look fine.
TEST_F(VersioningGatedTest, BranchPeriodicCommitWritesToBranch) {
  struct Case {
    const char *label;
    const char *query;
    int64_t rows;
  };

  static constexpr Case cases[] = {
      {"Batch", "USING PERIODIC COMMIT 10 UNWIND range(1, 100) AS x CREATE (:Batch {id: x})", 100},
      {"Batch2", "UNWIND range(1, 20) AS x CALL { WITH x CREATE (:Batch2 {id: x}) } IN TRANSACTIONS OF 5 ROWS", 20},
  };

  Fork();

  for (const auto &c : cases) {
    SCOPED_TRACE(c.label);
    const auto count = "MATCH (n:" + std::string{c.label} + ") RETURN count(n)";

    faker.Interpret(c.query);
    EXPECT_EQ(IntRows(faker, count), (Rows{{c.rows}})) << "all periodically committed rows must be visible in-session";

    faker.Interpret("CHECKOUT BRANCH 'main'");
    EXPECT_EQ(IntRows(faker, count), (Rows{{0}})) << "main never sees the branch's batches";

    faker.Interpret("CHECKOUT BRANCH 'b'");
    EXPECT_EQ(IntRows(faker, count), (Rows{{c.rows}})) << "the batches must survive a re-checkout replay";
  }
}

// Graph Versioning v1, chunk 9b (R17): locks in the BRANCHES-EXIST guard on STORAGE MODE, RECOVER
// SNAPSHOT, and DROP GRAPH -- these are DB-global, destructive/wholesale-replacing operations that
// would strand every branch's fork-point base (the historical_ storage a branch's diff engine reads
// through) if allowed to run. Chunk 8's guard (StatementsRejectedOnBranch, above) only fires while THIS
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
// caveat as StatementsRejectedOnBranch's own comment) -- it shares the identical
// `version_store()->Empty()` guard/exception path as the two ops asserted below, so no separate
// coverage gap is introduced.
TEST_F(VersioningGatedTest, BranchExistenceRejectsStorageModeAndDropGraph) {
  faker.Interpret("CREATE (:A {x:1})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  // Deliberately DO NOT checkout 'b' -- CREATE BRANCH does not auto-checkout (verified above), so
  // the session is already on main here. The branch merely EXISTS.
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());

  // R17: rejected because branch 'b' EXISTS, even though this session is on main.
  for (const auto &[query, message] : {std::pair<const char *, const char *>{"STORAGE MODE IN_MEMORY_ANALYTICAL",
                                                                              "Cannot switch storage mode while "
                                                                              "versioning branches exist"},
                                        {"DROP GRAPH", "Cannot drop the graph while versioning branches exist"},
                                        {"CREATE ENUM Status VALUES { Good, Bad }",
                                         "Cannot create an enum while versioning branches exist"},
                                        {"RECOVER SNAPSHOT \"/nonexistent/snapshot\"",
                                         "Cannot recover a snapshot while versioning branches exist"}}) {
    try {
      faker.Interpret(query);
      FAIL() << "must be rejected while a branch exists: " << query;
    } catch (const memgraph::query::QueryRuntimeException &e) {
      EXPECT_THAT(e.what(), ::testing::HasSubstr(message)) << query;
    }
  }

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
  faker.Interpret("DROP GRAPH");
  EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n)"), (Rows{{0}})) << "DROP GRAPH must actually run once no branch exists";
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
TEST_F(VersioningGatedTest, UsingVersionMainReadsMainWhileOnBranch) {
  faker.Interpret("CREATE (:MainOnly {v:1})");
  Fork();
  faker.Interpret("CREATE (:BranchOnly {v:2})");  // branch-native

  // Plain read: session-routed to branch 'b' -- sees the union (MainOnly inherited + BranchOnly).
  {
    EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n) AS c"), (Rows{{2}}));
  }

  // USING VERSION 'main': routes THIS query to main's own real storage -- MainOnly only, the
  // branch-native BranchOnly must not be visible.
  {
    EXPECT_EQ(IntRows(faker, "USING VERSION 'main' MATCH (n) RETURN count(n) AS c"), (Rows{{1}}));
  }

  // The override was per-query only -- the session is still checked out on 'b', and an unqualified
  // read goes right back to seeing both.
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  {
    EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n) AS c"), (Rows{{2}}));
  }
}

// `USING VERSION '<name>'` naming where ordinary routing already sends the query is a harmless no-op that must keep
// working: the session's own checked-out branch (the write lands on the branch, isolated from main exactly as an
// unqualified branch write), and 'main' on a session that is on main (after an explicit CHECKOUT BRANCH 'main' it is
// un-engaged and writable, so the override is redundant but not broken). The still-real escape hatch -- 'main' while
// checked out on a branch -- is UsingVersionMainWriteWhileOnBranchLandsOnMainOnly.
TEST_F(VersioningGatedTest, UsingVersionTargetsSessionOrMain) {
  Fork();

  faker.Interpret("USING VERSION 'b' CREATE (:OnB {v:5})");
  EXPECT_EQ(IntRows(faker, "MATCH (n:OnB) RETURN n.v"), (Rows{{5}}));

  faker.Interpret("CHECKOUT BRANCH 'main'");  // un-engages; back on main, writable again
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  EXPECT_EQ(IntRows(faker, "MATCH (n:OnB) RETURN count(n)"), (Rows{{0}}))
      << "USING VERSION naming the session's own branch must not leak to main";

  faker.Interpret("CREATE (:PlainWrite {v:6})");
  faker.Interpret("USING VERSION 'main' CREATE (:Allowed {v:7})");
  EXPECT_EQ(IntRows(faker, "MATCH (n:Allowed) RETURN n.v"), (Rows{{7}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:PlainWrite) RETURN n.v"), (Rows{{6}}));
}

// Step 2 boundary: naming any branch OTHER than 'main' or the session's own currently-checked-out
// branch is not yet supported and must throw utils::NotYetImplemented (interpreter.cpp's Prepare,
// the `else` arm of the USING VERSION resolution -- same exception type as the pre-existing
// ShowBranchDiffValidatesThenFlagsNotYetImplemented precedent above, not query::NotYetImplemented).
// The throwing query must not disturb the session, which stays perfectly usable on its own branch
// afterward.
TEST_F(VersioningGatedTest, UsingVersionOtherBranchNotYetImplemented) {
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CREATE BRANCH 'c' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  ASSERT_THROW(faker.Interpret("USING VERSION 'c' MATCH (n) RETURN n"), memgraph::utils::NotYetImplemented);

  // Session still usable on 'b' afterward.
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  faker.Interpret("MATCH (n) RETURN n");
}

// Regression lock for the force-main routing: a `USING VERSION 'main'` write issued while checked out on 'b'
// goes to main's own storage only (the session's branch_context() is non-null, but this transaction never runs
// against the diff engine), while a plain write goes to the branch only.
TEST_F(VersioningGatedTest, UsingVersionMainWriteWhileOnBranchLandsOnMainOnly) {
  Fork();

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
    EXPECT_EQ(stream.GetResults().size(), 0U) << "the branch write must not be visible on main";
  }

  // Back on the branch: :M was created on main after the fork, so the branch does not see it.
  faker.Interpret("CHECKOUT BRANCH 'b'");
  {
    auto stream = faker.Interpret("MATCH (n:M) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "a force-main write must not appear on the branch";
  }
  {
    EXPECT_EQ(IntRows(faker, "MATCH (n:BranchNode) RETURN n.v"), (Rows{{2}}));
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
TEST_F(VersioningGatedTest, BranchQueryPlanCacheServesCorrectResultsAcrossRepeatedRuns) {
  faker.Interpret("CREATE (:User {id: 1, tag: 'main'})");
  faker.Interpret("CREATE (:User {id: 2, tag: 'main'})");
  faker.Interpret("CREATE (:User {id: 3, tag: 'main'})");

  Fork();

  // The index is created on main AFTER the fork, so the branch has no such index: main's cached plan for the query
  // below is a property-index scan, which -- reused on the branch -- would scan the diff engine's empty index.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("CREATE INDEX ON :User(id)");
  ASSERT_EQ(IntRows(faker, "MATCH (n:User {id: 1}) RETURN n.id"), (Rows{{1}}));
  ASSERT_TRUE(PlanHasOperator(ExplainText(faker, "MATCH (n:User {id: 1}) RETURN n.tag"), "ScanAllByLabelProperties"));
  faker.Interpret("CHECKOUT BRANCH 'b'");
  EXPECT_FALSE(PlanHasOperator(ExplainText(faker, "MATCH (n:User {id: 1}) RETURN n.tag"), "ScanAllByLabelProperties"))
      << "the branch must plan against its own (index-less) view, not reuse main's cached index-scan plan";

  // First (cold) run: fork-state row must be visible through the branch's diff engine -- this is
  // exactly the hazard the old branch-context guard existed to prevent (a main-shaped plan reused
  // on a branch would instead scan the diff engine's own empty index and return 0 rows here).
  {
    auto stream = faker.Interpret("MATCH (n:User {id: 1}) RETURN n.tag");
    ASSERT_EQ(stream.GetResults().size(), 1U)
        << "fork-state row must be visible on the branch -- must NOT be the old 0-rows hazard";
    EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "main");
  }
  EXPECT_FALSE(PlanHasOperator(ExplainText(faker, "MATCH (n:User {id: 1}) RETURN n.tag"), "ScanAllByLabelProperties"));

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
TEST_F(VersioningGatedTest, UsingVersionMainDoesNotPolluteBranchPlanCache) {
  faker.Interpret("CREATE (:User {id: 1, tag: 'main'})");

  Fork();

  // Main gains an index after the fork that the branch does not have, so a main-shaped plan is a property-index scan
  // and a branch-shaped one is not: the two plans are distinguishable if one is served from the other's cache.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("CREATE INDEX ON :User(id)");
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
  EXPECT_FALSE(PlanHasOperator(ExplainText(faker, "MATCH (n:User {id: 100}) RETURN n.tag"), "ScanAllByLabelProperties"))
      << "the branch plan must not be main's index-scan plan";

  // Reverse direction: a second force-main query must not be answered by the branch-cached plan
  // either -- main must still correctly report 0 rows.
  {
    auto stream = faker.Interpret("USING VERSION 'main' MATCH (n:User {id: 100}) RETURN n.tag");
    EXPECT_EQ(stream.GetResults().size(), 0U)
        << "force-main path must not be polluted by the branch-cached plan either";
  }
}

// Bug fix (double-delete-on-a-branch crash): two Delete operators in one query each call DbAccessor::DetachDelete once;
// the first tombstones the entity, the second's accessor still points at the same gid, and CowEdge/CowVertex's
// idempotency check (a diff-engine miss at View::NEW) cannot tell "already deleted" from "never COW'd", so it used to
// fall through to CreateEdgeEx/CreateVertexEx at a gid the diff engine still physically held and abort the process
// (MG_ASSERT). On `main` re-deleting an already-deleted entity in the same query is a no-op; the branch must match.
// No RETURN clause: the point is that the call returns at all, state is checked by the follow-up reads.
TEST_F(VersioningGatedTest, BranchDoubleDeleteIsNoOpNotCrash) {
  faker.Interpret("CREATE (:A)-[:R]->(:B)");
  faker.Interpret("CREATE (:Iso {tag:1})");
  Fork();

  faker.Interpret("MATCH (:A)-[r:R]->(:B) DETACH DELETE r DELETE r");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[e:R]->() RETURN count(e)"), (Rows{{0}}))
      << "the double-deleted edge must be gone, exactly as a single delete";
  EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE n:A OR n:B RETURN count(n)"), (Rows{{2}}))
      << "both endpoints (:A and :B) must survive -- edge-only delete";

  faker.Interpret("MATCH (n:Iso) DETACH DELETE n DELETE n");
  EXPECT_EQ(IntRows(faker, "MATCH (n:Iso) RETURN count(n)"), (Rows{{0}}))
      << "the double-deleted vertex must be gone, exactly as a single delete";
}

// D12: the diff engine mirrors no constraints, so a branch commit validates main's existence constraints on the
// reconstructed (fork + stub) view of every vertex the transaction touched or created (type constraints are checked at
// the statement, see BranchTypeConstraintViolationsRaiseAtTheStatement). A violation fails the commit with main's own
// error and leaves the branch unchanged; main is never affected. Unique constraints are not enforced.
TEST_F(VersioningGatedTest, BranchCommitValidatesExistenceConstraints) {
  faker.Interpret("CREATE CONSTRAINT ON (n:P) ASSERT EXISTS (n.id)");
  faker.Interpret("CREATE CONSTRAINT ON (n:P) ASSERT n.age IS TYPED INTEGER");
  faker.Interpret("CREATE (:P {id: 1, age: 10}), (:P {id: 2, age: 20}), (:Q {tag: 'q'})");
  Fork();

  auto const expect_fail = [&](std::string const &query, std::string const &message) {
    try {
      faker.Interpret(query);
      FAIL() << "expected a constraint violation for: " << query;
    } catch (const memgraph::utils::BasicException &e) {
      EXPECT_THAT(std::string{e.what()}, ::testing::HasSubstr(message)) << query;
    }
  };
  auto const count = [&](std::string const &query) {
    auto stream = faker.Interpret(query);
    EXPECT_EQ(stream.GetResults().size(), 1U) << query;
    return stream.GetResults().at(0).at(0).ValueInt();
  };

  // (a) REMOVE of a constrained property on a fork vertex.
  expect_fail("MATCH (n:P {id: 1}) REMOVE n.id", "Unable to commit due to existence constraint violation on :P(id)");
  EXPECT_EQ(count("MATCH (n:P) RETURN count(n.id)"), 2) << "the failed commit must leave the fork vertex unchanged";

  // (b) Adding a constrained label to a fork vertex that lacks the property.
  expect_fail("MATCH (q:Q) SET q:P", "Unable to commit due to existence constraint violation on :P(id)");
  EXPECT_EQ(count("MATCH (n:P) RETURN count(n)"), 2);

  // (c) Creating a constrained vertex without the property.
  expect_fail("CREATE (:P {age: 1})", "Unable to commit due to existence constraint violation on :P(id)");
  EXPECT_EQ(count("MATCH (n:P) RETURN count(n)"), 2);

  // (d) Valid changes commit.
  faker.Interpret("MATCH (n:P {id: 1}) SET n.age = 11, n.extra = 'e'");
  faker.Interpret("MATCH (q:Q) SET q:P, q.id = 5");
  faker.Interpret("CREATE (:P {id: 9, age: 3})");
  EXPECT_EQ(count("MATCH (n:P) RETURN count(n)"), 4);
  EXPECT_EQ(count("MATCH (n:P {id: 1}) RETURN n.age"), 11);
  faker.Interpret("MATCH (n:P {id: 1}) REMOVE n.age");  // type constraints do not enforce existence
  EXPECT_EQ(count("MATCH (n:P {id: 1}) RETURN count(n.age)"), 0);

  // A vertex deleted in the same transaction is not validated.
  faker.Interpret("MATCH (n:P {id: 2}) REMOVE n.id DELETE n");
  EXPECT_EQ(count("MATCH (n:P) RETURN count(n)"), 3);

  // (e) Explicit transaction: the violation is detected at COMMIT.
  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (n:P {id: 9}) REMOVE n.id");
  expect_fail("COMMIT", "Unable to commit due to existence constraint violation on :P(id)");
  faker.Interpret("ROLLBACK");
  EXPECT_EQ(count("MATCH (n:P) RETURN count(n.id)"), 3);

  // PERIODIC COMMIT commits through a different path and is validated too.
  expect_fail("USING PERIODIC COMMIT 2 UNWIND range(1, 5) AS x CREATE (:P {age: x})",
              "PeriodicCommit failed: Unable to commit due to constraint violation");
  EXPECT_EQ(count("MATCH (n:P) RETURN count(n)"), 3);
  faker.Interpret("USING PERIODIC COMMIT 2 UNWIND range(1, 5) AS x CREATE (:P {id: 100 + x, age: x})");
  EXPECT_EQ(count("MATCH (n:P) RETURN count(n)"), 8);

  // (f) Main is unaffected throughout.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_EQ(count("MATCH (n:P) RETURN count(n)"), 2);
  EXPECT_EQ(count("MATCH (n:P {id: 1}) RETURN n.age"), 10);
  EXPECT_EQ(count("MATCH (n:Q) RETURN count(n)"), 1);
  EXPECT_EQ(count("MATCH (n:P:Q) RETURN count(n)"), 0);
}

// D14: a branch behaves exactly like main, so a type-constraint violation is raised at the statement (not at COMMIT)
// with main's own message, on fork vertices, native vertices and CREATE/MERGE alike.
TEST_F(VersioningGatedTest, BranchTypeConstraintViolationsRaiseAtTheStatement) {
  faker.Interpret("CREATE CONSTRAINT ON (n:P) ASSERT n.age IS TYPED INTEGER");
  faker.Interpret("CREATE (:P {id: 1, age: 10}), (:Q {id: 2, age: 'x'})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");

  auto const outcome = [&](std::string const &query) -> std::string {
    try {
      faker.Interpret(query);
      return "ok";
    } catch (const memgraph::utils::BasicException &e) {
      return e.what();
    }
  };
  auto const count = [&](std::string const &query) {
    auto stream = faker.Interpret(query);
    EXPECT_EQ(stream.GetResults().size(), 1U) << query;
    return stream.GetResults().at(0).at(0).ValueInt();
  };
  constexpr auto kMessage = "IS TYPED INTEGER violation on P(age)";
  std::vector<std::string> const statements{
      "MATCH (n:P) SET n.age = 'x'",
      "MATCH (n:P) SET n += {age: 'x'}",
      "MATCH (n:P) SET n = {age: 'x'}",
      "MATCH (q:Q) SET q:P",
      "CREATE (:P {age: 'x'})",
      "CREATE (:P:Z {age: 'x'})",
      "MERGE (:P {age: 'x'})",
  };

  // Reference messages from main.
  std::vector<std::string> main_messages;
  for (auto const &statement : statements) {
    main_messages.push_back(outcome(statement));
    EXPECT_THAT(main_messages.back(), ::testing::HasSubstr(kMessage)) << statement;
  }

  faker.Interpret("CHECKOUT BRANCH 'b'");
  for (size_t i = 0; i < statements.size(); ++i) {
    EXPECT_EQ(outcome(statements[i]), main_messages[i]) << statements[i];
  }
  EXPECT_EQ(count("MATCH (n:P) RETURN count(n)"), 1) << "failed statements must leave no partial write";
  EXPECT_EQ(count("MATCH (n:P) RETURN n.age"), 10);
  EXPECT_EQ(count("MATCH (q:Q) RETURN count(q)"), 1);
  EXPECT_EQ(count("MATCH (q:P) RETURN count(q)"), 1) << "SET q:P must not have added the label";

  // Valid writes still pass, on a native vertex too (created on the branch), and a native vertex is checked.
  faker.Interpret("CREATE (:P {id: 3, age: 3}), (:R {age: 'x'})");
  faker.Interpret("MATCH (n:P {id: 3}) SET n.age = 4");
  EXPECT_EQ(count("MATCH (n:P {id: 3}) RETURN n.age"), 4);
  EXPECT_EQ(outcome("MATCH (n:P {id: 3}) SET n.age = 'x'"), main_messages[0]);
  EXPECT_EQ(outcome("MATCH (r:R) SET r:P"), main_messages[3]);
  EXPECT_EQ(count("MATCH (n:P {id: 3}) RETURN n.age"), 4);
  faker.Interpret("MATCH (n:P {id: 3}) REMOVE n.age");  // null never violates
  EXPECT_EQ(outcome("MATCH (n:P {id: 3}) SET n.age = 7"), "ok");

  // Explicit transaction: the statement itself fails, and everything after it behaves exactly as on main.
  auto const explicit_txn_trace = [&]() {
    std::vector<std::string> trace;
    trace.push_back(outcome("BEGIN"));
    trace.push_back(outcome("MATCH (n:P {id: 1}) SET n.age = 'x'"));
    trace.push_back(outcome("MATCH (n:P {id: 1}) RETURN n.age"));
    trace.push_back(outcome("COMMIT"));
    outcome("ROLLBACK");
    return trace;
  };
  auto const branch_trace = explicit_txn_trace();
  faker.Interpret("CHECKOUT BRANCH 'main'");
  auto const main_trace = explicit_txn_trace();
  EXPECT_EQ(branch_trace, main_trace);
  EXPECT_THAT(branch_trace.at(1), ::testing::HasSubstr(kMessage)) << "the SET must fail inside the txn, not at COMMIT";
  EXPECT_EQ(count("MATCH (n:P {id: 1}) RETURN n.age"), 10);
  faker.Interpret("CHECKOUT BRANCH 'b'");
  EXPECT_EQ(count("MATCH (n:P {id: 1}) RETURN n.age"), 10);
}

// Slice 2 (deletes live in diff-engine MVCC): DELETE of a fork vertex marks its stub instead of recording a gid in a
// side set. The vertex is gone from scans, lookups and aggregates on the branch and untouched on main.
TEST_F(VersioningGatedTest, BranchDeleteForkVertexIsGoneOnBranchAndPresentOnMain) {
  faker.Interpret("CREATE (:K {i:1}), (:K {i:2}), (:K {i:3})");
  Fork();

  faker.Interpret("MATCH (n:K {i:2}) DELETE n");

  {
    auto stream = faker.Interpret("MATCH (n:K) RETURN count(n) AS c");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 2);
  }
  {
    auto stream = faker.Interpret("MATCH (n) RETURN n.i AS i ORDER BY i");
    ASSERT_EQ(stream.GetResults().size(), 2U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 1);
    EXPECT_EQ(stream.GetResults()[1][0].ValueInt(), 3);
  }
  {
    auto stream = faker.Interpret("MATCH (n:K {i:2}) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "the deleted vertex must not be found by a property scan";
  }

  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN count(n) AS c"), (Rows{{3}})) << "main must be untouched";
  }
}

// DETACH DELETE of a fork vertex writes only the vertex marker. Its fork neighbours -- never touched themselves -- must
// stop seeing the incident edges in both directions, in expansions, degrees and edge scans.
TEST_F(VersioningGatedTest, BranchDetachDeleteForkVertexHidesIncidentEdgesFromNeighbours) {
  faker.Interpret("CREATE (:N {n:'a'})-[:R]->(:X)-[:S]->(:N {n:'b'})");
  Fork();

  faker.Interpret("MATCH (x:X) DETACH DELETE x");

  {
    auto stream = faker.Interpret("MATCH (a:N {n:'a'})-[r]->() RETURN r");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "OUT expansion from the neighbour must not see the edge";
  }
  {
    auto stream = faker.Interpret("MATCH (b:N {n:'b'})<-[r]-() RETURN r");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "IN expansion from the neighbour must not see the edge";
  }
  {
    auto stream = faker.Interpret("MATCH (n:N) OPTIONAL MATCH (n)-[r]-() RETURN n.n AS n, count(r) AS d ORDER BY n");
    ASSERT_EQ(stream.GetResults().size(), 2U);
    EXPECT_EQ(stream.GetResults()[0][1].ValueInt(), 0);
    EXPECT_EQ(stream.GetResults()[1][1].ValueInt(), 0);
  }
  {
    EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() RETURN count(r) AS c"), (Rows{{0}}))
        << "the edge scan must not see edges of a deleted vertex";
  }

  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    auto stream = faker.Interpret("MATCH (n:N) OPTIONAL MATCH (n)-[r]-() RETURN n.n AS n, count(r) AS d ORDER BY n");
    ASSERT_EQ(stream.GetResults().size(), 2U);
    EXPECT_EQ(stream.GetResults()[0][1].ValueInt(), 1);
    EXPECT_EQ(stream.GetResults()[1][1].ValueInt(), 1);
  }
  {
    EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() RETURN count(r) AS c"), (Rows{{2}})) << "main must be untouched";
  }
}

// Plain DELETE of a fork vertex that still has a visible edge fails exactly like main; once the edge is deleted in the
// same statement it succeeds.
TEST_F(VersioningGatedTest, BranchPlainDeleteForkVertexWithEdgesFailsLikeMain) {
  faker.Interpret("CREATE (:A)-[:R]->(:B)");
  Fork();

  ASSERT_THROW(faker.Interpret("MATCH (a:A) DELETE a"), memgraph::query::RemoveAttachedVertexException);
  EXPECT_EQ(IntRows(faker, "MATCH (a:A)-[:R]->(:B) RETURN count(a)"), (Rows{{1}})) << "the failed DELETE must change nothing";

  faker.Interpret("MATCH (a:A)-[r:R]->(:B) DELETE r, a");
  {
    auto stream = faker.Interpret("MATCH (n) RETURN labels(n) AS l");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueList().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueList()[0].ValueString(), "B") << "the survivor must be :B";
  }
  {
    auto stream = faker.Interpret("MATCH ()-[r:R]->() RETURN r");
    EXPECT_EQ(stream.GetResults().size(), 0U);
  }
}

// DELETE of a single fork edge lists its gid on both endpoint stubs: it disappears from expansions in both directions,
// from edge scans and from counts, while a parallel edge survives and main is untouched.
TEST_F(VersioningGatedTest, BranchDeleteSingleForkEdgeIsGoneInBothDirectionsAndScans) {
  faker.Interpret("CREATE (a:A), (b:B), (a)-[:R {w:1}]->(b), (a)-[:R {w:2}]->(b)");
  Fork();

  faker.Interpret("MATCH ()-[r:R {w:1}]->() DELETE r");

  for (const char *query : {"MATCH (:A)-[r:R]->(:B) RETURN r.w AS w",
                            "MATCH (:B)<-[r:R]-(:A) RETURN r.w AS w",
                            "MATCH ()-[r:R]->() RETURN r.w AS w"}) {
    SCOPED_TRACE(query);
    EXPECT_EQ(IntRows(faker, query), (Rows{{2}}));
  }
  {
    EXPECT_EQ(IntRows(faker, "MATCH (a:A) OPTIONAL MATCH (a)-[r]->() RETURN count(r) AS c"), (Rows{{1}}));
  }

  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r) AS c"), (Rows{{2}})) << "main must be untouched";
}

// Deletes are ordinary diff-engine writes: an explicit transaction that deletes a fork vertex and a fork edge and then
// rolls back leaves everything visible again, with no side state to undo.
TEST_F(VersioningGatedTest, BranchDeleteThenRollbackRestoresEverything) {
  faker.Interpret("CREATE (:K {i:1})-[:R]->(:K {i:2})-[:R]->(:K {i:3})");
  Fork();

  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (n:K {i:3}) DETACH DELETE n");
  faker.Interpret("MATCH (:K {i:1})-[r:R]->(:K {i:2}) DELETE r");
  {
    EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN count(n) AS c"), (Rows{{2}}))
        << "the transaction must see its own delete";
  }
  {
    EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r) AS c"), (Rows{{0}}));
  }
  faker.Interpret("ROLLBACK");

  {
    EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN count(n) AS c"), (Rows{{3}}))
        << "a rolled-back delete must leave every vertex visible";
  }
  {
    EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r) AS c"), (Rows{{2}}))
        << "a rolled-back delete must leave every edge visible";
  }
}

namespace {
struct DeletedReadCase {
  const char *name;
  const char *seed;
  // Committed on the branch before `query`; empty for none.
  std::string_view pre_write;
  const char *query;
  // Substring of the raised error: identifies a deleted-object read rather than any other failure.
  const char *message;
  // Run after the failed statement; must return one integer row equal to `probe_expected`.
  std::string_view probe;
  int64_t probe_expected;
};

constexpr DeletedReadCase kDeletedReadCases[] = {
    {"VertexProperty",
     "CREATE (:P {prop: 1})",
     "",
     "MATCH (n:P) DETACH DELETE n RETURN n.prop AS prop",
     "deleted object",
     "MATCH (n:P) RETURN n.prop",
     1},
    {"VertexLabels",
     "CREATE (:P:Q)",
     "",
     "MATCH (n:P) DETACH DELETE n RETURN labels(n)",
     "deleted node",
     "MATCH (n:P:Q) RETURN count(n)",
     1},
    {"EdgeProperty",
     "CREATE (:S)-[:E {w: 1}]->(:A)",
     "",
     "MATCH (:S)-[e:E]->(:A) DELETE e RETURN e.w AS w",
     "deleted object",
     "MATCH (:S)-[e:E]->(:A) RETURN e.w",
     1},
    {"SetThenReturn",
     "CREATE (:P {prop: 1})",
     "",
     "MATCH (n:P) DETACH DELETE n SET n.prop = 2 RETURN n.prop AS prop",
     "deleted object",
     "MATCH (n:P) RETURN n.prop",
     1},
    {"RemoveThenReturn",
     "CREATE (:P {prop: 1})",
     "",
     "MATCH (n:P) DETACH DELETE n REMOVE n.prop RETURN n.prop AS prop",
     "deleted object",
     "MATCH (n:P) RETURN n.prop",
     1},
    {"SetLabelThenReturn",
     "CREATE (:P {prop: 1})",
     "",
     "MATCH (n:P) DETACH DELETE n SET n:X RETURN labels(n)",
     "deleted node",
     "MATCH (n:P) WHERE NOT 'X' IN labels(n) RETURN n.prop",
     1},
    // The edge is already diff-resident (modified) when it is deleted: the diff-resident short-circuit and the
    // branched()-bit gate must both defer to the tombstone guard, and the aborted DELETE must leave the edge alone.
    {"EdgeModifiedThenDeleted",
     "CREATE (:S)-[:E {w:1}]->(:A)",
     "MATCH (:S)-[e:E]->(:A) SET e.w = 42",
     "MATCH (:S)-[e:E]->(:A) DELETE e RETURN e.w AS w",
     "deleted object",
     "MATCH (:S)-[e:E]->(:A) RETURN e.w",
     42},
};

class VersioningDeletedReadTest : public VersioningGatedTest, public ::testing::WithParamInterface<DeletedReadCase> {};
}  // namespace

// Reading, in the same query, an entity deleted earlier in it raises exactly as on main (gql_behave delete.feature):
// the branch read methods consult the tombstone at View::NEW instead of falling through to the stale fork copy, whether
// the read follows the DELETE directly or a SET/REMOVE after it (the no-RETURN forms stay a silent no-op, see
// BranchDeleteThenWritePropertyIsNoOpNotError).
TEST_P(VersioningDeletedReadTest, BranchReadOfEntityDeletedInSameQueryRaises) {
  const auto &c = GetParam();

  faker.Interpret(c.seed);
  Fork();
  if (!c.pre_write.empty()) faker.Interpret(std::string{c.pre_write});

  try {
    faker.Interpret(c.query);
    FAIL() << "reading an entity deleted earlier in the SAME query must raise, exactly as on main -- the pre-fix bug "
              "silently returned the stale fork value";
  } catch (const memgraph::query::QueryRuntimeException &e) {
    EXPECT_THAT(e.what(), ::testing::HasSubstr(c.message));
  }

  EXPECT_EQ(IntRows(faker, std::string{c.probe}), (Rows{{c.probe_expected}}))
      << "the statement threw, so its transaction aborted and the pending tombstone must be discarded";
}

INSTANTIATE_TEST_SUITE_P(Reads, VersioningDeletedReadTest, ::testing::ValuesIn(kDeletedReadCases),
                         [](const ::testing::TestParamInfo<DeletedReadCase> &info) {
                           return std::string{info.param.name};
                         });

// Write-path regression guard: SET/REMOVE following a DETACH DELETE in the SAME query (no RETURN)
// must remain a silent no-op -- exactly as on main (gql_behave delete.feature, "the result should
// be empty", NO exception for the no-RETURN form). The write path (CowVertex/CowIfNeeded) was not
// touched by the S2 read-path fix; this pins down that the new view-gated read guards did not
// accidentally change write behavior. Param: the write operation (SET or REMOVE).
// (Collapses BranchDeleteThenSetPropertyIsNoOpNotError + BranchDeleteThenRemovePropertyIsNoOpNotError.)
TEST_F(VersioningGatedTest, BranchDeleteThenWritePropertyIsNoOpNotError) {
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
    Fork();

    auto stream = faker.Interpret(c.delete_and_write);
    EXPECT_EQ(stream.GetResults().size(), 0U) << "must be a silent no-op, matching main -- no exception, no rows";

    auto vertices = faker.Interpret("MATCH (n:P) RETURN n");
    EXPECT_EQ(vertices.GetResults().size(), 0U)
        << "the vertex must still be gone -- the " << c.name << " must not have resurrected it";
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

// Locks the multi-index mirror fix: MirrorMainIndexDefinitionsIntoDiffEngine gives each mirrored
// index its OWN fresh ReadOnlyAccess (branch_engine.cpp's `commit_one` helper) precisely because
// InMemoryStorage::InMemoryAccessor::CreateIndex calls DowngradeToReadIfValid() internally, which
// downgrades the access from READ_ONLY to READ after the FIRST index -- a second CreateIndex
// reusing that same (now-downgraded) access used to trip CreateIndex's own
// `MG_ASSERT(type() == UNIQUE || type() == READ_ONLY)` and abort the process. Two single-property
// label indexes at fork_ts is exactly the case that used to hit that abort.
TEST_F(VersioningGatedTest, BranchTwoLabelPropertyIndexesOrExpressionDoesNotAbort) {
  faker.Interpret("CREATE INDEX ON :Label1(id)");
  faker.Interpret("CREATE INDEX ON :Label2(id)");

  faker.Interpret("CREATE (:Label1 {id: 1})");
  faker.Interpret("CREATE (:Label2 {id: 2})");
  faker.Interpret("CREATE (:Label1:Label2 {id: 1})");

  // This is where the abort used to fire -- BuildFromFork mirrors BOTH indexes while constructing
  // the branch's diff engine.
  Fork();

  auto stream = faker.Interpret("MATCH (n:Label1|Label2) WHERE n.id < 2 RETURN n");
  EXPECT_EQ(stream.GetResults().size(), 2U) << "must return both id=1 vertices (:Label1 and :Label1:Label2) "
                                               "without throwing/aborting on the two-index mirror";
}

TEST_F(VersioningGatedTest, BranchEdgeTypeIndexScanReflectsBranchMutations) {
  faker.Interpret("CREATE EDGE INDEX ON :KNOWS");

  faker.Interpret("CREATE (a:N {n:'a'}),(b:N {n:'b'}),(c:N {n:'c'})");
  faker.Interpret("MATCH (a:N {n:'a'}),(b:N {n:'b'}) CREATE (a)-[:KNOWS]->(b)");
  faker.Interpret("MATCH (b:N {n:'b'}),(c:N {n:'c'}) CREATE (b)-[:KNOWS]->(c)");
  faker.Interpret("MATCH (a:N {n:'a'}),(c:N {n:'c'}) CREATE (a)-[:KNOWS]->(c)");
  faker.Interpret("MATCH (a:N {n:'a'}),(c:N {n:'c'}) CREATE (a)-[:LIKES]->(c)");  // distractor type

  Fork();

  // Branch mutations: tombstone the fork's a->b KNOWS edge, add a branch-native c->a KNOWS edge.
  faker.Interpret("MATCH (:N {n:'a'})-[r:KNOWS]->(:N {n:'b'}) DELETE r");
  faker.Interpret("MATCH (a:N {n:'c'}),(b:N {n:'a'}) CREATE (a)-[:KNOWS]->(b)");

  // The ORDER BY here is just to make the vector comparison deterministic -- the KNOWS scan itself
  // carries no property order.
  ExpectPlanUses(faker, "MATCH (x)-[r:KNOWS]->(y) RETURN x.n + '->' + y.n AS e ORDER BY e", "ScanAllByEdgeType");
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

namespace {
struct MirroredIndexCase {
  const char *name;
  const char *ddl;
  const char *seed;
  const char *query;
  const char *op;
  // The index scan already yields ASC order, so the planner must elide the explicit Sort.
  bool orderby_elided;
};

constexpr MirroredIndexCase kMirroredIndexCases[] = {
    {"EdgeType",
     "CREATE EDGE INDEX ON :KNOWS",
     "CREATE (:N {n:'a'})-[:KNOWS]->(:N {n:'b'})",
     "MATCH ()-[r:KNOWS]->() RETURN r",
     "ScanAllByEdgeType",
     false},
    {"EdgeTypeProperty",
     "CREATE EDGE INDEX ON :KNOWS(w)",
     "CREATE (:N {n:'a'})-[:KNOWS {w: 1}]->(:N {n:'b'})",
     "MATCH ()-[r:KNOWS]->() WHERE r.w > 0 RETURN r.w ORDER BY r.w",
     "ScanAllByEdgeTypeProperty",
     true},
    {"GlobalEdgeProperty",
     "CREATE GLOBAL EDGE INDEX ON :(w)",
     "CREATE (:N {n:'a'})-[:KNOWS {w: 1}]->(:N {n:'b'})",
     "MATCH ()-[r]->() WHERE r.w > 0 RETURN r.w ORDER BY r.w",
     "ScanAllByEdgeProperty",
     true},
    {"GlobalVertexProperty",
     "CREATE GLOBAL INDEX ON :(w)",
     "CREATE (:V {w: 1}), (:V {w: 2})",
     "MATCH (n) WHERE n.w > 0 RETURN n.w ORDER BY n.w",
     "ScanAllByVertexProperty",
     true},
    {"LabelOnly",
     "CREATE INDEX ON :L",
     "CREATE (:L {n:'a'}), (:L {n:'b'})",
     "MATCH (n:L) RETURN n",
     "ScanAllByLabel",
     false},
    {"CompositeLabelProperty",
     "CREATE INDEX ON :P(a, b)",
     "CREATE (:P {a:1, b:1}), (:P {a:2, b:2})",
     "MATCH (n:P) WHERE n.a > 0 RETURN n ORDER BY n.a, n.b",
     "ScanAllByLabelProperties",
     false},
    {"NestedLabelProperty",
     "CREATE INDEX ON :P(a.b)",
     "CREATE (:P {a:{b:1}}), (:P {a:{b:1}})",
     "MATCH (n:P) WHERE n.a.b > 0 RETURN n ORDER BY n.a.b",
     "ScanAllByLabelProperties",
     false},
};

class VersioningMirroredIndexPlanTest : public VersioningGatedTest,
                                        public ::testing::WithParamInterface<MirroredIndexCase> {};
}  // namespace

// MirrorMainIndexDefinitionsIntoDiffEngine registers every index main held at the fork on the branch's diff engine,
// one registration per index (no up-front emptiness guard, which once made a main holding only an edge index mirror
// nothing), so the planner selects the real index operator on a checked-out branch instead of ScanAll + Filter.
TEST_P(VersioningMirroredIndexPlanTest, BranchMirroredIndexIsSelectedInPlan) {
  const auto &c = GetParam();

  faker.Interpret(c.ddl);
  faker.Interpret(c.seed);
  Fork();

  const auto plan = ExplainText(faker, c.query);
  EXPECT_TRUE(PlanHasOperator(plan, c.op)) << "branch plan did not select " << c.op << "; full plan:\n" << plan;
  if (c.orderby_elided) {
    EXPECT_THAT(plan, ::testing::Not(::testing::HasSubstr("OrderBy")))
        << "branch plan should elide the explicit Sort once the index scan already produces ASC order; full plan:\n"
        << plan;
  }
}

INSTANTIATE_TEST_SUITE_P(Indexes, VersioningMirroredIndexPlanTest, ::testing::ValuesIn(kMirroredIndexCases),
                         [](const ::testing::TestParamInfo<MirroredIndexCase> &info) {
                           return std::string{info.param.name};
                         });

TEST_F(VersioningGatedTest, BranchEdgeTypePropertyIndexScanOrderedReflectsBranchMutations) {
  faker.Interpret("CREATE EDGE INDEX ON :KNOWS(w)");

  faker.Interpret("CREATE (:N {n:'a'}),(:N {n:'b'}),(:N {n:'c'}),(:N {n:'e'})");
  // Creation order deliberately does NOT match w order, so a gid-ordered (unsorted) result would be
  // trivially distinguishable from a property-ordered one.
  faker.Interpret("MATCH (a:N {n:'a'}),(b:N {n:'b'}) CREATE (a)-[:KNOWS {w: 50}]->(b)");
  faker.Interpret("MATCH (b:N {n:'b'}),(c:N {n:'c'}) CREATE (b)-[:KNOWS {w: 20}]->(c)");
  faker.Interpret("MATCH (a:N {n:'a'}),(c:N {n:'c'}) CREATE (a)-[:KNOWS {w: 40}]->(c)");
  faker.Interpret("MATCH (c:N {n:'c'}),(e:N {n:'e'}) CREATE (c)-[:KNOWS {w: 10}]->(e)");
  faker.Interpret("MATCH (a:N {n:'a'}),(e:N {n:'e'}) CREATE (a)-[:LIKES {w: 99}]->(e)");  // distractor type

  Fork();

  // Branch mutations: COW b->c's property, delete a->c, add a branch-native e->a edge.
  faker.Interpret("MATCH (:N {n:'b'})-[r:KNOWS]->(:N {n:'c'}) SET r.w = 99");
  faker.Interpret("MATCH (:N {n:'a'})-[r:KNOWS]->(:N {n:'c'}) DELETE r");
  faker.Interpret("MATCH (a:N {n:'e'}),(b:N {n:'a'}) CREATE (a)-[:KNOWS {w: 25}]->(b)");

  ExpectPlanUses(faker, "MATCH ()-[r:KNOWS]->() WHERE r.w > 0 RETURN r.w AS w ORDER BY r.w", "ScanAllByEdgeTypeProperty");
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

TEST_F(VersioningGatedTest, BranchGlobalEdgePropertyIndexScanOrderedReflectsBranchMutations) {
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");

  faker.Interpret("CREATE (:N {n:'a'}),(:N {n:'b'}),(:N {n:'c'})");
  // Creation order deliberately does NOT match w order, and edge types deliberately differ, so a
  // gid-ordered or type-filtered result would be trivially distinguishable from a correct
  // property-ordered, type-agnostic one.
  faker.Interpret("MATCH (a:N {n:'a'}),(b:N {n:'b'}) CREATE (a)-[:KNOWS {w: 50}]->(b)");
  faker.Interpret("MATCH (b:N {n:'b'}),(c:N {n:'c'}) CREATE (b)-[:LIKES {w: 20}]->(c)");
  faker.Interpret("MATCH (a:N {n:'a'}),(c:N {n:'c'}) CREATE (a)-[:KNOWS {w: 40}]->(c)");

  Fork();

  // Branch mutations: COW a cross-type (LIKES) edge, delete a KNOWS edge, add a branch-native edge
  // of a third type (FOLLOWS) to prove the global index has no type filter.
  faker.Interpret("MATCH (:N {n:'b'})-[r:LIKES]->(:N {n:'c'}) SET r.w = 99");
  faker.Interpret("MATCH (:N {n:'a'})-[r:KNOWS]->(:N {n:'c'}) DELETE r");
  faker.Interpret("MATCH (a:N {n:'c'}),(b:N {n:'a'}) CREATE (a)-[:FOLLOWS {w: 25}]->(b)");

  ExpectPlanUses(faker, "MATCH ()-[r]->() WHERE r.w > 0 RETURN r.w AS w ORDER BY r.w", "ScanAllByEdgeProperty");
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

TEST_F(VersioningGatedTest, BranchLabelOnlyIndexScanReflectsBranchMutations) {
  faker.Interpret("CREATE INDEX ON :L");

  faker.Interpret("CREATE (:L {n:'a'})");
  faker.Interpret("CREATE (:L {n:'b'})");
  faker.Interpret("CREATE (:L {n:'c'})");
  faker.Interpret("CREATE (:L {n:'e', t:1})");
  faker.Interpret("CREATE (:Other {n:'x'})");

  Fork();

  // Branch mutations: delete a fork L vertex, COW-remove the label off another fork L vertex,
  // COW-add the label onto a fork non-L vertex, and create a branch-native L vertex.
  faker.Interpret("MATCH (n:L {n:'a'}) DETACH DELETE n");
  faker.Interpret("MATCH (n:L {n:'b'}) REMOVE n:L");
  faker.Interpret("MATCH (n:Other {n:'x'}) SET n:L");
  faker.Interpret("CREATE (:L {n:'d'})");
  faker.Interpret("MATCH (n:L {n:'e'}) SET n.t = 2");  // stubbed fork vertex that keeps :L

  // The ORDER BY here is just to make the vector comparison deterministic -- the label scan itself
  // carries no property order (label-only indexes have no property order).
  auto stream = faker.Interpret("MATCH (n:L) RETURN n.n AS name ORDER BY n.n");
  const auto &rows = stream.GetResults();
  std::vector<std::string> names;
  names.reserve(rows.size());
  for (const auto &row : rows) {
    names.push_back(row[0].ValueString());
  }

  EXPECT_EQ(names, (std::vector<std::string>{"c", "d", "e", "x"}))
      << "locks the label-only set-union merge (diff-engine label index union main@fork's label "
         "index, skipping tombstoned/COW'd) including the COW label add/remove cases: 'a' was "
         "deleted on the branch and must be gone; 'b' lost the :L label via COW and must be excluded "
         "even though it is still sitting in main@fork's historical label index -- the "
         "COW-membership skip must catch it; 'x' gained the :L label via COW on a previously "
         "non-L fork vertex and must be included via the diff-engine's own label index; 'd' is "
         "branch-native and must be included; 'c' was never touched on the branch and must still "
         "surface from main@fork's index untouched; 'e' is a stubbed fork vertex that keeps :L and must "
         "appear once, at its branch value.";
  EXPECT_EQ(IntRows(faker, "MATCH (n:L) WHERE n.t IS NOT NULL RETURN n.t"), (Rows{{2}}));
}

// A fork vertex touched on a branch is a sparse stub; removals of fork state are recorded as markers
// on the stub. The tests below cover each removal kind and that no marker is ever observable.
TEST_F(VersioningGatedTest, BranchRemovePropertyOnForkVertexIsVisible) {
  faker.Interpret("CREATE (:P {id: 1, p: 7, q: 8})");
  Fork();

  faker.Interpret("MATCH (n:P) REMOVE n.p");
  faker.Interpret("MATCH (n:P) SET n.q = null");

  auto check_branch = [&] {
    auto stream = faker.Interpret("MATCH (n:P) RETURN n.p IS NULL, n.q IS NULL, keys(n), n.id");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    const auto &row = stream.GetResults()[0];
    EXPECT_TRUE(row[0].ValueBool());
    EXPECT_TRUE(row[1].ValueBool());
    EXPECT_EQ(SortedNames(row[2]), (std::vector<std::string>{"id"}));
    EXPECT_EQ(row[3].ValueInt(), 1);
  };
  check_branch();

  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_EQ(IntRows(faker, "MATCH (n:P) RETURN n.p, n.q"), (Rows{{7, 8}}));
}

TEST_F(VersioningGatedTest, BranchSetPropertyAfterRemoveRestoresIt) {
  faker.Interpret("CREATE (:P {id: 1, p: 7})");
  Fork();

  faker.Interpret("MATCH (n:P) REMOVE n.p");
  faker.Interpret("MATCH (n:P) SET n.p = 5");

  auto stream = faker.Interpret("MATCH (n:P) RETURN n.p, keys(n)");
  ASSERT_EQ(stream.GetResults().size(), 1U);
  EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 5);
  EXPECT_EQ(SortedNames(stream.GetResults()[0][1]), (std::vector<std::string>{"id", "p"}));
}

TEST_F(VersioningGatedTest, BranchRemoveThenReAddLabelLeaksNoMarker) {
  faker.Interpret("CREATE (:P:L {id: 1})");
  Fork();

  faker.Interpret("MATCH (n:P) REMOVE n:L");
  {
    auto stream = faker.Interpret("MATCH (n:P) RETURN labels(n), n:L");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(SortedNames(stream.GetResults()[0][0]), (std::vector<std::string>{"P"}));
    EXPECT_FALSE(stream.GetResults()[0][1].ValueBool());
  }
  faker.Interpret("MATCH (n:P) SET n:L");
  {
    auto stream = faker.Interpret("MATCH (n:P) RETURN labels(n), n:L");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(SortedNames(stream.GetResults()[0][0]), (std::vector<std::string>{"L", "P"}));
    EXPECT_TRUE(stream.GetResults()[0][1].ValueBool());
  }
}

TEST_F(VersioningGatedTest, BranchStubMarkersAreNeverVisibleAndReservedNamesAreRejected) {
  faker.Interpret("CREATE (:P:L {id: 1, p: 7})");
  Fork();

  faker.Interpret("MATCH (n:P) REMOVE n:L, n.p");

  auto stream = faker.Interpret("MATCH (n:P) RETURN labels(n), keys(n), properties(n), n{.*}");
  ASSERT_EQ(stream.GetResults().size(), 1U);
  const auto &row = stream.GetResults()[0];
  EXPECT_EQ(SortedNames(row[0]), (std::vector<std::string>{"P"}));
  EXPECT_EQ(SortedNames(row[1]), (std::vector<std::string>{"id"}));
  for (const auto idx : {2U, 3U}) {
    ASSERT_EQ(row[idx].ValueMap().size(), 1U);
    EXPECT_TRUE(row[idx].ValueMap().contains("id"));
  }

  ASSERT_THROW(faker.Interpret("MATCH (n:P) SET n:__mg_rm_x"), memgraph::query::QueryRuntimeException);
  ASSERT_THROW(faker.Interpret("MATCH (n:P) SET n:`__mg_rm:x`"), memgraph::query::QueryRuntimeException);
  ASSERT_THROW(faker.Interpret("MATCH (n:P) SET n:`__mg_deleted`"), memgraph::query::QueryRuntimeException);
  ASSERT_THROW(faker.Interpret("MATCH (n:P) SET n.`__mg_removed` = 1"), memgraph::query::QueryRuntimeException);
}

TEST_F(VersioningGatedTest, BranchNestedLabelPropertyIndexReflectsBranchMutations) {
  faker.Interpret("CREATE INDEX ON :P(a.b)");

  // Creation order deliberately does NOT match b-value order.
  faker.Interpret("CREATE (:P {n:'v1', a:{b:50}})");
  faker.Interpret("CREATE (:P {n:'v2', a:{b:20}})");
  faker.Interpret("CREATE (:P {n:'v3', a:{b:40}})");

  Fork();

  faker.Interpret("MATCH (n:P {n:'v2'}) SET n.a = {b: 99}");  // nested COW: (b:20) -> (b:99)
  faker.Interpret("MATCH (n:P {n:'v3'}) DETACH DELETE n");    // tombstone: (b:40) must vanish
  faker.Interpret("CREATE (:P {n:'v4', a:{b:25}})");          // branch-native: (b:25)

  ExpectPlanUses(faker, "MATCH (n:P) WHERE n.a.b > 0 RETURN n.a.b AS v ORDER BY n.a.b", "ScanAllByLabelProperties");
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

TEST_F(VersioningGatedTest, BranchHopsLimitEnforcedOnExpand) {
  // 100 disconnected 2-node edges -- an expansion that ignores the hops budget will happily
  // traverse all 100 of them.
  faker.Interpret("UNWIND range(1, 100) AS x CREATE ()-[:NEXT]->()");

  Fork();

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

TEST_F(VersioningGatedTest, BranchGetHopsCounterTracksTraversal) {
  faker.Interpret("UNWIND range(1, 100) AS x CREATE ()-[:NEXT]->()");

  Fork();

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
TEST_F(VersioningGatedTest, BranchShortestPathAcrossStubbedVertexHasConsistentIdentity) {
  // Diamond with unequal arms: the 2-hop arm 1 -> 2 -> 4 and the 3-hop arm 1 -> 5 -> 6 -> 4. The arms meet only if
  // vertex 2 has ONE identity across storages: edge 1 -> 2 is a fork edge (its endpoint 2 is the historical copy),
  // while 2 -> 4 is created on the branch after 2 was COW'd (its endpoint 2 is the diff-engine copy). A broken
  // Gid-hash would never match the two copies of 2 and the search would fall back to the 3-hop arm.
  faker.Interpret(
      "CREATE (s:Chain {id: 1}), (m:Chain {id: 2}), (x:Chain {id: 5}), (y:Chain {id: 6}), (t:Chain {id: 4}), "
      "(s)-[:R]->(m), (s)-[:R]->(x), (x)-[:R]->(y), (y)-[:R]->(t)");

  Fork();

  faker.Interpret("MATCH (n:Chain {id: 2}) SET n.tag = 'cowd'");
  faker.Interpret("MATCH (m:Chain {id: 2}), (t:Chain {id: 4}) CREATE (m)-[:R]->(t)");

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

  EXPECT_EQ(ids, (std::vector<int64_t>{1, 2, 4}))
      << "shortest path must take the 2-hop arm through the COW'd vertex id=2 (fork edge in, branch-native "
         "edge out) -- not the 3-hop arm -- a broken hash/equality contract on VertexAccessor (impl_-pointer hash vs Gid-based "
         "operator==) would mis-key STShortestPathCursor's VertexEdgeMapT, corrupting or truncating the "
         "reconstructed path.";
}

// Graph Versioning v1 (Gid-based std::hash<VertexAccessor>, vertex_accessor.hpp): companion regression to
// BranchShortestPathAcrossStubbedVertexHasConsistentIdentity above, exercising a DIFFERENT direct consumer of
// the same std::hash<VertexAccessor> specialization: SingleSourceShortestPathCursor::processed_
// (plan/operator.cpp), selected instead of STShortestPathCursor when the destination is introduced fresh
// in the SAME MATCH pattern as the traversal (not pre-bound in an earlier clause), i.e. existing_node ==
// false. Deliberately NOT written as a TypedValue-level DISTINCT/equality check: TypedValue::Hash's
// Type::Vertex branch (typed_value.cpp) independently re-derives Gid().AsUint() and never calls
// std::hash<VertexAccessor> at all, so a DISTINCT-based test would pass even without this fix and would
// not actually lock it -- this test's map is keyed DIRECTLY on VertexAccessor via the specialization.
TEST_F(VersioningGatedTest, BranchSingleSourceShortestPathAcrossStubbedVertexHasConsistentIdentity) {
  // Diamond with unequal arms: the 2-hop arm 1 -> 2 -> 4 and the 3-hop arm 1 -> 5 -> 6 -> 4. The arms meet only if
  // vertex 2 has ONE identity across storages: edge 1 -> 2 is a fork edge (its endpoint 2 is the historical copy),
  // while 2 -> 4 is created on the branch after 2 was COW'd (its endpoint 2 is the diff-engine copy). A broken
  // Gid-hash would never match the two copies of 2 and the search would fall back to the 3-hop arm.
  faker.Interpret(
      "CREATE (s:Chain {id: 1}), (m:Chain {id: 2}), (x:Chain {id: 5}), (y:Chain {id: 6}), (t:Chain {id: 4}), "
      "(s)-[:R]->(m), (s)-[:R]->(x), (x)-[:R]->(y), (y)-[:R]->(t)");

  Fork();

  faker.Interpret("MATCH (n:Chain {id: 2}) SET n.tag = 'cowd'");
  faker.Interpret("MATCH (m:Chain {id: 2}), (t:Chain {id: 4}) CREATE (m)-[:R]->(t)");

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

  EXPECT_EQ(ids, (std::vector<int64_t>{1, 2, 4}))
      << "shortest path must take the 2-hop arm through the COW'd vertex id=2 (fork edge in, branch-native "
         "edge out) -- not the 3-hop arm -- a broken hash/equality contract on VertexAccessor would mis-key "
         "SingleSourceShortestPathCursor::processed_, corrupting or truncating the reconstructed path.";
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
TEST_F(VersioningGatedTest, BranchEnumMirroredFromMainSupportsNativeCreateAndSetOnForkVertex) {
  faker.Interpret("CREATE ENUM Status VALUES { Good, Bad };");
  // 'fork' has no enum property yet (the branch-side SET adds one); 'forkenum' already holds one.
  faker.Interpret("CREATE (:X {tag: 'fork'})");
  faker.Interpret("CREATE (:X {tag: 'forkenum', status: Status::Good})");

  Fork();

  // Branch-native CREATE with an inline enum literal -- must not crash/throw, and must resolve
  // through the diff engine's own (now-mirrored) EnumStore.
  faker.Interpret("CREATE (:X {tag: 'native', status: Status::Good})");

  // SET on the still-historical fork vertex: triggers CowVertex (the pre-existing `tag` property
  // has no enum, so the COW itself succeeds), and the enum-literal SET must then resolve the same
  // way against the diff engine's mirrored EnumStore.
  faker.Interpret("MATCH (n:X {tag: 'fork'}) SET n.status = Status::Bad");

  // Stub the fork vertex that already holds an enum by writing an unrelated property.
  faker.Interpret("MATCH (n:X {tag: 'forkenum'}) SET n.other = 1");

  auto stream = faker.Interpret("MATCH (n:X) RETURN n.tag, n.status ORDER BY n.tag");
  ASSERT_EQ(stream.GetResults().size(), 3U);

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

  // ORDER BY n.tag ASC: "fork" < "forkenum" < "native" lexicographically.
  const auto &fork_row = stream.GetResults()[0];
  EXPECT_EQ(fork_row[0].ValueString(), "fork");
  EXPECT_EQ(get_enum_value_str(fork_row[1]), "Status::Bad")
      << "SET on a COW'd fork vertex must write and read back the exact enum value, not garble it";

  const auto &forkenum_row = stream.GetResults()[1];
  EXPECT_EQ(forkenum_row[0].ValueString(), "forkenum");
  EXPECT_EQ(get_enum_value_str(forkenum_row[1]), "Status::Good")
      << "a stubbed fork vertex must keep reading its pre-existing enum property exactly";

  const auto &native_row = stream.GetResults()[2];
  EXPECT_EQ(native_row[0].ValueString(), "native");
  EXPECT_EQ(get_enum_value_str(native_row[1]), "Status::Good")
      << "a branch-native CREATE with an inline enum literal must read back the exact enum value, "
         "not crash or garble it";

  faker.Interpret("CHECKOUT BRANCH 'main'");
  auto main_stream = faker.Interpret("MATCH (n:X {tag: 'forkenum'}) RETURN n.status, n.other");
  ASSERT_EQ(main_stream.GetResults().size(), 1U);
  EXPECT_EQ(get_enum_value_str(main_stream.GetResults()[0][0]), "Status::Good") << "main keeps the enum exactly";
  EXPECT_EQ(main_stream.GetResults()[0][1].type(), memgraph::communication::bolt::Value::Type::Null);
}

// Branch label+property index scans overlay a touched-set on the fork index (design R5): a stubbed fork vertex is
// rechecked on its reconstructed (label, key), and stubs that newly match are found through the diff engine's label
// and property indexes. Each case below is a distinct way a fork vertex can differ from its fork-time index entry.
namespace {
std::vector<std::string> BranchScanStrings(InterpreterFaker &faker, std::string const &query) {
  auto stream = faker.Interpret(query);
  std::vector<std::string> out;
  for (auto const &row : stream.GetResults()) out.push_back(row[0].ValueString());
  return out;
}
}  // namespace

TEST_F(VersioningGatedTest, BranchLabelPropertyPointLookupOverlay) {
  faker.Interpret("CREATE INDEX ON :L(p)");
  faker.Interpret("CREATE (:L {p: 1, tag: 'untouched'})");
  faker.Interpret("CREATE (:L {p: 1, tag: 'untouched2'})");
  faker.Interpret("CREATE (:L {p: 2, tag: 'changed'})");
  faker.Interpret("CREATE (:L {p: 3, tag: 'unlabeled'})");
  faker.Interpret("CREATE (:M {p: 4, tag: 'gained'})");
  faker.Interpret("CREATE (:L {p: 5, tag: 'other'})");
  faker.Interpret("CREATE (:L {p: 7, tag: 'deleted'})");
  Fork();

  faker.Interpret("MATCH (n:L {tag: 'changed'}) SET n.p = 20");
  faker.Interpret("MATCH (n:L {tag: 'unlabeled'}) REMOVE n:L");
  faker.Interpret("MATCH (n:M {tag: 'gained'}) SET n:L");
  faker.Interpret("MATCH (n:L {tag: 'other'}) SET n.tag = 'other2'");
  faker.Interpret("MATCH (n:L {tag: 'deleted'}) DETACH DELETE n");
  faker.Interpret("CREATE (:L {p: 6, tag: 'native'})");

  auto lookup = [&](int64_t p) {
    auto tags = BranchScanStrings(faker, "MATCH (n:L {p: " + std::to_string(p) + "}) RETURN n.tag");
    std::ranges::sort(tags);
    return tags;
  };
  using V = std::vector<std::string>;
  EXPECT_EQ(lookup(1), (V{"untouched", "untouched2"})) << "two untouched vertices with an equal key must both be found";
  EXPECT_EQ(lookup(2), V{}) << "the branch changed p away from 2: the fork index entry must not match";
  EXPECT_EQ(lookup(20), V{"changed"}) << "the branch-set value must be found although the stub is in no fork entry";
  EXPECT_EQ(lookup(3), V{}) << "a removed label must drop the fork index hit";
  EXPECT_EQ(lookup(4), V{"gained"}) << "a stub that gained :L keeps its fork p and must match";
  EXPECT_EQ(lookup(5), V{"other2"}) << "an unrelated change on a stub must not drop the vertex";
  EXPECT_EQ(lookup(6), V{"native"});
  EXPECT_EQ(lookup(7), V{}) << "a tombstoned fork vertex must not match";
  EXPECT_EQ(lookup(99), V{});

  auto plan = faker.Interpret("EXPLAIN MATCH (n:L {p: 1}) RETURN n.tag");
  std::string text;
  for (auto const &row : plan.GetResults()) text += row[0].ValueString() + "\n";
  EXPECT_THAT(text, ::testing::HasSubstr("ScanAllByLabelProperties")) << "plan must use the index operator:\n" << text;
}

TEST_F(VersioningGatedTest, BranchLabelPropertyRangeOverlayIsOrderedAcrossForkAndDiff) {
  faker.Interpret("CREATE INDEX ON :P(v)");
  faker.Interpret("CREATE (:P {v: 10, name: 'A'})");
  faker.Interpret("CREATE (:P {v: 20, name: 'B'})");
  faker.Interpret("CREATE (:P {v: 30, name: 'C'})");
  faker.Interpret("CREATE (:P {v: 40, name: 'D'})");
  faker.Interpret("CREATE (:P {v: 50, name: 'E'})");
  faker.Interpret("CREATE (:P {v: 15, name: 'G'})");
  faker.Interpret("CREATE (:Q {v: 25, name: 'F'})");
  Fork();

  faker.Interpret("MATCH (n:P {name: 'B'}) SET n.v = 45");      // moves within the range
  faker.Interpret("MATCH (n:P {name: 'D'}) SET n.note = 'x'");  // stubbed, key unchanged
  faker.Interpret("MATCH (n:P {name: 'E'}) REMOVE n:P");        // leaves the scan
  faker.Interpret("MATCH (n:Q {name: 'F'}) SET n:P");           // gains the label, v from the fork
  faker.Interpret("MATCH (n:P {name: 'G'}) DETACH DELETE n");   // in-range fork vertex deleted
  faker.Interpret("CREATE (:P {v: 35, name: 'N1'}), (:P {v: 5, name: 'N2'})");

  ExpectPlanUses(faker, "MATCH (n:P) WHERE n.v >= 10 AND n.v <= 60 RETURN n.name ORDER BY n.v", "ScanAllByLabelProperties");
  using V = std::vector<std::string>;
  EXPECT_EQ(BranchScanStrings(faker, "MATCH (n:P) WHERE n.v >= 10 AND n.v <= 60 RETURN n.name ORDER BY n.v"),
            (V{"A", "F", "C", "N1", "D", "B"}));
  EXPECT_EQ(BranchScanStrings(faker, "MATCH (n:P) WHERE n.v >= 10 AND n.v <= 60 RETURN n.name ORDER BY n.v DESC"),
            (V{"B", "D", "N1", "C", "F", "A"}));
}

TEST_F(VersioningGatedTest, BranchCompositeLabelPropertyPrefixOverlay) {
  faker.Interpret("CREATE INDEX ON :L(a, b)");
  faker.Interpret("CREATE (:L {a: 1, b: 1, t: 'x1'})");
  faker.Interpret("CREATE (:L {a: 1, b: 2, t: 'x2'})");
  faker.Interpret("CREATE (:L {a: 2, b: 5, t: 'x3'})");
  faker.Interpret("CREATE (:L {a: 1, b: 9, t: 'x4'})");
  faker.Interpret("CREATE (:L {a: 1, b: 4, t: 'x5'})");
  Fork();

  faker.Interpret("MATCH (n:L {t: 'x2'}) SET n.b = 0");      // still a = 1, moves earlier in (a, b) order
  faker.Interpret("MATCH (n:L {t: 'x3'}) SET n.a = 1");      // newly matches a = 1
  faker.Interpret("MATCH (n:L {t: 'x4'}) SET n.a = 7");      // no longer matches a = 1
  faker.Interpret("MATCH (n:L {t: 'x5'}) DETACH DELETE n");  // tombstoned
  faker.Interpret("CREATE (:L {a: 1, b: 3, t: 'n1'})");

  for (const auto *q : {"MATCH (n:L) WHERE n.a = 1 RETURN n.t ORDER BY n.a, n.b",
                        "MATCH (n:L) WHERE n.a = 1 AND n.b = 5 RETURN n.t",
                        "MATCH (n:L) WHERE n.a > 0 RETURN n.t ORDER BY n.a, n.b"}) {
    ExpectPlanUses(faker, q, "ScanAllByLabelProperties");
  }
  using V = std::vector<std::string>;
  EXPECT_EQ(BranchScanStrings(faker, "MATCH (n:L) WHERE n.a = 1 RETURN n.t ORDER BY n.a, n.b"),
            (V{"x2", "x1", "n1", "x3"}));
  EXPECT_EQ(BranchScanStrings(faker, "MATCH (n:L) WHERE n.a = 1 AND n.b = 5 RETURN n.t"), (V{"x3"}));
  EXPECT_EQ(BranchScanStrings(faker, "MATCH (n:L) WHERE n.a = 7 RETURN n.t"), (V{"x4"}));
  EXPECT_EQ(BranchScanStrings(faker, "MATCH (n:L) WHERE n.a > 0 RETURN n.t ORDER BY n.a, n.b"),
            (V{"x2", "x1", "n1", "x3", "x4"}));
}

TEST_F(VersioningGatedTest, BranchGlobalPropertyOverlay) {
  faker.Interpret("CREATE GLOBAL INDEX ON :(w)");
  faker.Interpret("CREATE (:A {w: 1, t: 'a'}), (:B {w: 2, t: 'b'}), (:C {w: 3, t: 'c'}), (:D {w: 4, t: 'd'})");
  Fork();

  faker.Interpret("MATCH (n {t: 'b'}) SET n.w = 10");
  faker.Interpret("MATCH (n {t: 'c'}) REMOVE n.w");
  faker.Interpret("MATCH (n {t: 'd'}) DETACH DELETE n");
  faker.Interpret("CREATE (:E {w: 2, t: 'e'})");

  ExpectPlanUses(faker, "MATCH (n) WHERE n.w > 0 RETURN n.t ORDER BY n.w", "ScanAllByVertexProperty");
  ExpectPlanUses(faker, "MATCH (n) WHERE n.w = 2 RETURN n.t", "ScanAllByVertexProperty");
  using V = std::vector<std::string>;
  EXPECT_EQ(BranchScanStrings(faker, "MATCH (n) WHERE n.w > 0 RETURN n.t ORDER BY n.w"), (V{"a", "e", "b"}));
  EXPECT_EQ(BranchScanStrings(faker, "MATCH (n) WHERE n.w = 2 RETURN n.t"), (V{"e"}));
  EXPECT_EQ(BranchScanStrings(faker, "MATCH (n) WHERE n.w = 10 RETURN n.t"), (V{"b"}));
}

// Graph Versioning v1, perf/versioning-branch-read D1 -- THE BLOCKER GUARD: `properties_on_edges =
// false` REFERENCE edges have no `storage::Edge` object at all (`EdgeRef` aliases a bare `Gid`
// instead of a pointer, edge_ref.hpp). `query::EdgeAccessor::MaybeBranchedHint`
// (storage/v2/edge_accessor.cpp) MUST guard `properties_on_edges` BEFORE dereferencing `edge_.ptr`,
// exactly like `Gid()` immediately above it -- a naive implementation that skipped the guard would
// alias the bare gid as a pointer and deref it, UB (the same crash class already fixed for edge-type
// indexes, commit 0c78ebee0). This fixture is VersioningGatedTest on a storage built with
// `properties_on_edges = false` (VersioningFixture<false>).
class VersioningInterpreterReferenceEdgeTest : public VersioningFixture<false> {
 protected:
  void SetUp() override {
    VersioningFixture<false>::SetUp();
    SatisfyGate();
  }
};

TEST_F(VersioningInterpreterReferenceEdgeTest, BranchReferenceEdgePropertyReadDoesNotCrashAndReturnsDefault) {
  faker.Interpret("CREATE (:S)-[:E]->(:A)");  // reference edge -- no properties possible at creation.
  Fork();
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

// ---------------------------------------------------------------------------------------------
// Multiple concurrent sessions on one branch: every session that checks a branch out attaches to the same shared
// diff engine, so cross-session visibility, isolation and write conflicts are the diff engine's own MVCC. Two
// interpreters ("A" = faker, "B" = faker2) on the same database, interleaved deterministically on one thread.
// ---------------------------------------------------------------------------------------------

namespace {
// Runs two overlapping explicit transactions: `a` executes `a_write`, then `b` executes `b_write` on the same vertex
// before `a` finishes. Returns the (mangled) type name of the exception `b`'s write raised, "" if it succeeded. Both
// transactions are rolled back, so the data is left as it was.
std::string OverlappingWriteOutcome(InterpreterFaker &a, InterpreterFaker &b, const std::string &a_write,
                                    const std::string &b_write) {
  a.Interpret("BEGIN");
  b.Interpret("BEGIN");
  a.Interpret(a_write);
  std::string outcome;
  try {
    b.Interpret(b_write);
  } catch (const std::exception &e) {
    outcome = typeid(e).name();
  }
  b.Interpret("ROLLBACK");
  a.Interpret("ROLLBACK");
  return outcome;
}
}  // namespace

class VersioningMultiSessionTest : public VersioningGatedTest {
 protected:
  InterpreterFaker faker2{&interpreter_context, db};

  void TearDown() override {
    // faker2 is destroyed before the base members; do not leave the context pointing at its auth checker.
    interpreter_context.auth_checker = &faker.auth_checker;
    VersioningGatedTest::TearDown();
  }

  // main: (:K {id:1, v:10, w:5}), (:K {id:2, v:20, w:6}); branch 'b' forked from it; both sessions attached to 'b'.
  void SetUpTwoSessionsOnBranch() {
    faker.Interpret("CREATE (:K {id:1, v:10, w:5}), (:K {id:2, v:20, w:6})");
    Fork();
    faker2.Interpret("CHECKOUT BRANCH 'b'");
    ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
    ASSERT_EQ(faker2.interpreter.current_db_.CurrentVersion(), "b");
  }

  static constexpr const char *kReadAll = "MATCH (n:K) RETURN n.id, n.v ORDER BY n.id";
};

TEST_F(VersioningGatedTest, MergeBranchIsRejectedAsUnsupported) {
  ASSERT_THROW(faker.Interpret("MERGE BRANCH 'nope'"), memgraph::query::QueryRuntimeException);
  faker.Interpret("CREATE BRANCH 'feature' FROM 'main'");
  try {
    faker.Interpret("MERGE BRANCH 'feature'");
    FAIL() << "MERGE BRANCH must be rejected";
  } catch (const memgraph::query::QueryRuntimeException &e) {
    EXPECT_THAT(std::string{e.what()}, ::testing::HasSubstr("MERGE BRANCH is not supported"));
  }
  EXPECT_TRUE(db->version_store()->Exists("feature")) << "a rejected MERGE must leave the branch untouched";
}

// (a) A's committed branch writes (SET, DELETE, CREATE) are what B sees when it checks the branch out afterwards; main
// is unchanged.
TEST_F(VersioningMultiSessionTest, SecondSessionSeesFirstSessionsCommittedWrites) {
  faker.Interpret("CREATE (:K {id:1, v:10}), (:K {id:2, v:20})");
  Fork();

  faker.Interpret("MATCH (n:K {id:1}) SET n.v = 11");
  faker.Interpret("MATCH (n:K {id:2}) DELETE n");
  faker.Interpret("CREATE (:K {id:3, v:30})");

  faker2.Interpret("CHECKOUT BRANCH 'b'");
  EXPECT_EQ(IntRows(faker2, kReadAll), (Rows{{1, 11}, {3, 30}}));

  faker2.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_EQ(IntRows(faker2, kReadAll), (Rows{{1, 10}, {2, 20}})) << "main must not see any branch write";
}

// (b) Snapshot isolation: B's open transaction keeps seeing the old value after A commits a change to the same
// vertex; a fresh transaction sees the new one.
TEST_F(VersioningMultiSessionTest, OpenTransactionKeepsItsSnapshotWhileAnotherSessionCommits) {
  SetUpTwoSessionsOnBranch();

  faker2.Interpret("BEGIN");
  EXPECT_EQ(IntRows(faker2, kReadAll), (Rows{{1, 10}, {2, 20}}));

  faker.Interpret("MATCH (n:K {id:1}) SET n.v = 11");
  EXPECT_EQ(IntRows(faker, kReadAll), (Rows{{1, 11}, {2, 20}}));

  EXPECT_EQ(IntRows(faker2, kReadAll), (Rows{{1, 10}, {2, 20}})) << "B's snapshot predates A's commit";
  faker2.Interpret("COMMIT");
  EXPECT_EQ(IntRows(faker2, kReadAll), (Rows{{1, 11}, {2, 20}})) << "a new transaction sees A's commit";
}

// (c) Two overlapping transactions writing the same fork vertex or fork edge -- whether its stub already exists or is
// first created by the race -- conflict exactly as they do on main (the second writer gets the error main raises for
// the same interleaving) and nothing crashes. A stub the rolled-back writers left behind is semantically invisible, and
// a retried write succeeds.
TEST_F(VersioningMultiSessionTest, OverlappingWritesConflictLikeMain) {
  faker.Interpret("CREATE (:K {id:1, v:10, w:5}), (:K {id:2, v:20, w:6})");
  faker.Interpret("CREATE (:A {id:1})-[:R {w:1, x:2}]->(:B {id:2})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");

  struct Case {
    const char *name;
    // Committed on the branch first, so the entity's stub already exists; empty means the race creates the stub.
    const char *stub_write;
    const char *a_write;
    const char *b_write;
  };

  static constexpr Case cases[] = {
      {"VertexFirstTouch", "", "MATCH (n:K {id:2}) SET n.v = 21", "MATCH (n:K {id:2}) SET n.w = 61"},
      {"VertexStubbed",
       "MATCH (n:K {id:1}) SET n.z = 0",
       "MATCH (n:K {id:1}) SET n.v = 11",
       "MATCH (n:K {id:1}) SET n.v = 12"},
      {"EdgeFirstTouch", "", "MATCH ()-[r:R]->() SET r.w = 10", "MATCH ()-[r:R]->() SET r.x = 20"},
      {"EdgeStubbed",
       "MATCH ()-[r:R]->() SET r.y = 0",
       "MATCH ()-[r:R]->() SET r.w = 10",
       "MATCH ()-[r:R]->() SET r.x = 20"},
  };

  auto const checkout_both = [&](const char *version) {
    faker.Interpret(std::string{"CHECKOUT BRANCH '"} + version + "'");
    faker2.Interpret(std::string{"CHECKOUT BRANCH '"} + version + "'");
  };

  for (const auto &c : cases) {
    SCOPED_TRACE(c.name);
    checkout_both("b");
    if (*c.stub_write != '\0') faker.Interpret(c.stub_write);
    const auto branch_outcome = OverlappingWriteOutcome(faker, faker2, c.a_write, c.b_write);

    checkout_both("main");
    const auto main_outcome = OverlappingWriteOutcome(faker, faker2, c.a_write, c.b_write);

    EXPECT_NE(main_outcome.find("TransactionSerializationException"), std::string::npos)
        << "sanity: main raises a serialization error for this interleaving, got '" << main_outcome << "'";
    EXPECT_EQ(branch_outcome, main_outcome);
  }

  checkout_both("b");
  EXPECT_EQ(IntRows(faker, kReadAll), (Rows{{1, 10}, {2, 20}})) << "every transaction was rolled back";
  EXPECT_EQ(IntRows(faker, "MATCH (n:K {id:2}) RETURN n.w"), (Rows{{6}})) << "a leftover stub is invisible";
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.w, r.x"), (Rows{{1, 2}}));

  EXPECT_NO_THROW(faker2.Interpret("MATCH ()-[r:R]->() SET r.x = 21"));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.w, r.x"), (Rows{{1, 21}}));
}

// (e) A's uncommitted DELETE is invisible to B until A commits; B's already-open transaction never sees it.
TEST_F(VersioningMultiSessionTest, DeleteIsInvisibleToOtherSessionsUntilCommit) {
  SetUpTwoSessionsOnBranch();

  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (n:K {id:1}) DELETE n");
  EXPECT_EQ(IntRows(faker, kReadAll), (Rows{{2, 20}})) << "A sees its own delete";

  EXPECT_EQ(IntRows(faker2, kReadAll), (Rows{{1, 10}, {2, 20}})) << "B must not see A's uncommitted delete";
  faker2.Interpret("BEGIN");
  EXPECT_EQ(IntRows(faker2, kReadAll), (Rows{{1, 10}, {2, 20}}));

  faker.Interpret("COMMIT");
  EXPECT_EQ(IntRows(faker2, kReadAll), (Rows{{1, 10}, {2, 20}})) << "B's open transaction predates the commit";
  faker2.Interpret("COMMIT");
  EXPECT_EQ(IntRows(faker2, kReadAll), (Rows{{2, 20}})) << "after A's commit a new transaction sees the delete";
}

// (f) A writes on the branch while B writes the same vertices on main: main never conflicts with the branch, and A
// keeps reading fork-time values for what it did not change.
TEST_F(VersioningMultiSessionTest, MainWritesNeverConflictWithBranchWrites) {
  faker.Interpret("CREATE (:K {id:1, v:10, w:5})");
  Fork();

  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (n:K {id:1}) SET n.v = 11");

  // B is on main: neither of these may raise a serialization error against A's open branch transaction.
  faker2.Interpret("MATCH (n:K {id:1}) SET n.v = 99");
  faker2.Interpret("MATCH (n:K {id:1}) SET n.w = 55");

  EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN n.v, n.w"), (Rows{{11, 5}}))
      << "A sees its own v and the FORK value of w, not main's later writes";
  faker.Interpret("COMMIT");
  EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN n.v, n.w"), (Rows{{11, 5}}));
  EXPECT_EQ(IntRows(faker2, "MATCH (n:K) RETURN n.v, n.w"), (Rows{{99, 55}})) << "main holds only its own writes";
}

// (g) DROP BRANCH is refused while another session is attached, and succeeds once every session has detached.
TEST_F(VersioningMultiSessionTest, DropBranchIsRefusedWhileAnotherSessionIsAttached) {
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker2.Interpret("CHECKOUT BRANCH 'b'");
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 1U);

  // A is on main: refused.
  ASSERT_THROW(faker.Interpret("DROP BRANCH 'b'"), memgraph::query::QueryRuntimeException);
  EXPECT_TRUE(db->version_store()->Exists("b"));

  // A attaches too, then drops: still refused (B is attached), and A stays on the branch.
  faker.Interpret("CHECKOUT BRANCH 'b'");
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 2U);
  ASSERT_THROW(faker.Interpret("DROP BRANCH 'b'"), memgraph::query::QueryRuntimeException);
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 2U);

  faker2.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 1U);

  // A is now the only attached session: dropping the branch it is on works and detaches it.
  faker.Interpret("DROP BRANCH 'b'");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  EXPECT_FALSE(db->version_store()->Exists("b"));
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 0U);
}

// (h) An idle session that has the branch checked out but no open transaction holds none of main's locks: main DDL
// is not blocked by it.
TEST_F(VersioningMultiSessionTest, IdleBranchSessionDoesNotBlockMainDdl) {
  SetUpTwoSessionsOnBranch();

  faker.Interpret("CHECKOUT BRANCH 'main'");  // B stays attached to 'b', idle.
  faker.Interpret("CREATE INDEX ON :K(id)");
  EXPECT_EQ(IntRows(faker, "MATCH (n:K {id:1}) RETURN n.v"), (Rows{{10}}));

  // The branch still works, unaffected by main's new index.
  EXPECT_EQ(IntRows(faker2, kReadAll), (Rows{{1, 10}, {2, 20}}));
}

// ---------------------------------------------------------------------------------------------
// Edge stubs: a fork edge whose properties change gets an empty, delta-less diff-engine edge holding only what
// changed (plus the `__mg_removed` list); reads overlay it on the fork edge.
// ---------------------------------------------------------------------------------------------

namespace {
// Keys of `properties(r)` for the single edge `query` matches.
std::set<std::string> EdgePropertyKeys(InterpreterFaker &session, const std::string &query) {
  auto stream = session.Interpret(query);
  std::set<std::string> keys;
  if (stream.GetResults().size() != 1U) return {"<not one row>"};
  for (const auto &[key, _] : stream.GetResults()[0][0].ValueMap()) keys.insert(key);
  return keys;
}
}  // namespace

// (a) SET on a fork edge is visible on the branch from both ends and in whole-property reads, leaves the edge's other
// properties intact, never shows a marker, and does not touch main.
TEST_F(VersioningGatedTest, BranchForkEdgeSetPropertyIsVisibleBothWaysAndIsolated) {
  faker.Interpret("CREATE (:A {id:1})-[:R {w:1, x:7}]->(:B {id:2})");
  Fork();

  EXPECT_EQ(IntRows(faker, "MATCH (:A)-[r:R]->(:B) SET r.w = 2 RETURN r.w"), (Rows{{2}}))
      << "a single-statement read must see the post-SET value, not the stale fork one";

  EXPECT_EQ(IntRows(faker, "MATCH (:A)-[r:R]->(:B) RETURN r.w, r.x"), (Rows{{2, 7}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:B)<-[r:R]-(:A) RETURN r.w, r.x"), (Rows{{2, 7}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.w, r.x"), (Rows{{2, 7}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{1}})) << "the stub is not a second edge";
  EXPECT_EQ(IntRows(faker, "MATCH (:A)-[r]->() RETURN count(r)"), (Rows{{1}}));
  EXPECT_EQ(EdgePropertyKeys(faker, "MATCH ()-[r:R]->() RETURN properties(r)"), (std::set<std::string>{"w", "x"}));

  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.w, r.x"), (Rows{{1, 7}})) << "main must be untouched";
}

// (b) REMOVE r.p, SET r.p = null, SET r = {...} and SET r += {...} on a fork edge: removed fork properties stay gone
// (a marker on the stub), re-setting one brings it back, and the marker is never visible.
TEST_F(VersioningGatedTest, BranchForkEdgeRemoveAndReplacePropertiesOverlayCorrectly) {
  faker.Interpret(
      "CREATE (:A {id:1})-[:R1 {w:1, x:7}]->(:B {id:2}), (:A {id:3})-[:R2 {w:1, x:7}]->(:B {id:4}), "
      "(:A {id:5})-[:R3 {w:1, x:7}]->(:B {id:6})");
  Fork();

  faker.Interpret("MATCH ()-[r:R1]->() REMOVE r.w");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R1]->() RETURN r.w, r.x"), (Rows{{kNullInt, 7}}));
  EXPECT_EQ(EdgePropertyKeys(faker, "MATCH ()-[r:R1]->() RETURN properties(r)"), (std::set<std::string>{"x"}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R1]->() RETURN r.__mg_removed"), (Rows{{kNullInt}})) << "markers never show";

  faker.Interpret("MATCH ()-[r:R2]->() SET r = {x: 1}");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R2]->() RETURN r.w, r.x"), (Rows{{kNullInt, 1}}));
  EXPECT_EQ(EdgePropertyKeys(faker, "MATCH ()-[r:R2]->() RETURN properties(r)"), (std::set<std::string>{"x"}));

  faker.Interpret("MATCH ()-[r:R3]->() SET r.w = null");
  faker.Interpret("MATCH ()-[r:R3]->() SET r += {y: 5}");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R3]->() RETURN r.w, r.x, r.y"), (Rows{{kNullInt, 7, 5}}));
  faker.Interpret("MATCH ()-[r:R3]->() SET r.w = 9");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R3]->() RETURN r.w, r.x, r.y"), (Rows{{9, 7, 5}}));
  EXPECT_EQ(EdgePropertyKeys(faker, "MATCH ()-[r:R3]->() RETURN properties(r)"),
            (std::set<std::string>{"w", "x", "y"}));

  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() RETURN r.w, r.x ORDER BY type(r)"), (Rows{{1, 7}, {1, 7}, {1, 7}}));
}

// (c) A rolled-back write to a fork edge must not wedge it: the retried write succeeds (the v1 full-copy edge left a
// rolled-back deleted object behind that every later COW of the same gid tripped over).
TEST_F(VersioningGatedTest, BranchForkEdgeWriteAfterRollbackSucceeds) {
  faker.Interpret("CREATE (:A)-[:R {w:1}]->(:B)");
  Fork();

  faker.Interpret("BEGIN");
  faker.Interpret("MATCH ()-[r:R]->() SET r.w = 2");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.w"), (Rows{{2}}));
  faker.Interpret("ROLLBACK");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.w"), (Rows{{1}}));

  EXPECT_NO_THROW(faker.Interpret("MATCH ()-[r:R]->() SET r.w = 3"));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.w"), (Rows{{3}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{1}}));
}

// (e) Deleting a stub-backed fork edge removes it everywhere on the branch (both directions, scans, index scans) and
// leaves main alone, while a parallel sibling edge between the same endpoints survives; DETACH DELETE of an endpoint
// hides a stubbed incident edge the same way.
TEST_F(VersioningGatedTest, BranchDeleteStubBackedForkEdgeIsGoneEverywhere) {
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret(
      "CREATE (a:A {id:1}), (b:B {id:2}), (a)-[:R {w:1}]->(b), (a)-[:R {w:4}]->(b), "
      "(:A {id:3})-[:R {w:5}]->(:B {id:4}), (:A {id:5})-[:R {w:6}]->(:B {id:6})");
  Fork();

  faker.Interpret("MATCH (:A {id:1})-[r:R {w:1}]->() SET r.w = 2");
  faker.Interpret("MATCH (:A {id:1})-[r:R {w:2}]->() DELETE r");
  EXPECT_EQ(IntRows(faker, "MATCH (:A {id:1})-[r]->() RETURN r.w"), (Rows{{4}})) << "only the parallel sibling is left";
  EXPECT_EQ(IntRows(faker, "MATCH (:B {id:2})<-[r]-() RETURN r.w"), (Rows{{4}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{3}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w > 0 RETURN r.w ORDER BY r.w"), (Rows{{4}, {5}, {6}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w = 2 RETURN count(r)"), (Rows{{0}}));

  faker.Interpret("MATCH (:A {id:3})-[r:R]->() SET r.w = 8");
  faker.Interpret("MATCH (a:A {id:3}) DETACH DELETE a");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{2}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w > 0 RETURN r.w ORDER BY r.w"), (Rows{{4}, {6}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w = 8 RETURN count(r)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:B {id:4})<-[r]-() RETURN count(r)"), (Rows{{0}}));

  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.w ORDER BY r.w"), (Rows{{1}, {4}, {5}, {6}}));
}

// (f) Edge-type+property and global edge-property index scans on a branch see a stubbed fork edge at its branch value
// only: the old value is gone, the new one is found, an unrelated property write keeps the fork value findable.
TEST_F(VersioningGatedTest, BranchEdgePropertyIndexScansOverlayForkEdgeStubs) {
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret(
      "CREATE (:A {id:1})-[:R {w:1}]->(:B {id:2}), (:A {id:3})-[:R {w:5}]->(:B {id:4}), "
      "(:A {id:5})-[:R {w:7}]->(:B {id:6}), (:A {id:7})-[:Q {w:3}]->(:B {id:8})");
  Fork();

  faker.Interpret("MATCH (:A {id:1})-[r:R]->() SET r.w = 9");  // value changed
  faker.Interpret("MATCH (:A {id:3})-[r:R]->() SET r.x = 1");  // other property only
  faker.Interpret("MATCH (:A {id:5})-[r:R]->() REMOVE r.w");   // value removed
  faker.Interpret("MATCH (:A {id:7})-[r:Q]->() SET r.w = 2");  // other type, global index only

  ExpectPlanUses(faker, "MATCH ()-[r:R]->() WHERE r.w = 1 RETURN count(r)", "ScanAllByEdgeTypeProperty");
  ExpectPlanUses(faker, "MATCH ()-[r:R]->() WHERE r.w > 0 RETURN r.w ORDER BY r.w", "ScanAllByEdgeTypeProperty");
  ExpectPlanUses(faker, "MATCH ()-[r]->() WHERE r.w IS NOT NULL RETURN r.w ORDER BY r.w", "ScanAllByEdgeProperty");
  ExpectPlanUses(faker, "MATCH ()-[r]->() WHERE r.w = 2 RETURN count(r)", "ScanAllByEdgeProperty");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w = 1 RETURN count(r)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w = 9 RETURN count(r)"), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w = 5 RETURN count(r)"), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w = 7 RETURN count(r)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w > 0 RETURN r.w ORDER BY r.w"), (Rows{{5}, {9}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w >= 5 AND r.w < 9 RETURN r.w"), (Rows{{5}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{3}})) << "the type scan lists each edge once";

  EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() WHERE r.w IS NOT NULL RETURN r.w ORDER BY r.w"), (Rows{{2}, {5}, {9}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() WHERE r.w = 3 RETURN count(r)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() WHERE r.w = 2 RETURN count(r)"), (Rows{{1}}));
}

#ifdef MG_ENTERPRISE
// A branch accessor must report main's database name: fine-grained permissions are loaded per database name.
TEST_F(VersioningGatedTest, BranchAccessorReportsMainDatabaseNameForFineGrainedAuth) {
  Fork();
  auto *ctx = faker.interpreter.current_db_.branch_context();
  ASSERT_NE(ctx, nullptr);

  auto main_name = db->storage()->name();
  auto diff_acc = ctx->diff_engine().Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor branch_dba{diff_acc.get(), ctx};
  ASSERT_EQ(branch_dba.DatabaseName(), main_name);
  ASSERT_EQ(branch_dba.id(), main_name);

  const auto a = branch_dba.NameToLabel("A");
  const auto b = branch_dba.NameToLabel("B");
  const auto read = memgraph::query::AuthQuery::FineGrainedPrivilege::READ;

  // (a) Grant on :A, access scoped to the default database only.
  memgraph::auth::User granted{"u"};
  granted.db_access().Grant(main_name);
  granted.fine_grained_access_handler().label_permissions().Grant({"A"}, memgraph::auth::kAllLabelPermissions);
  memgraph::glue::FineGrainedAuthChecker granted_checker{granted, &branch_dba};
  EXPECT_TRUE(granted_checker.Has(std::array{a}, read));
  EXPECT_FALSE(granted_checker.Has(std::array{b}, read));

  // (b) A DENY on :B still applies next to a global grant.
  memgraph::auth::User denied{"d"};
  denied.db_access().Grant(main_name);
  denied.fine_grained_access_handler().label_permissions().GrantGlobal(memgraph::auth::kAllLabelPermissions);
  denied.fine_grained_access_handler().label_permissions().Deny({"B"}, memgraph::auth::kAllLabelPermissions);
  memgraph::glue::FineGrainedAuthChecker denied_checker{denied, &branch_dba};
  EXPECT_TRUE(denied_checker.Has(std::array{a}, read));
  EXPECT_FALSE(denied_checker.Has(std::array{b}, read));
}
#endif
