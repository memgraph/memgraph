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

// Interpreter-level tests for versioning management queries (PrepareVersioningQuery) and branch read/write routing,
// driven through Interpreter::Prepare/Pull against an in-process Database.

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
#include "versioning/gate.hpp"
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

  void SatisfyGate() {
    FLAGS_versioning_enabled = true;
    memgraph::license::global_license_checker.EnableTesting();
  }

  static void Fork(InterpreterFaker &session, const std::string &name = "b") {
    session.Interpret("CREATE BRANCH '" + name + "' FROM 'main'");
    session.Interpret("CHECKOUT BRANCH '" + name + "'");
    ASSERT_EQ(session.interpreter.current_db_.CurrentVersion(), name);
    ASSERT_NE(session.interpreter.current_db_.branch_context(), nullptr);
  }

  void Fork(const std::string &name = "b") { Fork(faker, name); }
};

using VersioningInterpreterTest = VersioningFixture<true>;

class VersioningGatedTest : public VersioningInterpreterTest {
 protected:
  void SetUp() override {
    VersioningInterpreterTest::SetUp();
    SatisfyGate();
  }
};

// One satisfied precondition (feature flag or license) is not sufficient on its own.
TEST_F(VersioningInterpreterTest, GateRejectsWhenSinglePreconditionMissing) {
  struct Case {
    bool enable_flag;
    bool enable_license;
    memgraph::versioning::GateError expected;
    const char *label;
  };

  static constexpr Case cases[] = {
      {false, true, memgraph::versioning::GateError::kDisabled, "flag=OFF license=ON"},
      {true, false, memgraph::versioning::GateError::kNoLicense, "flag=ON  license=OFF"},
  };
  for (const auto &c : cases) {
    FLAGS_versioning_enabled = c.enable_flag;
    if (c.enable_license) memgraph::license::global_license_checker.EnableTesting();
    try {
      faker.Interpret("CREATE BRANCH 'feature' FROM 'main'");
      ADD_FAILURE() << "expected QueryRuntimeException: " << c.label;
    } catch (const memgraph::query::QueryRuntimeException &e) {
      EXPECT_EQ(std::string{e.what()}, std::string{memgraph::versioning::GateErrorMessage(c.expected)}) << c.label;
    }
    FLAGS_versioning_enabled = false;
    memgraph::license::global_license_checker.DisableTesting();
  }
}

// A replica must not mutate its local VersionStore: EnsureMainInstance rejects management queries. Uses the real
// SetReplicationRoleReplica so IsReplica() is genuine.
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
  // 'main' is reserved (versioning::ValidateBranchName).
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

  faker.Interpret("CREATE (:B)");
  EXPECT_EQ(IntRows(faker, "MATCH (n:B) RETURN count(n)"), (Rows{{1}}))
      << "CREATE BRANCH alone must not engage the write-routing rail";

  {
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

// PROFILE wraps an inner query; a PROFILE'd write on main after CHECKOUT 'main' must succeed like a bare write.
TEST_F(VersioningGatedTest, ProfileWriteOnMainAfterCheckoutMainSucceeds) {
  faker.Interpret("CREATE (:Seed)");
  Fork();
  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());

  auto profile = faker.Interpret("PROFILE CREATE (:NoLongerBlockedProfiledWrite)");
  EXPECT_FALSE(profile.GetResults().empty()) << "PROFILE must return its plan table";

  // PROFILE aborts its transaction by design (QueryHandlerResult::ABORT), so the write leaves nothing behind.
  EXPECT_EQ(IntRows(faker, "MATCH (n:NoLongerBlockedProfiledWrite) RETURN count(n)"), (Rows{{0}}));
  faker.Interpret("CREATE (:BareWrite)");
  EXPECT_EQ(IntRows(faker, "MATCH (n:BareWrite) RETURN count(n)"), (Rows{{1}}));

  EXPECT_FALSE(faker.Interpret("PROFILE MATCH (n:Seed) RETURN n").GetResults().empty());
}

// The session target is the last CHECKOUT, like USE DATABASE: x -> y -> main on one connection, and main stays
// writable.
TEST_F(VersioningGatedTest, CheckoutSwitchesBetweenBranchesThenBackToMainLikeUseDatabase) {
  faker.Interpret("CREATE (:Base {v:0})");

  Fork("x");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "x");
  faker.Interpret("CREATE (:OnX {v:1})");
  {
    EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n) AS c"), (Rows{{2}}))
        << "x must see :Base (forked) + :OnX (its own write)";
  }

  Fork("y");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "y");
  faker.Interpret("CREATE (:OnY {v:2})");
  {
    // Branches forked from main are isolated from each other.
    auto stream = faker.Interpret("MATCH (n:OnX) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "y must not see x's branch-native write";
  }
  {
    EXPECT_EQ(IntRows(faker, "MATCH (n:OnY) RETURN n.v"), (Rows{{2}}));
  }

  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  faker.Interpret("CREATE (:OnMain {v:3})");

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

  // USING VERSION 'main' reads main independently of the CHECKOUT-based read-back above.
  faker.Interpret("CHECKOUT BRANCH 'x'");
  {
    EXPECT_EQ(IntRows(faker, "USING VERSION 'main' MATCH (n:OnMain) RETURN n.v"), (Rows{{3}}));
  }

  // Back on a branch after main writes, writes route to the branch again and stay off main.
  faker.Interpret("CREATE (:AnotherBranchWrite)");
  EXPECT_EQ(IntRows(faker, "MATCH (n:AnotherBranchWrite) RETURN count(n)"), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, "USING VERSION 'main' MATCH (n:AnotherBranchWrite) RETURN count(n)"), (Rows{{0}}));
}

// Per-kind independence of the branch change filters: a property SET must not disturb label reads, and a label ADD must
// not disturb property reads; every changed value is still observed.
TEST_F(VersioningGatedTest, BranchChangeFilterKindIsolation) {
  faker.Interpret("CREATE (:User {id: 1, age: 40, name: 'a'})");
  Fork();

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

  faker.Interpret("CREATE (:User {id: 2, age: 50})");
  faker.Interpret("MATCH (n {id: 1}) SET n:VIP");
  {
    auto stream = faker.Interpret("MATCH (n {id: 1}) RETURN n.age AS age, ('VIP' IN labels(n)) AS vip");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 99) << "property read must stay correct after a label-only change";
    EXPECT_TRUE(stream.GetResults()[0][1].ValueBool()) << "the added label must be observed";
  }
}

// An entity changed on b1 must read as its fork value on b2: neither b1's change nor main's post-fork write.
TEST_F(VersioningGatedTest, BranchCrossBranchIsolationReadsOwnFork) {
  faker.Interpret("CREATE (:User {id: 1, age: 10})");
  faker.Interpret("CREATE (:User {id: 2, age: 20})");
  faker.Interpret("CREATE (:S)-[:E {w:1}]->(:A)");

  Fork("b1");
  faker.Interpret("MATCH (n {id: 1}) SET n.age = 111, n:Changed");
  faker.Interpret("MATCH (:S)-[e:E]->(:A) SET e.w = 999");
  EXPECT_EQ(IntRows(faker, "MATCH (n {id: 1}) RETURN n.age"), (Rows{{111}})) << "b1 must read its own write";
  EXPECT_EQ(IntRows(faker, "MATCH (:S)-[e:E]->(:A) RETURN e.w"), (Rows{{999}})) << "b1 must read its own edge write";
  faker.Interpret("CHECKOUT BRANCH 'main'");

  // b2 forks AFTER b1's changes (main itself is untouched), then main changes id=2: b2 must stay pinned.
  faker.Interpret("CREATE BRANCH 'b2' FROM 'main'");
  faker.Interpret("MATCH (n {id: 2}) SET n.age = 222");
  EXPECT_EQ(IntRows(faker, "MATCH (n {id: 2}) RETURN n.age"), (Rows{{222}}))
      << "main must read its own post-fork write";
  faker.Interpret("CHECKOUT BRANCH 'b2'");

  {
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

// Deletes and a label removal on b1 must not leak to an untouched b2 forked from the same main, through a label scan,
// an expansion and an edge scan; b1 itself must no longer see them.
TEST_F(VersioningGatedTest, BranchDeleteAndLabelRemovalOnOneBranchDoNotAffectUntouchedSibling) {
  faker.Interpret("CREATE (:K {i:1})-[:R]->(:K {i:2})-[:R]->(:K {i:3}), (:K:Z {i:4})");
  Fork("b1");
  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("CREATE BRANCH 'b2' FROM 'main'");

  faker.Interpret("CHECKOUT BRANCH 'b1'");
  faker.Interpret("MATCH (:K {i:1})-[r:R]->() DELETE r");
  faker.Interpret("MATCH (n:K {i:2}) DETACH DELETE n");
  faker.Interpret("MATCH (n:K {i:4}) REMOVE n:Z");
  EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN count(n)"), (Rows{{3}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:Z) RETURN count(n)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{0}}));

  faker.Interpret("CHECKOUT BRANCH 'b2'");
  EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN count(n)"), (Rows{{4}})) << "b2 must still see the deleted vertex";
  EXPECT_EQ(IntRows(faker, "MATCH (n:Z) RETURN n.i"), (Rows{{4}})) << "b2 must still see the removed label";
  EXPECT_EQ(IntRows(faker, "MATCH (:K {i:1})-[r:R]->(m) RETURN m.i"), (Rows{{2}}));
  EXPECT_EQ(IntRows(faker, "MATCH (:K {i:3})<-[r:R]-(m) RETURN m.i"), (Rows{{2}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{2}}));
}

// A changed property reached through an edge traversal must read the branch's new value and an unchanged property on
// the same vertex its fork value; main stays untouched.
TEST_F(VersioningGatedTest, BranchFinePropertyChangeReadViaTraversal) {
  faker.Interpret("CREATE (n:User {id: 1})-[:E]->(v:User {id: 2, age: 40, keep: 7})");
  Fork();
  faker.Interpret("MATCH (v {id: 2}) SET v.age = 99");

  {
    auto stream = faker.Interpret("MATCH (n {id: 1})-[:E]->(v) RETURN v.age AS age, v.keep AS keep");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 99) << "changed property must resolve to the new value";
    EXPECT_EQ(stream.GetResults()[0][1].ValueInt(), 7)
        << "an UNCHANGED property on the same vertex must read fork-state via the fine fast path";
  }

  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    EXPECT_EQ(IntRows(faker, "MATCH (v {id: 2}) RETURN v.age AS age"), (Rows{{40}}))
        << "main must not see the branch's property change";
  }
}

// CREATE BRANCH FROM a non-main parent is rejected before any VersionStore mutation.
TEST_F(VersioningGatedTest, CreateBranchFromNonMainParentIsRejected) {
  faker.Interpret("CREATE BRANCH 'feature' FROM 'main'");
  ASSERT_TRUE(db->version_store()->Exists("feature"));

  ASSERT_THROW(faker.Interpret("CREATE BRANCH 'child' FROM 'feature'"), memgraph::query::QueryRuntimeException);

  EXPECT_FALSE(db->version_store()->Exists("child"))
      << "a rejected CREATE BRANCH must leave the VersionStore unchanged";
}

// Counters are process-global, so assert deltas; the two gauges are asserted as absolute values.
TEST_F(VersioningGatedTest, VersioningMetricsIncrementOnLifecycle) {
  auto &g = memgraph::metrics::Metrics().global;
  const double created0 = g.versioning_branches_created->Value();
  const double dropped0 = g.versioning_branches_dropped->Value();

  faker.Interpret("CREATE (:A {x:1})");
  faker.Interpret("CREATE BRANCH 'b1' FROM 'main'");
  faker.Interpret("CREATE BRANCH 'b2' FROM 'main'");
  EXPECT_DOUBLE_EQ(g.versioning_branches_created->Value() - created0, 2.0)
      << "two CREATE BRANCH statements must increment the created counter by exactly 2";
  EXPECT_DOUBLE_EQ(g.versioning_active_branches->Value(), 2.0);

  faker.Interpret("CHECKOUT BRANCH 'b1'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b1");
  EXPECT_DOUBLE_EQ(g.versioning_active_checkouts->Value(), 1.0);
  faker.Interpret("CREATE (:OnBranch {y:1})");
  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_DOUBLE_EQ(g.versioning_active_checkouts->Value(), 0.0) << "ReleaseCheckout must Set() the gauge back to 0";

  faker.Interpret("DROP BRANCH 'b1'");
  EXPECT_DOUBLE_EQ(g.versioning_branches_dropped->Value() - dropped0, 1.0);
  EXPECT_DOUBLE_EQ(g.versioning_active_branches->Value(), 1.0);

  faker.Interpret("DROP BRANCH 'b2'");
  EXPECT_DOUBLE_EQ(g.versioning_branches_dropped->Value() - dropped0, 2.0)
      << "each DROP BRANCH increments the dropped counter by exactly 1";
  EXPECT_DOUBLE_EQ(g.versioning_active_branches->Value(), 0.0);
}

// RuntimeConfig::Configure re-applies SetCurrentDB for the same database when only the Bolt RUN extras change; that
// must not reset the checked-out branch. Drives CurrentDB directly, without SessionHL.
TEST_F(VersioningInterpreterTest, SetCurrentDBOnlyResetsCurrentVersionOnADifferentDatabase) {
  memgraph::query::CurrentDB current_db{db};
  current_db.SetCurrentVersion(std::string{"feature"});
  ASSERT_EQ(current_db.CurrentVersion(), "feature");

  auto same_db_acc_opt = db_gk.access();
  ASSERT_TRUE(same_db_acc_opt);
  current_db.SetCurrentDB(*same_db_acc_opt, /*in_explicit_db=*/false);
  EXPECT_EQ(current_db.CurrentVersion(), "feature") << "same-database SetCurrentDB must NOT reset the branch";

  memgraph::storage::Config other_config{};
  other_config.durability.storage_directory = data_directory / "other_db";
  other_config.disk.main_storage_directory = data_directory / "other_db" / "disk";
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> other_gk{other_config};
  auto other_db_acc_opt = other_gk.access();
  ASSERT_TRUE(other_db_acc_opt);
  current_db.SetCurrentDB(*other_db_acc_opt, false);
  EXPECT_FALSE(current_db.CurrentVersion().has_value()) << "a genuine database switch must reset the branch";

  // current_db holds an accessor into other_gk, whose destructor blocks until every accessor is released; release it
  // first, as locals destruct in reverse order.
  current_db.ResetDB();
}

// Re-applying a same-database DatabaseAccess to a live interpreter must not disturb an active CHECKOUT.
TEST_F(VersioningGatedTest, ReApplyingSameDatabaseAccessPreservesCheckout) {
  Fork("feature");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "feature");

  auto same_db_acc_opt = db_gk.access();
  ASSERT_TRUE(same_db_acc_opt);
  faker.interpreter.current_db_.SetCurrentDB(*same_db_acc_opt, /*in_explicit_db=*/false);
  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "feature");
  // A real checkout also holds a BranchContext and a checkout slot; a same-database re-apply must keep both.
  EXPECT_NE(faker.interpreter.current_db_.branch_context(), nullptr);
  EXPECT_EQ(db->version_store()->CheckoutCount("feature"), 1U);

  auto stream = faker.Interpret("SHOW BRANCH");
  ASSERT_EQ(stream.GetResults().size(), 1U);
  EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "feature");
}

// A branch touching one of two main vertices must copy-on-write only that one; the untouched vertex stays
// un-materialized in the diff engine. Labels are filtered via labels(n) to stay independent of any label index.
TEST_F(VersioningGatedTest, BranchWriteStubsOnlyTheTouchedVertex) {
  faker.Interpret("CREATE (:A {x:1}), (:B {y:9})");
  Fork();
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");

  faker.Interpret("MATCH (n) WHERE 'A' IN labels(n) SET n.x = 2");

  // Exactly one vertex (A) must have been COW'd into the diff engine.
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

  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  {
    EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE 'A' IN labels(n) RETURN n.x"), (Rows{{1}}));
  }
}

// Whole-map property writes on a fork vertex (`SET n += {...}`, `SET n = {...}`) must be isolated from main.
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

// Single-statement read-your-write: `SET n.x = 2 RETURN n.x` must see the new value, not the pre-COW historical one.
TEST_F(VersioningGatedTest, BranchSingleStatementSetThenReturnSeesNewValue) {
  faker.Interpret("CREATE (:A {x:1})");
  Fork();

  EXPECT_EQ(IntRows(faker, "MATCH (n) SET n.x = 2 RETURN n.x"), (Rows{{2}}))
      << "must see the post-SET value, not the stale historical one";
}

// A branch-native edge between two still-historical endpoints must be visible from both directions, and an endpoint
// COW'd by a later statement must be re-resolved when expanding across an untouched historical edge.
TEST_F(VersioningGatedTest, BranchEdgeCreateAndEndpointShadowingIsUnionAware) {
  faker.Interpret("CREATE (:A {name:'a'}), (:B {name:'b'})");
  faker.Interpret("CREATE (:S {name:'s'})-[:E]->(:A {name:'a2'})");

  Fork("br");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "br");

  faker.Interpret("MATCH (x:A {name:'a'}), (y:B {name:'b'}) CREATE (x)-[:R]->(y)");

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

  faker.Interpret("MATCH (a2:A {name:'a2'}) SET a2.name = 'a2_mod'");
  {
    auto stream = faker.Interpret("MATCH (:S)-[:E]->(a2) RETURN a2.name");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "a2_mod") << "endpoint must be re-Resolved, not stale";
  }

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

// `WHERE id(e) = <gid>` plans a ScanAllByEdgeId that must resolve a pre-fork edge and a branch-native edge, and return
// no rows for an unknown gid.
TEST_F(VersioningGatedTest, BranchFindEdgeByIdResolvesForkAndBranchNativeEdges) {
  faker.Interpret("CREATE (:S)-[:E]->(:A)");

  int64_t fork_edge_gid;
  {
    auto s = faker.Interpret("MATCH ()-[e:E]->() RETURN id(e)");
    ASSERT_EQ(s.GetResults().size(), 1U);
    fork_edge_gid = s.GetResults()[0][0].ValueInt();
  }

  Fork();

  ExpectPlanUses(
      faker, "MATCH ()-[e]->() WHERE id(e) = " + std::to_string(fork_edge_gid) + " RETURN type(e)", "ScanAllByEdgeId");

  {
    auto s = faker.Interpret("MATCH ()-[e]->() WHERE id(e) = " + std::to_string(fork_edge_gid) + " RETURN type(e)");
    ASSERT_EQ(s.GetResults().size(), 1U) << "the pre-fork edge must resolve by id via the full-scan fallback";
    EXPECT_EQ(s.GetResults()[0][0].ValueString(), "E");
  }

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

  {
    auto s = faker.Interpret("MATCH ()-[e]->() WHERE id(e) = 999999 RETURN type(e)");
    EXPECT_EQ(s.GetResults().size(), 0U) << "a non-existent gid must return no rows, not throw";
  }
}

// Indexes, constraints, descriptions, triggers, streams and storage-global operations are rejected on a branch; the
// session stays usable, SHOW INDEX INFO still matches main, and the same statements succeed on main.
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

  EXPECT_EQ(IntRows(faker, "MATCH (n:L) RETURN n.p"), (Rows{{1}}));
  faker.Interpret("CREATE (:L {p:2})");

  EXPECT_EQ(index_rows(), main_rows);
  EXPECT_TRUE(
      PlanHasOperator(ExplainText(faker, "MATCH (n:Person) WHERE n.age > 0 RETURN n"), "ScanAllByLabelProperties"));

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

// Edge value reads: a changed edge resolves through the diff engine, while an unchanged edge must read fork-state even
// when hanging off a vertex the branch COW'd (e3 beside the SET e1) or off untouched vertices (e2).
TEST_F(VersioningGatedTest, BranchEdgeBitFastPathSkipsResolveForUnchangedEdges) {
  faker.Interpret("CREATE (s:S)-[:E1 {w:1}]->(:A), (s)-[:E3 {w:3}]->(:C)");
  faker.Interpret("CREATE (:S2)-[:E2 {w:2}]->(:B)");
  Fork();
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");

  faker.Interpret("MATCH (:S)-[e:E1]->(:A) SET e.w = 100");

  {
    EXPECT_EQ(IntRows(faker, "MATCH (:S)-[e:E1]->(:A) RETURN e.w"), (Rows{{100}}))
        << "the changed edge must reflect the new value";
  }
  {
    auto stream = faker.Interpret("MATCH (:S)-[e:E3]->(:C) RETURN e.w");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 3)
        << "an edge untouched by the branch must read fork-state via the branched()-bit fast path, "
           "even though its FROM vertex was COW'd by an unrelated SET on a sibling edge";
  }
  {
    EXPECT_EQ(IntRows(faker, "MATCH (:S2)-[e:E2]->(:B) RETURN e.w"), (Rows{{2}}))
        << "a completely untouched edge must read fork-state";
  }

  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    EXPECT_EQ(IntRows(faker, "MATCH (:S)-[e:E1]->(:A) RETURN e.w"), (Rows{{1}}))
        << "main must be untouched by the branch's SET";
  }
}

// The branched() bit is per edge object, not per vertex: a self-loop and two parallel edges sharing both endpoints must
// resolve independently (P2 stays at fork-state although P1's SET branched both endpoints).
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

  faker.Interpret("CHECKOUT BRANCH 'main'");
  {
    EXPECT_EQ(IntRows(faker, "MATCH (:S)-[e:SELF]->(:S) RETURN e.w"), (Rows{{1}}));
  }
  {
    EXPECT_EQ(IntRows(faker, "MATCH (:S2)-[e:P1]->(:T2) RETURN e.w"), (Rows{{10}}));
  }
}

// A branch-native vertex created and deleted in the same checkout must be hidden from the branch immediately and never
// be visible on main.
TEST_F(VersioningGatedTest, BranchDeleteBranchNativeVertexHiddenAndMainNeverSawIt) {
  Fork();

  faker.Interpret("CREATE (:X {id:1})");
  EXPECT_EQ(IntRows(faker, "MATCH (n:X) RETURN count(n)"), (Rows{{1}}))
      << "the native vertex must be visible before its DELETE";
  faker.Interpret("MATCH (n:X) DELETE n");

  {
    auto stream = faker.Interpret("MATCH (n:X) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "the branch-native vertex must be hidden after its own DELETE";
  }

  faker.Interpret("CHECKOUT BRANCH 'main'");

  auto stream = faker.Interpret("MATCH (n:X) RETURN n");
  EXPECT_EQ(stream.GetResults().size(), 0U) << "main must never have seen the created-then-deleted vertex";
}

// Every kind of branch write must read back identically after CHECKOUT away and back (the same live branch is
// reattached), including reads through an edge traversal, and main must stay untouched.
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

// `USING PERIODIC COMMIT` and `CALL {} IN TRANSACTIONS` flush mid-query through DbAccessor::PeriodicCommit; every
// flushed batch must survive CHECKOUT away and back.
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
    EXPECT_EQ(IntRows(faker, count), (Rows{{c.rows}})) << "the batches must survive CHECKOUT away and back";
  }
}

// DB-global operations (storage mode, drop graph, enum, recover snapshot) are rejected while any branch exists, even
// from a session on main (CREATE BRANCH does not check out); the guard clears once the last branch is dropped.
TEST_F(VersioningGatedTest, BranchExistenceRejectsStorageModeAndDropGraph) {
  faker.Interpret("CREATE (:A {x:1})");
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());

  for (const auto &[query, message] :
       {std::pair<const char *, const char *>{"STORAGE MODE IN_MEMORY_ANALYTICAL",
                                              "Cannot switch storage mode while "
                                              "versioning branches exist"},
        {"DROP GRAPH", "Cannot drop the graph while versioning branches exist"},
        {"CREATE ENUM Status VALUES { Good, Bad }", "Cannot create an enum while versioning branches exist"},
        {"RECOVER SNAPSHOT \"/nonexistent/snapshot\"", "Cannot recover a snapshot while versioning branches exist"}}) {
    try {
      faker.Interpret(query);
      FAIL() << "must be rejected while a branch exists: " << query;
    } catch (const memgraph::query::QueryRuntimeException &e) {
      EXPECT_THAT(e.what(), ::testing::HasSubstr(message)) << query;
    }
  }

  {
    auto s = faker.Interpret("MATCH (n:A) RETURN n.x");
    ASSERT_EQ(s.GetResults().size(), 1U);
    EXPECT_EQ(s.GetResults()[0][0].ValueInt(), 1);
  }

  faker.Interpret("DROP BRANCH 'b'");
  faker.Interpret("STORAGE MODE IN_MEMORY_ANALYTICAL");
  faker.Interpret("DROP GRAPH");
  EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n)"), (Rows{{0}}))
      << "DROP GRAPH must actually run once no branch exists";
}

// `USING VERSION 'main'` routes that one query to main's own storage while the session stays checked out: the branch
// sees MainOnly (fork-era) plus BranchOnly, `USING VERSION 'main'` sees MainOnly only.
TEST_F(VersioningGatedTest, UsingVersionMainReadsMainWhileOnBranch) {
  faker.Interpret("CREATE (:MainOnly {v:1})");
  Fork();
  faker.Interpret("CREATE (:BranchOnly {v:2})");

  {
    EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n) AS c"), (Rows{{2}}));
  }

  {
    EXPECT_EQ(IntRows(faker, "USING VERSION 'main' MATCH (n) RETURN count(n) AS c"), (Rows{{1}}));
  }

  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  {
    EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n) AS c"), (Rows{{2}}));
  }
}

// `USING VERSION` naming the session's own branch, or 'main' on a session on main, is a harmless no-op. The
// 'main'-while- on-a-branch escape hatch is UsingVersionMainWriteWhileOnBranchLandsOnMainOnly.
TEST_F(VersioningGatedTest, UsingVersionTargetsSessionOrMain) {
  Fork();

  faker.Interpret("USING VERSION 'b' CREATE (:OnB {v:5})");
  EXPECT_EQ(IntRows(faker, "MATCH (n:OnB) RETURN n.v"), (Rows{{5}}));

  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  EXPECT_EQ(IntRows(faker, "MATCH (n:OnB) RETURN count(n)"), (Rows{{0}}))
      << "USING VERSION naming the session's own branch must not leak to main";

  faker.Interpret("CREATE (:PlainWrite {v:6})");
  faker.Interpret("USING VERSION 'main' CREATE (:Allowed {v:7})");
  EXPECT_EQ(IntRows(faker, "MATCH (n:Allowed) RETURN n.v"), (Rows{{7}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:PlainWrite) RETURN n.v"), (Rows{{6}}));
}

// Naming a branch other than 'main' or the session's own throws utils::NotYetImplemented and leaves the session usable.
TEST_F(VersioningGatedTest, UsingVersionOtherBranchNotYetImplemented) {
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker.Interpret("CREATE BRANCH 'c' FROM 'main'");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  ASSERT_THROW(faker.Interpret("USING VERSION 'c' MATCH (n) RETURN n"), memgraph::utils::NotYetImplemented);

  EXPECT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  faker.Interpret("MATCH (n) RETURN n");
}

// A `USING VERSION 'main'` write while on 'b' lands on main only; a plain write lands on the branch only.
TEST_F(VersioningGatedTest, UsingVersionMainWriteWhileOnBranchLandsOnMainOnly) {
  Fork();

  faker.Interpret("USING VERSION 'main' CREATE (:M {v:1})");
  faker.Interpret("CREATE (:BranchNode {v:2})");

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

  faker.Interpret("CHECKOUT BRANCH 'b'");
  {
    auto stream = faker.Interpret("MATCH (n:M) RETURN n");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "a force-main write must not appear on the branch";
  }
  {
    EXPECT_EQ(IntRows(faker, "MATCH (n:BranchNode) RETURN n.v"), (Rows{{2}}));
  }
}

// The branch-local plan cache must serve correct results: a main-shaped plan must never be reused on a branch (it would
// scan the diff engine's empty index), and a cache hit must still reflect newly written branch data.
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

  {
    auto stream = faker.Interpret("MATCH (n:User {id: 1}) RETURN n.tag");
    ASSERT_EQ(stream.GetResults().size(), 1U)
        << "fork-state row must be visible on the branch -- must NOT be the old 0-rows hazard";
    EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "main");
  }
  EXPECT_FALSE(PlanHasOperator(ExplainText(faker, "MATCH (n:User {id: 1}) RETURN n.tag"), "ScanAllByLabelProperties"));

  {
    auto stream = faker.Interpret("MATCH (n:User {id: 1}) RETURN n.tag");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "main");
  }

  // Same stripped query after a fresh branch-native row: the cached plan must pick it up.
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

  // Same stripped text, different literal: the cached plan must resolve the new id.
  {
    auto stream = faker.Interpret("MATCH (n:User {id: 2}) RETURN n.tag");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "main");
  }
}

// A force-main query on a checked-out session must not populate or read the branch's plan-cache slot (selection keys
// off the per-transaction current_diff_txn(), not session-level branch_context()).
TEST_F(VersioningGatedTest, UsingVersionMainDoesNotPolluteBranchPlanCache) {
  faker.Interpret("CREATE (:User {id: 1, tag: 'main'})");

  Fork();

  // Main gains an index after the fork that the branch does not have, so a main-shaped plan is a property-index scan
  // and a branch-shaped one is not: the two plans are distinguishable if one is served from the other's cache.
  faker.Interpret("CHECKOUT BRANCH 'main'");
  faker.Interpret("CREATE INDEX ON :User(id)");
  faker.Interpret("CHECKOUT BRANCH 'b'");

  faker.Interpret("CREATE (:User {id: 100, tag: 'branch'})");

  // Force-main query routes at main, which has no id=100 row; it must not prime the branch's cache slot.
  {
    auto stream = faker.Interpret("USING VERSION 'main' MATCH (n:User {id: 100}) RETURN n.tag");
    EXPECT_EQ(stream.GetResults().size(), 0U) << "main has no id=100 row";
  }

  {
    auto stream = faker.Interpret("MATCH (n:User {id: 100}) RETURN n.tag");
    ASSERT_EQ(stream.GetResults().size(), 1U)
        << "branch plan cache must not have been polluted by the force-main query above";
    EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "branch");
  }
  EXPECT_FALSE(PlanHasOperator(ExplainText(faker, "MATCH (n:User {id: 100}) RETURN n.tag"), "ScanAllByLabelProperties"))
      << "the branch plan must not be main's index-scan plan";

  {
    auto stream = faker.Interpret("USING VERSION 'main' MATCH (n:User {id: 100}) RETURN n.tag");
    EXPECT_EQ(stream.GetResults().size(), 0U)
        << "force-main path must not be polluted by the branch-cached plan either";
  }
}

// Two Delete operators on the same entity in one query must be a no-op as on main, not an MG_ASSERT abort. No RETURN:
// the point is that the call returns at all.
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

// A branch commit validates main's existence constraints on every touched or created vertex (the diff engine mirrors
// none) and fails with main's error, leaving the branch unchanged.
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

// A branch raises type-constraint violations at the statement (not at COMMIT) with main's own message, for fork and
// native vertices and CREATE/MERGE alike.
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

// DELETE of a fork vertex: gone from scans, lookups and aggregates on the branch, untouched on main.
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

// DETACH DELETE of a fork vertex: its untouched fork neighbours must stop seeing the incident edges in both directions,
// in expansions, degrees and edge scans.
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
  EXPECT_EQ(IntRows(faker, "MATCH (a:A)-[:R]->(:B) RETURN count(a)"), (Rows{{1}}))
      << "the failed DELETE must change nothing";

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

// DELETE of a single fork edge: gone from expansions in both directions, edge scans and counts while a parallel edge
// survives; main is untouched.
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

// An explicit transaction that deletes a fork vertex and a fork edge and rolls back leaves everything visible again.
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

// The label scan resolves only fork vertices whose membership of the scanned label may differ: an unrelated label
// change must not change the result, while a removal, a delete, a gain and a branch-native vertex all must.
TEST_F(VersioningGatedTest, BranchLabelScanExactTouchedRecordMatchesMembership) {
  faker.Interpret("CREATE INDEX ON :User");
  faker.Interpret("CREATE (:User {i:1}), (:User {i:2}), (:User {i:3}), (:User {i:4}), (:User {i:5}), (:Other {i:6})");
  Fork();

  faker.Interpret("MATCH (n:User) WHERE n.i IN [1, 2] SET n:Probe");
  faker.Interpret("MATCH (n:User {i:3}) REMOVE n:User");
  faker.Interpret("MATCH (n:User {i:4}) DETACH DELETE n");
  faker.Interpret("MATCH (n:Other {i:6}) SET n:User");
  faker.Interpret("CREATE (:User {i:7})");

  EXPECT_EQ(IntRows(faker, "MATCH (n:User) RETURN count(n)"), (Rows{{5}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) RETURN n.i ORDER BY n.i"), (Rows{{1}, {2}, {5}, {6}, {7}}));

  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (n:User {i:5}) REMOVE n:User");
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) RETURN count(n)"), (Rows{{4}}));
  faker.Interpret("ROLLBACK");
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) RETURN n.i ORDER BY n.i"), (Rows{{1}, {2}, {5}, {6}, {7}}))
      << "a rolled-back removal must not hide the vertex";

  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) RETURN n.i ORDER BY n.i"), (Rows{{1}, {2}, {3}, {4}, {5}}))
      << "main must be untouched";
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
    // The edge is already diff-resident (modified) when deleted: the aborted DELETE must leave it alone.
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

// Reading, in the same query, an entity deleted earlier in it raises as on main; the no-RETURN forms stay a silent
// no-op (BranchDeleteThenWritePropertyIsNoOpNotError).
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

// SET/REMOVE after a DETACH DELETE in the same query (no RETURN) must stay a silent no-op, as on main.
TEST_F(VersioningGatedTest, BranchDeleteThenWritePropertyIsNoOpNotError) {
  struct Case {
    const char *name;
    const char *delete_and_write;
  };

  static constexpr Case cases[] = {
      {"SET", "MATCH (n:P) DETACH DELETE n SET n.prop = 2"},
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

// Mirroring two label-property indexes at fork time must not abort: each index needs its own ReadOnlyAccess, since the
// first CreateIndex downgrades the access.
TEST_F(VersioningGatedTest, BranchTwoLabelPropertyIndexesOrExpressionDoesNotAbort) {
  faker.Interpret("CREATE INDEX ON :Label1(id)");
  faker.Interpret("CREATE INDEX ON :Label2(id)");

  faker.Interpret("CREATE (:Label1 {id: 1})");
  faker.Interpret("CREATE (:Label2 {id: 2})");
  faker.Interpret("CREATE (:Label1:Label2 {id: 1})");

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

  // ORDER BY only makes the comparison deterministic; the KNOWS scan itself has no property order.
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

// Every index main held at the fork must be mirrored onto the branch's diff engine, so the planner selects the real
// index operator instead of ScanAll + Filter.
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
  // Creation order deliberately differs from w order.
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

  ExpectPlanUses(
      faker, "MATCH ()-[r:KNOWS]->() WHERE r.w > 0 RETURN r.w AS w ORDER BY r.w", "ScanAllByEdgeTypeProperty");
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
  // Creation order deliberately differs from w order, and edge types differ, so a gid-ordered or type-filtered result
  // is distinguishable.
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

  // ORDER BY only makes the comparison deterministic; label-only indexes have no property order.
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

// A fork vertex touched on a branch must show its removals, and no internal marker may ever be observable.
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

// Stub-bound reads gate on the per-key property filter: a stub created only by a label or an edge change must still
// return the fork's property, and a key dropped by `SET n = {...}` must read null.
TEST_F(VersioningGatedTest, BranchStubPropertyReadsFollowFineFilterNotJustStubPresence) {
  faker.Interpret("CREATE (:L {id: 1, p: 7}), (:M {id: 2, p: 8}), (:R {id: 3, p: 9, q: 10})");
  Fork();

  faker.Interpret("MATCH (n:L) SET n:Extra");
  faker.Interpret("MATCH (a:L), (b:M) CREATE (a)-[:E]->(b)");
  EXPECT_EQ(IntRows(faker, "MATCH (n:Extra) RETURN n.id, n.p"), (Rows{{1, 7}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:M) RETURN n.id, n.p"), (Rows{{2, 8}}));

  faker.Interpret("MATCH (n:R) SET n = {id: 3}");
  EXPECT_EQ(IntRows(faker, "MATCH (n:R) RETURN n.id, n.p, n.q"), (Rows{{3, kNullInt, kNullInt}}));

  faker.Interpret("CHECKOUT BRANCH 'main'");
  EXPECT_EQ(IntRows(faker, "MATCH (n:R) RETURN n.p, n.q"), (Rows{{9, 10}}));
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

  EXPECT_EQ(c, 5) << "USING HOPS LIMIT 5 did not truncate expansion on a checked-out branch (got " << c
                  << " instead of 5) -- the branch edge-iteration path is not threading the hops budget "
                     "through incident edges.";
}

TEST_F(VersioningGatedTest, BranchGetHopsCounterTracksTraversal) {
  faker.Interpret("UNWIND range(1, 100) AS x CREATE ()-[:NEXT]->()");

  Fork();

  // A CALL subquery traverses 50 of the 100 available edges, within the 100-hop budget.
  auto stream = faker.Interpret(
      "USING HOPS LIMIT 100 CALL { MATCH (a)-[r]->(b) WITH a, r, b LIMIT 50 RETURN count(*) AS cnt } "
      "RETURN getHopsCounter() AS hc");
  const auto &rows = stream.GetResults();
  ASSERT_EQ(rows.size(), 1U);
  const auto hc = rows[0][0].ValueInt();

  EXPECT_EQ(hc, 50) << "getHopsCounter() did not reflect the branch traversal (got " << hc
                    << " instead of 50) -- hops are not being counted on the branch InEdges/OutEdges path.";
}

// A vertex COW'd mid-query has two copies (historical and diff-engine) that must be one identity for the bidirectional
// shortest-path maps keyed on VertexAccessor; locks the gid comparison in query::VertexAccessor/EdgeAccessor
// operator==.
TEST_F(VersioningGatedTest, BranchShortestPathAcrossStubbedVertexHasConsistentIdentity) {
  // Diamond: 2-hop arm 1 -> 2 -> 4 and 3-hop arm 1 -> 5 -> 6 -> 4. The arms meet only if vertex 2 is one identity
  // across storages: fork edge 1 -> 2 holds the historical copy, branch edge 2 -> 4 the COW'd one.
  faker.Interpret(
      "CREATE (s:Chain {id: 1}), (m:Chain {id: 2}), (x:Chain {id: 5}), (y:Chain {id: 6}), (t:Chain {id: 4}), "
      "(s)-[:R]->(m), (s)-[:R]->(x), (x)-[:R]->(y), (y)-[:R]->(t)");

  Fork();

  faker.Interpret("MATCH (n:Chain {id: 2}) SET n.tag = 'cowd'");
  faker.Interpret("MATCH (m:Chain {id: 2}), (t:Chain {id: 4}) CREATE (m)-[:R]->(t)");

  // Both endpoints pre-bound in a separate MATCH select the bidirectional STShortestPathCursor.
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
         "edge out) -- not the 3-hop arm -- a broken hash/equality contract on VertexAccessor (impl_-pointer hash vs "
         "Gid-based "
         "operator==) would mis-key STShortestPathCursor's VertexEdgeMapT, corrupting or truncating the "
         "reconstructed path.";
}

// As above, but the destination is introduced fresh in the same MATCH, which selects SingleSourceShortestPathCursor;
// locks the same operator== gid comparison.
TEST_F(VersioningGatedTest, BranchSingleSourceShortestPathAcrossStubbedVertexHasConsistentIdentity) {
  // Same diamond as BranchShortestPathAcrossStubbedVertexHasConsistentIdentity.
  faker.Interpret(
      "CREATE (s:Chain {id: 1}), (m:Chain {id: 2}), (x:Chain {id: 5}), (y:Chain {id: 6}), (t:Chain {id: 4}), "
      "(s)-[:R]->(m), (s)-[:R]->(x), (x)-[:R]->(y), (y)-[:R]->(t)");

  Fork();

  faker.Interpret("MATCH (n:Chain {id: 2}) SET n.tag = 'cowd'");
  faker.Interpret("MATCH (m:Chain {id: 2}), (t:Chain {id: 4}) CREATE (m)-[:R]->(t)");

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

// An enum defined on main before the fork must be usable on the branch, in a branch-native CREATE and in a SET on a
// COW'd fork vertex, and read back as the exact value written.
TEST_F(VersioningGatedTest, BranchEnumMirroredFromMainSupportsNativeCreateAndSetOnForkVertex) {
  faker.Interpret("CREATE ENUM Status VALUES { Good, Bad };");
  // 'fork' has no enum property yet (the branch-side SET adds one); 'forkenum' already holds one.
  faker.Interpret("CREATE (:X {tag: 'fork'})");
  faker.Interpret("CREATE (:X {tag: 'forkenum', status: Status::Good})");

  Fork();

  faker.Interpret("CREATE (:X {tag: 'native', status: Status::Good})");

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

// Label+property index scans on a branch: each case is a distinct way a stubbed fork vertex can differ from its
// fork-time index entry.
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

  ExpectPlanUses(
      faker, "MATCH (n:P) WHERE n.v >= 10 AND n.v <= 60 RETURN n.name ORDER BY n.v", "ScanAllByLabelProperties");
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

// Reference edges (properties_on_edges=false) have no storage::Edge object, so reading a property on a branch must not
// dereference one (MaybeBranchedHint guards on properties_on_edges).
class VersioningInterpreterReferenceEdgeTest : public VersioningFixture<false> {
 protected:
  void SetUp() override {
    VersioningFixture<false>::SetUp();
    SatisfyGate();
  }
};

TEST_F(VersioningInterpreterReferenceEdgeTest, BranchReferenceEdgePropertyReadDoesNotCrashAndReturnsDefault) {
  faker.Interpret("CREATE (:S)-[:E]->(:A)");
  Fork();
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");

  auto stream = faker.Interpret("MATCH (:S)-[e:E]->(:A) RETURN e.w AS w");
  ASSERT_EQ(stream.GetResults().size(), 1U);
  EXPECT_EQ(stream.GetResults()[0][0].type(), memgraph::communication::bolt::Value::Type::Null)
      << "a reference edge (properties_on_edges=false) has no properties -- GetProperty returns a "
         "default (Null) PropertyValue as a SUCCESS, not an error, and MUST NOT crash reaching it";

  auto native_stream = faker.Interpret("MATCH (a:A) CREATE (a)-[e2:E2]->(:B) RETURN e2.w AS w");
  ASSERT_EQ(native_stream.GetResults().size(), 1U);
  EXPECT_EQ(native_stream.GetResults()[0][0].type(), memgraph::communication::bolt::Value::Type::Null)
      << "a branch-native reference edge must be readable the same way, without crashing";
}

// Multiple sessions on one branch share its diff engine; A = faker and B = faker2, interleaved deterministically on one
// thread.

namespace {
// Runs two overlapping explicit transactions (`a` runs a_write, then `b` runs b_write on the same vertex) and returns
// the type name of the exception b's write raised, or "" on success. Both are rolled back.
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

// A's committed writes (SET, DELETE, CREATE) are what B sees on checkout; main is unchanged.
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

// Snapshot isolation: B's open transaction keeps the old value after A commits to the same vertex; a fresh one sees it.
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

// Overlapping transactions writing the same fork vertex or edge (stub existing or created by the race) must conflict as
// on main without crashing; leftover stubs are invisible and a retried write succeeds.
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

// A's uncommitted DELETE is invisible to B until A commits; B's already-open transaction never sees it.
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

// A writes on the branch while B writes the same vertices on main: no conflict, and A keeps reading fork-time values
// for what it did not change.
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

// DROP BRANCH is refused while another session is attached, and succeeds once every session has detached.
TEST_F(VersioningMultiSessionTest, DropBranchIsRefusedWhileAnotherSessionIsAttached) {
  faker.Interpret("CREATE BRANCH 'b' FROM 'main'");
  faker2.Interpret("CHECKOUT BRANCH 'b'");
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 1U);

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

  faker.Interpret("DROP BRANCH 'b'");
  EXPECT_FALSE(faker.interpreter.current_db_.CurrentVersion().has_value());
  EXPECT_FALSE(db->version_store()->Exists("b"));
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 0U);
}

// An idle session with the branch checked out and no open transaction holds none of main's locks: main DDL is not
// blocked.
TEST_F(VersioningMultiSessionTest, IdleBranchSessionDoesNotBlockMainDdl) {
  SetUpTwoSessionsOnBranch();

  faker.Interpret("CHECKOUT BRANCH 'main'");  // B stays attached to 'b', idle.
  faker.Interpret("CREATE INDEX ON :K(id)");
  EXPECT_EQ(IntRows(faker, "MATCH (n:K {id:1}) RETURN n.v"), (Rows{{10}}));

  EXPECT_EQ(IntRows(faker2, kReadAll), (Rows{{1, 10}, {2, 20}}));
}

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

// SET on a fork edge is visible from both ends and in whole-property reads, keeps the other properties, shows no
// marker, and leaves main untouched.
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

// REMOVE r.p, SET r.p = null, SET r = {...} and SET r += {...} on a fork edge: removed fork properties stay gone,
// re-setting one brings it back, and no marker is ever visible.
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

// A rolled-back write to a fork edge must not wedge it: the retried write succeeds.
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

// Deleting a stub-backed fork edge removes it everywhere on the branch (both directions, scans, index scans) and leaves
// main alone while a parallel sibling survives; DETACH DELETE of an endpoint hides a stubbed incident edge the same
// way.
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

// Edge-type+property and global edge-property index scans on a branch see a stubbed fork edge at its branch value only;
// an unrelated property write keeps the fork value findable.
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

  // Grant on :A, access scoped to the default database only.
  memgraph::auth::User granted{"u"};
  granted.db_access().Grant(main_name);
  granted.fine_grained_access_handler().label_permissions().Grant({"A"}, memgraph::auth::kAllLabelPermissions);
  memgraph::glue::FineGrainedAuthChecker granted_checker{granted, &branch_dba};
  EXPECT_TRUE(granted_checker.Has(std::array{a}, read));
  EXPECT_FALSE(granted_checker.Has(std::array{b}, read));

  // A DENY on :B still applies next to a global grant.
  memgraph::auth::User denied{"d"};
  denied.db_access().Grant(main_name);
  denied.fine_grained_access_handler().label_permissions().GrantGlobal(memgraph::auth::kAllLabelPermissions);
  denied.fine_grained_access_handler().label_permissions().Deny({"B"}, memgraph::auth::kAllLabelPermissions);
  memgraph::glue::FineGrainedAuthChecker denied_checker{denied, &branch_dba};
  EXPECT_TRUE(denied_checker.Has(std::array{a}, read));
  EXPECT_FALSE(denied_checker.Has(std::array{b}, read));
}
#endif

// An index dropped on main after the fork must not change what the branch returns, in order, for index-planned scans.
TEST_F(VersioningGatedTest, BranchOrderedEdgePropertyScanSurvivesMainDroppingIndex) {
  struct Case {
    const char *ddl;
    const char *drop;
    const char *match;
    const char *op;
  };

  static constexpr Case cases[] = {
      {"CREATE EDGE INDEX ON :T(p)", "DROP EDGE INDEX ON :T(p)", "()-[r:T]->()", "ScanAllByEdgeTypeProperty"},
      {"CREATE GLOBAL EDGE INDEX ON :(p)", "DROP GLOBAL EDGE INDEX ON :(p)", "()-[r]->()", "ScanAllByEdgeProperty"},
  };

  for (const auto &c : cases) {
    SCOPED_TRACE(c.ddl);
    faker.Interpret("MATCH (n) DETACH DELETE n");
    faker.Interpret(c.ddl);
    // Creation order (3, 1, 2) differs from property order.
    faker.Interpret("CREATE (:V)-[:T {p: 3}]->(:V)");
    faker.Interpret("CREATE (:V)-[:T {p: 1}]->(:V)");
    faker.Interpret("CREATE (:V)-[:T {p: 2}]->(:V)");
    faker.Interpret("CREATE BRANCH 'd' FROM 'main'");
    faker.Interpret("CHECKOUT BRANCH 'd'");
    ASSERT_EQ(IntRows(faker, "MATCH (n:V) RETURN count(n)"), (Rows{{6}}));

    faker.Interpret("CHECKOUT BRANCH 'main'");
    faker.Interpret(c.drop);
    faker.Interpret("CHECKOUT BRANCH 'd'");

    const std::string presence = std::string{"MATCH "} + c.match + " WHERE r.p IS NOT NULL RETURN r.p ORDER BY r.p";
    const std::string range = std::string{"MATCH "} + c.match + " WHERE r.p > 0 RETURN r.p ORDER BY r.p";
    EXPECT_EQ(IntRows(faker, presence), (Rows{{1}, {2}, {3}})) << "presence scan on branch";
    EXPECT_EQ(IntRows(faker, range), (Rows{{1}, {2}, {3}})) << "range scan on branch";
    EXPECT_TRUE(PlanHasOperator(ExplainText(faker, range), c.op)) << ExplainText(faker, range);

    faker.Interpret("CHECKOUT BRANCH 'main'");
    faker.Interpret("DROP BRANCH 'd'");
  }
}
