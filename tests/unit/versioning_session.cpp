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

// A session's branch state: checkout bookkeeping across database switches, detach/reattach, parking, nested sessions,
// a branch dropped under the session, and the per-checkout plan cache.

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "query/exceptions.hpp"
#include "query/interpreter.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/gatekeeper.hpp"
#include "versioning_fixture.hpp"

using memgraph::tests::versioning_fixture::ExplainText;
using memgraph::tests::versioning_fixture::IntRows;
using memgraph::tests::versioning_fixture::PlanHasOperator;
using memgraph::tests::versioning_fixture::Rows;
using memgraph::tests::versioning_fixture::SortedNames;
using memgraph::tests::versioning_fixture::ThrownMessage;

TEST_F(VersioningGatedTest, SetCurrentDBOnlyResetsCurrentVersionOnADifferentDatabase) {
  Fork("feature");
  auto &current_db = faker.interpreter.current_db_;
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
  EXPECT_EQ(current_db.branch_context(), nullptr);
  EXPECT_EQ(db->version_store()->CheckoutCount("feature"), 0U) << "the switch must release the checkout";

  // current_db holds an accessor into other_gk, whose destructor blocks until every accessor is released; move it back
  // to the fixture's database first, as locals destruct in reverse order.
  current_db.SetCurrentDB(*same_db_acc_opt, false);
}

// A branch's diff engine is a separate storage, but its database name is what auth, FGA, SHOW and DUMP see through
// DbAccessor::DatabaseName(), so it must follow a RENAME of main, for a core built before and one built after.
TEST_F(VersioningGatedTest, BranchSessionReportsRenamedDatabaseName) {
  Fork("feature");
  auto &live_engine = faker.interpreter.current_db_.branch_context()->diff_engine();
  const auto old_name = db->name();
  ASSERT_EQ(live_engine.name(), old_name);

  db->Rename("renamed_db");

  EXPECT_EQ(live_engine.name(), "renamed_db") << "core built before the rename kept the old name";

  Fork("later");
  ASSERT_NE(faker.interpreter.current_db_.branch_context(), nullptr);
  EXPECT_EQ(faker.interpreter.current_db_.branch_context()->diff_engine().name(), "renamed_db");
}

// Re-applying a same-database DatabaseAccess to a live interpreter must not disturb an active checkout.
TEST_F(VersioningGatedTest, ReApplyingSameDatabaseAccessPreservesCheckout) {
  Fork("feature");
  auto &current_db = faker.interpreter.current_db_;
  ASSERT_EQ(current_db.CurrentVersion(), "feature");

  auto same_db_acc_opt = db_gk.access();
  ASSERT_TRUE(same_db_acc_opt);
  current_db.SetCurrentDB(*same_db_acc_opt, /*in_explicit_db=*/false);
  EXPECT_EQ(current_db.CurrentVersion(), "feature");
  // A real checkout also holds a BranchContext and a checkout slot; a same-database re-apply must keep both.
  EXPECT_NE(current_db.branch_context(), nullptr);
  EXPECT_EQ(db->version_store()->CheckoutCount("feature"), 1U);
  const auto number = db->version_store()->Get("feature")->number;
  EXPECT_EQ(current_db.foreign_branch_number(), number);
  EXPECT_TRUE(current_db.OnBranch(number));
  EXPECT_FALSE(current_db.OnBranch(number + 1));
}

// Branch writes live in the branch's shared core, so they survive the session detaching and attaching again, and
// never reach main. Both commit paths run: the plain commit and the periodic commit's batches.
TEST_F(VersioningGatedTest, BranchStateSurvivesDetachReattach) {
  faker.Interpret("CREATE (:Person:Employee {id: 1})");
  faker.Interpret("CREATE (:User {id: 1})-[:E]->(:User {id: 2, age: 40, keep: 7})");
  faker.Interpret("CREATE (:RP {id: 1, p: 7, q: 8})");
  Fork();

  faker.Interpret("CREATE (:M1 {v:1})");
  faker.Interpret("USING PERIODIC COMMIT 1 UNWIND [1, 2] AS i CREATE (:M2 {v: i})");
  faker.Interpret("MATCH (n:M1) SET n.v = 2");
  faker.Interpret("MATCH (n:Person {id: 1}) REMOVE n:Employee");
  faker.Interpret("MATCH (v:User {id: 2}) SET v.age = 99");
  faker.Interpret("MATCH (n:RP) REMOVE n.p");
  faker.Interpret("MATCH (n:RP) SET n.q = null");

  auto const check_branch = [&] {
    EXPECT_EQ(IntRows(faker, "MATCH (n:M1) RETURN n.v"), (Rows{{2}})) << "the create and the later SET must be visible";
    EXPECT_EQ(IntRows(faker, "MATCH (n:M2) RETURN n.v ORDER BY n.v"), (Rows{{1}, {2}}));
    auto labels = faker.Interpret("MATCH (n:Person {id: 1}) RETURN labels(n)");
    ASSERT_EQ(labels.GetResults().size(), 1U);
    EXPECT_EQ(SortedNames(labels.GetResults()[0][0]), (std::vector<std::string>{"Person"}));
    EXPECT_EQ(IntRows(faker, "MATCH (:User {id: 1})-[:E]->(v) RETURN v.age, v.keep"), (Rows{{99, 7}}));
    auto removed = faker.Interpret("MATCH (n:RP) RETURN keys(n), n.id");
    ASSERT_EQ(removed.GetResults().size(), 1U);
    EXPECT_EQ(SortedNames(removed.GetResults()[0][0]), (std::vector<std::string>{"id"}));
    EXPECT_EQ(removed.GetResults()[0][1].ValueInt(), 1);
  };

  check_branch();

  faker.interpreter.current_db_.LeaveBranch();
  EXPECT_EQ(db->version_store()->CheckoutCount("b"), 0U);
  Attach(faker, "b");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "b");
  check_branch();

  faker.interpreter.current_db_.LeaveBranch();
  EXPECT_EQ(IntRows(faker, "MATCH (n:M1) RETURN count(n)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:M2) RETURN count(n)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:Person:Employee) RETURN count(n)"), (Rows{{1}}));
  EXPECT_EQ(IntRows(faker, "MATCH (v:User {id: 2}) RETURN v.age"), (Rows{{40}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:RP) RETURN n.p, n.q"), (Rows{{7, 8}}));
}

// A parked branch comes back when the transaction ends, releasing what was attached meanwhile; a discarded one is
// released instead.
TEST_F(VersioningGatedTest, ParkedBranchIsRestoredAfterTheTransactionOrDiscarded) {
  ASSERT_TRUE(db->version_store()->CreateBranch("other"));
  Fork("b");
  auto &current_db = faker.interpreter.current_db_;
  auto *version_store = db->version_store();

  current_db.ParkSessionBranch();
  EXPECT_FALSE(current_db.CurrentVersion().has_value());
  EXPECT_EQ(current_db.foreign_branch_number(), 0U);
  EXPECT_EQ(version_store->CheckoutCount("b"), 1U) << "a parked branch keeps its checkout";
  Attach(faker, "other");
  EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n)"), (Rows{{0}}));
  EXPECT_EQ(current_db.CurrentVersion(), "b") << "the transaction's end restores the parked branch";
  EXPECT_EQ(current_db.foreign_branch_number(), version_store->Get("b")->number);
  EXPECT_EQ(version_store->CheckoutCount("other"), 0U);
  EXPECT_EQ(version_store->CheckoutCount("b"), 1U);

  current_db.ParkSessionBranch();
  Attach(faker, "other");
  current_db.DiscardParkedBranch();
  EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n)"), (Rows{{0}}));
  EXPECT_EQ(current_db.CurrentVersion(), "other");
  EXPECT_EQ(version_store->CheckoutCount("b"), 0U);
  EXPECT_EQ(version_store->CheckoutCount("other"), 1U);
}

// A nested session (mgp_execute_query) joins the caller's branch with a checkout of its own, released when it ends.
TEST_F(VersioningGatedTest, NestedSessionAttachesToTheCallersBranch) {
  Fork();
  auto *version_store = db->version_store();
  auto &outer = *faker.interpreter.current_db_.branch_context();
  {
    memgraph::query::Interpreter nested{&interpreter_context, db};
    nested.current_db_.AttachBranchOf(version_store, outer);
    EXPECT_EQ(nested.current_db_.CurrentVersion(), "b");
    ASSERT_NE(nested.current_db_.branch_context(), nullptr);
    EXPECT_EQ(nested.current_db_.branch_context()->core(), outer.core());
    EXPECT_EQ(version_store->CheckoutCount("b"), 2U);
  }
  EXPECT_EQ(version_store->CheckoutCount("b"), 1U) << "a destroyed session must release its checkout";

  ASSERT_TRUE(version_store->ForceDropBranch("b"));
  memgraph::query::Interpreter nested{&interpreter_context, db};
  EXPECT_THROW(nested.current_db_.AttachBranchOf(version_store, outer), memgraph::query::QueryRuntimeException);
  EXPECT_EQ(nested.current_db_.branch_context(), nullptr);
  EXPECT_FALSE(nested.current_db_.CurrentVersion().has_value());
}

// A branch dropped under the session never falls back to main: every query, with or without a transaction, fails until
// the session leaves it; the dead checkout is released on the first refusal.
TEST_F(VersioningGatedTest, ABranchDroppedUnderTheSessionFailsEveryQueryUntilItLeaves) {
  Fork();
  auto &current_db = faker.interpreter.current_db_;
  ASSERT_TRUE(db->version_store()->ForceDropBranch("b"));

  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("SHOW VERSION"); }),
            "Branch 'b' no longer exists. Run USE BRANCH or USE DATABASE to continue.");
  EXPECT_EQ(current_db.branch_context(), nullptr);
  EXPECT_EQ(current_db.CurrentVersion(), "b");
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("MATCH (n) RETURN count(n)"); }),
            "Branch 'b' no longer exists. Run USE BRANCH or USE DATABASE to continue.");

  auto same_db_acc_opt = db_gk.access();
  ASSERT_TRUE(same_db_acc_opt);
  current_db.SetCurrentDB(*same_db_acc_opt, /*in_explicit_db=*/true);
  EXPECT_EQ(ThrownMessage([&] { faker.Interpret("RETURN 1"); }), "Branch 'b' no longer exists.")
      << "a db-pinned session gets no USE hint";

  current_db.LeaveBranch();
  EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n)"), (Rows{{0}}));
}

// Plans are cached per checkout: the branch never reuses main's cached plan (built against main's indexes), and main's
// cache is never filled from the branch.
TEST_F(VersioningGatedTest, BranchQueryPlanCacheServesCorrectResultsAcrossRepeatedRuns) {
  faker.Interpret("CREATE (:User {id: 1, tag: 'main'})");
  faker.Interpret("CREATE (:User {id: 2, tag: 'main'})");
  faker.Interpret("CREATE (:User {id: 3, tag: 'main'})");

  Fork();

  // The index is created on main AFTER the fork, so the branch has no such index: main's cached plan for the query
  // below is a property-index scan, which -- reused on the branch -- would scan the diff engine's empty index.
  auto &current_db = faker.interpreter.current_db_;
  current_db.LeaveBranch();
  faker.Interpret("CREATE INDEX ON :User(id)");
  ASSERT_EQ(IntRows(faker, "MATCH (n:User {id: 1}) RETURN n.id"), (Rows{{1}}));
  ASSERT_TRUE(PlanHasOperator(ExplainText(faker, "MATCH (n:User {id: 1}) RETURN n.tag"), "ScanAllByLabelProperties"));
  const auto main_cached = db->plan_cache()->ReadLock()->size();

  Attach(faker, "b");
  ASSERT_EQ(current_db.branch_plan_cache()->ReadLock()->size(), 0U) << "each checkout starts with an empty cache";
  for (int run = 0; run < 2; ++run) {
    EXPECT_FALSE(PlanHasOperator(ExplainText(faker, "MATCH (n:User {id: 1}) RETURN n.tag"), "ScanAllByLabelProperties"))
        << "run " << run << ": the branch must plan against its own (index-less) view, not reuse main's cached plan";
  }
  EXPECT_EQ(current_db.branch_plan_cache()->ReadLock()->size(), 1U) << "the branch plan is cached per checkout";
  EXPECT_EQ(db->plan_cache()->ReadLock()->size(), main_cached) << "branch plans must not land in main's cache";

  // The cached branch plan serves the fork state, also for a different literal.
  for (auto const id : {1, 1, 2}) {
    auto stream = faker.Interpret("MATCH (n:User {id: " + std::to_string(id) + "}) RETURN n.tag");
    ASSERT_EQ(stream.GetResults().size(), 1U) << "fork-state row " << id << " must be visible on the branch";
    EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "main");
  }

  // ...and a fresh branch-native row.
  faker.Interpret("CREATE (:User {id: 1, tag: 'branch'})");
  auto stream = faker.Interpret("MATCH (n:User {id: 1}) RETURN n.tag ORDER BY n.tag");
  ASSERT_EQ(stream.GetResults().size(), 2U);
  EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "branch");
  EXPECT_EQ(stream.GetResults()[1][0].ValueString(), "main");
}

// Branches work when main narrows the commit lock: the branch's private diff engine never does, and a branch
// session commits (plain, explicit and periodic) while main commits around it.
TEST_F(VersioningNarrowingGatedTest, BranchSessionRunsWithMainCommitLockNarrowingOn) {
  ASSERT_TRUE(db->storage()->config_.experimental_commit_lock_narrowing);
  faker.Interpret("CREATE (:M {v: 1})");
  Fork();
  EXPECT_FALSE(
      faker.interpreter.current_db_.branch_context()->diff_engine().config_.experimental_commit_lock_narrowing);

  faker.Interpret("CREATE (:B {v: 2})");
  faker.Interpret("USING PERIODIC COMMIT 1 UNWIND [3, 4] AS i CREATE (:B {v: i})");
  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (n:B) SET n.v = n.v + 10");
  faker.Interpret("COMMIT");
  EXPECT_EQ(IntRows(faker, "MATCH (n:B) RETURN n.v ORDER BY n.v"), (Rows{{12}, {13}, {14}}));

  faker.interpreter.current_db_.LeaveBranch();
  faker.Interpret("CREATE (:M {v: 5})");
  EXPECT_EQ(IntRows(faker, "MATCH (n:B) RETURN count(n)"), (Rows{{0}})) << "branch writes never reach main";
  Attach(faker, "b");
  EXPECT_EQ(IntRows(faker, "MATCH (n:B) RETURN count(n)"), (Rows{{3}}));
}

// A branch transaction runs at the isolation level main is currently configured with (SET GLOBAL ISOLATION LEVEL
// applies to branches too); a per-transaction override still wins.
TEST_F(VersioningGatedTest, BranchTransactionDefaultsToMainsIsolationLevelUnlessOverridden) {
  Fork();
  auto &current_db = faker.interpreter.current_db_;
  const auto isolation_of = [&](std::optional<memgraph::storage::IsolationLevel> override_level) {
    current_db.SetupDatabaseTransaction(override_level, false, memgraph::storage::StorageAccessType::READ);
    const auto level = current_db.db_transactional_accessor_->GetTransaction()->isolation_level;
    current_db.CleanupDBTransaction(false);
    return level;
  };
  using memgraph::storage::IsolationLevel;

  ASSERT_EQ(isolation_of(std::nullopt), IsolationLevel::SNAPSHOT_ISOLATION);
  ASSERT_TRUE(db->storage()->SetIsolationLevel(IsolationLevel::READ_COMMITTED));
  EXPECT_EQ(isolation_of(std::nullopt), IsolationLevel::READ_COMMITTED);
  EXPECT_EQ(isolation_of(IsolationLevel::READ_UNCOMMITTED), IsolationLevel::READ_UNCOMMITTED);
}

// A branch transaction that fails to set up after the fork snapshot is opened must release main's storage guard at
// once, not when the session next ends a transaction.
TEST_F(VersioningGatedTest, FailedBranchTransactionSetupReleasesMainsStorageGuard) {
  Fork();
  auto &current_db = faker.interpreter.current_db_;
  auto blocker = current_db.branch_context()->diff_engine().UniqueAccess();  // The branch's accessor cannot be granted.

  EXPECT_THROW(current_db.SetupDatabaseTransaction(std::nullopt, false, memgraph::storage::StorageAccessType::WRITE),
               memgraph::storage::SharedAccessTimeout);

  EXPECT_NO_THROW(db->storage()->UniqueAccess(std::nullopt, std::chrono::milliseconds{200}))
      << "main's READ guard is still held by the half-open branch transaction";
}

// The session commits through the BranchContext, which publishes the transaction's fork deletes: the interpreter's
// COMMIT of an explicit branch transaction.
TEST_F(VersioningGatedTest, BranchSessionCommitPublishesTheForkDeletes) {
  faker.Interpret("CREATE (:V), (:V)");
  Fork();
  auto &ctx = *faker.interpreter.current_db_.branch_context();

  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (n:NoSuchLabel) SET n.x = 1");  // Opens the branch's write transaction.
  ASSERT_TRUE(ctx.in_transaction());
  ASSERT_TRUE(ctx.DeleteForkVertex(memgraph::storage::Gid::FromUint(0)).value());
  EXPECT_EQ(ctx.core()->ForkDeletes(), (memgraph::versioning::ObjectCounts{0, 0})) << "published by the commit";
  faker.Interpret("COMMIT");

  EXPECT_EQ(ctx.core()->ForkDeletes(), (memgraph::versioning::ObjectCounts{1, 0}));
}

// DbAccessor::PeriodicCommit on a branch transaction goes through the BranchContext: each batch publishes only the
// deletes made since the previous one.
TEST_F(VersioningGatedTest, BranchPeriodicCommitPublishesEachBatchsForkDeletes) {
  faker.Interpret("CREATE (:V), (:V)");
  Fork();
  auto &current_db = faker.interpreter.current_db_;
  auto &ctx = *current_db.branch_context();
  const auto gid = [](uint64_t g) { return memgraph::storage::Gid::FromUint(g); };

  current_db.SetupDatabaseTransaction(std::nullopt, false, memgraph::storage::StorageAccessType::WRITE);
  ASSERT_TRUE(ctx.DeleteForkVertex(gid(0)).value());
  ASSERT_TRUE(current_db.execution_db_accessor_->PeriodicCommit(memgraph::tests::MakeMainCommitArgs()).has_value());
  EXPECT_EQ(ctx.core()->ForkDeletes(), (memgraph::versioning::ObjectCounts{1, 0}));

  ASSERT_TRUE(ctx.DeleteForkVertex(gid(1)).value());
  ASSERT_TRUE(ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
  current_db.CleanupDBTransaction(false);
  EXPECT_EQ(ctx.core()->ForkDeletes(), (memgraph::versioning::ObjectCounts{2, 0})) << "the first batch counted once";
}

// The first checkout of a branch whose ledger already holds gid floors (built before a restart, nothing to replay)
// builds the core on those floors, not on main's counters of the day: main objects created since the fork are not fork
// objects.
TEST_F(VersioningGatedTest, LazyCoreBuildUsesTheLedgersGidFloors) {
  faker.Interpret("CREATE (:V), (:V)");
  auto &main = static_cast<memgraph::storage::InMemoryStorage &>(*db->storage());
  ASSERT_TRUE(db->version_store()->CreateBranch("b"));
  const auto info = *db->version_store()->Get("b");
  ASSERT_FALSE(info.vertex_gid_floor.has_value());
  const auto vertex_floor = main.vertex_id_.load();
  const auto edge_floor = main.edge_id_.load();
  ASSERT_TRUE(db->version_store()->RecordGidFloors(info.number, vertex_floor, edge_floor));
  faker.Interpret("CREATE (:V), (:V), (:V)");
  ASSERT_GT(main.vertex_id_.load(), vertex_floor);

  Attach(faker, "b");

  EXPECT_EQ(faker.interpreter.current_db_.branch_context()->core()->vertex_gid_floor(), vertex_floor);
  EXPECT_EQ(faker.interpreter.current_db_.branch_context()->core()->edge_gid_floor(), edge_floor);
}
