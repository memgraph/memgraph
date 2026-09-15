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

// Unit tests for the drop-driven idle-session reaper (always-on, enterprise-only).
// The reaper releases a connected-but-idle Bolt session's db_acc_ when its tenant is being dropped,
// matched by UUID; it is driven by the deferred-drop worker's per-tick drain hook
// (src/dbms/handler.hpp + memgraph.cpp), not a timeout sweep. These tests drive
// TryReleaseDbAccessorForDrop directly (white-box), standing in for that hook.

#include "gtest/gtest.h"

#ifdef MG_ENTERPRISE

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <optional>
#include <string>
#include <thread>

#include "auth/auth.hpp"
#include "communication/result_stream_faker.hpp"
#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "flags/general.hpp"
#include "flags/run_time_configurable.hpp"
#include "interpreter_faker.hpp"
#include "license/license.hpp"
#include "parameters/parameters.hpp"
#include "query/auth_checker.hpp"
#include "query/config.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "query/typed_value.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/logging.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/synchronized.hpp"

namespace {

memgraph::storage::Config MakeConfig(const std::filesystem::path &dir) {
  memgraph::storage::Config cfg{};
  memgraph::storage::UpdatePaths(cfg, dir);
  cfg.durability.snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
  cfg.durability.recover_on_startup = false;
  cfg.durability.snapshot_on_exit = false;
  return cfg;
}

struct MinMemgraph {
  explicit MinMemgraph(const memgraph::storage::Config &conf)
      : settings{conf.durability.storage_directory / "settings"},
        auth{conf.durability.storage_directory / "auth", memgraph::auth::Auth::Config{}},
        parameters{conf.durability.storage_directory},
        repl_state{ReplicationStateRootPath(conf)},
        dbms{conf},
        interpreter_context{{}, &settings, &parameters, &dbms, &repl_state, system, nullptr, nullptr, nullptr} {
    memgraph::license::RegisterLicenseSettings(memgraph::license::global_license_checker, settings);
    memgraph::flags::run_time::Initialize(settings);
    memgraph::license::global_license_checker.CheckEnvLicense(settings);
  }

  auto NewInterpreter() { return InterpreterFaker{&interpreter_context, dbms.Get()}; }

  memgraph::utils::Settings settings;
  memgraph::auth::SynchedAuth auth;
  memgraph::system::System system;
  memgraph::parameters::Parameters parameters;
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state;
  memgraph::dbms::DbmsHandler dbms;
  memgraph::query::InterpreterContext interpreter_context;
};

}  // namespace

class IdleSessionReaperTest : public ::testing::Test {
 public:
  std::filesystem::path data_directory = std::filesystem::temp_directory_path() / "MG_tests_unit_idle_session_reaper";

  void SetUp() override {
    TearDown();
    min_mg.emplace(MakeConfig(data_directory));
  }

  void TearDown() override {
    min_mg.reset();
    if (std::filesystem::exists(data_directory)) std::filesystem::remove_all(data_directory);
  }

  auto &DBMS() { return min_mg->dbms; }

  void CreateAndPopulate(const std::string &name, int n) {
    ASSERT_TRUE(DBMS().New(name).has_value());
    auto db_acc = DBMS().Get(name);
    auto storage_acc = db_acc->Access(memgraph::storage::WRITE);
    for (int i = 0; i < n; ++i) storage_acc->CreateVertex();
    ASSERT_TRUE(storage_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  std::optional<MinMemgraph> min_mg;
};

// R1: a reapable idle session's accessor is released when its tenant is being dropped; the tenant
// (no other holders) then suspends.
TEST_F(IdleSessionReaperTest, ReapsIdleSessionAndTenantBecomesSuspendable) {
  const std::string db_name = "reap_r1";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value());
  const auto db_uuid = interpreter.interpreter.current_db_.db_acc_->get()->uuid();

  // Held accessor blocks suspend.
  EXPECT_FALSE(DBMS().Suspend(db_name).has_value());

  // Drop-driven release: releases db_acc_ for the matching UUID.
  std::optional<memgraph::dbms::DatabaseAccess> released_r1;
  EXPECT_TRUE(interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released_r1));
  EXPECT_FALSE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "reaper must release the idle session's accessor";
  // Destroy the collected accessor outside the reaping span so the tenant has zero holders.
  released_r1.reset();

  // Tenant now has zero holders -> suspendable.
  EXPECT_TRUE(DBMS().Suspend(db_name).has_value()) << "tenant must be suspendable after its sessions are reaped";
}

// R2: after a drop-driven release, the next query re-acquires the (still-HOT) accessor via Get() and reads data.
TEST_F(IdleSessionReaperTest, ReapedSessionReacquiresOnNextQuery) {
  const std::string db_name = "reap_r2";
  CreateAndPopulate(db_name, 3);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  const auto db_uuid = interpreter.interpreter.current_db_.db_acc_->get()->uuid();

  std::optional<memgraph::dbms::DatabaseAccess> released_r2;
  ASSERT_TRUE(interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released_r2));
  released_r2.reset();  // destroy outside the interpreter before re-acquiring
  ASSERT_FALSE(interpreter.interpreter.current_db_.db_acc_.has_value());

  {
    auto [stream, qid] = interpreter.Prepare("MATCH (n) RETURN count(n) AS c");
    EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value()) << "next query must re-acquire the accessor";
    interpreter.Pull(&stream);
    const auto &results = stream.GetResults();
    ASSERT_EQ(results.size(), 1U);
    EXPECT_EQ(results[0][0].ValueInt(), 3) << "reheated tenant must expose the original 3 nodes";
  }
}

// R4: a non-reapable interpreter (e.g. a stream consumer / internal interpreter) is never reaped.
TEST_F(IdleSessionReaperTest, DoesNotReapNonReapableInterpreter) {
  const std::string db_name = "reap_r4";
  CreateAndPopulate(db_name, 1);

  auto interpreter = min_mg->NewInterpreter();  // NOT marked reapable
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value());
  const auto db_uuid = interpreter.interpreter.current_db_.db_acc_->get()->uuid();

  std::optional<memgraph::dbms::DatabaseAccess> released_r4;
  EXPECT_FALSE(interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released_r4))
      << "non-reapable interpreters must never be reaped";
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value());
}

// R5: a session in an explicit transaction (status != IDLE) is never reaped.
TEST_F(IdleSessionReaperTest, DoesNotReapMidExplicitTransaction) {
  const std::string db_name = "reap_r5";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  const auto db_uuid = interpreter.interpreter.current_db_.db_acc_->get()->uuid();

  {
    auto [stream, qid] = interpreter.Prepare("BEGIN");
    interpreter.Pull(&stream);
  }
  // Inside an explicit transaction transaction_status_ is ACTIVE, so the reaper's IDLE pre-check fails.
  std::optional<memgraph::dbms::DatabaseAccess> released_r5;
  EXPECT_FALSE(interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released_r5))
      << "must not reap a session that is in an explicit transaction";
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value());

  {
    auto [stream, qid] = interpreter.Prepare("COMMIT");
    interpreter.Pull(&stream);
  }
}

// R10: after a drop-driven release, the native Bolt BeginTransaction() path re-acquires the accessor
// and opens an explicit transaction successfully. Prior to the B3 fix, BeginTransaction() bypassed
// EnsureDbAccessForQuery() and threw because db_acc_ was null for a healthy (HOT) tenant.
TEST_F(IdleSessionReaperTest, ReapedSessionReacquiresOnBoltBegin) {
  const std::string db_name = "reap_r10";
  CreateAndPopulate(db_name, 4);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  const auto db_uuid = interpreter.interpreter.current_db_.db_acc_->get()->uuid();

  // Release the accessor, leaving the session db-less while the tenant stays HOT.
  std::optional<memgraph::dbms::DatabaseAccess> released_r10;
  ASSERT_TRUE(interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released_r10));
  released_r10.reset();  // destroy outside the reaping span before re-acquiring
  ASSERT_FALSE(interpreter.interpreter.current_db_.db_acc_.has_value());

  // Native Bolt BEGIN (the B3 fix path): must not throw even with a null db_acc_.
  // EnsureDbAccessForQuery() always runs (enterprise-only, unconditional), so BeginTransaction()
  // re-acquires a reaper-released accessor transparently.
  ASSERT_NO_THROW(interpreter.interpreter.BeginTransaction(memgraph::query::QueryExtras{}));

  // EnsureDbAccessForQuery() inside BeginTransaction() must have re-acquired the accessor.
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "BeginTransaction must re-acquire the db_acc_ for the named HOT tenant";

  // Inside the open explicit transaction, a read must see the data written before the reap.
  {
    auto [stream, qid] = interpreter.Prepare("MATCH (n) RETURN count(n) AS c");
    interpreter.Pull(&stream);
    const auto &results = stream.GetResults();
    ASSERT_EQ(results.size(), 1U);
    EXPECT_EQ(results[0][0].ValueInt(), 4) << "re-acquired tenant must expose the original 4 nodes";
  }

  // Commit to leave the interpreter in a clean IDLE state.
  ASSERT_NO_THROW(interpreter.interpreter.CommitTransaction());
}

// NOTE: default-DB protection is now structural (default is never a drop target) rather than a
// reaper special-case — DbmsHandler::Delete_ rejects kDefaultDB, so its UUID is never handed to
// the drop-driven reaper.

// R8: recycle safety. After a drop-driven release, if the tenant's NAME is dropped and a DIFFERENT
// tenant is recreated under it, the next query must NOT silently attach to the new tenant.
// The UUID captured when the session bound the database no longer matches, so re-acquire fails closed.
TEST_F(IdleSessionReaperTest, ReacquireFallsBackToDbLessWhenTenantRecycled) {
  const std::string db_name = "reap_r8";
  CreateAndPopulate(db_name, 5);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  ASSERT_TRUE(interpreter.interpreter.current_db_.current_db_uuid_.has_value());
  const auto original_uuid = *interpreter.interpreter.current_db_.current_db_uuid_;
  const auto db_uuid = interpreter.interpreter.current_db_.db_acc_->get()->uuid();

  // Drop-driven release: release the idle accessor (current_db_name_ + current_db_uuid_ are kept).
  std::optional<memgraph::dbms::DatabaseAccess> released_r8;
  ASSERT_TRUE(interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released_r8));
  released_r8.reset();  // destroy outside the reaping span so DBMS().Delete succeeds (zero holders)
  ASSERT_FALSE(interpreter.interpreter.current_db_.db_acc_.has_value());

  // Recycle the NAME: drop it (no holders after the reap) and recreate a different tenant under it.
  ASSERT_TRUE(DBMS().Delete(db_name).has_value());
  ASSERT_TRUE(DBMS().New(db_name).has_value());
  ASSERT_NE(DBMS().Get(db_name)->uuid(), original_uuid) << "the recreated tenant must have a fresh UUID";

  // Next query: the re-acquire detects the recycle (UUID mismatch) and falls back to a db-less session
  // rather than silently attaching to the new tenant or wedging. A db-requiring query then errors with
  // the normal "no current database", NOT the recycled tenant's (empty) data.
  try {
    auto [stream, qid] = interpreter.Prepare("MATCH (n) RETURN count(n)");
    interpreter.Pull(&stream);
    FAIL() << "a db-requiring query on a recycled/db-less session must not succeed";
  } catch (const memgraph::query::QueryException &) {
  }
  EXPECT_FALSE(interpreter.interpreter.current_db_.db_acc_.has_value()) << "must not attach to the recycled tenant";
  EXPECT_FALSE(interpreter.interpreter.current_db_.current_db_name_.has_value())
      << "session must fall back to db-less, not stay wedged on the recycled name";

  // In-session recovery: USE rebinds identity to the new tenant and works (no reconnect needed).
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  {
    auto [stream, qid] = interpreter.Prepare("MATCH (n) RETURN count(n) AS c");
    interpreter.Pull(&stream);
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 0) << "the recreated tenant is empty";
  }
}

// R9: the current DB is DROPPED out from under the session (not recreated) -> db-less fallback,
// recoverable in-session (USE another database) rather than a forced reconnect.
TEST_F(IdleSessionReaperTest, ReacquireFallsBackToDbLessWhenTenantDropped) {
  const std::string db_name = "reap_r9";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  const auto db_uuid = interpreter.interpreter.current_db_.db_acc_->get()->uuid();
  std::optional<memgraph::dbms::DatabaseAccess> released_r9;
  ASSERT_TRUE(interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released_r9));  // release the accessor
  released_r9.reset();  // destroy outside the reaping span so DBMS().Delete succeeds (zero holders)
  ASSERT_FALSE(interpreter.interpreter.current_db_.db_acc_.has_value());

  ASSERT_TRUE(DBMS().Delete(db_name).has_value());  // drop it: the tenant is gone

  // Next query: the re-acquire finds the tenant gone (UnknownDatabaseException) and falls back to db-less.
  try {
    auto [stream, qid] = interpreter.Prepare("MATCH (n) RETURN count(n)");
    interpreter.Pull(&stream);
    FAIL() << "a db-requiring query on a dropped/db-less session must not succeed";
  } catch (const memgraph::query::QueryException &) {
  }
  EXPECT_FALSE(interpreter.interpreter.current_db_.db_acc_.has_value());
  EXPECT_FALSE(interpreter.interpreter.current_db_.current_db_name_.has_value())
      << "session must fall back to db-less after its current DB is dropped";

  // In-session recovery: create another DB and USE it on the same session.
  ASSERT_TRUE(DBMS().New("reap_r9b").has_value());
  interpreter.interpreter.SetCurrentDB("reap_r9b", /*in_explicit_db=*/false);
  {
    auto [stream, qid] = interpreter.Prepare("RETURN 1 AS x");
    interpreter.Pull(&stream);
    ASSERT_EQ(stream.GetResults().size(), 1U);
  }
}

// R11: EnsureDbAccessForQuery must not re-pin a tenant whose in-map gatekeeper has been marked for
// deletion. The fix: after the drop-driven release releases db_acc_, re-acquisition via Get() can
// still grant an Accessor (the gatekeeper remains HOT in the map during the Delete_() critical
// section), but is_marked_for_deletion() detects the drop in progress; ResetDB() is called and the
// session falls back to db-less rather than re-pinning the dying tenant and delaying the drop.
//
// The simulation: call prepare_for_deletion() on a helper DatabaseAccess obtained from the same
// gatekeeper while it is still in the handler's map. This sets the shared pimpl's
// is_marked_for_deletion flag — exactly what Delete_() does between its prepare_for_deletion() call
// and DeferDelete(). The helper accessor is then released (out of scope); the flag persists on the
// pimpl, and the gatekeeper remains in the map. Any subsequent Get() for that name grants an
// accessor with is_marked_for_deletion() == true, which is the condition the fix detects.
TEST_F(IdleSessionReaperTest, DoesNotRepinMarkedForDeletionTenant) {
  const std::string db_name = "reap_r11";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  const auto db_uuid = interpreter.interpreter.current_db_.db_acc_->get()->uuid();

  // Release the accessor: keeps current_db_name_ + current_db_uuid_ so the next query re-acquires.
  std::optional<memgraph::dbms::DatabaseAccess> released_r11;
  ASSERT_TRUE(interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released_r11));
  released_r11.reset();  // destroy outside the reaping span before the marked-for-deletion check
  ASSERT_FALSE(interpreter.interpreter.current_db_.db_acc_.has_value());

  // Mark the in-map gatekeeper for deletion without DeferDelete (simulates Delete_()'s critical
  // section). The gatekeeper stays HOT in the map, so Get() will grant an accessor, but the
  // accessor's is_marked_for_deletion() will be true.
  {
    auto dying_acc = DBMS().Get(db_name);
    dying_acc.prepare_for_deletion();
    // dying_acc released here; is_marked_for_deletion persists on the shared pimpl.
  }

  // Next query: EnsureDbAccessForQuery acquires via Get() and detects is_marked_for_deletion().
  // It calls ResetDB(), leaving the session db-less. SetupDatabaseTransaction then throws because
  // db_acc_ is null.
  try {
    auto [stream, qid] = interpreter.Prepare("MATCH (n) RETURN count(n)");
    interpreter.Pull(&stream);
    FAIL() << "a db-requiring query on a db-less session must not succeed";
  } catch (const memgraph::query::QueryException &) {
  }

  EXPECT_FALSE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "must not re-pin the marked-for-deletion tenant";
  EXPECT_FALSE(interpreter.interpreter.current_db_.current_db_name_.has_value())
      << "session must fall back to db-less after the marked-for-deletion detection";
}

// NF2: USE DATABASE on a being-dropped tenant must throw, not re-pin. SetCurrentDB(name, true) is
// the code path exercised by a Bolt USE DATABASE message. Before the fix, Get() succeeded (tenant
// still HOT in the map) and the accessor was pinned without checking is_marked_for_deletion(),
// stalling DROP ... FORCE teardown. The guard throws UnknownDatabaseException BEFORE calling
// current_db_.SetCurrentDB(), so the session remains on its CURRENT database unchanged — mirroring
// how a normal failed USE (nonexistent DB) behaves.
TEST_F(IdleSessionReaperTest, UseDatabaseRefusesMarkedForDeletionTenant) {
  const std::string db_name = "reap_use_marked";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();

  // The interpreter starts on the default DB (passed by NewInterpreter via dbms.Get()).
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "session must start on the default database before the USE attempt";
  const std::string original_db_name = interpreter.interpreter.current_db_.db_acc_->get()->name();
  EXPECT_EQ(original_db_name, std::string{memgraph::dbms::kDefaultDB});

  // Mark the in-map gatekeeper for deletion (same simulation as R11): prepare_for_deletion() sets
  // the shared pimpl flag; the gatekeeper stays HOT in the map so Get() still grants an accessor.
  {
    auto dying_acc = DBMS().Get(db_name);
    dying_acc.prepare_for_deletion();
    // dying_acc released here; is_marked_for_deletion persists on the shared pimpl.
  }

  // SetCurrentDB with in_explicit_db=true mirrors the USE DATABASE Bolt path. It must throw
  // UnknownDatabaseException before touching current_db_ — the guard fires before the swap.
  EXPECT_THROW(interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/true),
               memgraph::dbms::UnknownDatabaseException);

  // A failed USE must leave the session on its current database, not switch to — or be evicted by —
  // the dying tenant. current_db_ is unchanged because the throw precedes current_db_.SetCurrentDB().
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "a failed USE DATABASE must not drop the session's current accessor";
  EXPECT_EQ(interpreter.interpreter.current_db_.db_acc_->get()->name(), original_db_name)
      << "session must remain on '" << original_db_name << "', not switch to the dying tenant";
}

// R7: a background drop-driven release races a session that is continuously running queries on the
// same interpreter (autocommit + explicit BEGIN/COMMIT). This drives the real sync protocol: the
// reaper CAS-es IDLE->REAPING and resets db_acc_ in the gaps between queries, while the session
// CAS-es IDLE->PREPARING at Prepare/Pull entry and re-acquires via EnsureDbAccessForQuery. The
// query results must stay correct (the tenant transparently re-acquires every time) and nothing may
// crash. This is the test worth running under ThreadSanitizer; it is the only multi-threaded
// exercise of TryReleaseDbAccessorForDrop against a live Prepare/Pull path (including the
// deferred-setup BEGIN-Pull window).
TEST_F(IdleSessionReaperTest, ConcurrentReaperVsSessionQueries) {
  const std::string db_name = "reap_r7";
  constexpr int kNodes = 5;
  CreateAndPopulate(db_name, kNodes);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  const auto db_uuid = interpreter.interpreter.current_db_.db_acc_->get()->uuid();

  std::atomic<bool> stop{false};
  std::atomic<uint64_t> reaps{0};
  std::thread reaper([&] {
    while (!stop.load(std::memory_order_acquire)) {
      // Drop-driven release: matched by UUID; any IDLE window with a held accessor is released.
      // The local collector destructs at end of iteration, outside the reaping span.
      std::optional<memgraph::dbms::DatabaseAccess> released;
      if (interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released)) {
        reaps.fetch_add(1, std::memory_order_relaxed);
      }
    }
  });

  constexpr int kIterations = 400;
  // A production reapable session is a SessionHL, and every Bolt message flows through Execute_, which
  // arms the reaper-exclusion gate (SetMessageInFlight) for the whole message and clears it when the
  // session parks. The InterpreterFaker drives Prepare/Pull directly, bypassing that layer, so the test
  // MUST arm the gate itself around each query -- otherwise it models an impossible "reapable session
  // whose queries never arm the gate", which no real client can produce. Each query below is one
  // gated Bolt message; the gate clears between messages so the reaper still reaps in the idle gaps.
  auto gated_run = [&](const char *query) {
    interpreter.interpreter.SetMessageInFlight();
    memgraph::utils::OnScopeExit clear_gate{[&] { interpreter.interpreter.ClearMessageInFlight(); }};
    auto [stream, qid] = interpreter.Prepare(query);
    interpreter.Pull(&stream);
    const auto &results = stream.GetResults();
    return results.empty() || results[0].empty() ? int64_t{-1} : results[0][0].ValueInt();
  };
  for (int i = 0; i < kIterations; ++i) {
    // Autocommit read: must observe the original node count after any re-acquire.
    ASSERT_EQ(gated_run("MATCH (n) RETURN count(n) AS c"), kNodes) << "autocommit read saw wrong count at iter " << i;
    // Explicit transaction: exercises the deferred-setup BEGIN-Pull claim window vs the reaper.
    gated_run("BEGIN");
    ASSERT_EQ(gated_run("MATCH (n) RETURN count(n) AS c"), kNodes) << "explicit-tx read saw wrong count at iter " << i;
    gated_run("COMMIT");
  }

  stop.store(true, std::memory_order_release);
  reaper.join();

  // Sanity: the reaper genuinely contended (otherwise the test would pass trivially).
  EXPECT_GT(reaps.load(std::memory_order_relaxed), 0U)
      << "reaper never won a single IDLE window — test is not exercising the race";

  // The session is left consistent and queryable after the storm.
  {
    auto [stream, qid] = interpreter.Prepare("MATCH (n) RETURN count(n) AS c");
    interpreter.Pull(&stream);
    const auto &results = stream.GetResults();
    ASSERT_EQ(results.size(), 1U);
    EXPECT_EQ(results[0][0].ValueInt(), kNodes);
  }
}

// F1: TryReleaseDbAccessorForDrop releases the idle accessor when the session is pinned on the
// dropped DB (matched by UUID). current_db_name_ is retained so EnsureDbAccessForQuery can detect
// the stale name on the next query and fall back to db-less via the marked-for-deletion guard.
TEST_F(IdleSessionReaperTest, ForceDropReleasesIdleAccessorForMatchingDb) {
  const std::string db_name = "reap_f1";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value());
  const auto db_uuid = interpreter.interpreter.current_db_.db_acc_->get()->uuid();

  std::optional<memgraph::dbms::DatabaseAccess> released_f1;
  EXPECT_TRUE(interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released_f1));
  EXPECT_FALSE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "accessor must be released when the dropped UUID matches the session's current DB";

  // current_db_name_ is kept (ReleaseDbAccessor does not clear it) so the next query can re-check
  // the tenant and detect the pending drop via is_marked_for_deletion().
  EXPECT_TRUE(interpreter.interpreter.current_db_.current_db_name_.has_value())
      << "current_db_name_ must be retained after TryReleaseDbAccessorForDrop";
  EXPECT_EQ(*interpreter.interpreter.current_db_.current_db_name_, db_name);
}

// F2: TryReleaseDbAccessorForDrop does NOT release a session whose UUID does NOT match the dropped
// UUID. The session's accessor must remain held; only the session whose current DB UUID matches the
// dropped UUID is a candidate for release.
TEST_F(IdleSessionReaperTest, ForceDropDoesNotReleaseAccessorForDifferentDb) {
  const std::string db_name = "reap_f2";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value());

  const memgraph::utils::UUID other_uuid{};  // fresh random UUID, guaranteed != the session's UUID
  std::optional<memgraph::dbms::DatabaseAccess> released_f2;
  EXPECT_FALSE(interpreter.interpreter.TryReleaseDbAccessorForDrop(other_uuid, &released_f2))
      << "must not release an accessor when the dropped UUID does not match the session's current DB UUID";
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "accessor must remain held when the dropped UUID differs from the session's DB UUID";
}

// F3: TryReleaseDbAccessorForDrop does NOT release a session that is inside an explicit transaction.
// Mirrors R5 (DoesNotReapMidExplicitTransaction): after BEGIN the transaction_status_ is ACTIVE, so
// the IDLE->REAPING CAS fails and the method returns false without touching db_acc_.
TEST_F(IdleSessionReaperTest, ForceDropDoesNotReleaseAccessorMidExplicitTransaction) {
  const std::string db_name = "reap_f3";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  const auto db_uuid = interpreter.interpreter.current_db_.db_acc_->get()->uuid();

  {
    auto [stream, qid] = interpreter.Prepare("BEGIN");
    interpreter.Pull(&stream);
  }
  // Inside an explicit transaction transaction_status_ is ACTIVE, so the IDLE->REAPING CAS fails.
  std::optional<memgraph::dbms::DatabaseAccess> released_f3;
  EXPECT_FALSE(interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released_f3))
      << "must not release from a session that is in an explicit transaction";
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "accessor must remain held for an in-flight explicit transaction";

  {
    auto [stream, qid] = interpreter.Prepare("COMMIT");
    interpreter.Pull(&stream);
  }
}

// F4: a tenant name recreated while the old husk still drains: the drop-driven reaper (keyed by
// UUID) must release ONLY the session pinned to the OLD uuid, never the freshly-recreated
// same-name tenant's session.
TEST_F(IdleSessionReaperTest, ForceDropReleasesByUuidNotName) {
  const std::string db_name = "recycled_name";
  CreateAndPopulate(db_name, 1);

  // Session A pinned to the original tenant (uuid_a).
  auto session_a = min_mg->NewInterpreter();
  session_a.interpreter.MarkReapable();
  session_a.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  const auto uuid_a = session_a.interpreter.current_db_.db_acc_->get()->uuid();

  // Drop the original (deferred) and recreate the same name -> a fresh tenant with a new uuid.
  ASSERT_TRUE(DBMS().Delete(db_name).has_value());
  ASSERT_TRUE(DBMS().New(db_name).has_value());

  // Session B pinned to the recreated tenant (uuid_b != uuid_a).
  auto session_b = min_mg->NewInterpreter();
  session_b.interpreter.MarkReapable();
  session_b.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  const auto uuid_b = session_b.interpreter.current_db_.db_acc_->get()->uuid();
  ASSERT_NE(std::string{uuid_a}, std::string{uuid_b});

  // Reaping the OLD uuid releases A but must leave B (same name, new uuid) pinned.
  std::optional<memgraph::dbms::DatabaseAccess> released_f4_a;
  EXPECT_TRUE(session_a.interpreter.TryReleaseDbAccessorForDrop(uuid_a, &released_f4_a));
  EXPECT_FALSE(session_a.interpreter.current_db_.db_acc_.has_value());
  std::optional<memgraph::dbms::DatabaseAccess> released_f4_b;
  EXPECT_FALSE(session_b.interpreter.TryReleaseDbAccessorForDrop(uuid_a, &released_f4_b))
      << "the recreated same-name tenant's session must NOT be released by the old husk's uuid";
  EXPECT_TRUE(session_b.interpreter.current_db_.db_acc_.has_value());
}

// NF1 (positive guard): ReleaseDbIfMarked() must NOT release db_acc_ while a storage transaction
// (db_transactional_accessor_) is live, even when the tenant is marked-for-deletion.
//
// Reproduces the UAF scenario the fix prevents: without the guard, dropping the last Accessor lets
// the deferred ~Gatekeeper destroy the storage (and its main_lock_) out from under the live
// db_transactional_accessor_ ResourceLockGuard. With the fix, ReleaseDbIfMarked() returns early and
// db_acc_ stays held until the transaction is cleaned up by the next Prepare or the destructor.
//
// Precondition established via the real code path: Prepare() calls SetupDatabaseTransaction() during
// its own execution (interpreter.cpp line ~10874), which sets db_transactional_accessor_ before
// returning. Not pulling the stream leaves the storage transaction open.
TEST_F(IdleSessionReaperTest, ReleaseDbIfMarkedKeepsAccessorWhileTransactionLive) {
  const std::string db_name = "reap_nf1_guard";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value());

  // Open a storage transaction via the real Prepare path (SetupDatabaseTransaction is called inside
  // Prepare for autocommit data queries). Intentionally do NOT pull: the transaction stays live.
  auto [stream, qid] = interpreter.Prepare("MATCH (n) RETURN count(n)");

  // Confirm the precondition: a storage transaction accessor is now live.
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_transactional_accessor_)
      << "precondition: Prepare must have opened db_transactional_accessor_";

  // Mark the in-map gatekeeper for deletion (mirrors the Delete_() critical section in R11).
  {
    auto dying_acc = DBMS().Get(db_name);
    dying_acc.prepare_for_deletion();
    // dying_acc released here; is_marked_for_deletion persists on the shared pimpl.
  }
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_->is_marked_for_deletion())
      << "precondition: db_acc_ must reflect is_marked_for_deletion after prepare_for_deletion";

  // Exercise the fix: the guard must bail out without releasing db_acc_.
  interpreter.interpreter.current_db_.ReleaseDbIfMarked();

  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "NF-1 fix: db_acc_ must NOT be released while db_transactional_accessor_ is live";
  EXPECT_TRUE(interpreter.interpreter.current_db_.current_db_name_.has_value())
      << "NF-1 fix: current_db_name_ must be kept when the release is suppressed by the guard";

  // ~InterpreterFaker -> ~Interpreter -> Abort() -> CleanupDBTransaction(true) aborts and resets
  // db_transactional_accessor_, then db_acc_ is released safely in ~CurrentDB.
}

// B1: TryReleaseDbAccessorForDrop must NOT release db_acc_ while a storage transaction
// (db_transactional_accessor_ / execution_db_accessor_) is live, even when the IDLE->REAPING CAS
// would otherwise succeed.
//
// The Commit-cleanup-gap: the commit path stores TransactionStatus::IDLE before
// db_transactional_accessor_ is cleaned up. Without the B1 guard, the CAS would succeed and
// db_acc_ would be dropped out from under the ResourceLockGuard that db_transactional_accessor_
// still holds — a UAF on main_lock_. The fix mirrors ReleaseDbIfMarked's guard.
//
// White-box: we force transaction_status_ to IDLE after Prepare opens db_transactional_accessor_,
// precisely reproducing the window that B1 closes.
TEST_F(IdleSessionReaperTest, ReaperKeepsAccessorWhileStorageTransactionLive) {
  const std::string db_name = "reap_b1_live_txn";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value());
  const auto db_uuid = interpreter.interpreter.current_db_.db_acc_->get()->uuid();

  // Open a storage transaction via the real Prepare path (SetupDatabaseTransaction is called inside
  // Prepare for autocommit data queries). Intentionally do NOT pull: the storage transaction stays live.
  auto [stream, qid] = interpreter.Prepare("MATCH (n) RETURN count(n)");

  // Precondition: db_transactional_accessor_ must be set by Prepare.
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_transactional_accessor_)
      << "precondition: Prepare must have opened db_transactional_accessor_";

  // Simulate the Commit-cleanup-gap: force transaction_status_ to IDLE even though
  // db_transactional_accessor_ is still live. This is the exact window B1 targets.
  interpreter.interpreter.transaction_status_.store(memgraph::query::TransactionStatus::IDLE,
                                                    std::memory_order_seq_cst);

  // B1 guard fires inside WithReapingLock: db_transactional_accessor_ is non-null, so the
  // drop-driven release path cannot release db_acc_.
  std::optional<memgraph::dbms::DatabaseAccess> released_b1_a;
  EXPECT_FALSE(interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released_b1_a))
      << "must not reap while db_transactional_accessor_ is live";
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "db_acc_ must remain held while the storage transaction is live";

  std::optional<memgraph::dbms::DatabaseAccess> released_b1_b;
  EXPECT_FALSE(interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released_b1_b))
      << "must not force-drop while db_transactional_accessor_ is live";
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "db_acc_ must remain held when TryReleaseDbAccessorForDrop sees a live storage transaction";

  // ~InterpreterFaker -> ~Interpreter -> Abort() -> CleanupDBTransaction aborts and resets
  // db_transactional_accessor_, then db_acc_ is released safely in ~CurrentDB.
}

// NB1: Cypher USE DATABASE must throw when the target tenant is marked for deletion.
// This exercises PrepareUseDatabaseQuery's is_marked_for_deletion() guard (the NB1 fix site),
// NOT Interpreter::SetCurrentDB — the Bolt USE path exercised by UseDatabaseRefusesMarkedForDeletionTenant.
// The throw happens at Pull time (inside the query_handler lambda), not at Prepare time.
// UnknownDatabaseException derives from utils::BasicException; the handler's catch rewraps it as
// QueryRuntimeException, which is what propagates out of Pull.
TEST_F(IdleSessionReaperTest, CypherUseDatabaseRefusesMarkedForDeletionTenant) {
  const std::string db_name = "reap_nb1_use_cypher";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  // The interpreter starts on the default DB (passed by NewInterpreter via dbms.Get()).
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "session must start on the default database before the USE attempt";
  const std::string original_db_name = interpreter.interpreter.current_db_.db_acc_->get()->name();
  EXPECT_EQ(original_db_name, std::string{memgraph::dbms::kDefaultDB});

  // Mark the in-map gatekeeper for deletion (same simulation as R11/NF2): prepare_for_deletion()
  // sets the shared pimpl flag; the gatekeeper stays HOT in the map so Get() still grants an
  // accessor, but is_marked_for_deletion() returns true on that accessor.
  {
    auto dying_acc = DBMS().Get(db_name);
    dying_acc.prepare_for_deletion();
    // dying_acc released here; is_marked_for_deletion persists on the shared pimpl.
  }

  // Drive the Cypher USE path: Prepare assembles the handler without executing it (the
  // is_marked_for_deletion() check is inside the handler lambda). Pull invokes the handler, which
  // detects is_marked_for_deletion() and throws UnknownDatabaseException; the handler's catch
  // (const utils::BasicException &) rewraps it as QueryRuntimeException.
  auto [stream, qid] = interpreter.Prepare("USE DATABASE " + db_name);
  ASSERT_THROW(interpreter.Pull(&stream), memgraph::query::QueryRuntimeException);

  // The throw occurred before current_db_.SetCurrentDB() was reached: the session must remain on
  // its current database, not switch to — or be evicted by — the dying tenant.
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "a failed Cypher USE must not drop the session's current accessor";
  EXPECT_EQ(interpreter.interpreter.current_db_.db_acc_->get()->name(), original_db_name)
      << "session must remain on '" << original_db_name << "' after the failed Cypher USE";
}

// NF1 (control): the same marked-for-deletion tenant with NO live storage transaction IS released
// by ReleaseDbIfMarked(). This proves that the guard in the positive test (above) is what makes the
// difference — not that ReleaseDbIfMarked() simply never releases.
TEST_F(IdleSessionReaperTest, ReleaseDbIfMarkedReleasesAccessorWhenNoTransactionLive) {
  const std::string db_name = "reap_nf1_ctrl";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value());

  // Confirm no storage transaction is open (no Prepare has been called yet).
  ASSERT_FALSE(interpreter.interpreter.current_db_.db_transactional_accessor_)
      << "precondition: db_transactional_accessor_ must be null before any query";

  // Mark the in-map gatekeeper for deletion (same mechanism as R11 and NF1 positive test).
  {
    auto dying_acc = DBMS().Get(db_name);
    dying_acc.prepare_for_deletion();
  }
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_->is_marked_for_deletion())
      << "precondition: db_acc_ must reflect is_marked_for_deletion after prepare_for_deletion";

  // Without a live transaction the guard does not fire: db_acc_ is released and identity cleared.
  interpreter.interpreter.current_db_.ReleaseDbIfMarked();

  EXPECT_FALSE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "control: db_acc_ must be released when no storage transaction is live";
  EXPECT_FALSE(interpreter.interpreter.current_db_.current_db_name_.has_value())
      << "control: current_db_name_ must be cleared when ReleaseDbIfMarked releases the accessor";
}

#endif  // MG_ENTERPRISE
