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

// Unit tests for the drop-driven idle-session reaper (enterprise-only).
// Drives TryReleaseDbAccessorForDrop directly (white-box) — the deferred-drop worker's per-tick hook.

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

  EXPECT_FALSE(DBMS().Suspend(db_name).has_value());

  std::optional<memgraph::dbms::DatabaseAccess> released_r1;
  EXPECT_TRUE(interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released_r1));
  EXPECT_FALSE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "reaper must release the idle session's accessor";
  // Destroy outside the reaping span so the tenant has zero holders before testing Suspend().
  released_r1.reset();

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
  // transaction_status_ is ACTIVE inside BEGIN, so the reaper's IDLE pre-check fails.
  std::optional<memgraph::dbms::DatabaseAccess> released_r5;
  EXPECT_FALSE(interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released_r5))
      << "must not reap a session that is in an explicit transaction";
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value());

  {
    auto [stream, qid] = interpreter.Prepare("COMMIT");
    interpreter.Pull(&stream);
  }
}

// R10: BeginTransaction() must re-acquire db_acc_ after a drop-driven release; before this fix it
// bypassed EnsureDbAccessForQuery() and threw on a null accessor for a healthy (HOT) tenant.
TEST_F(IdleSessionReaperTest, ReapedSessionReacquiresOnBoltBegin) {
  const std::string db_name = "reap_r10";
  CreateAndPopulate(db_name, 4);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  const auto db_uuid = interpreter.interpreter.current_db_.db_acc_->get()->uuid();

  std::optional<memgraph::dbms::DatabaseAccess> released_r10;
  ASSERT_TRUE(interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released_r10));
  released_r10.reset();  // destroy outside the reaping span before re-acquiring
  ASSERT_FALSE(interpreter.interpreter.current_db_.db_acc_.has_value());

  ASSERT_NO_THROW(interpreter.interpreter.BeginTransaction(memgraph::query::QueryExtras{}));

  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "BeginTransaction must re-acquire the db_acc_ for the named HOT tenant";

  {
    auto [stream, qid] = interpreter.Prepare("MATCH (n) RETURN count(n) AS c");
    interpreter.Pull(&stream);
    const auto &results = stream.GetResults();
    ASSERT_EQ(results.size(), 1U);
    EXPECT_EQ(results[0][0].ValueInt(), 4) << "re-acquired tenant must expose the original 4 nodes";
  }

  ASSERT_NO_THROW(interpreter.interpreter.CommitTransaction());
}

// Default-DB protection is structural: DbmsHandler::Delete_ rejects kDefaultDB, so its UUID is
// never handed to the reaper — no separate test needed.

// R8: after a drop-driven release the session retains its UUID; if the name is recycled under a new
// tenant, UUID mismatch on re-acquire causes db-less fallback rather than silent attachment.
TEST_F(IdleSessionReaperTest, ReacquireFallsBackToDbLessWhenTenantRecycled) {
  const std::string db_name = "reap_r8";
  CreateAndPopulate(db_name, 5);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  ASSERT_TRUE(interpreter.interpreter.current_db_.current_db_uuid_.has_value());
  const auto original_uuid = *interpreter.interpreter.current_db_.current_db_uuid_;
  const auto db_uuid = interpreter.interpreter.current_db_.db_acc_->get()->uuid();

  // TryReleaseDbAccessorForDrop keeps current_db_name_ + current_db_uuid_ for the re-acquire check.
  std::optional<memgraph::dbms::DatabaseAccess> released_r8;
  ASSERT_TRUE(interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released_r8));
  released_r8.reset();  // destroy outside the reaping span so DBMS().Delete succeeds (zero holders)
  ASSERT_FALSE(interpreter.interpreter.current_db_.db_acc_.has_value());

  ASSERT_TRUE(DBMS().Delete(db_name).has_value());
  ASSERT_TRUE(DBMS().New(db_name).has_value());
  ASSERT_NE(DBMS().Get(db_name)->uuid(), original_uuid) << "the recreated tenant must have a fresh UUID";

  try {
    auto [stream, qid] = interpreter.Prepare("MATCH (n) RETURN count(n)");
    interpreter.Pull(&stream);
    FAIL() << "a db-requiring query on a recycled/db-less session must not succeed";
  } catch (const memgraph::query::QueryException &) {
  }
  EXPECT_FALSE(interpreter.interpreter.current_db_.db_acc_.has_value()) << "must not attach to the recycled tenant";
  EXPECT_FALSE(interpreter.interpreter.current_db_.current_db_name_.has_value())
      << "session must fall back to db-less, not stay wedged on the recycled name";

  // In-session recovery: USE rebinds to the new tenant without reconnect.
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
  ASSERT_TRUE(interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released_r9));
  released_r9.reset();  // destroy outside the reaping span so DBMS().Delete succeeds (zero holders)
  ASSERT_FALSE(interpreter.interpreter.current_db_.db_acc_.has_value());

  ASSERT_TRUE(DBMS().Delete(db_name).has_value());

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

// R11: EnsureDbAccessForQuery must not re-pin a gatekeeper marked for deletion. Re-acquisition via
// Get() still succeeds (gatekeeper stays HOT in the map during Delete_()'s critical section) but
// is_marked_for_deletion() must divert to ResetDB(). Simulated by calling prepare_for_deletion()
// directly on a helper accessor, mirroring what Delete_() does before DeferDelete().
TEST_F(IdleSessionReaperTest, DoesNotRepinMarkedForDeletionTenant) {
  const std::string db_name = "reap_r11";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  const auto db_uuid = interpreter.interpreter.current_db_.db_acc_->get()->uuid();

  // Release keeps current_db_name_ + current_db_uuid_ so the next query attempts re-acquisition.
  std::optional<memgraph::dbms::DatabaseAccess> released_r11;
  ASSERT_TRUE(interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released_r11));
  released_r11.reset();  // destroy outside the reaping span before the marked-for-deletion check
  ASSERT_FALSE(interpreter.interpreter.current_db_.db_acc_.has_value());

  // Mark HOT gatekeeper for deletion (no DeferDelete): Get() still grants an accessor but
  // is_marked_for_deletion() is true.
  {
    auto dying_acc = DBMS().Get(db_name);
    dying_acc.prepare_for_deletion();
    // dying_acc released here; is_marked_for_deletion persists on the shared pimpl.
  }

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

// NF2: SetCurrentDB(name, true) (the Bolt USE path) must throw UnknownDatabaseException on a
// marked-for-deletion tenant — guard fires before current_db_.SetCurrentDB(), so the session is unchanged.
TEST_F(IdleSessionReaperTest, UseDatabaseRefusesMarkedForDeletionTenant) {
  const std::string db_name = "reap_use_marked";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();

  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "session must start on the default database before the USE attempt";
  const std::string original_db_name = interpreter.interpreter.current_db_.db_acc_->get()->name();
  EXPECT_EQ(original_db_name, std::string{memgraph::dbms::kDefaultDB});

  // Mark HOT gatekeeper for deletion: Get() still grants an accessor but is_marked_for_deletion() is true.
  {
    auto dying_acc = DBMS().Get(db_name);
    dying_acc.prepare_for_deletion();
    // dying_acc released here; is_marked_for_deletion persists on the shared pimpl.
  }

  EXPECT_THROW(interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/true),
               memgraph::dbms::UnknownDatabaseException);

  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "a failed USE DATABASE must not drop the session's current accessor";
  EXPECT_EQ(interpreter.interpreter.current_db_.db_acc_->get()->name(), original_db_name)
      << "session must remain on '" << original_db_name << "', not switch to the dying tenant";
}

// R7: reaper races a session running continuous queries (autocommit + BEGIN/COMMIT). CAS protocol:
// reaper wins IDLE->REAPING gaps; session re-acquires via EnsureDbAccessForQuery on each Prepare.
// The only multi-threaded exercise of TryReleaseDbAccessorForDrop — run under ThreadSanitizer.
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
      // released destructs at end of iteration, outside the reaping span.
      std::optional<memgraph::dbms::DatabaseAccess> released;
      if (interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released)) {
        reaps.fetch_add(1, std::memory_order_relaxed);
      }
    }
  });

  constexpr int kIterations = 400;
  // InterpreterFaker bypasses SessionHL (which arms SetMessageInFlight per Bolt message), so the
  // test must arm it manually — without it the model is unreachable in production.
  auto gated_run = [&](const char *query) {
    interpreter.interpreter.SetMessageInFlight();
    memgraph::utils::OnScopeExit clear_gate{[&] { interpreter.interpreter.ClearMessageInFlight(); }};
    auto [stream, qid] = interpreter.Prepare(query);
    interpreter.Pull(&stream);
    const auto &results = stream.GetResults();
    return results.empty() || results[0].empty() ? int64_t{-1} : results[0][0].ValueInt();
  };
  for (int i = 0; i < kIterations; ++i) {
    ASSERT_EQ(gated_run("MATCH (n) RETURN count(n) AS c"), kNodes) << "autocommit read saw wrong count at iter " << i;
    // Exercises the deferred-setup BEGIN-Pull window vs the reaper.
    gated_run("BEGIN");
    ASSERT_EQ(gated_run("MATCH (n) RETURN count(n) AS c"), kNodes) << "explicit-tx read saw wrong count at iter " << i;
    gated_run("COMMIT");
  }

  stop.store(true, std::memory_order_release);
  reaper.join();

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

// F1: UUID-matched TryReleaseDbAccessorForDrop releases db_acc_ but retains current_db_name_ so
// EnsureDbAccessForQuery can detect the pending drop on the next query.
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

// F2: UUID mismatch — TryReleaseDbAccessorForDrop must not release an unrelated session's accessor.
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

// F4: UUID-keyed reaper must release only the old-tenant session, not a same-name but
// different-UUID session created after the name was recycled.
TEST_F(IdleSessionReaperTest, ForceDropReleasesByUuidNotName) {
  const std::string db_name = "recycled_name";
  CreateAndPopulate(db_name, 1);

  // Session A pinned to the original tenant (uuid_a).
  auto session_a = min_mg->NewInterpreter();
  session_a.interpreter.MarkReapable();
  session_a.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  const auto uuid_a = session_a.interpreter.current_db_.db_acc_->get()->uuid();

  ASSERT_TRUE(DBMS().Delete(db_name).has_value());
  ASSERT_TRUE(DBMS().New(db_name).has_value());

  // Session B pinned to the recreated tenant (uuid_b != uuid_a).
  auto session_b = min_mg->NewInterpreter();
  session_b.interpreter.MarkReapable();
  session_b.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  const auto uuid_b = session_b.interpreter.current_db_.db_acc_->get()->uuid();
  ASSERT_NE(std::string{uuid_a}, std::string{uuid_b});

  std::optional<memgraph::dbms::DatabaseAccess> released_f4_a;
  EXPECT_TRUE(session_a.interpreter.TryReleaseDbAccessorForDrop(uuid_a, &released_f4_a));
  EXPECT_FALSE(session_a.interpreter.current_db_.db_acc_.has_value());
  std::optional<memgraph::dbms::DatabaseAccess> released_f4_b;
  EXPECT_FALSE(session_b.interpreter.TryReleaseDbAccessorForDrop(uuid_a, &released_f4_b))
      << "the recreated same-name tenant's session must NOT be released by the old husk's uuid";
  EXPECT_TRUE(session_b.interpreter.current_db_.db_acc_.has_value());
}

// NF1 (positive guard): ReleaseDbIfMarked() must not release db_acc_ while db_transactional_accessor_
// is live — without the guard ~Gatekeeper destroys main_lock_ under the live ResourceLockGuard (UAF).
// Setup: Prepare opens db_transactional_accessor_; intentionally not pulled so the transaction stays live.
TEST_F(IdleSessionReaperTest, ReleaseDbIfMarkedKeepsAccessorWhileTransactionLive) {
  const std::string db_name = "reap_nf1_guard";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value());

  // Intentionally not pulled: leaves db_transactional_accessor_ live for the guard check.
  auto [stream, qid] = interpreter.Prepare("MATCH (n) RETURN count(n)");

  ASSERT_TRUE(interpreter.interpreter.current_db_.db_transactional_accessor_)
      << "precondition: Prepare must have opened db_transactional_accessor_";

  // Mark HOT gatekeeper for deletion (mirrors Delete_() critical section).
  {
    auto dying_acc = DBMS().Get(db_name);
    dying_acc.prepare_for_deletion();
    // dying_acc released here; is_marked_for_deletion persists on the shared pimpl.
  }
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_->is_marked_for_deletion())
      << "precondition: db_acc_ must reflect is_marked_for_deletion after prepare_for_deletion";

  interpreter.interpreter.current_db_.ReleaseDbIfMarked();

  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "NF-1 fix: db_acc_ must NOT be released while db_transactional_accessor_ is live";
  EXPECT_TRUE(interpreter.interpreter.current_db_.current_db_name_.has_value())
      << "NF-1 fix: current_db_name_ must be kept when the release is suppressed by the guard";

  // ~InterpreterFaker -> ~Interpreter -> Abort() -> CleanupDBTransaction(true) aborts and resets
  // db_transactional_accessor_, then db_acc_ is released safely in ~CurrentDB.
}

// B1: TryReleaseDbAccessorForDrop must not release db_acc_ while db_transactional_accessor_ is live.
// Commit-cleanup-gap: IDLE status is stored before clearing db_transactional_accessor_, so the reaper
// CAS succeeds but db_acc_ would be dropped under the live ResourceLockGuard — UAF on main_lock_.
// White-box: force transaction_status_ to IDLE while db_transactional_accessor_ is still open.
TEST_F(IdleSessionReaperTest, ReaperKeepsAccessorWhileStorageTransactionLive) {
  const std::string db_name = "reap_b1_live_txn";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value());
  const auto db_uuid = interpreter.interpreter.current_db_.db_acc_->get()->uuid();

  // Intentionally not pulled: leaves db_transactional_accessor_ live.
  auto [stream, qid] = interpreter.Prepare("MATCH (n) RETURN count(n)");

  ASSERT_TRUE(interpreter.interpreter.current_db_.db_transactional_accessor_)
      << "precondition: Prepare must have opened db_transactional_accessor_";

  // Simulate the Commit-cleanup-gap: force transaction_status_ to IDLE even though
  // db_transactional_accessor_ is still live. This is the exact window B1 targets.
  interpreter.interpreter.transaction_status_.store(memgraph::query::TransactionStatus::IDLE,
                                                    std::memory_order_seq_cst);

  std::optional<memgraph::dbms::DatabaseAccess> released_b1_a;
  EXPECT_FALSE(interpreter.interpreter.TryReleaseDbAccessorForDrop(db_uuid, &released_b1_a))
      << "must not reap while db_transactional_accessor_ is live";
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "db_acc_ must remain held while the storage transaction is live";

  // ~InterpreterFaker -> ~Interpreter -> Abort() -> CleanupDBTransaction aborts and resets
  // db_transactional_accessor_, then db_acc_ is released safely in ~CurrentDB.
}

// NB1: Cypher "USE DATABASE x" must throw when x is marked for deletion — exercises
// PrepareUseDatabaseQuery's guard, not SetCurrentDB (the Bolt USE path tested by NF2).
// The throw happens at Pull time and is rewrapped as QueryRuntimeException by the handler's catch.
TEST_F(IdleSessionReaperTest, CypherUseDatabaseRefusesMarkedForDeletionTenant) {
  const std::string db_name = "reap_nb1_use_cypher";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "session must start on the default database before the USE attempt";
  const std::string original_db_name = interpreter.interpreter.current_db_.db_acc_->get()->name();
  EXPECT_EQ(original_db_name, std::string{memgraph::dbms::kDefaultDB});

  // Mark HOT gatekeeper for deletion: Get() still grants an accessor but is_marked_for_deletion() is true.
  {
    auto dying_acc = DBMS().Get(db_name);
    dying_acc.prepare_for_deletion();
    // dying_acc released here; is_marked_for_deletion persists on the shared pimpl.
  }

  // Prepare assembles the handler without running it; Pull invokes it, detects is_marked_for_deletion(),
  // and throws UnknownDatabaseException rewrapped as QueryRuntimeException.
  auto [stream, qid] = interpreter.Prepare("USE DATABASE " + db_name);
  ASSERT_THROW(interpreter.Pull(&stream), memgraph::query::QueryRuntimeException);

  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "a failed Cypher USE must not drop the session's current accessor";
  EXPECT_EQ(interpreter.interpreter.current_db_.db_acc_->get()->name(), original_db_name)
      << "session must remain on '" << original_db_name << "' after the failed Cypher USE";
}

// NF1 (control): same tenant, no live storage transaction — ReleaseDbIfMarked() must release.
// Proves the guard in the positive test is what makes the difference, not a permanent no-op.
TEST_F(IdleSessionReaperTest, ReleaseDbIfMarkedReleasesAccessorWhenNoTransactionLive) {
  const std::string db_name = "reap_nf1_ctrl";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value());

  ASSERT_FALSE(interpreter.interpreter.current_db_.db_transactional_accessor_)
      << "precondition: db_transactional_accessor_ must be null before any query";

  {
    auto dying_acc = DBMS().Get(db_name);
    dying_acc.prepare_for_deletion();
  }
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_->is_marked_for_deletion())
      << "precondition: db_acc_ must reflect is_marked_for_deletion after prepare_for_deletion";

  interpreter.interpreter.current_db_.ReleaseDbIfMarked();

  EXPECT_FALSE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "control: db_acc_ must be released when no storage transaction is live";
  EXPECT_FALSE(interpreter.interpreter.current_db_.current_db_name_.has_value())
      << "control: current_db_name_ must be cleared when ReleaseDbIfMarked releases the accessor";
}

#endif  // MG_ENTERPRISE
