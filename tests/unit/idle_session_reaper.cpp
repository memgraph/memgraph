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

// Unit tests for the idle-session reaper (Interpreter::TryReapIdleDbAccessor) under IDLE_SESSION_REAPER.
// The reaper releases a connected-but-idle Bolt session's db_acc_ so a tenant pinned only by idle
// connections drops its gatekeeper count; the connection stays open and the next query transparently
// re-acquires the accessor by name via Get(). These tests drive TryReapIdleDbAccessor directly
// (white-box), standing in for the background sweep in src/memgraph.cpp.

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
#include "flags/experimental.hpp"
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

constexpr uint64_t kHugeNs = std::numeric_limits<uint64_t>::max() / 2;  // "now" far in the future
constexpr uint64_t kOneHourNs = 3'600ULL * 1'000'000'000ULL;

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
    memgraph::flags::SetExperimental(memgraph::flags::Experiments::IDLE_SESSION_REAPER);
    min_mg.emplace(MakeConfig(data_directory));
  }

  void TearDown() override {
    min_mg.reset();
    memgraph::flags::SetExperimental(memgraph::flags::Experiments{});
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

// R1: a reapable idle session's accessor is released; the tenant (no other holders) then suspends.
TEST_F(IdleSessionReaperTest, ReapsIdleSessionAndTenantBecomesSuspendable) {
  const std::string db_name = "reap_r1";
  CreateAndPopulate(db_name, 2);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value());

  // Held accessor blocks suspend.
  EXPECT_FALSE(DBMS().Suspend(db_name).has_value());

  // Reap (idle far past the timeout): releases db_acc_.
  EXPECT_TRUE(interpreter.interpreter.TryReapIdleDbAccessor(kHugeNs, /*idle_timeout_ns=*/0));
  EXPECT_FALSE(interpreter.interpreter.current_db_.db_acc_.has_value())
      << "reaper must release the idle session's accessor";

  // Tenant now has zero holders -> suspendable.
  EXPECT_TRUE(DBMS().Suspend(db_name).has_value()) << "tenant must be suspendable after its sessions are reaped";
}

// R2: after a reap, the next query re-acquires the (still-HOT) accessor via Get() and reads data.
TEST_F(IdleSessionReaperTest, ReapedSessionReacquiresOnNextQuery) {
  const std::string db_name = "reap_r2";
  CreateAndPopulate(db_name, 3);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);

  ASSERT_TRUE(interpreter.interpreter.TryReapIdleDbAccessor(kHugeNs, 0));
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

// R3: a session whose tenant was used within the timeout is NOT reaped.
TEST_F(IdleSessionReaperTest, DoesNotReapRecentlyUsedTenant) {
  const std::string db_name = "reap_r3";
  CreateAndPopulate(db_name, 1);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);

  // Run a GATED query so last_activity_ns_ is stamped to "now" (ClearMessageInFlight stamps it; the
  // faker bypasses the Bolt Execute_ layer that normally does this, so arm/clear it here).
  {
    interpreter.interpreter.SetMessageInFlight();
    memgraph::utils::OnScopeExit clear_gate{[&] { interpreter.interpreter.ClearMessageInFlight(); }};
    auto [stream, qid] = interpreter.Prepare("RETURN 1");
    interpreter.Pull(&stream);
  }
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value());

  const auto now_ns = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
  EXPECT_FALSE(interpreter.interpreter.TryReapIdleDbAccessor(now_ns, /*idle_timeout_ns=*/kOneHourNs))
      << "a tenant used within the idle timeout must not be reaped";
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value());
}

// R4: a non-reapable interpreter (e.g. a stream consumer / internal interpreter) is never reaped.
TEST_F(IdleSessionReaperTest, DoesNotReapNonReapableInterpreter) {
  const std::string db_name = "reap_r4";
  CreateAndPopulate(db_name, 1);

  auto interpreter = min_mg->NewInterpreter();  // NOT marked reapable
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value());

  EXPECT_FALSE(interpreter.interpreter.TryReapIdleDbAccessor(kHugeNs, 0))
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

  {
    auto [stream, qid] = interpreter.Prepare("BEGIN");
    interpreter.Pull(&stream);
  }
  // Inside an explicit transaction transaction_status_ is ACTIVE, so the reaper's IDLE pre-check fails.
  EXPECT_FALSE(interpreter.interpreter.TryReapIdleDbAccessor(kHugeNs, 0))
      << "must not reap a session that is in an explicit transaction";
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value());

  {
    auto [stream, qid] = interpreter.Prepare("COMMIT");
    interpreter.Pull(&stream);
  }
}

// R6: the default database is never reaped (it is never suspendable anyway).
TEST_F(IdleSessionReaperTest, DoesNotReapDefaultDatabase) {
  auto interpreter = min_mg->NewInterpreter();  // stays on the default DB
  interpreter.interpreter.MarkReapable();
  // Run a query on the default DB so it has an engaged accessor.
  {
    auto [stream, qid] = interpreter.Prepare("RETURN 1");
    interpreter.Pull(&stream);
  }
  ASSERT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value());
  EXPECT_EQ(interpreter.interpreter.current_db_.db_acc_->get()->name(), memgraph::dbms::kDefaultDB);

  EXPECT_FALSE(interpreter.interpreter.TryReapIdleDbAccessor(kHugeNs, 0))
      << "the default database must never be reaped";
  EXPECT_TRUE(interpreter.interpreter.current_db_.db_acc_.has_value());
}

// R8: recycle safety. After a reap releases the accessor, if the tenant's NAME is dropped and a
// DIFFERENT tenant is recreated under it, the next query must NOT silently attach to the new tenant.
// The UUID captured when the session bound the database no longer matches, so re-acquire fails closed.
TEST_F(IdleSessionReaperTest, ReacquireFallsBackToDbLessWhenTenantRecycled) {
  const std::string db_name = "reap_r8";
  CreateAndPopulate(db_name, 5);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);
  ASSERT_TRUE(interpreter.interpreter.current_db_.current_db_uuid_.has_value());
  const auto original_uuid = *interpreter.interpreter.current_db_.current_db_uuid_;

  // Reap: release the idle accessor (current_db_name_ + current_db_uuid_ are kept).
  ASSERT_TRUE(interpreter.interpreter.TryReapIdleDbAccessor(kHugeNs, 0));
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
  ASSERT_TRUE(interpreter.interpreter.TryReapIdleDbAccessor(kHugeNs, 0));  // release the accessor
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

// R7: a background reaper sweep thread races a session that is continuously running queries on the
// same interpreter (autocommit + explicit BEGIN/COMMIT). This drives the real sync protocol: the
// reaper CAS-es IDLE->REAPING and resets db_acc_ in the gaps between queries, while the session
// CAS-es IDLE->PREPARING at Prepare/Pull entry and re-acquires via EnsureDbAccessForQuery. With
// idle_timeout_ns == 0 the reaper is maximally aggressive. The query results must stay correct
// (the tenant transparently re-acquires every time) and nothing may crash. This is the test worth
// running under ThreadSanitizer; it is the only multi-threaded exercise of TryReapIdleDbAccessor
// against a live Prepare/Pull path (including the deferred-setup BEGIN-Pull window).
TEST_F(IdleSessionReaperTest, ConcurrentReaperVsSessionQueries) {
  const std::string db_name = "reap_r7";
  constexpr int kNodes = 5;
  CreateAndPopulate(db_name, kNodes);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);

  std::atomic<bool> stop{false};
  std::atomic<uint64_t> reaps{0};
  std::thread reaper([&] {
    while (!stop.load(std::memory_order_acquire)) {
      // now far in the future, timeout 0 => any IDLE window with a held non-default accessor is reaped.
      if (interpreter.interpreter.TryReapIdleDbAccessor(kHugeNs, /*idle_timeout_ns=*/0)) {
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

#endif  // MG_ENTERPRISE
