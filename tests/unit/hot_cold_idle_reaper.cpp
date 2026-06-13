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

// Unit tests for the hot/cold idle-session reaper (Interpreter::TryReapIdleDbAccessor) under
// HOT_COLD_TENANTS. The reaper releases a connected-but-idle Bolt session's db_acc_ so a tenant
// pinned only by idle connections becomes suspendable; the connection stays open and the next query
// transparently re-acquires the accessor (block-and-resume). These tests drive TryReapIdleDbAccessor
// directly (white-box), standing in for the background sweep in src/memgraph.cpp.

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
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "query/typed_value.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/logging.hpp"
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

class HotColdIdleReaperTest : public ::testing::Test {
 public:
  std::filesystem::path data_directory = std::filesystem::temp_directory_path() / "MG_tests_unit_hot_cold_idle_reaper";

  void SetUp() override {
    TearDown();
    FLAGS_storage_hot_cold_min_hot_residency_sec = 0;
    memgraph::flags::SetExperimental(memgraph::flags::Experiments::HOT_COLD_TENANTS);
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
TEST_F(HotColdIdleReaperTest, ReapsIdleSessionAndTenantBecomesSuspendable) {
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

// R2: after a reap, the next query transparently re-acquires the accessor (block-and-resume) and reads data.
TEST_F(HotColdIdleReaperTest, ReapedSessionReacquiresOnNextQuery) {
  const std::string db_name = "reap_r2";
  CreateAndPopulate(db_name, 3);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);

  ASSERT_TRUE(interpreter.interpreter.TryReapIdleDbAccessor(kHugeNs, 0));
  ASSERT_FALSE(interpreter.interpreter.current_db_.db_acc_.has_value());

  // Suspend it so the next query exercises a real COLD block-and-resume (not just Get).
  ASSERT_TRUE(DBMS().Suspend(db_name).has_value());

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
TEST_F(HotColdIdleReaperTest, DoesNotReapRecentlyUsedTenant) {
  const std::string db_name = "reap_r3";
  CreateAndPopulate(db_name, 1);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_name, /*in_explicit_db=*/false);

  // Touch the tenant so LastUsedNs is current.
  {
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
TEST_F(HotColdIdleReaperTest, DoesNotReapNonReapableInterpreter) {
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
TEST_F(HotColdIdleReaperTest, DoesNotReapMidExplicitTransaction) {
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
TEST_F(HotColdIdleReaperTest, DoesNotReapDefaultDatabase) {
  auto interpreter = min_mg->NewInterpreter();  // stays on the default DB
  interpreter.interpreter.MarkReapable();
  // Run a query on the default DB so it has an engaged accessor and a fresh LastUsedNs.
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

// R7: a background reaper sweep thread races a session that is continuously running queries on the
// same interpreter (autocommit + explicit BEGIN/COMMIT). This drives the real sync protocol: the
// reaper CAS-es IDLE->REAPING and resets db_acc_ in the gaps between queries, while the session
// CAS-es IDLE->PREPARING at Prepare/Pull entry and re-acquires via EnsureDbAccessForQuery. With
// idle_timeout_ns == 0 the reaper is maximally aggressive. The query results must stay correct
// (the tenant transparently reheats every time) and nothing may crash. This is the test worth
// running under ThreadSanitizer; it is the only multi-threaded exercise of TryReapIdleDbAccessor
// against a live Prepare/Pull path (including the deferred-setup BEGIN-Pull window).
TEST_F(HotColdIdleReaperTest, ConcurrentReaperVsSessionQueries) {
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
  for (int i = 0; i < kIterations; ++i) {
    // Autocommit read: must observe the original node count after any reheat.
    {
      auto [stream, qid] = interpreter.Prepare("MATCH (n) RETURN count(n) AS c");
      interpreter.Pull(&stream);
      const auto &results = stream.GetResults();
      ASSERT_EQ(results.size(), 1U);
      ASSERT_EQ(results[0][0].ValueInt(), kNodes) << "autocommit read saw wrong count at iter " << i;
    }
    // Explicit transaction: exercises the deferred-setup BEGIN-Pull claim window vs the reaper.
    {
      auto [b, bqid] = interpreter.Prepare("BEGIN");
      interpreter.Pull(&b);
      auto [r, rqid] = interpreter.Prepare("MATCH (n) RETURN count(n) AS c");
      interpreter.Pull(&r);
      const auto &results = r.GetResults();
      ASSERT_EQ(results.size(), 1U);
      ASSERT_EQ(results[0][0].ValueInt(), kNodes) << "explicit-tx read saw wrong count at iter " << i;
      auto [c, cqid] = interpreter.Prepare("COMMIT");
      interpreter.Pull(&c);
    }
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

// R8: a background reaper sweep races the SESSION-DB mutators that run BETWEEN queries — the path the
// query-entry claim does NOT cover. SessionHL drives these from connect (TryDefaultDB), every bolt
// run-time `db` switch (RuntimeConfig::Configure), and logoff: Interpreter::SetCurrentDB / ResetDB
// reassign current_db_.db_acc_ while transaction_status_ is IDLE — exactly when the reaper can win
// IDLE->REAPING and reset() the same optional. Before the fix these mutators took no claim, so this is
// a data race (TSan) / torn write / accessor refcount corruption on db_acc_. The fix makes SetCurrentDB
// and ResetDB take the same IDLE->PREPARING claim, mutually excluding the reaper. This is the second
// TSan-worthy test (alongside R7): it is the only multi-threaded exercise of the between-query mutators
// against the reaper. Correctness bar: no crash/race and the session stays queryable after the storm.
TEST_F(HotColdIdleReaperTest, ConcurrentReaperVsSessionDbMutators) {
  const std::string db_a = "reap_r8_a";
  const std::string db_b = "reap_r8_b";
  constexpr int kNodesA = 4;
  constexpr int kNodesB = 7;
  CreateAndPopulate(db_a, kNodesA);
  CreateAndPopulate(db_b, kNodesB);

  auto interpreter = min_mg->NewInterpreter();
  interpreter.interpreter.MarkReapable();
  interpreter.interpreter.SetCurrentDB(db_a, /*in_explicit_db=*/false);

  std::atomic<bool> stop{false};
  std::atomic<uint64_t> reaps{0};
  std::thread reaper([&] {
    while (!stop.load(std::memory_order_acquire)) {
      // Maximally aggressive: any IDLE window with a held non-default accessor is reaped.
      if (interpreter.interpreter.TryReapIdleDbAccessor(kHugeNs, /*idle_timeout_ns=*/0)) {
        reaps.fetch_add(1, std::memory_order_relaxed);
      }
    }
  });

  // Hammer the between-query mutators: switch A<->B and clear, exactly as Configure/TryDefaultDB/LogOff
  // do, with no surrounding Prepare/Pull. Each call must serialize against the reaper's db_acc_.reset().
  constexpr int kIterations = 600;
  for (int i = 0; i < kIterations; ++i) {
    interpreter.interpreter.SetCurrentDB(db_a, /*in_explicit_db=*/false);
    interpreter.interpreter.SetCurrentDB(db_b, /*in_explicit_db=*/false);
    interpreter.interpreter.ResetDB();
  }

  stop.store(true, std::memory_order_release);
  reaper.join();

  // The session is left consistent: re-acquire and read the expected tenant's data after the storm.
  interpreter.interpreter.SetCurrentDB(db_b, /*in_explicit_db=*/false);
  {
    auto [stream, qid] = interpreter.Prepare("MATCH (n) RETURN count(n) AS c");
    interpreter.Pull(&stream);
    const auto &results = stream.GetResults();
    ASSERT_EQ(results.size(), 1U);
    EXPECT_EQ(results[0][0].ValueInt(), kNodesB) << "session-db mutator storm corrupted the accessor";
  }
}

#endif  // MG_ENTERPRISE
