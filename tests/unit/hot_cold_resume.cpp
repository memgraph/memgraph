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

// Unit tests for the hot/cold RESUME engine (DbmsHandler::Resume / KickResume).
// Test-only at runtime in this commit: Resume()/KickResume() are exercised directly here; the
// interpreter query-seam caller lands in a later commit.

#include "gtest/gtest.h"

#ifdef MG_ENTERPRISE

#include <atomic>
#include <chrono>
#include <filesystem>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "auth/auth.hpp"
#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "flags/general.hpp"
#include "flags/run_time_configurable.hpp"
#include "license/license.hpp"
#include "query/interpreter_context.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/logging.hpp"
#include "utils/synchronized.hpp"

namespace {

memgraph::storage::Config MakeConfig(
    const std::filesystem::path &dir, bool recover_on_startup,
    memgraph::storage::Config::Durability::SnapshotWalMode mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL) {
  memgraph::storage::Config cfg{};
  memgraph::storage::UpdatePaths(cfg, dir);
  cfg.durability.snapshot_wal_mode = mode;
  cfg.durability.recover_on_startup = recover_on_startup;
  // Disable periodic background snapshots; we only need the {snapshot+WAL} durability *mode* to be
  // declared, the suspend teardown writes the durable artifacts.
  cfg.durability.snapshot_on_exit = false;
  return cfg;
}

// Minimal DbmsHandler harness (mirrors hot_cold_suspend.cpp). Owns everything DbmsHandler depends
// on, in a self-contained scope so it can be torn down and re-created on the SAME data_directory.
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

  memgraph::utils::Settings settings;
  memgraph::auth::SynchedAuth auth;
  memgraph::system::System system;
  memgraph::parameters::Parameters parameters;
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state;
  memgraph::dbms::DbmsHandler dbms;
  memgraph::query::InterpreterContext interpreter_context;
};

}  // namespace

class HotColdResumeTest : public ::testing::Test {
 public:
  std::filesystem::path data_directory = std::filesystem::temp_directory_path() / "MG_tests_unit_hot_cold_resume";

  void SetUp() override {
    TearDown();
    // Debounce off so a freshly-created tenant is immediately suspendable.
    FLAGS_storage_hot_cold_min_hot_residency_sec = 0;
    min_mg.emplace(MakeConfig(data_directory, /*recover_on_startup=*/true));
  }

  void TearDown() override {
    min_mg.reset();
    if (std::filesystem::exists(data_directory)) std::filesystem::remove_all(data_directory);
  }

  auto &DBMS() { return min_mg->dbms; }

  // Create a tenant, write N nodes through a real storage accessor (so the WAL/snapshot has content).
  void CreateAndPopulate(const std::string &name, int n) {
    ASSERT_TRUE(DBMS().New(name).has_value());
    auto db_acc = DBMS().Get(name);
    auto storage_acc = db_acc->Access(memgraph::storage::WRITE);
    for (int i = 0; i < n; ++i) {
      storage_acc->CreateVertex();
    }
    ASSERT_TRUE(storage_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  static int64_t CountNodes(memgraph::dbms::DatabaseAccess &db_acc) {
    auto storage_acc = db_acc->Access(memgraph::storage::READ);
    int64_t count = 0;
    for ([[maybe_unused]] auto _ : storage_acc->Vertices(memgraph::storage::View::OLD)) ++count;
    return count;
  }

  // Look up a tenant's runtime state, if present.
  std::optional<memgraph::dbms::DbmsHandler::TenantState> StateOf(const std::string &name) {
    for (const auto &info : DBMS().TenantRuntimeInfos()) {
      if (info.name == name) return info.state;
    }
    return std::nullopt;
  }

  // Poll until the tenant is HOT (accessible) or the timeout expires. Returns true if HOT.
  bool WaitHot(const std::string &name, std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      try {
        (void)DBMS().Get(name);
        return true;
      } catch (const memgraph::dbms::UnknownDatabaseException &) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }
    }
    return false;
  }

  std::optional<MinMemgraph> min_mg;
};

// ---------------------------------------------------------------------------
// Inline resume success / error cases
// ---------------------------------------------------------------------------

TEST_F(HotColdResumeTest, ResumeRecoversDataInline) {
  constexpr int kNodes = 11;
  CreateAndPopulate("inline_db", kNodes);
  ASSERT_TRUE(DBMS().Suspend("inline_db").has_value());
  EXPECT_EQ(StateOf("inline_db"), memgraph::dbms::DbmsHandler::TenantState::COLD);

  auto res = DBMS().Resume("inline_db");
  ASSERT_TRUE(res.has_value()) << "resume should succeed inline";
  // The returned accessor is HOT and carries the recovered data.
  EXPECT_EQ(CountNodes(*res), kNodes);
  // The tenant is now READY (HOT) in the runtime infos.
  EXPECT_EQ(StateOf("inline_db"), memgraph::dbms::DbmsHandler::TenantState::READY);
}

TEST_F(HotColdResumeTest, ResumeUnknownReturnsNonExistent) {
  auto res = DBMS().Resume("never");
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), memgraph::dbms::DbmsHandler::ResumeError::NON_EXISTENT);
}

TEST_F(HotColdResumeTest, ResumeAlreadyHotReturnsAccessor) {
  ASSERT_TRUE(DBMS().New("hot").has_value());
  auto res = DBMS().Resume("hot");
  ASSERT_TRUE(res.has_value()) << "resuming an already-HOT tenant must return its accessor";
  EXPECT_EQ((*res)->name(), "hot");
  // No duplicate map entry created.
  int count = 0;
  for (const auto &info : DBMS().TenantRuntimeInfos()) {
    if (info.name == "hot") ++count;
  }
  EXPECT_EQ(count, 1);
}

TEST_F(HotColdResumeTest, SuspendResumeCycleStable) {
  constexpr int kNodes = 7;
  CreateAndPopulate("cycle_db", kNodes);

  for (int cycle = 0; cycle < 2; ++cycle) {
    ASSERT_TRUE(DBMS().Suspend("cycle_db").has_value()) << "suspend cycle " << cycle;
    auto res = DBMS().Resume("cycle_db");
    ASSERT_TRUE(res.has_value()) << "resume cycle " << cycle;
    EXPECT_EQ(CountNodes(*res), kNodes) << "data after cycle " << cycle;
  }

  // Exactly one map entry for the tenant (move-assign replaced the shell, did not duplicate).
  int count = 0;
  for (const auto &info : DBMS().TenantRuntimeInfos()) {
    if (info.name == "cycle_db") ++count;
  }
  EXPECT_EQ(count, 1);
}

// ---------------------------------------------------------------------------
// Concurrency: single-flight
// ---------------------------------------------------------------------------

TEST_F(HotColdResumeTest, ConcurrentResumeSingleFlight) {
  constexpr int kNodes = 13;
  CreateAndPopulate("sf_db", kNodes);
  ASSERT_TRUE(DBMS().Suspend("sf_db").has_value());

  std::atomic<int> recoveries{0};
  // Pre-publish arm sleeps so concurrent callers pile up while the winner is RESUMING.
  DBMS().SetOnResume([&recoveries](memgraph::dbms::DatabaseAccess) {
    ++recoveries;
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
  });

  constexpr int kThreads = 8;
  std::vector<std::thread> threads;
  std::atomic<int> successes{0};
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&] {
      auto res = DBMS().Resume("sf_db");
      if (res.has_value()) ++successes;
    });
  }
  for (auto &t : threads) t.join();

  DBMS().SetOnResume({});

  EXPECT_EQ(successes.load(), kThreads) << "all callers must observe HOT";
  EXPECT_EQ(recoveries.load(), 1) << "single-flight: exactly one recovery";
  auto acc = DBMS().Get("sf_db");
  EXPECT_EQ(CountNodes(acc), kNodes);
}

// ---------------------------------------------------------------------------
// Background executor path
// ---------------------------------------------------------------------------

TEST_F(HotColdResumeTest, KickResumeEventuallyHot) {
  constexpr int kNodes = 9;
  CreateAndPopulate("kick_db", kNodes);
  ASSERT_TRUE(DBMS().Suspend("kick_db").has_value());
  EXPECT_EQ(StateOf("kick_db"), memgraph::dbms::DbmsHandler::TenantState::COLD);

  DBMS().KickResume("kick_db");

  ASSERT_TRUE(WaitHot("kick_db")) << "background resume should bring the tenant HOT";
  auto acc = DBMS().Get("kick_db");
  EXPECT_EQ(CountNodes(acc), kNodes);
  EXPECT_EQ(StateOf("kick_db"), memgraph::dbms::DbmsHandler::TenantState::READY);
}

// ---------------------------------------------------------------------------
// Failure: stays COLD + retriable
// ---------------------------------------------------------------------------

TEST_F(HotColdResumeTest, OnResumeFailureStaysColdRetriable) {
  constexpr int kNodes = 4;
  CreateAndPopulate("fail_db", kNodes);
  ASSERT_TRUE(DBMS().Suspend("fail_db").has_value());

  std::atomic<bool> should_throw{true};
  DBMS().SetOnResume([&should_throw](memgraph::dbms::DatabaseAccess) {
    if (should_throw.load()) throw std::runtime_error("simulated on_resume failure");
  });

  auto res = DBMS().Resume("fail_db");
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), memgraph::dbms::DbmsHandler::ResumeError::RECOVERY_FAILED);
  // The tenant must remain COLD and retriable (suspended_ untouched).
  EXPECT_EQ(StateOf("fail_db"), memgraph::dbms::DbmsHandler::TenantState::COLD);

  // Clear the failure injection — resume now succeeds with intact data.
  should_throw.store(false);
  auto res2 = DBMS().Resume("fail_db");
  ASSERT_TRUE(res2.has_value()) << "resume must be retriable after the failure clears";
  EXPECT_EQ(CountNodes(*res2), kNodes);

  DBMS().SetOnResume({});
}

// ---------------------------------------------------------------------------
// Post-publish replication-arm failure: tenant stays HOT, not lost
// ---------------------------------------------------------------------------

// Regression for D2: a throwing on_resume_repl_ (POST-PUBLISH arm) must NOT roll the tenant back
// (the publish already happened — abort_resume on a HOT gatekeeper asserts/loses the tenant). The
// failure must be logged and the live accessor returned with intact data.
TEST_F(HotColdResumeTest, OnResumeReplFailureKeepsTenantHot) {
  constexpr int kNodes = 6;
  CreateAndPopulate("repl_fail_db", kNodes);
  ASSERT_TRUE(DBMS().Suspend("repl_fail_db").has_value());
  EXPECT_EQ(StateOf("repl_fail_db"), memgraph::dbms::DbmsHandler::TenantState::COLD);

  // Normal pre-publish arm; the post-publish replication arm throws.
  std::atomic<int> pre_publish_calls{0};
  std::atomic<int> repl_calls{0};
  DBMS().SetOnResume([&pre_publish_calls](memgraph::dbms::DatabaseAccess) { ++pre_publish_calls; });
  DBMS().SetOnResumeRepl([&repl_calls](memgraph::dbms::DatabaseAccess) {
    ++repl_calls;
    throw std::runtime_error("repl wiring failed");
  });

  auto res = DBMS().Resume("repl_fail_db");
  // Resume must SUCCEED: the tenant is published HOT before the replication arm runs.
  ASSERT_TRUE(res.has_value()) << "post-publish repl failure must NOT fail the resume";
  EXPECT_EQ(pre_publish_calls.load(), 1);
  EXPECT_EQ(repl_calls.load(), 1) << "the throwing post-publish arm must have been invoked";
  // The returned accessor is HOT and carries the recovered data.
  EXPECT_EQ(CountNodes(*res), kNodes);
  // The tenant is READY (HOT) — NOT lost, NOT rolled back to COLD.
  EXPECT_EQ(StateOf("repl_fail_db"), memgraph::dbms::DbmsHandler::TenantState::READY);

  // It is still reachable via a fresh Get (publish + suspended_.erase happened).
  auto acc = DBMS().Get("repl_fail_db");
  EXPECT_EQ(CountNodes(acc), kNodes);

  DBMS().SetOnResume({});
  DBMS().SetOnResumeRepl({});
}

// ---------------------------------------------------------------------------
// Teardown ordering: resume_pool_ joins before db_handler_ is destroyed
// ---------------------------------------------------------------------------

TEST_F(HotColdResumeTest, ResumePoolJoinsBeforeMapTeardown) {
  // Build a dedicated handler, suspend + kick a resume, then destroy the handler promptly. The
  // resume_pool_ must stop+join before db_handler_ (the map the in-flight job points into) is
  // destroyed — no crash/UAF. (Run under ASan if available; otherwise asserts no crash.)
  const auto dir = data_directory / "teardown";
  auto mg = std::make_optional<MinMemgraph>(MakeConfig(dir, /*recover_on_startup=*/true));
  ASSERT_TRUE(mg->dbms.New("td_db").has_value());
  ASSERT_TRUE(mg->dbms.Suspend("td_db").has_value());
  mg->dbms.KickResume("td_db");
  // Do NOT wait for HOT — destroy while the job may still be in-flight.
  EXPECT_NO_THROW(mg.reset());
}

#endif  // MG_ENTERPRISE
