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

// Unit tests for the hot/cold RESUME engine (DbmsHandler::Resume_).
//
// The NODE-LOCAL synchronous resume engine provides single-flight semantics via the gatekeeper,
// off-lock recovery, a pre-publish (on_resume_) arm, and the COLD-access query seam (Get() on a
// suspended tenant errors with an actionable message). Resume is exercised directly here; the
// interpreter query-seam caller is wired in the same commit but verified via the e2e suite later.
//
// Coverage:
//   ResumeNonExistentRejected             — resuming an absent tenant returns NON_EXISTENT
//   ResumeAlreadyHotReturnsAccessor       — resuming a HOT tenant is a no-op that shares its accessor
//   SuspendResumeRoundTripRecoversData    — data survives a HOT -> COLD -> HOT cycle (WAL replay)
//   ColdAccessThrowsSuspendedError        — Get() on a COLD tenant throws the suspended-seam message
//   OnResumePrePublishArmRuns             — the pre-publish arm fires exactly once on a real resume
//   OnResumeFailureStaysColdRetriable     — a throwing pre-publish arm rolls back to COLD; retriable
//   ConcurrentResumeSingleFlight          — concurrent resumers rebuild once; all observe HOT
//   WrongTypedColdMetadataBootsDegraded   — valid JSON but wrong-typed metadata field boots degraded, not abort

#ifdef MG_ENTERPRISE

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "kvstore/kvstore.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/view.hpp"
#include "system/system.hpp"
#include "tests/test_commit_args_helper.hpp"

namespace fs = std::filesystem;
using memgraph::dbms::DbmsHandler;

// Per-test-binary storage root.
static fs::path g_storage_root{fs::temp_directory_path() / "MG_test_unit_hot_cold_resume"};

class HotColdResume : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = g_storage_root / ::testing::UnitTest::GetInstance()->current_test_info()->name();
    fs::remove_all(test_dir_);
    fs::create_directories(test_dir_);

    memgraph::storage::UpdatePaths(conf_, test_dir_);
    conf_.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    conf_.durability.recover_on_startup = true;
    handler_ = std::make_unique<DbmsHandler>(conf_);
  }

  void TearDown() override {
    handler_.reset();
    fs::remove_all(test_dir_);
  }

  // Simulate a process restart: tear down the handler (closes durability + storage) and reconstruct
  // it over the SAME data directory, so the new handler runs its restore loop against what the old
  // one persisted. Used by the cross-restart tests.
  void Restart() {
    handler_.reset();
    handler_ = std::make_unique<DbmsHandler>(conf_);
  }

  std::string CreateTenant(std::string name) {
    auto result = handler_->New(name);
    EXPECT_TRUE(result.has_value()) << "Failed to create tenant: " << name;
    return name;
  }

  // Create a tenant and write `n` vertices through a real storage accessor, so the suspend teardown
  // persists content the resume must replay.
  std::string CreateAndPopulate(std::string name, int n) {
    CreateTenant(name);
    auto db_acc = handler_->Get(name);
    auto storage_acc = db_acc->Access(memgraph::storage::WRITE);
    for (int i = 0; i < n; ++i) storage_acc->CreateVertex();
    EXPECT_TRUE(storage_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    return name;
  }

  static int64_t CountNodes(memgraph::dbms::DatabaseAccess &db_acc) {
    auto storage_acc = db_acc->Access(memgraph::storage::READ);
    int64_t count = 0;
    for ([[maybe_unused]] auto _ : storage_acc->Vertices(memgraph::storage::View::OLD)) ++count;
    return count;
  }

  bool InAll(std::string_view name) {
    auto all = handler_->All();
    return std::find(all.begin(), all.end(), name) != all.end();
  }

  // Clobber every durability file under a tenant's data dir with garbage, so the next boot's
  // recovery trips a RecoveryFailure (bad magic) instead of recovering — exercising the leave-cold
  // path deterministically. Call between handler_.reset() and reconstruction (NOT via Restart()): a
  // live storage holds the WAL open and its dtor would rewrite it.
  void CorruptTenantDurability(const std::string &uuid_str) {
    auto dir = test_dir_ / std::string(memgraph::dbms::kMultiTenantDir) / uuid_str;
    ASSERT_TRUE(fs::exists(dir)) << "expected a data dir for the tenant at " << dir;
    size_t clobbered = 0;
    for (const auto &f : fs::recursive_directory_iterator(dir)) {
      if (f.is_regular_file()) {
        std::ofstream out(f.path(), std::ios::binary | std::ios::trunc);
        out << "CORRUPTED-BY-TEST-NOT-A-VALID-DURABILITY-MAGIC";
        ++clobbered;
      }
    }
    ASSERT_GT(clobbered, 0U) << "no durability files to corrupt under " << dir;
  }

  fs::path test_dir_;
  memgraph::storage::Config conf_;
  std::unique_ptr<DbmsHandler> handler_;
};

// Resuming a tenant that was never created (nor suspended) returns NON_EXISTENT.
TEST_F(HotColdResume, ResumeNonExistentRejected) {
  auto result = handler_->Resume("never_existed");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), DbmsHandler::ResumeError::NON_EXISTENT);
}

// Resuming an already-HOT tenant is a no-op: Resume_ races to Get() and returns its live accessor.
TEST_F(HotColdResume, ResumeAlreadyHotReturnsAccessor) {
  auto name = CreateTenant("already_hot");
  auto result = handler_->Resume(name);
  ASSERT_TRUE(result.has_value()) << "Resuming a HOT tenant must return its accessor, not error";
  EXPECT_EQ(result.value()->name(), name);
  EXPECT_TRUE(InAll(name));
}

// The core round-trip: data written before a suspend must be present after the resume (WAL replay).
TEST_F(HotColdResume, SuspendResumeRoundTripRecoversData) {
  constexpr int kNodes = 7;
  auto name = CreateAndPopulate("round_trip", kNodes);

  ASSERT_TRUE(handler_->Suspend(name).has_value());
  EXPECT_FALSE(InAll(name)) << "Tenant must be COLD after suspend";

  auto result = handler_->Resume(name);
  ASSERT_TRUE(result.has_value()) << "Resume of a populated COLD tenant must succeed";
  EXPECT_TRUE(InAll(name)) << "Tenant must be HOT again after resume";

  auto acc = result.value();
  EXPECT_EQ(CountNodes(acc), kNodes) << "All vertices must survive the HOT -> COLD -> HOT cycle";

  // The fresh accessor is independently usable via Get().
  auto via_get = handler_->Get(name);
  EXPECT_EQ(CountNodes(via_get), kNodes);
}

// Cold-access query seam: once suspended, Get() throws (it is no longer HOT). The message must point
// the user at RESUME rather than claim the database is unknown.
TEST_F(HotColdResume, ColdAccessThrowsSuspendedError) {
  auto name = CreateTenant("cold_access");
  ASSERT_TRUE(handler_->Suspend(name).has_value());

  EXPECT_THROW(
      {
        try {
          handler_->Get(name);
        } catch (const std::exception &e) {
          EXPECT_THAT(e.what(), ::testing::HasSubstr("suspended"));
          throw;
        }
      },
      std::exception);
}

// The pre-publish arm (on_resume_) runs exactly once on a real COLD -> HOT resume.
TEST_F(HotColdResume, OnResumePrePublishArmRuns) {
  auto name = CreateTenant("pre_publish");
  ASSERT_TRUE(handler_->Suspend(name).has_value());

  std::atomic<int> arm_calls{0};
  handler_->SetOnResume([&arm_calls](memgraph::dbms::DatabaseAccess) { ++arm_calls; });

  auto result = handler_->Resume(name);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(arm_calls.load(), 1) << "The pre-publish arm must fire exactly once on a real resume";

  handler_->SetOnResume({});
}

// A throwing pre-publish arm aborts the resume (RESUMING -> COLD); the tenant stays COLD and a later
// (non-throwing) resume succeeds.
TEST_F(HotColdResume, OnResumeFailureStaysColdRetriable) {
  auto name = CreateTenant("retriable");
  ASSERT_TRUE(handler_->Suspend(name).has_value());

  std::atomic<bool> should_throw{true};
  handler_->SetOnResume([&should_throw](memgraph::dbms::DatabaseAccess) {
    if (should_throw.load()) throw std::runtime_error("injected pre-publish failure");
  });

  auto failed = handler_->Resume(name);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error(), DbmsHandler::ResumeError::RECOVERY_FAILED);
  EXPECT_FALSE(InAll(name)) << "A failed resume must leave the tenant COLD";

  // Retry without the injected failure: the tenant must recover.
  should_throw.store(false);
  auto ok = handler_->Resume(name);
  ASSERT_TRUE(ok.has_value()) << "A COLD tenant must be resumable after a transient pre-publish failure";
  EXPECT_TRUE(InAll(name));

  handler_->SetOnResume({});
}

// Concurrent resumers single-flight through the gatekeeper: exactly one thread rebuilds (the
// on_resume_ arm fires once), and every caller observes the published HOT tenant.
TEST_F(HotColdResume, ConcurrentResumeSingleFlight) {
  auto name = CreateTenant("single_flight");
  ASSERT_TRUE(handler_->Suspend(name).has_value());

  std::atomic<int> rebuilds{0};
  handler_->SetOnResume([&rebuilds](memgraph::dbms::DatabaseAccess) {
    // Hold briefly so the losers are guaranteed to enter the poll path while the winner builds.
    ++rebuilds;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  });

  constexpr int kThreads = 8;
  std::atomic<int> successes{0};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&] {
      auto res = handler_->Resume(name);
      if (res.has_value()) ++successes;
    });
  }
  for (auto &t : threads) t.join();

  EXPECT_EQ(rebuilds.load(), 1) << "Exactly one thread must rebuild the storage (single-flight)";
  EXPECT_EQ(successes.load(), kThreads) << "Every concurrent resumer must observe the published HOT tenant";
  EXPECT_TRUE(InAll(name));

  // Post-join deterministic confirmation: the tenant is usable (HOT) for every subsequent caller —
  // not just the racing threads. Get() would throw if the tenant were COLD; a successful accessor
  // proves the published state is durable and not a transient winner artifact.
  // Resume() is also called one more time to confirm the idempotent HOT fast-path works post-flight.
  EXPECT_NO_THROW({
    auto post_join_acc = handler_->Get(name);
    EXPECT_TRUE(post_join_acc) << "Get() after all resumers joined must return a valid accessor";
  }) << "tenant must be usable (HOT) for all callers after the concurrent single-flight completes";

  auto post_resume = handler_->Resume(name);
  ASSERT_TRUE(post_resume.has_value())
      << "Resume() on an already-HOT tenant must return the live accessor (idempotent HOT fast-path)";
  EXPECT_EQ(post_resume.value()->name(), name);

  handler_->SetOnResume({});
}

// Regression: a loser in the single-flight poll must NOT return RECOVERY_FAILED when the winner is
// running a slow-but-healthy rebuild that exceeds the OLD fixed kPollTimeout (10 s). The fix re-arms
// the liveness deadline on each poll iteration where the winner is observably still RESUMING, so a
// build that takes longer than 10 s but shorter than kWinnerLivenessWindow (30 s) succeeds for all
// concurrent callers.
//
// NOTE: this test intentionally takes ~12 seconds — it must exceed the old 10 s deadline to confirm
// the regression is closed. The on_resume_ arm sleeps 12 s on the first call only, so only one
// rebuild stalls; subsequent calls return immediately (idempotent HOT fast-path).
TEST_F(HotColdResume, ConcurrentResumeSlowWinnerDoesNotSpuriouslyFail) {
  auto name = CreateTenant("slow_winner");
  ASSERT_TRUE(handler_->Suspend(name).has_value());

  std::atomic<int> rebuilds{0};
  // The arm sleeps 12 s on the first call (winner's on_resume_) to simulate a large-tenant rebuild
  // that would have tripped the OLD fixed 10 s kPollTimeout. Subsequent calls (there should be none
  // — single-flight guarantees exactly one rebuild) return immediately.
  handler_->SetOnResume([&rebuilds](memgraph::dbms::DatabaseAccess) {
    if (rebuilds.fetch_add(1) == 0) {
      std::this_thread::sleep_for(std::chrono::seconds(12));
    }
  });

  constexpr int kThreads = 4;
  std::atomic<int> successes{0};
  std::atomic<int> recovery_failed{0};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&] {
      auto res = handler_->Resume(name);
      if (res.has_value()) {
        ++successes;
      } else if (res.error() == DbmsHandler::ResumeError::RECOVERY_FAILED) {
        ++recovery_failed;
      }
    });
  }
  for (auto &t : threads) t.join();

  EXPECT_EQ(rebuilds.load(), 1) << "Exactly one thread must rebuild the storage (single-flight)";
  EXPECT_EQ(recovery_failed.load(), 0)
      << "No loser must spuriously return RECOVERY_FAILED when the winner is still alive and building";
  EXPECT_EQ(successes.load(), kThreads) << "Every concurrent resumer must observe the published HOT tenant";
  EXPECT_TRUE(InAll(name));

  handler_->SetOnResume({});
}

// Suspend/resume driven through a real system::Transaction must record their actions and complete
// the HOT -> COLD -> HOT round-trip. Committing the transaction (DoNothing on a node with no
// replicas) drives the action's DoDurability + the system-ts finalize without dropping data.
TEST_F(HotColdResume, SuspendResumeThroughSystemTransaction) {
  constexpr int kNodes = 5;
  auto name = CreateAndPopulate("sys_tx_route", kNodes);

  memgraph::system::System sys;
  {
    auto txn = sys.TryCreateTransaction();
    ASSERT_TRUE(txn.has_value());
    ASSERT_TRUE(handler_->Suspend(name, &*txn).has_value()) << "Suspend via a system transaction must succeed";
    txn->Commit(memgraph::system::DoNothing{});
  }
  EXPECT_FALSE(InAll(name)) << "Tenant must be COLD after a system-transaction suspend";

  {
    auto txn = sys.TryCreateTransaction();
    ASSERT_TRUE(txn.has_value());
    auto result = handler_->Resume(name, &*txn);
    ASSERT_TRUE(result.has_value()) << "Resume via a system transaction must succeed";
    txn->Commit(memgraph::system::DoNothing{});
    EXPECT_EQ(CountNodes(result.value()), kNodes) << "Data must survive the system-transaction round-trip";
  }
  EXPECT_TRUE(InAll(name)) << "Tenant must be HOT after a system-transaction resume";
}

// The by-UUID entrypoints (used by the replica Suspend/ResumeDatabaseRpc apply handlers, which only
// carry the tenant UUID on the wire) complete the HOT -> COLD -> HOT round-trip and recover data,
// identically to the by-name path. Resolved UUID -> name internally.
TEST_F(HotColdResume, SuspendResumeByUUIDRoundTrip) {
  constexpr int kNodes = 4;
  auto name = CreateAndPopulate("by_uuid", kNodes);
  memgraph::utils::UUID uuid;
  {
    auto acc = handler_->Get(name);
    uuid = acc->config().salient.uuid;
  }

  ASSERT_TRUE(handler_->SuspendByUUID(uuid).has_value()) << "SuspendByUUID must succeed for a HOT tenant";
  EXPECT_FALSE(InAll(name)) << "Tenant must be COLD after SuspendByUUID";

  auto result = handler_->ResumeByUUID(uuid);
  ASSERT_TRUE(result.has_value()) << "ResumeByUUID must succeed for a COLD tenant";
  EXPECT_EQ(CountNodes(result.value()), kNodes) << "Data must survive the by-UUID round-trip";
  EXPECT_TRUE(InAll(name)) << "Tenant must be HOT after ResumeByUUID";
}

// An unknown UUID is rejected cleanly (NON_EXISTENT, not a crash) by both by-UUID entrypoints — this
// is the idempotent re-apply path the replica handlers map to NO_NEED.
TEST_F(HotColdResume, SuspendResumeByUnknownUUIDRejected) {
  const memgraph::utils::UUID unknown{};
  auto s = handler_->SuspendByUUID(unknown);
  ASSERT_FALSE(s.has_value());
  EXPECT_EQ(s.error(), DbmsHandler::SuspendError::NON_EXISTENT);

  auto r = handler_->ResumeByUUID(unknown);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error(), DbmsHandler::ResumeError::NON_EXISTENT);
}

// Regression: creating a NEW tenant while another is COLD must not abort. New()'s data-directory
// collision scan iterates every gatekeeper; a COLD shell yields no accessor, and the old MG_ASSERT
// there aborted the process. This covers the replica-recovery steady state (materialize an absent
// tenant while a COLD shell exists), and also a plain CREATE-while-suspended.
TEST_F(HotColdResume, CreateTenantWhileAnotherSuspended) {
  auto cold = CreateAndPopulate("cold_one", 3);
  ASSERT_TRUE(handler_->Suspend(cold).has_value());
  EXPECT_FALSE(InAll(cold)) << "first tenant must be COLD";

  // New() runs the collision scan across all gatekeepers, including the COLD shell.
  auto fresh = handler_->New("fresh_one");
  ASSERT_TRUE(fresh.has_value()) << "creating a tenant while another is COLD must succeed (no abort)";
  EXPECT_TRUE(InAll("fresh_one"));
  EXPECT_FALSE(InAll(cold)) << "the COLD tenant stays COLD";

  // And the COLD tenant still resumes cleanly afterward.
  auto resumed = handler_->Resume(cold);
  ASSERT_TRUE(resumed.has_value());
  EXPECT_EQ(CountNodes(resumed.value()), 3);
}

// Cross-restart: a tenant suspended before a restart must come back COLD (durable cold marker),
// not HOT, and must still resume with all its data. A HOT tenant present across the same restart must
// recover HOT and unchanged — proving the restore loop branches on the marker and the cleanup pass
// preserves both tenants' data directories.
TEST_F(HotColdResume, CrossRestartColdTenantStaysColdHotTenantRecovers) {
  constexpr int kColdNodes = 7;
  constexpr int kHotNodes = 4;
  auto cold = CreateAndPopulate("cold_persist", kColdNodes);
  auto hot = CreateAndPopulate("hot_persist", kHotNodes);

  // Capture the gauge BEFORE the suspend. DbmsHandler's constructor calls Set(suspended_.size()),
  // so Restart() resets the gauge to the new handler's cold count (1 here). Capturing before
  // Suspend() means: baseline=N, after_restart=N+1 → delta=1. Capturing after Suspend() would
  // give baseline=N+1, after_restart=N+1 → delta=0 (wrong). The SetUp() ctor already sets gauge=0
  // for a fresh test directory, so N=0 in isolation; the delta form is safe even if other tests
  // in this binary leave residue (each test's SetUp constructs a fresh handler that calls Set(0)).
  const double cold_gauge_before_suspend = memgraph::metrics::Metrics().global.cold_databases->Value();

  ASSERT_TRUE(handler_->Suspend(cold).has_value());
  EXPECT_FALSE(InAll(cold)) << "tenant must be COLD before the restart";

  Restart();

  // After restart the new handler sets the gauge to its own cold-tenant count (1).
  // The delta vs. the pre-suspend baseline is exactly 1 (one tenant restored COLD on boot).
  EXPECT_DOUBLE_EQ(memgraph::metrics::Metrics().global.cold_databases->Value() - cold_gauge_before_suspend, 1.0)
      << "the cold-tenant gauge must increase by 1 (net vs. pre-suspend baseline) after restart with one COLD tenant";

  // The cold tenant is restored as a COLD shell: not in All(), and Get() trips the cold seam.
  EXPECT_FALSE(InAll(cold)) << "a tenant suspended before restart must recover COLD";
  EXPECT_THROW(
      {
        try {
          handler_->Get(cold);
        } catch (const std::exception &e) {
          EXPECT_THAT(e.what(), ::testing::HasSubstr("suspended"));
          throw;
        }
      },
      std::exception);

  // The hot tenant is restored HOT with its data intact.
  ASSERT_TRUE(InAll(hot)) << "a HOT tenant must recover HOT across a restart";
  auto hot_acc = handler_->Get(hot);
  EXPECT_EQ(CountNodes(hot_acc), kHotNodes);

  // The durable cold_stats round-trips: the COLD tenant's restored StorageInfo (read back through
  // the public recovery snapshot) carries the as-of-suspend vertex_count. This exercises the 23-field
  // StatsToJson/StatsFromJson path end-to-end across the restart.
  {
    auto cold_set = handler_->SuspendedConfigsForRecovery();
    bool found_cold_stats = false;
    for (const auto &c : cold_set) {
      if (c.salient.name.str() == cold) {
        EXPECT_EQ(c.stats.vertex_count, static_cast<uint64_t>(kColdNodes))
            << "the durably-restored cold_stats must carry the as-of-suspend vertex_count";
        found_cold_stats = true;
      }
    }
    EXPECT_TRUE(found_cold_stats) << "the cold tenant must appear in the recovery snapshot after restart";
  }

  // The cold tenant resumes from its preserved data directory with all data intact.
  auto resumed = handler_->Resume(cold);
  ASSERT_TRUE(resumed.has_value()) << "a COLD tenant restored from disk must resume";
  EXPECT_TRUE(InAll(cold));
  EXPECT_EQ(CountNodes(resumed.value()), kColdNodes) << "all data must survive suspend -> restart -> resume";
}

// A RESUME clears the durable cold marker, so a tenant that was resumed before a restart recovers
// HOT (not COLD) on the next boot — the inverse of the test above.
TEST_F(HotColdResume, CrossRestartResumedTenantRecoversHot) {
  constexpr int kNodes = 5;
  auto name = CreateAndPopulate("resumed_persist", kNodes);

  ASSERT_TRUE(handler_->Suspend(name).has_value());
  ASSERT_TRUE(handler_->Resume(name).has_value());
  EXPECT_TRUE(InAll(name)) << "tenant must be HOT after resume";

  Restart();

  ASSERT_TRUE(InAll(name)) << "a resumed tenant must recover HOT across a restart (cold marker cleared)";
  auto acc = handler_->Get(name);
  EXPECT_EQ(CountNodes(acc), kNodes);
}

// A COLD tenant restarted+resumed keeps its original epoch — resume rebuilds the storage from disk
// (BuildDetached) and runs the epoch recovered from the tenant's own WAL/snapshot, unchanged. (The
// cold-tenant epoch-rewrite-on-promotion machinery was intentionally removed.)
TEST_F(HotColdResume, CrossRestartColdTenantWithoutPromotionKeepsEpoch) {
  constexpr int kNodes = 3;
  auto cold = CreateAndPopulate("unpromoted_cold", kNodes);

  std::string e1;
  {
    auto acc = handler_->Get(cold);
    e1 = std::string{acc->storage()->repl_storage_state_.epoch_.id()};
  }
  ASSERT_FALSE(e1.empty());

  ASSERT_TRUE(handler_->Suspend(cold).has_value());
  Restart();
  auto resumed = handler_->Resume(cold);
  ASSERT_TRUE(resumed.has_value());
  EXPECT_EQ(CountNodes(resumed.value()), kNodes);

  auto &rss = resumed.value()->storage()->repl_storage_state_;
  EXPECT_EQ(std::string{rss.epoch_.id()}, e1) << "a non-promoted resume must preserve the original epoch";
}

// On a replica, MAIN's authoritative as-of-suspend stats arrive over the V3 SystemRecovery wire and
// ApplyColdRecoveryMeta installs them into the cold entry, so cold SHOW STORAGE INFO / SHOW DATABASES
// on the replica match MAIN. (No epoch is carried; cold-tenant epoch machinery was removed.)
TEST_F(HotColdResume, AppliedWireStatsRefreshColdEntry) {
  constexpr int kNodes = 4;
  auto cold = CreateAndPopulate("wire_stats_cold", kNodes);

  ASSERT_TRUE(handler_->Suspend(cold).has_value());

  // MAIN's authoritative stats snapshot, distinct from anything the local suspend captured.
  memgraph::storage::ColdTenantRecovery meta;
  for (const auto &c : handler_->SuspendedConfigsForRecovery()) {
    if (c.salient.name.str() == cold) meta.salient = c.salient;
  }
  meta.stats.vertex_count = 4242;
  meta.stats.edge_count = 99;
  handler_->ApplyColdRecoveryMeta(cold, meta);

  bool found = false;
  for (const auto &c : handler_->SuspendedConfigsForRecovery()) {
    if (c.salient.name.str() == cold) {
      EXPECT_EQ(c.stats.vertex_count, 4242) << "ApplyColdRecoveryMeta must install MAIN's stats snapshot";
      EXPECT_EQ(c.stats.edge_count, 99);
      found = true;
    }
  }
  ASSERT_TRUE(found) << "the suspended tenant must appear in the recovery snapshot";

  // The refreshed stats are durable: a restart recovers them (still COLD, no resume).
  Restart();
  auto info = handler_->GetColdShowInfo(cold);
  ASSERT_TRUE(info.has_value()) << "the tenant must recover COLD with its refreshed stats";
  EXPECT_EQ(info->stats.vertex_count, 4242) << "MAIN's stats must survive a restart via the cold marker";
}

// Observability: a successful SUSPEND/RESUME pair moves the global hot/cold Prometheus metrics — the
// suspends/resumes counters and the cold-tenant gauge.
//   - The counters are process-global monotonic singletons shared across every test in this binary, so
//     assert on DELTAS captured around the operation.
//   - The gauge is a process-global singleton too (SET to the live DbmsHandler's suspended_ size), so
//     it is also asserted as a DELTA: capture the baseline before the operation and assert the change
//     (+1 after suspend, 0 after resume), rather than an absolute value that would be flaky if sibling
//     tests in this binary leave residue.
TEST_F(HotColdResume, SuspendResumeMoveObservabilityMetrics) {
  auto &m = memgraph::metrics::Metrics().global;
  const double suspends0 = m.database_suspends->Value();
  const double resumes0 = m.database_resumes->Value();
  // Latency-histogram sample-count baselines: a dropped Observe() would leave these unchanged, a
  // regression the counters alone would NOT catch.
  const uint64_t suspend_lat0 = m.database_suspend_latency_seconds->Collect().histogram.sample_count;
  const uint64_t resume_lat0 = m.database_resume_latency_seconds->Collect().histogram.sample_count;
  // Capture the gauge baseline BEFORE any suspend so both gauge assertions are deltas, not absolutes.
  // The gauge is a process-global singleton; other tests in this binary may have left residue.
  const double cold_gauge0 = m.cold_databases->Value();

  auto t = CreateAndPopulate("metrics_tenant", 3);
  ASSERT_TRUE(handler_->Suspend(t).has_value());
  EXPECT_DOUBLE_EQ(m.database_suspends->Value() - suspends0, 1.0)
      << "a successful SUSPEND increments the suspends counter";
  EXPECT_DOUBLE_EQ(m.cold_databases->Value() - cold_gauge0, 1.0)
      << "the cold-tenant gauge increases by 1 when the tenant is suspended";
  EXPECT_EQ(m.database_suspend_latency_seconds->Collect().histogram.sample_count - suspend_lat0, 1u)
      << "a successful SUSPEND observes the suspend-latency histogram exactly once";

  ASSERT_TRUE(handler_->Resume(t).has_value());
  EXPECT_DOUBLE_EQ(m.database_resumes->Value() - resumes0, 1.0) << "a successful RESUME increments the resumes counter";
  EXPECT_EQ(m.database_resume_latency_seconds->Collect().histogram.sample_count - resume_lat0, 1u)
      << "a successful RESUME observes the resume-latency histogram exactly once";
  EXPECT_DOUBLE_EQ(m.cold_databases->Value() - cold_gauge0, 0.0)
      << "the cold-tenant gauge returns to its pre-test level after resume";
}

// A HOT tenant whose recovery FAILS at boot must ABORT the process — fail loud, exactly as on master
// (and on a single-tenant instance). It is never silently demoted to COLD: a HOT database that cannot
// be brought up at boot must surface the problem rather than serve a tenant that lost data. (A
// user-initiated RESUME is the recoverable counterpart: it fails + returns ResumeError::RECOVERY_FAILED
// and leaves the tenant COLD/retriable — see the ResumeRecoveryFailure tests above.) Death test uses
// the "threadsafe" style: the DbmsHandler ctor has already spawned storage threads by the time the
// assert fires, so a plain fork()-based death test could deadlock (see toolchain notes).
TEST_F(HotColdResume, BootRecoveryFailureAbortsProcess) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  constexpr int kBadNodes = 5;
  auto bad = CreateAndPopulate("recover_fail", kBadNodes);

  // HOT (durable cold:false). Capture its uuid to locate the on-disk data.
  std::string bad_uuid;
  {
    auto acc = handler_->Get(bad);
    bad_uuid = static_cast<std::string>(acc->storage()->uuid());
  }

  // Tear down (flush + close), corrupt the HOT tenant's durability, then reconstructing the handler
  // must die: a HOT database that cannot be recovered at boot aborts the process.
  handler_.reset();
  CorruptTenantDurability(bad_uuid);

  ASSERT_DEATH(
      {
        auto h = std::make_unique<DbmsHandler>(conf_);
        (void)h;
      },
      "Failed while recovering database");
}

// Regression: a durable cold entry that is VALID JSON but has a WRONG-TYPED optional metadata field
// (e.g. cold_stats stored as a string instead of an object) must NOT abort the boot. nlohmann's
// j.value() only provides the default for MISSING keys; calling value() on a non-object throws
// type_error (306), which previously escaped the parse guard and propagated out of the DbmsHandler
// ctor → std::terminate.
//
// After the fix, make_cold_entry is wrapped in its own nlohmann::json::exception catch so:
//   (a) the instance boots (no terminate, no throw from the ctor),
//   (b) a healthy sibling tenant still recovers HOT with its data,
//   (c) the corrupt tenant's data directory is NOT deleted (durability_view_incomplete → skip cleanup).
TEST_F(HotColdResume, WrongTypedColdMetadataBootsDegraded) {
  constexpr int kSiblingNodes = 4;
  constexpr int kVictimNodes = 7;
  auto sibling = CreateAndPopulate("sibling_tenant", kSiblingNodes);
  auto victim = CreateAndPopulate("victim_cold", kVictimNodes);

  // Capture the victim's UUID + data dir before suspending.
  std::string victim_uuid_str;
  {
    auto acc = handler_->Get(victim);
    victim_uuid_str = static_cast<std::string>(acc->storage()->uuid());
  }
  const auto victim_dir = test_dir_ / std::string(memgraph::dbms::kMultiTenantDir) / victim_uuid_str;
  ASSERT_TRUE(fs::exists(victim_dir));

  // Suspend the victim so a real cold durable entry (valid JSON) is written.
  ASSERT_TRUE(handler_->Suspend(victim).has_value());
  EXPECT_FALSE(InAll(victim));

  // Tear down (flushes/closes all storage).
  handler_.reset();

  // Overwrite the victim's durable KVStore value with VALID JSON that has a wrong-typed field:
  // cold_stats stored as a string instead of an object. StatsFromJson calls j.value(key, ...) on it,
  // which throws type_error(306) on a non-object. The uuid and rel_dir fields remain correct so the
  // entry passes the outer parse guard — only make_cold_entry sees the bad type. Constructed as a raw
  // string to avoid an nlohmann include in the test TU.
  {
    // clang-format off
    const std::string bad_json =
        std::string(R"({"uuid":")") + victim_uuid_str +
        R"(","rel_dir":")" + victim_uuid_str +
        R"(","cold":true,"cold_stats":"not_an_object"})";
    // clang-format on
    memgraph::kvstore::KVStore kv(test_dir_ / std::string(memgraph::dbms::kMultiTenantDir) / ".durability");
    ASSERT_TRUE(kv.Put(std::string("database:") + victim, bad_json));
  }

  // Reconstruct — must not throw or terminate.
  ASSERT_NO_THROW(handler_ = std::make_unique<DbmsHandler>(conf_))
      << "DbmsHandler ctor must survive a cold entry with a wrong-typed metadata field (boot must not abort)";

  // (b) The healthy sibling recovered HOT with its data.
  EXPECT_TRUE(InAll(sibling)) << "the intact sibling must recover HOT despite the victim's wrong-typed entry";
  auto sibling_acc = handler_->Get(sibling);
  EXPECT_EQ(CountNodes(sibling_acc), kSiblingNodes);

  // (a) The corrupt entry was skipped — tenant is neither HOT nor suspended.
  EXPECT_FALSE(InAll(victim)) << "the wrong-typed cold entry is skipped, so its tenant is not recovered";
  EXPECT_FALSE(handler_->IsSuspended(victim)) << "a skipped entry must not appear as a cold shell";

  // (c) The victim's data directory was NOT deleted (cleanup skipped when view is incomplete).
  EXPECT_TRUE(fs::exists(victim_dir))
      << "the victim's data dir must be preserved — cleanup is skipped when durability_view_incomplete";
}

// A corrupt/truncated durable entry must leave the instance starting DEGRADED — the entry is
// skipped, the rest of the tenants recover, and (crucially) NO data directory is deleted, because a
// corrupt entry whose rel_dir we cannot read would otherwise be reaped by the unused-directory cleanup.
TEST_F(HotColdResume, CorruptDurableEntrySkippedAndDataPreserved) {
  constexpr int kGoodNodes = 3;
  constexpr int kVictimNodes = 5;
  auto good = CreateAndPopulate("good_tenant", kGoodNodes);
  auto victim = CreateAndPopulate("victim_tenant", kVictimNodes);

  // Capture the victim's on-disk data dir before we corrupt its durable entry.
  std::string victim_uuid;
  {
    auto acc = handler_->Get(victim);
    victim_uuid = static_cast<std::string>(acc->storage()->uuid());
  }
  const auto victim_dir = test_dir_ / std::string(memgraph::dbms::kMultiTenantDir) / victim_uuid;
  ASSERT_TRUE(fs::exists(victim_dir));

  // Tear down, then clobber the victim's durability VALUE (valid key, non-JSON value) so the next boot's
  // restore loop trips json::parse — a different failure class from data-dir corruption (tested above).
  handler_.reset();
  {
    memgraph::kvstore::KVStore kv(test_dir_ / std::string(memgraph::dbms::kMultiTenantDir) / ".durability");
    ASSERT_TRUE(kv.Put(std::string("database:") + victim, "NOT-VALID-JSON{{{"));
  }

  handler_ = std::make_unique<DbmsHandler>(conf_);

  // The instance booted (did not abort). The good tenant recovered HOT; the corrupt entry was skipped.
  EXPECT_TRUE(InAll(good)) << "an intact tenant must recover HOT despite a sibling's corrupt durable entry";
  auto good_acc = handler_->Get(good);
  EXPECT_EQ(CountNodes(good_acc), kGoodNodes);
  EXPECT_FALSE(InAll(victim)) << "the corrupt entry is skipped, so its tenant is not recovered";
  EXPECT_FALSE(handler_->IsSuspended(victim));

  // CRUCIAL: the victim's data dir must NOT have been deleted — cleanup is skipped when the durable view
  // is incomplete, so a parse glitch never escalates to data loss.
  EXPECT_TRUE(fs::exists(victim_dir)) << "a corrupt entry's data dir must be preserved, not reaped by cleanup";
}

#else

#include <gtest/gtest.h>

TEST(HotColdResume, NotApplicableInCommunity) { GTEST_SKIP() << "hot/cold resume is an enterprise-only feature"; }

#endif  // MG_ENTERPRISE
