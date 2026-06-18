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
// C5 is the NODE-LOCAL synchronous resume engine: single-flight via the gatekeeper, off-lock
// recovery, pre-publish (on_resume_) arm, and the COLD-access query seam (Get() on a suspended
// tenant errors with an actionable message). Resume is exercised directly here; the interpreter
// query-seam caller is wired in the same commit but verified via the e2e suite later.
//
// Coverage:
//   ResumeNonExistentRejected           — resuming an absent tenant returns NON_EXISTENT
//   ResumeAlreadyHotReturnsAccessor     — resuming a HOT tenant is a no-op that shares its accessor
//   SuspendResumeRoundTripRecoversData  — data survives a HOT -> COLD -> HOT cycle (WAL replay)
//   ColdAccessThrowsSuspendedError      — Get() on a COLD tenant throws the suspended-seam message
//   OnResumePrePublishArmRuns           — the pre-publish arm fires exactly once on a real resume
//   OnResumeFailureStaysColdRetriable   — a throwing pre-publish arm rolls back to COLD; retriable
//   ConcurrentResumeSingleFlight        — concurrent resumers rebuild once; all observe HOT

#ifdef MG_ENTERPRISE

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
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
  // one persisted. Used by the cross-restart (C9) tests.
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

  handler_->SetOnResume({});
}

// C6 routing: suspend/resume driven through a real system::Transaction must record their actions and
// complete the HOT -> COLD -> HOT round-trip. Committing the transaction (DoNothing on a node with no
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

// C7 engine: the by-UUID entrypoints (used by the replica Suspend/ResumeDatabaseRpc apply handlers,
// which only carry the tenant UUID on the wire) complete the HOT -> COLD -> HOT round-trip and
// recover data, identically to the by-name path. Resolved UUID -> name internally.
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

// C8 regression: creating a NEW tenant while another is COLD must not abort. New()'s data-directory
// collision scan iterates every gatekeeper; a COLD shell yields no accessor, and the old MG_ASSERT
// there aborted the process. This is the C8 replica-recovery steady state (materialize an absent
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

// C9 cross-restart: a tenant suspended before a restart must come back COLD (durable cold marker),
// not HOT, and must still resume with all its data. A HOT tenant present across the same restart must
// recover HOT and unchanged — proving the restore loop branches on the marker and the cleanup pass
// preserves both tenants' data directories.
TEST_F(HotColdResume, CrossRestartColdTenantStaysColdHotTenantRecovers) {
  constexpr int kColdNodes = 7;
  constexpr int kHotNodes = 4;
  auto cold = CreateAndPopulate("cold_persist", kColdNodes);
  auto hot = CreateAndPopulate("hot_persist", kHotNodes);

  ASSERT_TRUE(handler_->Suspend(cold).has_value());
  EXPECT_FALSE(InAll(cold)) << "tenant must be COLD before the restart";

  Restart();

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
    auto [configs, stats] = handler_->SuspendedConfigsForRecovery();
    ASSERT_EQ(configs.size(), stats.size());
    bool found_cold_stats = false;
    for (size_t i = 0; i < configs.size(); ++i) {
      if (configs[i].name.str() == cold) {
        EXPECT_EQ(stats[i].vertex_count, static_cast<uint64_t>(kColdNodes))
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

// C9: a RESUME clears the durable cold marker, so a tenant that was resumed before a restart recovers
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

// C10 (RE-9′): a COLD tenant whose durable epoch metadata was rewritten by PromoteColdTenants (eager
// promotion) runs the NEW epoch after a restart+resume, and its epoch history carries the promotion
// boundary (the pre-promotion epoch). Without this, a lagging replica reconnecting at the old epoch
// would fail MAIN's continuous-history check and spuriously DIVERGE after failover.
TEST_F(HotColdResume, CrossRestartPromotedColdTenantRunsNewEpoch) {
  constexpr int kNodes = 6;
  auto cold = CreateAndPopulate("promoted_cold", kNodes);

  // Capture the pre-promotion epoch (E1) from the live HOT storage.
  std::string e1;
  {
    auto acc = handler_->Get(cold);
    e1 = std::string{acc->storage()->repl_storage_state_.epoch_.id()};
  }
  ASSERT_FALSE(e1.empty());

  ASSERT_TRUE(handler_->Suspend(cold).has_value());

  // Eager-epoch promotion: rewrite the COLD tenant's durable epoch metadata to a new epoch (this is
  // what DoToMainPromotion calls for cold tenants the ForEach epoch loop cannot reach). Metadata-only.
  const std::string e2 = "c10-promotion-epoch";
  handler_->PromoteColdTenants(e2);

  Restart();

  // Promotion is metadata-only: the tenant is still COLD after the restart (no resume happened).
  EXPECT_FALSE(InAll(cold)) << "a promoted-but-not-resumed cold tenant must recover COLD";

  auto resumed = handler_->Resume(cold);
  ASSERT_TRUE(resumed.has_value());
  EXPECT_EQ(CountNodes(resumed.value()), kNodes) << "data survives suspend -> promote -> restart -> resume";

  auto &rss = resumed.value()->storage()->repl_storage_state_;
  EXPECT_EQ(std::string{rss.epoch_.id()}, e2)
      << "the resumed tenant must run the post-promotion epoch, not the disk-recovered one (RE-9′)";
  ASSERT_FALSE(rss.history.empty()) << "the promotion boundary must be present in the epoch history";
  EXPECT_EQ(rss.history.back().first, e1)
      << "the epoch history must carry the pre-promotion epoch as the promotion boundary (continuous history)";
}

// C10: a COLD tenant restarted+resumed WITHOUT a promotion keeps its original epoch — the RE-9′ resume
// override restores the captured epoch verbatim and must not corrupt or rotate it.
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

// C10 (race): a promotion that lands in the resume's off-lock BuildDetached window must win. The
// on_resume_ hook fires after BuildDetached and before the publish lock, so promoting from inside it
// rewrites the in-map suspended_ entry to a new epoch AFTER the resume copied the (now stale) entry in
// Phase A. The resume must re-read the entry under the publish lock and apply the PROMOTED epoch, not
// the stale Phase-A snapshot. (Deterministically fails if the override uses the Phase-A copy.)
TEST_F(HotColdResume, PromotionDuringResumeAppliesPromotedEpoch) {
  constexpr int kNodes = 5;
  auto cold = CreateAndPopulate("promote_during_resume", kNodes);

  std::string e1;
  {
    auto acc = handler_->Get(cold);
    e1 = std::string{acc->storage()->repl_storage_state_.epoch_.id()};
  }
  ASSERT_TRUE(handler_->Suspend(cold).has_value());

  const std::string e2 = "epoch-promoted-mid-resume";
  handler_->SetOnResume([&](memgraph::dbms::DatabaseAccess) { handler_->PromoteColdTenants(e2); });
  auto resumed = handler_->Resume(cold);
  handler_->SetOnResume({});
  ASSERT_TRUE(resumed.has_value());

  auto &rss = resumed.value()->storage()->repl_storage_state_;
  EXPECT_EQ(std::string{rss.epoch_.id()}, e2)
      << "a promotion racing the resume's build window must win (epoch re-read under the publish lock)";
  ASSERT_FALSE(rss.history.empty());
  EXPECT_EQ(rss.history.back().first, e1) << "the promotion boundary must still carry the pre-promotion epoch";
}

#else

#include <gtest/gtest.h>

TEST(HotColdResume, NotApplicableInCommunity) { GTEST_SKIP() << "hot/cold resume is an enterprise-only feature"; }

#endif  // MG_ENTERPRISE
