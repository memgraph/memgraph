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

// Unit tests for the hot/cold suspend engine (DbmsHandler::Suspend_).
//
// These tests cover the NODE-LOCAL suspend engine only. Suspension is verified
// through the primitives: the Suspend() result code, Get() accessibility
// (a COLD tenant is no longer HOT, so Get() throws), and All() membership (the
// HOT set excludes the COLD shell). Durable round-trip and SHOW-facing
// observability views are covered in separate test suites.
//
// Coverage:
//   SuspendDefaultDbRejected            — kDefaultDB is never suspendable
//   SuspendNonExistentRejected          — absent tenant returns NON_EXISTENT
//   SuspendWithActiveAccessorRejected   — live accessor -> ACTIVE_CONNECTIONS, rolls back to HOT
//   SuspendDurabilityGateRejectsUserButBypassesForRecovery — DISABLED durability: user suspend
//     rejected with DURABILITY_INCOMPLETE; SuspendForRecovery bypasses the gate and forces COLD.
//     Suspend itself takes no snapshot, so the forced shell is only locally recoverable if
//     --storage-snapshot-on-exit is enabled (the exit-snapshot path exercised here)
//   SuspendSuccessMakesColdShellInaccessible — happy path: Get() throws, name leaves All()
//   SuspendMultipleTenants              — two independent suspends both succeed

#ifdef MG_ENTERPRISE

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/ttl.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"

namespace fs = std::filesystem;
using memgraph::dbms::DbmsHandler;

// Per-test-binary storage root.
static fs::path g_storage_root{fs::temp_directory_path() / "MG_test_unit_hot_cold_suspend"};

class HotColdSuspend : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = g_storage_root / ::testing::UnitTest::GetInstance()->current_test_info()->name();
    fs::remove_all(test_dir_);
    fs::create_directories(test_dir_);

    memgraph::storage::Config conf;
    memgraph::storage::UpdatePaths(conf, test_dir_);
    conf.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    handler_ = std::make_unique<DbmsHandler>(conf);
  }

  void TearDown() override {
    handler_.reset();
    fs::remove_all(test_dir_);
  }

  /// Create a secondary (non-default) tenant with full snapshot+WAL durability.
  std::string CreateTenant(std::string name) {
    auto result = handler_->New(name);
    EXPECT_TRUE(result.has_value()) << "Failed to create tenant: " << name;
    return name;
  }

  /// True iff `name` is present in the HOT set returned by All().
  bool InAll(std::string_view name) {
    auto all = handler_->All();
    return std::find(all.begin(), all.end(), name) != all.end();
  }

  fs::path test_dir_;
  std::unique_ptr<DbmsHandler> handler_;
};

// The default database ("memgraph") is never suspendable.
TEST_F(HotColdSuspend, SuspendDefaultDbRejected) {
  auto result = handler_->Suspend(memgraph::dbms::kDefaultDB);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), DbmsHandler::SuspendError::DEFAULT_DB);
}

// Suspending a name that was never created returns NON_EXISTENT.
TEST_F(HotColdSuspend, SuspendNonExistentRejected) {
  auto result = handler_->Suspend("ghost_db");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), DbmsHandler::SuspendError::NON_EXISTENT);
}

// A live DatabaseAccess keeps the gatekeeper count > 1, so try_begin_suspend
// times out and Suspend_ returns ACTIVE_CONNECTIONS. The rollback guard must
// leave the tenant HOT (still accessible via Get()).
TEST_F(HotColdSuspend, SuspendWithActiveAccessorRejected) {
  auto name = CreateTenant("busy_db");

  auto acc = handler_->Get(name);  // bumps the gatekeeper count to 2
  ASSERT_TRUE(acc);

  auto result = handler_->Suspend(name);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), DbmsHandler::SuspendError::ACTIVE_CONNECTIONS);

  // Rolled back to HOT: the tenant is still in the HOT set and still accessible.
  EXPECT_TRUE(InAll(name)) << "Tenant must remain HOT after a failed (rolled-back) suspend";
  auto acc2 = handler_->Get(name);
  EXPECT_TRUE(acc2) << "Gatekeeper must be back in HOT after abort_suspend";
}

// Two complementary halves of the durability-gate rule, exercised in one shared setup:
//
//   (1) A USER suspend of a durability-incomplete tenant (DISABLED snapshot+WAL) is REJECTED with
//       DURABILITY_INCOMPLETE — the gate prevents creating an unrecoverable cold shell.
//
//   (2) SuspendForRecovery() BYPASSES that gate: a replica converging to MAIN's authoritative cold set
//       must not be stuck in a BEHIND retry loop. Suspend itself takes no snapshot, so the forced
//       cold shell is only locally recoverable here because --storage-snapshot-on-exit is enabled:
//       the exit-snapshot path (InMemoryStorage's destructor, run during finish_suspend()'s teardown)
//       is the ONLY durability this shell gets with WAL disabled.
TEST(HotColdSuspendNoDurability, SuspendDurabilityGateRejectsUserButBypassesForRecovery) {
  fs::path dir{g_storage_root / "no_durability_gate_test"};
  fs::remove_all(dir);
  fs::create_directories(dir);

  memgraph::storage::Config conf;
  memgraph::storage::UpdatePaths(conf, dir);
  conf.durability.snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::DISABLED;
  // Suspend no longer takes a proactive snapshot (see Suspend_ in dbms_handler.cpp). With WAL
  // disabled, snapshot_on_exit is the ONLY thing that can make the forced cold shell below locally
  // recoverable — it is set here to keep that half of the test meaningful (real hot/cold deployments
  // that want suspend-time durability without periodic WAL must set this flag).
  conf.durability.snapshot_on_exit = true;

  auto handler = std::make_unique<DbmsHandler>(conf);
  std::string uuid_str;
  {
    // Release the New() accessor before suspending: a held accessor pins the gatekeeper and the
    // suspend freeze would never reach sole-accessor (count == 1).
    auto result = handler->New("no_wal_db");
    ASSERT_TRUE(result.has_value()) << "Failed to create no-durability tenant";
    uuid_str = static_cast<std::string>(result.value()->storage()->uuid());
  }

  // (1) User-initiated suspend is rejected — the gate protects against an unrecoverable cold tenant.
  {
    auto suspend = handler->Suspend("no_wal_db");
    ASSERT_FALSE(suspend.has_value());
    EXPECT_EQ(suspend.error(), DbmsHandler::SuspendError::DURABILITY_INCOMPLETE);
  }

  // (2) Recovery bypasses the gate and succeeds.
  {
    auto recovery = handler->SuspendForRecovery("no_wal_db");
    ASSERT_TRUE(recovery.has_value()) << "SuspendForRecovery must bypass the durability-complete gate";
  }
  EXPECT_TRUE(handler->IsSuspended("no_wal_db")) << "the recovery-forced tenant must be COLD";
  EXPECT_THROW(handler->Get("no_wal_db"), std::exception) << "a COLD tenant's Get() must fail";

  // Suspend takes no proactive snapshot; with WAL disabled the shell's ONLY local durability is the
  // exit snapshot InMemoryStorage's destructor writes during finish_suspend()'s teardown, gated by
  // --storage-snapshot-on-exit (set true above). Lock that in: the tenant's snapshot directory must
  // hold a snapshot after SuspendForRecovery — otherwise a regression that broke the exit-snapshot
  // path would silently make a no-WAL cold shell unrecoverable.
  const auto snap_dir = dir / std::string(memgraph::dbms::kMultiTenantDir) / uuid_str / "snapshots";
  EXPECT_TRUE(fs::exists(snap_dir) && !fs::is_empty(snap_dir))
      << "the exit-snapshot must be written (via --storage-snapshot-on-exit) so the recovery-forced "
         "cold shell is recoverable: "
      << snap_dir;

  handler.reset();
  fs::remove_all(dir);
}

// Happy path: a successful Suspend() leaves a COLD shell that is no longer HOT,
// so the tenant drops out of the HOT set (All()) and Get() throws.
TEST_F(HotColdSuspend, SuspendSuccessMakesColdShellInaccessible) {
  auto name = CreateTenant("freeze_db");
  ASSERT_TRUE(InAll(name));

  auto result = handler_->Suspend(name);
  ASSERT_TRUE(result.has_value()) << "Suspend unexpectedly failed";
  EXPECT_TRUE(handler_->IsSuspended(name)) << "Tenant must be in the COLD/suspended state after a successful suspend";

  // COLD: not in the HOT set, and a Get() no longer yields an accessor.
  EXPECT_FALSE(InAll(name)) << "A COLD tenant must not appear in the HOT set";
  EXPECT_THROW(handler_->Get(name), std::exception) << "Get() on a COLD tenant must fail";
}

// The pre-teardown suspend arm (SetOnSuspend) runs BEFORE the freeze and can release the
// accessors that pin the tenant HOT, so a tenant that would otherwise be ACTIVE_CONNECTIONS becomes
// suspendable. A live stream consumer holds a DatabaseAccess via its captured Interpreter; here a plain
// held accessor stands in for it, and the arm releases it exactly as Streams::Shutdown() does for real.
TEST_F(HotColdSuspend, OnSuspendArmUnpinsTenantThenSuspendSucceeds) {
  auto name = CreateTenant("pinned_db");

  // A pinning accessor (count > 1) — without the arm this makes Suspend() fail ACTIVE_CONNECTIONS.
  auto pin = std::make_shared<std::optional<memgraph::dbms::DatabaseAccess>>(handler_->Get(name));
  ASSERT_TRUE(pin->has_value());

  // The arm releases the pin before the freeze (what stopping the stream consumers achieves for real).
  handler_->SetOnSuspend([pin](memgraph::dbms::DatabaseAccess) { pin->reset(); });

  auto result = handler_->Suspend(name);
  ASSERT_TRUE(result.has_value()) << "suspend must succeed once the arm unpins the tenant";
  EXPECT_FALSE(InAll(name)) << "the tenant must be COLD after the successful suspend";
}

// A suspend that does NOT commit (a foreign accessor the arm cannot release — sole-accessor
// count is never reached — ACTIVE_CONNECTIONS) must run the streams-restore UNDO, so a failed
// SUSPEND never silently leaves the stream consumers stopped. The tenant also stays HOT.
TEST_F(HotColdSuspend, FailedSuspendRunsStreamRestoreUndo) {
  auto name = CreateTenant("undo_db");

  auto foreign = handler_->Get(name);  // a real foreign connection the arm does not (cannot) release
  ASSERT_TRUE(foreign);

  bool stopped = false;
  bool restored = false;
  handler_->SetOnSuspend([&](memgraph::dbms::DatabaseAccess) { stopped = true; });        // "stop streams"
  handler_->SetRestoreStreams([&](memgraph::dbms::DatabaseAccess) { restored = true; });  // undo

  auto result = handler_->Suspend(name);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), DbmsHandler::SuspendError::ACTIVE_CONNECTIONS);
  EXPECT_TRUE(stopped) << "the suspend arm must run (off-lock) before the freeze";
  EXPECT_TRUE(restored) << "a failed suspend must run the streams-restore undo";
  EXPECT_TRUE(InAll(name)) << "the tenant must stay HOT after a failed (rolled-back) suspend";
}

// Resume must invoke the on_resume_ arm (which re-arms triggers and streams from durable
// metadata). This pins the wiring: before the fix the arm was never set, so a resumed tenant
// silently lost its streams and triggers.
TEST_F(HotColdSuspend, ResumeInvokesOnResumeArm) {
  auto name = CreateTenant("resume_arm_db");
  ASSERT_TRUE(handler_->Suspend(name).has_value());

  bool armed = false;
  handler_->SetOnResume([&](memgraph::dbms::DatabaseAccess) { armed = true; });

  {
    auto r = handler_->Resume(name);
    ASSERT_TRUE(r.has_value()) << "resume must succeed";
    EXPECT_TRUE(armed) << "resume must invoke the on_resume_ arm (triggers/streams restore)";
  }  // release the resumed accessor before TearDown resets the handler (~Gatekeeper waits for count==0)
}

// DROP DATABASE on a COLD (suspended) tenant must clean up (suspended_ entry, durable cold
// marker, data dir, cold shell) and succeed — not bail with NON_EXISTENT and leak the tenant (which
// then re-materializes on restart). After the drop the name must be reusable.
TEST_F(HotColdSuspend, DropColdTenantCleansUp) {
  auto name = CreateTenant("drop_cold");
  ASSERT_TRUE(handler_->Suspend(name).has_value());
  ASSERT_TRUE(handler_->IsSuspended(name));

  // Delete(std::string_view) is the single-arg, no-transaction drop entry point.
  auto del = handler_->Delete(name);
  EXPECT_TRUE(del.has_value()) << "DROP of a COLD tenant must succeed";
  EXPECT_FALSE(handler_->IsSuspended(name)) << "the suspended_ entry must be erased";
  // Get() on a non-existent tenant throws UnknownDatabaseException — same pattern as
  // SuspendSuccessMakesColdShellInaccessible above.
  EXPECT_THROW(handler_->Get(name), std::exception) << "the cold shell must be gone";

  // The durable cold marker + data dir are gone, so the name is reusable.
  auto recreate = handler_->New(name);
  EXPECT_TRUE(recreate.has_value()) << "name must be reusable after dropping the cold tenant";
}

// RENAME DATABASE on a COLD tenant must be rejected with RenameError::SUSPENDED, NOT abort the
// process (pre-fix it tripped MG_ASSERT after moving the no-value shell).
TEST_F(HotColdSuspend, RenameColdTenantRejected) {
  auto name = CreateTenant("rename_cold");
  ASSERT_TRUE(handler_->Suspend(name).has_value());

  auto res = handler_->Rename(name, "rename_cold_new", nullptr);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), memgraph::dbms::RenameError::SUSPENDED);
  EXPECT_TRUE(handler_->IsSuspended(name)) << "a rejected rename must leave the tenant COLD under its old name";
}

// Two independent tenants can be suspended; each goes COLD without affecting the
// other or the default DB.
TEST_F(HotColdSuspend, SuspendMultipleTenants) {
  auto a = CreateTenant("cold_a");
  auto b = CreateTenant("cold_b");

  ASSERT_TRUE(handler_->Suspend(a).has_value());
  EXPECT_FALSE(InAll(a));
  EXPECT_TRUE(InAll(b)) << "Suspending one tenant must not affect another";
  EXPECT_TRUE(InAll(memgraph::dbms::kDefaultDB));

  ASSERT_TRUE(handler_->Suspend(b).has_value());
  EXPECT_FALSE(InAll(b));
  EXPECT_THROW(handler_->Get(a), std::exception);
  EXPECT_THROW(handler_->Get(b), std::exception);
}

// Deadlock regression: a concurrent DROP DATABASE while a Resume is parked in its
// on_resume_ arm (state == RESUMING, suspended_ still holds the entry) must be rejected with
// DeleteError::USING and must NOT deadlock. The resumer must complete to HOT after the rejected DROP.
//
// Reproduces the race window exactly:
//   1. Suspend the tenant (COLD, suspended_ populated).
//   2. Install an on_resume_ arm that parks (spin) until the main thread has attempted the DROP.
//   3. Spawn a thread that calls Resume() — it wins COLD->RESUMING, enters on_resume_, parks.
//   4. Main thread calls Delete(name) under exclusive lock_: DeleteCold_ sees state==RESUMING ->
//      returns USING. Without the fix, DeleteCold_ would erase the gatekeeper and ~Gatekeeper
//      would block forever (deadlock) because the resumer can never reach the publish block.
//   5. Release the park flag, join the resumer — it must complete to HOT.
TEST_F(HotColdSuspend, DropDuringResumeRejectedNotDeadlock) {
  auto name = CreateTenant("drop_during_resume");
  ASSERT_TRUE(handler_->Suspend(name).has_value());

  std::atomic<bool> in_arm{false};
  std::atomic<bool> release{false};

  // on_resume_ runs AFTER begin_resume() (state==RESUMING) and BEFORE publish (suspended_ still
  // populated) — exactly the race window. Park here until the main thread has attempted the DROP.
  handler_->SetOnResume([&](memgraph::dbms::DatabaseAccess) {
    in_arm.store(true, std::memory_order_release);
    while (!release.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
  });

  std::thread resumer([&] { (void)handler_->Resume(name); });

  // Wait until the resumer is parked inside the on_resume_ arm (state == RESUMING).
  while (!in_arm.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }

  // Attempt a DROP while the tenant is RESUMING. Must return USING (retriable), NOT deadlock.
  auto del = handler_->Delete(name);
  EXPECT_FALSE(del.has_value()) << "DROP during resume must be rejected (not succeed or deadlock)";
  EXPECT_EQ(del.error(), memgraph::dbms::DeleteError::USING) << "DROP during RESUMING must return USING (retriable)";

  // Release the parked resumer and wait for it to complete.
  release.store(true, std::memory_order_release);
  resumer.join();

  // The resume must have completed: the tenant is now HOT and reachable via All().
  EXPECT_TRUE(InAll(name)) << "the resume must complete to HOT after the rejected DROP";

  // Clean up the resumed accessor before TearDown resets the handler.
  // (Get() returns the HOT accessor; letting it drop here avoids a ~Gatekeeper wait.)
}

// Regression for bug #17: Suspend_ must capture the FULL StorageInfo (via GetInfo()), not just the
// base counters (GetBaseInfo()). GetBaseInfo() only fills vertex/edge/memory/disk counters and leaves
// index/constraint counts, storage_mode, isolation_level, and the durability/compression flags
// value-initialized (all zero/default). storage_mode itself is NOT a usable observable here: Suspend_
// requires IN_MEMORY_TRANSACTIONAL (the only suspendable mode), whose enum value happens to be 0 — the
// same as a value-initialized default — so a storage_mode assertion would pass even with the bug.
// label_indices has no such coincidence: it is populated only inside GetInfo()'s Access(READ) block and
// is always 0 from GetBaseInfo(), so creating a LABEL INDEX before suspending gives an unambiguous
// 0-(bug)-vs-1-(fixed) signal. The captured cold_stats is read back via GetColdShowInfo (the same view
// SHOW STORAGE INFO ON <cold> uses).
TEST_F(HotColdSuspend, SuspendCapturesFullStorageInfoNotBase) {
  auto name = CreateTenant("full_info_db");

  {
    auto db_acc = handler_->Get(name);
    auto label = db_acc->storage()->NameToLabel("SomeLabel");
    auto index_acc = db_acc->ReadOnlyAccess();
    ASSERT_TRUE(index_acc->CreateIndex(label).has_value());
    ASSERT_TRUE(index_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }  // release the accessor before suspending -> sole-accessor freeze can proceed

  ASSERT_TRUE(handler_->Suspend(name).has_value());
  ASSERT_TRUE(handler_->IsSuspended(name));

  auto cold_info = handler_->GetColdShowInfo(name);
  ASSERT_TRUE(cold_info.has_value());
  EXPECT_EQ(cold_info->stats.label_indices, 1u)
      << "Suspend_ must capture the full StorageInfo (GetInfo()), not just the base counters "
         "(GetBaseInfo()), which always leaves label_indices at 0";
}

// Deadlock regression: Suspend_'s finish_suspend() holds the gatekeeper mutex across
// ~Database/~InMemoryStorage, which JOINS the storage's TTL scheduler thread. If that join races a
// live TTL tick calling make_database_protector() -> DatabaseHandler::Get() -> Gatekeeper::access(),
// that call blocks on the SAME mutex the joiner holds -> circular wait -> permanent deadlock. The fix
// stops the tenant's background tasks (StopAllBackgroundTasks()) BEFORE finish_suspend() so the
// in-destructor join is a no-op.
//
// Reproducing it reliably needs the TTL scheduler to actually be MID-BATCH when the winning SUSPEND's
// finish_suspend() joins it (if TTL has drained its backlog and parked in the scheduler CV wait, the
// join returns instantly and no deadlock occurs -- that is exactly why tenant_0/tenant_1 suspended
// fine but tenant_2 hung in the CI incident). So each tenant here is kept under CONTINUOUS TTL load by
// a feeder thread that keeps re-inserting past-dated :TTL nodes, and several tenants are suspended in
// sequence -- each suspend is an independent shot at the mid-batch join. A pre-fix build deadlocks on
// the first shot that lands mid-batch (very likely across N tenants); the watchdog then converts the
// hang into a fast, loud SIGABRT instead of blocking the test binary until the CI timeout. With the
// fix every suspend completes quickly.
TEST_F(HotColdSuspend, SuspendWhileTtlActiveDoesNotDeadlock) {
  constexpr int kTenants = 4;
  constexpr int kSeed = 150000;  // large backlog -> TTL stays inside its delete loop for a long stretch

  auto past_ts_us = []() -> int64_t {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               (std::chrono::system_clock::now() - std::chrono::seconds(1)).time_since_epoch())
        .count();
  };

  // Insert `count` past-dated :TTL vertices through a live accessor (mirrors ttl.cpp's Periodic test).
  auto seed_nodes = [&](auto &acc, int count) {
    auto ttl_label = acc->storage()->NameToLabel("TTL");
    auto ttl_property = acc->storage()->NameToProperty("ttl");
    auto wacc = acc->Access(memgraph::storage::WRITE);
    for (int i = 0; i < count; ++i) {
      auto v = wacc->CreateVertex();
      if (v.AddLabel(ttl_label).has_value()) {
        (void)v.SetProperty(ttl_property, memgraph::storage::PropertyValue(past_ts_us()));
      }
    }
    (void)wacc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  };

  // Count currently-visible vertices (mirrors ttl.cpp's CountVisibleVertices).
  auto count_vertices = [](auto &acc) -> size_t {
    auto racc = acc->Access(memgraph::storage::READ);
    size_t n = 0;
    for (const auto v : racc->Vertices(memgraph::storage::View::NEW)) {
      if (v.IsVisible(memgraph::storage::View::NEW)) ++n;
    }
    return n;
  };

  // Watchdog: turns a hang into a fast, loud SIGABRT rather than blocking the binary until CI timeout.
  std::atomic<bool> done{false};
  std::thread watchdog([&done] {
    constexpr auto kDeadline = std::chrono::seconds(90);
    const auto start = std::chrono::steady_clock::now();
    while (!done.load(std::memory_order_acquire)) {
      if (std::chrono::steady_clock::now() - start > kDeadline) {
        std::cerr << "DEADLOCK: SUSPEND DATABASE did not complete within 90s -- finish_suspend joined a "
                     "background thread while holding the gatekeeper mutex"
                  << std::endl;
        std::abort();
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  });

  int suspended = 0;
  std::vector<std::string> names;

  for (int t = 0; t < kTenants; ++t) {
    auto name = CreateTenant("ttl_deadlock_db_" + std::to_string(t));
    names.push_back(name);

    // ---- setup: enable TTL, create the :TTL(ttl) index, seed a large backlog, start the scheduler ----
    {
      auto acc = handler_->Get(name);
      ASSERT_TRUE(acc);

      auto &ttl = acc->ttl();
      ttl.SetUserCheck([]() -> bool { return true; });  // behave as MAIN
      ttl.Enable();

      auto ttl_label = acc->storage()->NameToLabel("TTL");
      auto ttl_property = acc->storage()->NameToProperty("ttl");
      std::vector<memgraph::storage::PropertyPath> ttl_property_path = {ttl_property};

      // Create the :TTL(ttl) label+property index synchronously (mirrors ttl.cpp's
      // EnsureTTLIndicesReady) so lp_index_ready is true and every TTL run actually deletes.
      {
        auto unique_acc = acc->storage()->UniqueAccess();
        ASSERT_TRUE(unique_acc->CreateIndex(ttl_label, ttl_property_path).has_value());
        ASSERT_TRUE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
      }

      seed_nodes(acc, kSeed);

      ttl.Configure(/*should_run_edge_ttl=*/false);
      ttl.SetInterval(std::chrono::milliseconds(10));
      ttl.Resume();

      // GATE: confirm the TTL scheduler is actually DELETING before we depend on it. Without this the
      // test could pass vacuously (a parked / no-op scheduler cannot reproduce the mid-batch-join
      // deadlock -- a false green would hide a reintroduced bug).
      bool ttl_deleting = false;
      const auto gate_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
      while (std::chrono::steady_clock::now() < gate_deadline) {
        if (count_vertices(acc) < static_cast<size_t>(kSeed)) {
          ttl_deleting = true;
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
      }
      ASSERT_TRUE(ttl_deleting) << "TTL scheduler never deleted a node -- setup is wrong, not a real pass";
    }  // release the setup accessor before suspending (a held accessor pins count>1 -> ACTIVE_CONNECTIONS)

    // ---- dedicated feeder: top this tenant's backlog back up so TTL keeps finding >1 batch of work
    //      and stays INSIDE its delete loop (never parks) while we race SUSPEND against it. Fresh
    //      short-lived accessor per insert + a tiny gap so SUSPEND can still catch the count==1 window;
    //      Get() throws SuspendedDatabaseException once COLD, which we swallow. ----
    std::atomic<bool> stop_feeder{false};
    std::thread feeder([&, name] {
      while (!stop_feeder.load(std::memory_order_acquire)) {
        try {
          auto racc = handler_->Get(name);
          if (racc) {
            auto ttl_label = racc->storage()->NameToLabel("TTL");
            auto ttl_property = racc->storage()->NameToProperty("ttl");
            auto wacc = racc->Access(memgraph::storage::WRITE);
            for (int i = 0; i < 10000; ++i) {
              auto v = wacc->CreateVertex();
              if (v.AddLabel(ttl_label).has_value()) {
                (void)v.SetProperty(ttl_property, memgraph::storage::PropertyValue(past_ts_us()));
              }
            }
            (void)wacc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
          }
        } catch (const std::exception &) {
          // tenant went COLD / mid-suspend -> stop feeding it
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    });

    // ---- suspend this tenant (retry until it wins the count==1 race). The winning attempt's
    //      finish_suspend() joins the still-running TTL thread -- the deadlock trigger pre-fix. ----
    for (int attempt = 0; attempt < 5000; ++attempt) {
      if (handler_->Suspend(name).has_value()) {
        ++suspended;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }

    stop_feeder.store(true, std::memory_order_release);
    feeder.join();
  }

  done.store(true, std::memory_order_release);
  watchdog.join();

  EXPECT_EQ(suspended, kTenants) << "every tenant must suspend without deadlocking";
  for (const auto &name : names) {
    EXPECT_TRUE(handler_->IsSuspended(name)) << "tenant not COLD after suspend: " << name;
  }
}

#else

// Community build: the entire suspend feature is enterprise-only.
#include <gtest/gtest.h>

TEST(HotColdSuspend, NotApplicableInCommunity) { GTEST_SKIP() << "hot/cold suspend is an enterprise-only feature"; }

#endif  // MG_ENTERPRISE
