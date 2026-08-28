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

#include <atomic>
#include <chrono>
#include <optional>
#include <thread>

#include <gtest/gtest.h>

#include "dbms/handler.hpp"
#include "utils/gatekeeper.hpp"

using namespace memgraph::utils;

// Trivial stub type — no Database/DBMS dependencies.
struct Widget {
  int v;
};

using GK = Gatekeeper<Widget>;
using State = GatekeeperState;

// Helper: construct a HOT gatekeeper holding Widget{42}.
static GK make_hot() { return GK{42}; }

// ---------------------------------------------------------------------------
// AccessOnlyInHot
// ---------------------------------------------------------------------------
// access() works in HOT; after try_begin_suspend() (SUSPENDING) returns
// nullopt; after finish_suspend() (COLD) still nullopt; state() is correct
// at each step.
TEST(HotColdGatekeeper, AccessOnlyInHot) {
  auto gk = make_hot();

  // Starts in HOT and is accessible.
  EXPECT_EQ(gk.state(), State::HOT);
  {
    auto acc = gk.access();
    ASSERT_TRUE(acc.has_value());
    EXPECT_EQ((*acc)->v, 42);

    // With acc alive (count==1), transition HOT->SUSPENDING.
    EXPECT_TRUE(gk.try_begin_suspend(std::chrono::milliseconds(200)));
    EXPECT_EQ(gk.state(), State::SUSPENDING);

    // While SUSPENDING, access() must return nullopt.
    EXPECT_FALSE(gk.access().has_value());
  }
  // acc released here; count drops to 0.

  // finish_suspend destroys value_ and sets COLD.
  gk.finish_suspend();
  EXPECT_EQ(gk.state(), State::COLD);

  // While COLD, access() must return nullopt.
  EXPECT_FALSE(gk.access().has_value());
}

// ---------------------------------------------------------------------------
// TryBeginSuspendRequiresSoleAccessor
// ---------------------------------------------------------------------------
// With 2 live Accessors (count==2), try_begin_suspend with a short timeout
// returns false and leaves state HOT.  After the 2nd Accessor is released
// (count==1), the same call returns true.
TEST(HotColdGatekeeper, TryBeginSuspendRequiresSoleAccessor) {
  auto gk = make_hot();

  // Primary accessor — remains alive for the duration of the test.
  auto primary = gk.access();
  ASSERT_TRUE(primary.has_value());

  {
    // Second accessor — count becomes 2 while this scope is active.
    auto second = gk.access();
    ASSERT_TRUE(second.has_value());

    // count==2; must time out quickly and leave state HOT.
    EXPECT_FALSE(gk.try_begin_suspend(std::chrono::milliseconds(20)));
    EXPECT_EQ(gk.state(), State::HOT);
  }
  // second released; count drops to 1.

  // Now count==1 (only primary); should succeed.
  EXPECT_TRUE(gk.try_begin_suspend(std::chrono::milliseconds(200)));
  EXPECT_EQ(gk.state(), State::SUSPENDING);

  // Release primary before finish_suspend so dtor is clean.
  primary->reset();
  gk.finish_suspend();
}

// ---------------------------------------------------------------------------
// AbortSuspendRestoresHot
// ---------------------------------------------------------------------------
// SUSPENDING -> abort_suspend() -> HOT; access() works again.
TEST(HotColdGatekeeper, AbortSuspendRestoresHot) {
  auto gk = make_hot();

  {
    auto acc = gk.access();
    ASSERT_TRUE(acc.has_value());
    EXPECT_TRUE(gk.try_begin_suspend(std::chrono::milliseconds(200)));
    EXPECT_EQ(gk.state(), State::SUSPENDING);

    // Abort — reverses the freeze.
    gk.abort_suspend();
    EXPECT_EQ(gk.state(), State::HOT);
    // acc is still alive here; count remains 1.
  }
  // acc released.

  // Back in HOT — access() must work again.
  auto acc2 = gk.access();
  ASSERT_TRUE(acc2.has_value());
  EXPECT_EQ((*acc2)->v, 42);
}

// ---------------------------------------------------------------------------
// BeginResumeSingleFlight
// ---------------------------------------------------------------------------
// From COLD: first begin_resume() → true (RESUMING); second → false;
// abort_resume() → COLD; begin_resume() → true again.
TEST(HotColdGatekeeper, BeginResumeSingleFlight) {
  auto gk = make_hot();

  // Drive to COLD.
  {
    auto acc = gk.access();
    ASSERT_TRUE(acc.has_value());
    EXPECT_TRUE(gk.try_begin_suspend(std::chrono::milliseconds(200)));
    acc->reset();
  }
  gk.finish_suspend();
  ASSERT_EQ(gk.state(), State::COLD);

  // First begin_resume wins the single-flight token.
  EXPECT_TRUE(gk.begin_resume());
  EXPECT_EQ(gk.state(), State::RESUMING);

  // A concurrent second begin_resume must see RESUMING and return false.
  EXPECT_FALSE(gk.begin_resume());
  EXPECT_EQ(gk.state(), State::RESUMING);

  // Abort — back to COLD, retriable.
  gk.abort_resume();
  EXPECT_EQ(gk.state(), State::COLD);

  // The token is available again.
  EXPECT_TRUE(gk.begin_resume());
  EXPECT_EQ(gk.state(), State::RESUMING);

  // Clean up by aborting again so dtor sees terminal state.
  gk.abort_resume();
}

// ---------------------------------------------------------------------------
// DtorOnColdReturnsPromptly
// ---------------------------------------------------------------------------
// A COLD gatekeeper (after finish_suspend) must destruct without hanging.
TEST(HotColdGatekeeper, DtorOnColdReturnsPromptly) {
  // Wrap in a separate scope to time the destruction.
  auto start = std::chrono::steady_clock::now();
  {
    auto gk = make_hot();
    {
      auto acc = gk.access();
      ASSERT_TRUE(acc.has_value());
      EXPECT_TRUE(gk.try_begin_suspend(std::chrono::milliseconds(200)));
      acc->reset();
    }
    gk.finish_suspend();
    // Pin COLD (terminal) before timing the dtor: a bug making COLD non-terminal
    // would otherwise let this test pass for the wrong reason.
    ASSERT_EQ(gk.state(), State::COLD);
    // gk destructs here.
  }
  auto elapsed = std::chrono::steady_clock::now() - start;
  // Should destruct in well under 1 s (not hang).
  EXPECT_LT(elapsed, std::chrono::seconds(1));
}

// ---------------------------------------------------------------------------
// DtorWaitsForAccessorRelease
// ---------------------------------------------------------------------------
// HOT gatekeeper with a live Accessor on another thread; the dtor must block
// until the Accessor is reset.  Verifies the existing count==0 drain still
// works after the state machine addition.
TEST(HotColdGatekeeper, DtorWaitsForAccessorRelease) {
  auto gk = std::make_unique<GK>(99);

  // Grab an Accessor and hold it for a short while in another thread.
  auto acc = gk->access();
  ASSERT_TRUE(acc.has_value());

  constexpr auto kHold = std::chrono::milliseconds(50);
  std::atomic<bool> accessor_released{false};

  // Capture start BEFORE spawning the worker so its hold window begins strictly
  // after `start`, keeping the elapsed lower bound below valid.
  const auto start = std::chrono::steady_clock::now();
  std::thread t([&] {
    // Hold the accessor briefly so the dtor below has to block on it.
    std::this_thread::sleep_for(kHold);
    // Publish the flag BEFORE releasing: acc->reset() is what drops count_ to 0
    // and unblocks the dtor, and its mutex release synchronizes-with the dtor's
    // count_==0 read — so a dtor that truly waits always observes this store.
    // Storing AFTER reset() (the previous code) left the store and the dtor's
    // return unordered, so the dtor could wake and return first: the flake.
    accessor_released.store(true, std::memory_order_release);
    acc->reset();
  });

  // This reset + dtor must block until t releases the accessor.
  gk.reset();  // triggers ~Gatekeeper() which waits for count==0.
  const auto elapsed = std::chrono::steady_clock::now() - start;

  // The dtor must have waited for the accessor release rather than returning early.
  EXPECT_TRUE(accessor_released.load(std::memory_order_acquire));
  EXPECT_GE(elapsed, kHold);
  t.join();
}

// ---------------------------------------------------------------------------
// DeferDeleteErasesButDefersDestruction
// ---------------------------------------------------------------------------
// DeferDeleteProbe: dtor sleeps then flips an atomic, so destruction is observable and slow enough
// that inline (non-deferred) destruction would visibly block the DeferDelete() call itself.
namespace {
struct DeferDeleteProbe {
  DeferDeleteProbe(std::atomic<bool> *destroyed, std::chrono::milliseconds dtor_delay)
      : destroyed_{destroyed}, dtor_delay_{dtor_delay} {}

  DeferDeleteProbe(DeferDeleteProbe const &) = delete;
  DeferDeleteProbe(DeferDeleteProbe &&) = delete;
  DeferDeleteProbe &operator=(DeferDeleteProbe const &) = delete;
  DeferDeleteProbe &operator=(DeferDeleteProbe &&) = delete;

  ~DeferDeleteProbe() {
    std::this_thread::sleep_for(dtor_delay_);
    destroyed_->store(true, std::memory_order_release);
  }

 private:
  std::atomic<bool> *destroyed_;
  std::chrono::milliseconds dtor_delay_;
};
}  // namespace

TEST(HotColdGatekeeper, DeferDeleteErasesButDefersDestruction) {
  // Declared ahead of `handler` on purpose: the probe's dtor writes through a raw pointer to these
  // flags from a worker thread, so `~Handler`'s joins (reverse-declaration-order unwind) must finish first.
  std::atomic<bool> destroyed{false};
  std::atomic<bool> callback_fired{false};
  memgraph::dbms::Handler<DeferDeleteProbe> handler;

  constexpr auto kDtorDelay = std::chrono::milliseconds(200);

  auto new_result = handler.New(std::piecewise_construct, "probe", &destroyed, kDtorDelay);
  ASSERT_TRUE(new_result.has_value());
  auto held_acc = std::move(*new_result);
  ASSERT_TRUE(held_acc);

  // held_acc + DeferDelete's own minted accessor push count_ to 2, so try_delete()'s count_==1 check
  // times out (~100ms) and takes the deferred path; the 500ms bound catches a regression to inline delete.
  const auto call_start = std::chrono::steady_clock::now();
  handler.DeferDelete("probe", [&callback_fired] { callback_fired.store(true, std::memory_order_release); });
  const auto call_elapsed = std::chrono::steady_clock::now() - call_start;
  EXPECT_LT(call_elapsed, std::chrono::milliseconds(500));

  // Erased from the map immediately even though held_acc keeps the object alive.
  EXPECT_FALSE(handler.Has("probe"));

  // Poll a bounded window (not an instant check) that destruction hasn't happened while held_acc is alive.
  constexpr auto kPollUntil = std::chrono::milliseconds(250);
  const auto poll_start = std::chrono::steady_clock::now();
  while (std::chrono::steady_clock::now() - poll_start < kPollUntil) {
    ASSERT_FALSE(destroyed.load(std::memory_order_acquire));
    ASSERT_FALSE(callback_fired.load(std::memory_order_acquire));
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // Dropping held_acc's count to 0 is what lets the worker thread's already-blocked ~Gatekeeper()
  // proceed to destroy the probe.
  held_acc.reset();

  const auto wait_start = std::chrono::steady_clock::now();
  while (!destroyed.load(std::memory_order_acquire) || !callback_fired.load(std::memory_order_acquire)) {
    ASSERT_LT(std::chrono::steady_clock::now() - wait_start, std::chrono::seconds(5));
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(destroyed.load(std::memory_order_acquire));
  EXPECT_TRUE(callback_fired.load(std::memory_order_acquire));
}

// ---------------------------------------------------------------------------
// DeferDeleteDoesNotHeadOfLineBlockAnotherEntry
// ---------------------------------------------------------------------------
// Regression guard for the old shared-worker starvation bug: one pool worker serialized all deferred
// destructions, so an entry nobody released occupied it forever and every later entry's destruction
// queued behind it and never ran. "stuck" stays pinned; "freed" must be destroyed anyway.
TEST(HotColdGatekeeper, DeferDeleteDoesNotHeadOfLineBlockAnotherEntry) {
  // Declared ahead of `handler`: these flags are written through raw pointers/references from a worker
  // thread (the probe's dtor or the post-delete callback), so `~Handler`'s joins must finish first.
  std::atomic<bool> stuck_destroyed{false};
  std::atomic<bool> freed_destroyed{false};
  std::atomic<bool> freed_callback_fired{false};
  memgraph::dbms::Handler<DeferDeleteProbe> handler;

  auto stuck = handler.New(std::piecewise_construct, "stuck", &stuck_destroyed, std::chrono::milliseconds(0));
  ASSERT_TRUE(stuck.has_value());
  auto stuck_acc = std::move(*stuck);
  ASSERT_TRUE(stuck_acc);

  auto freed = handler.New(std::piecewise_construct, "freed", &freed_destroyed, std::chrono::milliseconds(0));
  ASSERT_TRUE(freed.has_value());
  auto freed_acc = std::move(*freed);
  ASSERT_TRUE(freed_acc);

  // Both drops take the deferred path: each entry's own accessor is still held, so DeferDelete's
  // minted accessor pushes count_ to 2 and try_delete()'s count_==1 check fails for both.
  handler.DeferDelete("stuck", [] {});
  handler.DeferDelete("freed",
                      [&freed_callback_fired] { freed_callback_fired.store(true, std::memory_order_release); });

  // Release only "freed" -- "stuck" stays pinned, so if the two shared a worker "freed" would never
  // complete either.
  freed_acc.reset();

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!freed_destroyed.load(std::memory_order_acquire) || !freed_callback_fired.load(std::memory_order_acquire)) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline)
        << "\"freed\" never got destroyed while \"stuck\" was pinned: head-of-line blocking between "
           "unrelated entries";
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // "stuck" must still be alive. Without this the test would also pass if something had released
  // "stuck" -- i.e. for the wrong reason, with the two entries never actually decoupled.
  EXPECT_FALSE(stuck_destroyed.load(std::memory_order_acquire));

  // Release "stuck" and wait for it: not load-bearing for raw-pointer safety (see the flags' declaration
  // comment above) but is the only check that "stuck" actually gets destroyed once released.
  stuck_acc.reset();
  const auto drain_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!stuck_destroyed.load(std::memory_order_acquire)) {
    ASSERT_LT(std::chrono::steady_clock::now(), drain_deadline) << "\"stuck\" was not destroyed after being released";
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}
