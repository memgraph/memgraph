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

// Regression type for the try_delete() destroy-outside-the-lock invariant:
// its destructor re-enters the owning gatekeeper via access(), mirroring
// ~Database -> StopAllBackgroundTasks -> ... -> Gatekeeper::access().
struct Reentrant {
  Gatekeeper<Reentrant> *gk = nullptr;
  bool *dtor_saw_nullopt = nullptr;

  ~Reentrant() {
    if (gk == nullptr) return;
    // If ~Reentrant ran while try_delete() still held mutex_, this access()
    // would self-deadlock on the non-recursive mutex. On the fixed code the
    // lock is released, so access() returns nullopt (value_ already moved out).
    auto acc = gk->access();
    if (dtor_saw_nullopt != nullptr) *dtor_saw_nullopt = !acc.has_value();
  }
};

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
// TryDeleteDestroysValueWithMutexReleased
// ---------------------------------------------------------------------------
// try_delete() must destroy ~T with mutex_ released; a ~T that re-enters via
// access() proves it (self-deadlocks under the old destroy-under-lock code).
TEST(HotColdGatekeeper, TryDeleteDestroysValueWithMutexReleased) {
  bool dtor_saw_nullopt = false;
  Gatekeeper<Reentrant> gk{};
  {
    auto acc = gk.access();
    ASSERT_TRUE(acc.has_value());
    (*acc)->gk = &gk;
    (*acc)->dtor_saw_nullopt = &dtor_saw_nullopt;
  }
  auto acc = gk.access();
  ASSERT_TRUE(acc.has_value());
  EXPECT_TRUE(acc->try_delete());         // completes (no deadlock) on fixed code
  EXPECT_TRUE(dtor_saw_nullopt);          // access() during ~Reentrant saw value_ gone
  EXPECT_FALSE(gk.access().has_value());  // value destroyed
}

// ---------------------------------------------------------------------------
// DeferDeleteErasesButDefersDestruction
// ---------------------------------------------------------------------------
// memgraph::dbms::Handler<T>::DeferDelete (src/dbms/handler.hpp) must, when another
// Accessor is still live on the entry:
//   1) return promptly to the caller — the actual ~T() must run on the 1-thread
//      defer_pool_, NOT inline on the caller's thread;
//   2) erase `name` from the map synchronously, before the object dies;
//   3) NOT yet have destroyed the object (nor fired post_delete_func) while the
//      other Accessor is still held;
//   4) complete the destruction and fire post_delete_func once that Accessor is
//      released.
//
// Probe managed type: a destructor that, once actually invoked, sleeps for a
// configurable delay and then flips an atomic flag — so "destroyed" is directly
// observable, and slow enough that an implementation which destroyed the probe
// on the caller's own thread (instead of deferring it) would make the
// DeferDelete() call itself visibly block for at least that long.
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
  memgraph::dbms::Handler<DeferDeleteProbe> handler;

  std::atomic<bool> destroyed{false};
  std::atomic<bool> callback_fired{false};
  constexpr auto kDtorDelay = std::chrono::milliseconds(200);

  // New() constructs the probe in place and hands back the sole live Accessor,
  // which this test holds for the whole "not yet destroyed" window below.
  auto new_result = handler.New(std::piecewise_construct, "probe", &destroyed, kDtorDelay);
  ASSERT_TRUE(new_result.has_value());
  auto held_acc = std::move(*new_result);
  ASSERT_TRUE(held_acc);

  // Property 1: DeferDelete must return promptly even though `held_acc` keeps the
  // entry's accessor count at 2 while DeferDelete mints its own db_acc (count 1->2),
  // which makes the internal db_acc->try_delete() fail its count_==1 check (it waits
  // up to its own 100ms default timeout, then returns false) and take the deferred
  // path instead of destroying inline. Bound is well above that ~100ms internal wait
  // but far below kDtorDelay/the later release point: an implementation that instead
  // destroyed the probe synchronously on the caller's thread would have to block here
  // until held_acc is released — which this test does only much later, well past this
  // bound — so a regression to inline destruction fails this assertion (or hangs).
  const auto call_start = std::chrono::steady_clock::now();
  handler.DeferDelete("probe", [&callback_fired] { callback_fired.store(true, std::memory_order_release); });
  const auto call_elapsed = std::chrono::steady_clock::now() - call_start;
  EXPECT_LT(call_elapsed, std::chrono::milliseconds(500));

  // Property 2: the name leaves the map immediately, even though the probe object
  // itself is still alive (kept alive by held_acc).
  EXPECT_FALSE(handler.Has("probe"));

  // Property 3: with held_acc still alive, the probe must NOT have been destroyed
  // yet, and the post-delete callback must not have fired. Poll for a bounded
  // interval so this is a meaningful (not instantaneous) check.
  constexpr auto kPollUntil = std::chrono::milliseconds(250);
  const auto poll_start = std::chrono::steady_clock::now();
  while (std::chrono::steady_clock::now() - poll_start < kPollUntil) {
    ASSERT_FALSE(destroyed.load(std::memory_order_acquire));
    ASSERT_FALSE(callback_fired.load(std::memory_order_acquire));
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // Release the only remaining Accessor: this drops the deferred Gatekeeper's
  // accessor count to 0, which is what lets its ~Gatekeeper() — already blocked
  // on defer_pool_'s single worker thread — proceed to destroy the probe.
  held_acc.reset();

  // Property 4: destruction and the post-delete callback both complete within a
  // bounded (never unbounded) wait.
  const auto wait_start = std::chrono::steady_clock::now();
  while (!destroyed.load(std::memory_order_acquire) || !callback_fired.load(std::memory_order_acquire)) {
    ASSERT_LT(std::chrono::steady_clock::now() - wait_start, std::chrono::seconds(5));
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(destroyed.load(std::memory_order_acquire));
  EXPECT_TRUE(callback_fired.load(std::memory_order_acquire));
}
