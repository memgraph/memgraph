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

#include <chrono>
#include <optional>
#include <thread>

#include <gtest/gtest.h>

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
// FinishSuspendPublishesCold
// ---------------------------------------------------------------------------
// SUSPENDING -> finish_suspend() -> COLD; value gone (access nullopt),
// count==0.
TEST(HotColdGatekeeper, FinishSuspendPublishesCold) {
  auto gk = make_hot();

  {
    auto acc = gk.access();
    ASSERT_TRUE(acc.has_value());
    EXPECT_TRUE(gk.try_begin_suspend(std::chrono::milliseconds(200)));
    EXPECT_EQ(gk.state(), State::SUSPENDING);
    // Release acc; count -> 0.
    acc->reset();
  }

  gk.finish_suspend();
  EXPECT_EQ(gk.state(), State::COLD);

  // Value must be gone and no accessors live.
  EXPECT_FALSE(gk.access().has_value());
  // use_count() returns nullopt because value_ is disengaged.
  EXPECT_FALSE(gk.use_count().has_value());
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

  std::atomic<bool> accessor_released{false};
  std::thread t([&] {
    // Give the main thread a chance to start the dtor.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    acc->reset();
    accessor_released.store(true, std::memory_order_release);
  });

  // This reset + dtor must block until t releases the accessor.
  gk.reset();  // triggers ~Gatekeeper() which waits for count==0.

  EXPECT_TRUE(accessor_released.load(std::memory_order_acquire));
  t.join();
}
