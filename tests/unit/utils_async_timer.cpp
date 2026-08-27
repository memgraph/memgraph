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

#include <cmath>
#include <limits>
#include <thread>

#include "gtest/gtest.h"

#include "utils/async_timer.hpp"

using AsyncTimer = memgraph::utils::AsyncTimer;

namespace {

// A timer fires by delivering a signal to one process-wide thread, so how long after its deadline
// it is observed depends on when that thread is next scheduled. Nothing bounds that, which is why
// every upper bound here is a liveness bound - the timer eventually fires - rather than a latency
// one. Lower bounds are different: only a broken timer fires early, so those stay tight.
inline constexpr auto kExpirationSlack = std::chrono::seconds(1);

// Test timing parameters - extracted magic numbers
inline constexpr auto kDefaultTimerDuration = std::chrono::milliseconds(50);
inline constexpr auto kShortTimer = std::chrono::milliseconds(30);
inline constexpr auto kMediumTimer = std::chrono::milliseconds(60);
inline constexpr auto kVeryLongTimer = std::chrono::milliseconds(100);

// RelativeTimingOrder asserts that a later timer has *not* fired yet, so it cannot use the slack
// above: waiting a second for the first timer would let the others fire too. Separating its timers
// by more than the slack is what makes the ordering hold when the delivering thread stalls.
inline constexpr auto kOrderedShortTimer = std::chrono::milliseconds(100);
inline constexpr auto kOrderedMediumTimer = std::chrono::milliseconds(400);
inline constexpr auto kOrderedLongTimer = std::chrono::milliseconds(900);
inline constexpr auto kOrderedShortTimeout = std::chrono::milliseconds(300);
inline constexpr auto kOrderedMediumTimeout = std::chrono::milliseconds(700);
inline constexpr auto kOrderedLongTimeout = std::chrono::milliseconds(1500);

// Window parameters for verification
inline constexpr auto kDefaultNotExpiredWindow = std::chrono::milliseconds(40);
inline constexpr auto kDefaultExpiredWindow = kDefaultTimerDuration + kExpirationSlack;

// Timing tolerances
inline constexpr auto kShortTimeout = kShortTimer + kExpirationSlack;
inline constexpr auto kQuickExpirationWindow = std::chrono::milliseconds(30);

// Loop and polling parameters
inline constexpr auto kPollInterval = std::chrono::milliseconds(1);

// Edge case values
inline constexpr double kMinimalTimerDuration = 0.000001;  // 1 microsecond

// Helper to get current time
std::chrono::steady_clock::time_point Now() { return std::chrono::steady_clock::now(); }

// Convert milliseconds to seconds for timer constructor
double ToSeconds(std::chrono::milliseconds ms) { return std::chrono::duration<double>(ms).count(); }

// Helper to wait for timer expiration until a fixed point in time. A test that waits for several
// timers in turn has to bound each wait against one origin: bounding each against the moment the
// previous wait returned lets the bounds accumulate, so a late wait can outlast a timer that a
// later assertion says has not fired yet.
bool WaitForExpirationUntil(AsyncTimer &timer, std::chrono::steady_clock::time_point deadline) {
  // The timer is checked before the deadline is, so a caller that arrives late still reports a
  // timer that has already fired rather than one it never looked at.
  while (true) {
    if (timer.IsExpired()) return true;
    if (Now() >= deadline) return false;
    std::this_thread::sleep_for(kPollInterval);
  }
}

// Helper to wait for timer expiration with timeout
bool WaitForExpiration(AsyncTimer &timer, std::chrono::milliseconds timeout) {
  return WaitForExpirationUntil(timer, Now() + timeout);
}

// Helper to verify timer is NOT expired until a fixed point in time
bool VerifyNotExpiredUntil(AsyncTimer &timer, std::chrono::steady_clock::time_point deadline) {
  while (Now() < deadline) {
    if (timer.IsExpired()) {
      ADD_FAILURE() << "Timer expired too early";
      return false;
    }
    std::this_thread::sleep_for(kPollInterval);
  }
  return true;
}

// Main helper to check timer state within time windows
// First verifies timer is NOT expired during the "not expired" window
// Then verifies timer DOES expire within the "expired" window
//
// Both windows run from `start`, which the caller takes before arming the timer. Taking it here
// instead would put an unmeasured gap between arming and the first observation: the timer's
// deadline would have moved on while the window had not, and a timer that fired correctly could
// land inside the window that says it must not have fired yet.
bool VerifyTimerExpiration(std::chrono::steady_clock::time_point start, AsyncTimer &timer,
                           std::chrono::milliseconds not_expired_window = kDefaultNotExpiredWindow,
                           std::chrono::milliseconds expired_window = kDefaultExpiredWindow) {
  if (!VerifyNotExpiredUntil(timer, start + not_expired_window)) {
    return false;
  }

  if (WaitForExpirationUntil(timer, start + expired_window)) {
    return true;
  }

  ADD_FAILURE() << "Timer did not expire within " << expired_window.count() << "ms";
  return false;
}

}  // namespace

TEST(AsyncTimer, BasicExpiration) {
  const auto start = Now();
  AsyncTimer timer{ToSeconds(kDefaultTimerDuration)};
  EXPECT_TRUE(VerifyTimerExpiration(start, timer));
}

TEST(AsyncTimer, SequentialTimers) {
  // Test two timers sequentially
  {
    const auto start = Now();
    AsyncTimer timer1{ToSeconds(kDefaultTimerDuration)};
    EXPECT_TRUE(VerifyTimerExpiration(start, timer1));
  }
  {
    const auto start = Now();
    AsyncTimer timer2{ToSeconds(kDefaultTimerDuration)};
    EXPECT_TRUE(VerifyTimerExpiration(start, timer2));
  }
}

TEST(AsyncTimer, RelativeTimingOrder) {
  // Every wait below is bounded against this one origin, taken before the timers are armed, so that
  // each wait is guaranteed to end before the next timer is due however late the previous one ran.
  const auto start = Now();

  // Create timers with different durations to test ordering
  AsyncTimer timer_short{ToSeconds(kOrderedShortTimer)};
  AsyncTimer timer_medium{ToSeconds(kOrderedMediumTimer)};
  AsyncTimer timer_long{ToSeconds(kOrderedLongTimer)};

  // Wait for short timer (should expire first)
  EXPECT_TRUE(WaitForExpirationUntil(timer_short, start + kOrderedShortTimeout));
  EXPECT_TRUE(timer_short.IsExpired());
  EXPECT_FALSE(timer_medium.IsExpired());
  EXPECT_FALSE(timer_long.IsExpired());

  // Wait for medium timer (should expire second)
  EXPECT_TRUE(WaitForExpirationUntil(timer_medium, start + kOrderedMediumTimeout));
  EXPECT_TRUE(timer_short.IsExpired());
  EXPECT_TRUE(timer_medium.IsExpired());
  EXPECT_FALSE(timer_long.IsExpired());

  // Wait for long timer (should expire last)
  EXPECT_TRUE(WaitForExpirationUntil(timer_long, start + kOrderedLongTimeout));
  EXPECT_TRUE(timer_short.IsExpired());
  EXPECT_TRUE(timer_medium.IsExpired());
  EXPECT_TRUE(timer_long.IsExpired());
}

TEST(AsyncTimer, MoveConstructor) {
  const auto start = Now();
  AsyncTimer timer_1{ToSeconds(kDefaultTimerDuration)};

  // Move construct timer_2 from timer_1
  AsyncTimer timer_2{std::move(timer_1)};

  // After move, timer_1 is in moved-from state
  // The implementation returns false for moved-from timers (null expiration_flag)
  EXPECT_FALSE(timer_1.IsExpired());

  // timer_2 should work normally
  EXPECT_TRUE(VerifyTimerExpiration(start, timer_2));

  // timer_1 should still return false (it's moved-from)
  EXPECT_FALSE(timer_1.IsExpired());
}

TEST(AsyncTimer, MoveAssignment) {
  const auto start = Now();
  AsyncTimer timer_1{ToSeconds(kMediumTimer)};  // Medium timer
  AsyncTimer timer_2{ToSeconds(kShortTimer)};   // Short timer

  // Initially both should not be expired
  EXPECT_FALSE(timer_1.IsExpired());
  EXPECT_FALSE(timer_2.IsExpired());

  // Move assign timer_1 to timer_2 (timer_2's original short timer is cancelled)
  timer_2 = std::move(timer_1);

  // After move, timer_1 is moved-from, timer_2 has the medium timer
  EXPECT_FALSE(timer_1.IsExpired());
  EXPECT_FALSE(timer_2.IsExpired());

  // Verify timer_2 now has the medium timer behavior
  // Should NOT expire quickly (original would have expired by now)
  // Should expire within reasonable window for medium timer
  auto not_expired_window = kShortTimer + std::chrono::milliseconds(10);  // Longer than original short timer
  auto expired_window = kMediumTimer + kExpirationSlack;

  EXPECT_TRUE(VerifyTimerExpiration(start, timer_2, not_expired_window, expired_window));

  // timer_1 remains false (moved-from)
  EXPECT_FALSE(timer_1.IsExpired());
}

TEST(AsyncTimer, AssignmentToExpiredTimer) {
  // The point of the test is assigning an unexpired timer onto an expired one, so timer_1 must
  // still be running when the assignment happens. That is a claim about a timer *not* having fired,
  // so as in RelativeTimingOrder it is bounded against a single origin and the two timers are
  // separated by much more than the wait for the first one can overrun.
  const auto start = Now();
  AsyncTimer timer_1{ToSeconds(kOrderedLongTimer)};
  AsyncTimer timer_2{ToSeconds(kShortTimer)};

  // First verify timer_2 expires quickly
  EXPECT_TRUE(WaitForExpirationUntil(timer_2, start + kOrderedShortTimeout));
  EXPECT_TRUE(timer_2.IsExpired());
  EXPECT_FALSE(timer_1.IsExpired());

  // Now assign unexpired timer_1 to expired timer_2
  timer_2 = std::move(timer_1);

  // timer_2 should now be unexpired (has timer_1's timer)
  EXPECT_FALSE(timer_2.IsExpired());
  EXPECT_FALSE(timer_1.IsExpired());  // moved-from

  // Calculate remaining time windows for the moved timer
  auto remaining_not_expired = kShortTimer + std::chrono::milliseconds(10);  // Should not expire for at least this long
  auto remaining_should_expire = kOrderedLongTimer + kExpirationSlack;

  // Verify timer_2 with the moved timer behavior
  EXPECT_TRUE(VerifyTimerExpiration(start, timer_2, remaining_not_expired, remaining_should_expire));

  // timer_1 should still be false (moved-from)
  EXPECT_FALSE(timer_1.IsExpired());
}

TEST(AsyncTimer, DestructionWhileRunning) {
  // Create and immediately destroy a timer
  {
    AsyncTimer timer_to_destroy{ToSeconds(kDefaultTimerDuration)};
  }

  // Create another timer to ensure the system still works
  const auto start = Now();
  AsyncTimer timer_to_wait{ToSeconds(kMediumTimer + std::chrono::milliseconds(10))};

  // Verify the second timer works correctly
  auto not_expired_window = kMediumTimer - std::chrono::milliseconds(10);
  auto expired_window = kMediumTimer + kExpirationSlack;

  EXPECT_TRUE(VerifyTimerExpiration(start, timer_to_wait, not_expired_window, expired_window));
}

TEST(AsyncTimer, ExtremeValues) {
  // Test very small but non-zero timeout - should expire quickly
  {
    AsyncTimer timer_with_minimal{kMinimalTimerDuration};

    EXPECT_TRUE(WaitForExpiration(timer_with_minimal, kShortTimeout)) << "Minimal-duration timer should expire quickly";

    // Verify it expired relatively quickly
    auto start = Now();
    while (!timer_with_minimal.IsExpired() && (Now() - start) < kShortTimeout) {
      std::this_thread::sleep_for(kPollInterval);
    }
    auto elapsed = Now() - start;
    EXPECT_LT(elapsed, kQuickExpirationWindow)
        << "Minimal-duration timer took too long: "
        << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() << "ms";
  }

  // Test maximum value - should not expire in any reasonable test time
  {
    const double expected_maximum_value = std::nexttoward(std::numeric_limits<time_t>::max(), 0.0);
    AsyncTimer timer_with_max_value{expected_maximum_value};

    // Wait a reasonable amount and verify it hasn't expired
    std::this_thread::sleep_for(kVeryLongTimer);
    EXPECT_FALSE(timer_with_max_value.IsExpired()) << "Max-duration timer should not expire in test timeframe";
  }
}
