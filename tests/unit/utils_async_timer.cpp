// Copyright 2025 Memgraph Ltd.
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

// Test timing parameters - extracted magic numbers
inline constexpr auto kDefaultTimerDuration = std::chrono::milliseconds(50);
inline constexpr auto kShortTimer = std::chrono::milliseconds(30);
inline constexpr auto kMediumTimer = std::chrono::milliseconds(60);
inline constexpr auto kLongTimer = std::chrono::milliseconds(90);
inline constexpr auto kVeryLongTimer = std::chrono::milliseconds(100);

// Window parameters for verification
inline constexpr auto kDefaultNotExpiredWindow = std::chrono::milliseconds(40);
inline constexpr auto kDefaultExpiredWindow = std::chrono::milliseconds(65);

// Timing tolerances
inline constexpr auto kShortTimeout = std::chrono::milliseconds(50);
inline constexpr auto kMediumTimeout = std::chrono::milliseconds(80);
inline constexpr auto kLongTimeout = std::chrono::milliseconds(120);
inline constexpr auto kQuickExpirationWindow = std::chrono::milliseconds(30);

// Loop and polling parameters
inline constexpr auto kPollInterval = std::chrono::milliseconds(1);

// Edge case values
inline constexpr double kMinimalTimerDuration = 0.000001;  // 1 microsecond

// Helper to get current time
std::chrono::steady_clock::time_point Now() { return std::chrono::steady_clock::now(); }

// Convert milliseconds to seconds for timer constructor
double ToSeconds(std::chrono::milliseconds ms) { return std::chrono::duration<double>(ms).count(); }

// Helper to wait for timer expiration with timeout
bool WaitForExpiration(AsyncTimer &timer, std::chrono::milliseconds timeout) {
  auto deadline = Now() + timeout;
  while (Now() < deadline) {
    if (timer.IsExpired()) return true;
    std::this_thread::sleep_for(kPollInterval);
  }
  return false;
}

// Helper to verify timer is NOT expired during a window
bool VerifyNotExpiredDuring(AsyncTimer &timer, std::chrono::milliseconds window) {
  auto deadline = Now() + window;
  while (Now() < deadline) {
    if (timer.IsExpired()) {
      ADD_FAILURE() << "Timer expired too early (within " << window.count() << "ms)";
      return false;
    }
    std::this_thread::sleep_for(kPollInterval);
  }
  return true;
}

// Main helper to check timer state within time windows
// First verifies timer is NOT expired during the "not expired" window
// Then verifies timer DOES expire within the "expired" window
bool VerifyTimerExpiration(AsyncTimer &timer, std::chrono::milliseconds not_expired_window = kDefaultNotExpiredWindow,
                           std::chrono::milliseconds expired_window = kDefaultExpiredWindow) {
  auto start = Now();

  // Phase 1: Verify timer is NOT expired during the initial window
  if (!VerifyNotExpiredDuring(timer, not_expired_window)) {
    return false;
  }

  // Phase 2: Wait for timer to expire within the expired window
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(Now() - start);
  if (elapsed < expired_window) {
    auto remaining_time = expired_window - elapsed;
    if (WaitForExpiration(timer, remaining_time)) {
      return true;
    }
  }

  ADD_FAILURE() << "Timer did not expire within " << expired_window.count() << "ms";
  return false;
}

}  // namespace

TEST(AsyncTimer, BasicExpiration) {
  AsyncTimer timer{ToSeconds(kDefaultTimerDuration)};
  EXPECT_TRUE(VerifyTimerExpiration(timer));
}

TEST(AsyncTimer, SequentialTimers) {
  // Test two timers sequentially
  {
    AsyncTimer timer1{ToSeconds(kDefaultTimerDuration)};
    EXPECT_TRUE(VerifyTimerExpiration(timer1));
  }
  {
    AsyncTimer timer2{ToSeconds(kDefaultTimerDuration)};
    EXPECT_TRUE(VerifyTimerExpiration(timer2));
  }
}

TEST(AsyncTimer, RelativeTimingOrder) {
  // Create timers with different durations to test ordering
  AsyncTimer timer_short{ToSeconds(kShortTimer)};
  AsyncTimer timer_medium{ToSeconds(kMediumTimer)};
  AsyncTimer timer_long{ToSeconds(kLongTimer)};

  // Wait for short timer (should expire first)
  EXPECT_TRUE(WaitForExpiration(timer_short, kShortTimeout));
  EXPECT_TRUE(timer_short.IsExpired());
  EXPECT_FALSE(timer_medium.IsExpired());
  EXPECT_FALSE(timer_long.IsExpired());

  // Wait for medium timer (should expire second)
  EXPECT_TRUE(WaitForExpiration(timer_medium, kMediumTimeout));
  EXPECT_TRUE(timer_short.IsExpired());
  EXPECT_TRUE(timer_medium.IsExpired());
  EXPECT_FALSE(timer_long.IsExpired());

  // Wait for long timer (should expire last)
  EXPECT_TRUE(WaitForExpiration(timer_long, kLongTimeout));
  EXPECT_TRUE(timer_short.IsExpired());
  EXPECT_TRUE(timer_medium.IsExpired());
  EXPECT_TRUE(timer_long.IsExpired());
}

TEST(AsyncTimer, MoveConstructor) {
  AsyncTimer timer_1{ToSeconds(kDefaultTimerDuration)};

  // Move construct timer_2 from timer_1
  AsyncTimer timer_2{std::move(timer_1)};

  // After move, timer_1 is in moved-from state
  // The implementation returns false for moved-from timers (null expiration_flag)
  EXPECT_FALSE(timer_1.IsExpired());

  // timer_2 should work normally
  EXPECT_TRUE(VerifyTimerExpiration(timer_2));

  // timer_1 should still return false (it's moved-from)
  EXPECT_FALSE(timer_1.IsExpired());
}

TEST(AsyncTimer, MoveAssignment) {
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
  auto expired_window = kMediumTimer + std::chrono::milliseconds(20);     // Medium timer + tolerance

  EXPECT_TRUE(VerifyTimerExpiration(timer_2, not_expired_window, expired_window));

  // timer_1 remains false (moved-from)
  EXPECT_FALSE(timer_1.IsExpired());
}

TEST(AsyncTimer, AssignmentToExpiredTimer) {
  AsyncTimer timer_1{ToSeconds(kVeryLongTimer)};  // Long timer
  AsyncTimer timer_2{ToSeconds(kShortTimer)};     // Short timer

  // First verify timer_2 expires quickly
  EXPECT_TRUE(WaitForExpiration(timer_2, kShortTimeout));
  EXPECT_TRUE(timer_2.IsExpired());
  EXPECT_FALSE(timer_1.IsExpired());

  // Now assign unexpired timer_1 to expired timer_2
  timer_2 = std::move(timer_1);

  // timer_2 should now be unexpired (has timer_1's timer)
  EXPECT_FALSE(timer_2.IsExpired());
  EXPECT_FALSE(timer_1.IsExpired());  // moved-from

  // Calculate remaining time windows for the moved timer
  auto remaining_not_expired = kShortTimer + std::chrono::milliseconds(10);  // Should not expire for at least this long
  auto remaining_should_expire = kVeryLongTimer + std::chrono::milliseconds(20);  // Should expire within this

  // Verify timer_2 with the moved timer behavior
  EXPECT_TRUE(VerifyTimerExpiration(timer_2, remaining_not_expired, remaining_should_expire));

  // timer_1 should still be false (moved-from)
  EXPECT_FALSE(timer_1.IsExpired());
}

TEST(AsyncTimer, DestructionWhileRunning) {
  // Create and immediately destroy a timer
  { AsyncTimer timer_to_destroy{ToSeconds(kDefaultTimerDuration)}; }

  // Create another timer to ensure the system still works
  AsyncTimer timer_to_wait{ToSeconds(kMediumTimer + std::chrono::milliseconds(10))};

  // Verify the second timer works correctly
  auto not_expired_window = kMediumTimer - std::chrono::milliseconds(10);
  auto expired_window = kMediumTimer + kQuickExpirationWindow;

  EXPECT_TRUE(VerifyTimerExpiration(timer_to_wait, not_expired_window, expired_window));
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
