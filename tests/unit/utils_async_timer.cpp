// Copyright 2022 Memgraph Ltd.
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
#include <cmath>
#include <limits>

#include "gtest/gtest.h"

#include "utils/async_timer.hpp"

using AsyncTimer = memgraph::utils::AsyncTimer;

inline constexpr auto kSecondsInMillis = 1000.0;
inline constexpr auto kIntervalInSeconds = 0.3;
inline constexpr auto kIntervalInMillis = kIntervalInSeconds * kSecondsInMillis;
inline constexpr auto kAbsoluteErrorInMillis = 50;

std::chrono::steady_clock::time_point Now() { return std::chrono::steady_clock::now(); }

int ElapsedMillis(const std::chrono::steady_clock::time_point &start, const std::chrono::steady_clock::time_point &end) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
}

void CheckTimeSimple() {
  const auto before = Now();
  AsyncTimer timer{kIntervalInSeconds};
  while (!timer.IsExpired()) {
    ASSERT_LT(ElapsedMillis(before, Now()), 2 * kIntervalInMillis);
  }

  const auto after = Now();

  EXPECT_NEAR(ElapsedMillis(before, after), kIntervalInMillis, kAbsoluteErrorInMillis);
}

TEST(AsyncTimer, SimpleWait) { CheckTimeSimple(); }

TEST(AsyncTimer, DoubleWait) {
  CheckTimeSimple();
  CheckTimeSimple();
}

TEST(AsyncTimer, MoveConstruct) {
  const auto before = Now();
  AsyncTimer timer_1{kIntervalInSeconds};
  AsyncTimer timer_2{std::move(timer_1)};

  EXPECT_FALSE(timer_1.IsExpired());
  EXPECT_FALSE(timer_2.IsExpired());
  const auto first_check_point = Now();

  while (!timer_2.IsExpired()) {
    ASSERT_LT(ElapsedMillis(before, Now()), 2 * kIntervalInMillis);
  }
  const auto second_check_point = Now();

  EXPECT_FALSE(timer_1.IsExpired());
  EXPECT_TRUE(timer_2.IsExpired());

  EXPECT_LT(ElapsedMillis(before, first_check_point), kIntervalInMillis / 2);
  EXPECT_NEAR(ElapsedMillis(before, second_check_point), kIntervalInMillis, kAbsoluteErrorInMillis);
}

TEST(AsyncTimer, MoveAssign) {
  const auto before = Now();
  AsyncTimer timer_1{2 * kIntervalInSeconds};
  AsyncTimer timer_2{kIntervalInSeconds};

  EXPECT_FALSE(timer_1.IsExpired());
  EXPECT_FALSE(timer_2.IsExpired());
  const auto first_check_point = Now();

  timer_2 = std::move(timer_1);
  EXPECT_FALSE(timer_1.IsExpired());
  EXPECT_FALSE(timer_2.IsExpired());

  while (!timer_2.IsExpired()) {
    ASSERT_LT(ElapsedMillis(before, Now()), 3 * kIntervalInMillis);
  }
  const auto second_check_point = Now();

  EXPECT_FALSE(timer_1.IsExpired());
  EXPECT_TRUE(timer_2.IsExpired());

  EXPECT_LT(ElapsedMillis(before, first_check_point), kIntervalInMillis / 2);
  EXPECT_NEAR(ElapsedMillis(before, second_check_point), 2 * kIntervalInMillis, kAbsoluteErrorInMillis);
}

TEST(AsyncTimer, AssignToExpiredTimer) {
  const auto before = Now();
  AsyncTimer timer_1{2 * kIntervalInSeconds};
  AsyncTimer timer_2{kIntervalInSeconds};

  EXPECT_FALSE(timer_1.IsExpired());
  EXPECT_FALSE(timer_2.IsExpired());
  const auto first_check_point = Now();

  while (!timer_2.IsExpired()) {
    ASSERT_LT(ElapsedMillis(before, Now()), 3 * kIntervalInMillis);
  }

  EXPECT_FALSE(timer_1.IsExpired());
  EXPECT_TRUE(timer_2.IsExpired());
  const auto second_check_point = Now();

  timer_2 = std::move(timer_1);
  EXPECT_FALSE(timer_1.IsExpired());
  EXPECT_FALSE(timer_2.IsExpired());
  const auto third_check_point = Now();

  while (!timer_2.IsExpired()) {
    ASSERT_LT(ElapsedMillis(before, Now()), 3 * kIntervalInMillis);
  }

  EXPECT_FALSE(timer_1.IsExpired());
  EXPECT_TRUE(timer_2.IsExpired());
  const auto fourth_check_point = Now();

  EXPECT_LT(ElapsedMillis(before, first_check_point), kIntervalInMillis / 2);
  EXPECT_NEAR(ElapsedMillis(before, second_check_point), kIntervalInMillis, kAbsoluteErrorInMillis);
  EXPECT_LT(ElapsedMillis(before, third_check_point), 1.5 * kIntervalInMillis);
  EXPECT_NEAR(ElapsedMillis(before, fourth_check_point), 2 * kIntervalInMillis, kAbsoluteErrorInMillis);
}

TEST(AsyncTimer, DestroyTimerWhileItIsStillRunning) {
  { AsyncTimer timer_to_destroy{kIntervalInSeconds}; }
  const auto before = Now();
  AsyncTimer timer_to_wait{1.5 * kIntervalInSeconds};
  while (!timer_to_wait.IsExpired()) {
    ASSERT_LT(ElapsedMillis(before, Now()), 3 * kIntervalInMillis);
  }
  // At this point the timer_to_destroy has expired, nothing bad happened. This doesn't mean the timer cancellation
  // works properly, it just means that nothing bad happens if a timer get cancelled.
}

TEST(AsyncTimer, TimersWithExtremeValues) {
  AsyncTimer timer_with_zero{0};
  const double expected_maximum_value = std::nexttoward(std::numeric_limits<time_t>::max(), 0.0);
  AsyncTimer timer_with_max_value{expected_maximum_value};
}
