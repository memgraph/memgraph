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

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <atomic>
#include "utils/scheduler.hpp"

/**
 * Scheduler runs every 2 seconds and increases one variable. Test thread
 * increases other variable. Scheduler checks if variables have the same
 * value.
 */
TEST(Scheduler, TestFunctionExecuting) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;
  scheduler.Run("Test", std::chrono::seconds(1), func);

  EXPECT_EQ(x, 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(900));
  EXPECT_EQ(x, 0);

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_EQ(x, 1);

  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  EXPECT_EQ(x, 3);

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  scheduler.Stop();
  EXPECT_EQ(x, 3);
}
