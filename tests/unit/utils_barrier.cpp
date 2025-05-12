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

#include "gtest/gtest.h"

#include <chrono>
#include <cstdint>
#include <optional>
#include <thread>

#include "utils/barrier.hpp"

TEST(Barrier, Wait) {
  using namespace memgraph::utils;
  constexpr auto kN = 12U;
  uint16_t all = (1U << kN) - 1;

  // Set bit and wait
  std::atomic<uint16_t> pre_barrier = 0;
  std::atomic<uint16_t> post_barrier = 0;
  auto test = [&](const uint8_t th_id, SimpleBarrier &barrier) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    pre_barrier |= 1U << th_id;
    barrier.arrive_and_wait();
    EXPECT_EQ(pre_barrier, all);  // Check all arrived at the barrier correctly
    post_barrier |= 1U << th_id;
  };

  // Generate kN threads that run test()
  auto gen_threads = [&](SimpleBarrier &barrier) {
    std::vector<std::jthread> threads;
    threads.reserve(kN);
    for (uint8_t i = 0; i < kN; ++i) {
      threads.emplace_back([i, test, &barrier] { test(i, barrier); });
    }
    return threads;
  };

  // Run tests multiple times
  for (uint16_t run = 0; run < 1000; ++run) {
    // Check if all threads completed the task before arriving at the barrier
    SimpleBarrier barrier{kN};
    auto threads = gen_threads(barrier);
    barrier.wait();
    ASSERT_EQ(pre_barrier, all);  // Check all arrived at the barrier correctly
    threads.clear();
    EXPECT_EQ(post_barrier, all);  // Check that the threads finished
  }
}

TEST(Barrier, Destroy) {
  using namespace memgraph::utils;
  constexpr auto kN = 12U;
  uint16_t all = (1U << kN) - 1;

  // Set bit and wait
  std::atomic<uint16_t> pre_barrier = 0;
  std::atomic<uint16_t> post_barrier = 0;
  auto test = [&](const uint8_t th_id, SimpleBarrier &barrier) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    pre_barrier |= 1U << th_id;
    barrier.arrive_and_wait();
    EXPECT_EQ(pre_barrier, all);  // Check all arrived at the barrier correctly
    post_barrier |= 1U << th_id;
  };

  // Generate kN threads that run test()
  auto gen_threads = [&](SimpleBarrier &barrier) {
    std::vector<std::jthread> threads;
    threads.reserve(kN);
    for (uint8_t i = 0; i < kN; ++i) {
      threads.emplace_back([i, test, &barrier] { test(i, barrier); });
    }
    return threads;
  };

  // Run tests multiple times
  for (uint16_t run = 0; run < 1000; ++run) {
    // Check if all threads completed the task before arriving at the barrier
    std::optional<SimpleBarrier> barrier{kN};
    auto threads = gen_threads(*barrier);
    barrier.reset();  // This will wait
  }
}
