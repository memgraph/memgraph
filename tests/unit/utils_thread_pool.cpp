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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include <utils/thread_pool.hpp>

using namespace std::chrono_literals;

TEST(ThreadPool, Basic) {
  static constexpr size_t adder_count = 500000;
  static constexpr std::array<size_t, 5> pool_sizes{1, 2, 4, 8, 100};

  for (const auto pool_size : pool_sizes) {
    memgraph::utils::ThreadPool pool{pool_size};

    std::atomic<int> count{0};
    for (size_t i = 0; i < adder_count; ++i) {
      pool.AddTask([&] { count.fetch_add(1); });
    }

    while (pool.UnfinishedTasksNum() != 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_EQ(count.load(), adder_count);
  }
}

// Test that move-only lambdas (capturing unique_ptr) work with std::move_only_function
TEST(ThreadPool, MoveOnlyLambda) {
  static constexpr size_t task_count = 1000;
  memgraph::utils::ThreadPool pool{4};

  std::atomic<int> count{0};
  for (size_t i = 0; i < task_count; ++i) {
    auto ptr = std::make_unique<int>(1);
    pool.AddTask([p = std::move(ptr), &count]() { count.fetch_add(*p); });
  }

  while (pool.UnfinishedTasksNum() != 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_EQ(count.load(), task_count);
}

// High concurrency test with move-only lambdas
TEST(ThreadPool, MoveOnlyLambdaHighConcurrency) {
  static constexpr size_t task_count = 100000;
  static constexpr std::array<size_t, 4> pool_sizes{1, 4, 8, 32};

  for (const auto pool_size : pool_sizes) {
    memgraph::utils::ThreadPool pool{pool_size};

    std::atomic<int64_t> sum{0};
    for (size_t i = 0; i < task_count; ++i) {
      auto ptr = std::make_unique<int64_t>(static_cast<int64_t>(i));
      pool.AddTask([p = std::move(ptr), &sum]() { sum.fetch_add(*p); });
    }

    while (pool.UnfinishedTasksNum() != 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Sum of 0 to task_count-1 = (task_count-1) * task_count / 2
    const int64_t expected = static_cast<int64_t>(task_count - 1) * static_cast<int64_t>(task_count) / 2;
    ASSERT_EQ(sum.load(), expected);
  }
}
