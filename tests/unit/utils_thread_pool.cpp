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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include <utils/thread_pool.hpp>

using namespace std::chrono_literals;

namespace {
thread_local int thread_pool_init_marker = 0;
}

TEST(ThreadPool, Basic) {
  static constexpr size_t adder_count = 500'000;
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

TEST(ThreadPool, ThreadInitializerRunsBeforeTasks) {
  static constexpr size_t task_count = 1000;
  static constexpr int initialized_marker = 42;

  std::atomic<size_t> initialized_threads{0};
  memgraph::utils::ThreadPool pool{4, [&]() -> memgraph::utils::ThreadPool::TaskSignature {
                                     thread_pool_init_marker = initialized_marker;
                                     initialized_threads.fetch_add(1);
                                     return {};
                                   }};
  while (initialized_threads.load() != 4U) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  std::atomic<size_t> initialized_tasks{0};
  for (size_t i = 0; i < task_count; ++i) {
    pool.AddTask([&] {
      if (thread_pool_init_marker == initialized_marker) {
        initialized_tasks.fetch_add(1);
      }
    });
  }

  while (pool.UnfinishedTasksNum() != 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_EQ(initialized_threads.load(), 4U);
  ASSERT_EQ(initialized_tasks.load(), task_count);
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
  static constexpr size_t task_count = 100'000;
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

TEST(ThreadPool, ShutDownReportsDiscardedTasks) {
  static constexpr size_t task_count = 5;

  std::atomic<int> ran{0};
  // Pool size 0 spawns no worker thread, so nothing can pop task_queue_ before ShutDown() runs --
  // every added task is still queued, making the discarded count exact with no synchronization needed.
  memgraph::utils::ThreadPool pool{0};

  for (size_t i = 0; i < task_count; ++i) {
    pool.AddTask([&] { ran.fetch_add(1); });
  }

  ASSERT_EQ(pool.UnfinishedTasksNum(), task_count);
  ASSERT_EQ(pool.ShutDown(), task_count);
  ASSERT_EQ(ran.load(), 0);
  ASSERT_EQ(pool.UnfinishedTasksNum(), 0U);
}

TEST(ThreadPool, AddTaskRejectedAfterShutDown) {
  memgraph::utils::ThreadPool pool{1};

  std::atomic<int> ran{0};
  ASSERT_TRUE(pool.AddTask([&] { ran.fetch_add(1); }));

  while (pool.UnfinishedTasksNum() != 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  pool.ShutDown();

  ASSERT_FALSE(pool.AddTask([&] { ran.fetch_add(1); }));
  ASSERT_EQ(ran.load(), 1);
  ASSERT_EQ(pool.UnfinishedTasksNum(), 0U);
}

TEST(ThreadPool, ShutDownDrainsNothingButFinishesTheRunningTask) {
  static constexpr size_t queued_after_start = 3;

  memgraph::utils::ThreadPool pool{1};

  std::atomic<bool> started{false};
  std::atomic<bool> release{false};
  std::atomic<bool> finished{false};

  pool.AddTask([&] {
    started = true;
    while (!release.load()) {
      std::this_thread::sleep_for(1ms);
    }
    finished = true;
  });

  while (!started.load()) {
    std::this_thread::sleep_for(1ms);
  }

  std::atomic<int> ran{0};
  for (size_t i = 0; i < queued_after_start; ++i) {
    pool.AddTask([&] { ran.fetch_add(1); });
  }

  std::atomic<size_t> discarded{0};
  std::jthread shutdown_thread([&] { discarded.store(pool.ShutDown()); });

  release = true;
  shutdown_thread.join();

  // Guaranteed, not racy: the running task has no stop-token early-out, so the worker can only leave it by
  // setting finished=true; ShutDown()'s thread_pool_.clear() joins that worker, and the shutdown_thread.join()
  // above sequences that join before this line.
  ASSERT_TRUE(finished.load());

  // Unlike the pool-size-0 test above, a real worker may drain any number of the queued tasks before
  // ShutDown() takes pool_lock_; that race is genuine, so only the discarded+ran total is exact.
  ASSERT_LE(discarded.load(), queued_after_start);
  ASSERT_EQ(discarded.load() + static_cast<size_t>(ran.load()), queued_after_start);
}

TEST(ThreadPool, ShutDownOnIdlePoolReportsZero) {
  memgraph::utils::ThreadPool pool{2};
  ASSERT_EQ(pool.ShutDown(), 0U);
}
