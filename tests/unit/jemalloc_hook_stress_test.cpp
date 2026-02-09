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
#include <chrono>
#include <cstdlib>
#include <random>
#include <thread>
#include <vector>

#include "memory/global_memory_control.hpp"
#include "utils/memory_tracker.hpp"

namespace {

void ThreadFunc(int iterations) {
  std::random_device random_dev;
  std::mt19937 gen(random_dev());
  std::uniform_int_distribution<size_t> size_dist(1, 1024ULL * 1024);  // 1B to 1MB
  std::uniform_int_distribution<int> sleep_dist(0, 10);                // 0 to 10ms

  for (int i = 0; i < iterations; ++i) {
    size_t size = size_dist(gen);
    // 1. Allocate memory to trigger jemalloc hooks
    void *ptr = std::malloc(size);
    if (ptr) {
      // 2. Use OutOfMemoryExceptionBlocker which accesses thread_local counter_
      // This is exactly what happens in my_commit and other hooks
      memgraph::utils::MemoryTracker::OutOfMemoryExceptionBlocker blocker;

      // 3. Deallocate
      std::free(ptr);
    }

    // 4. Trigger the OOM path which accesses MemoryTrackerStatus (another TLS)
    {
      memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler enabler;
      size_t oom_size = size_dist(gen) % 1024;  // smaller for OOM path usually
      // We don't actually need to fail the allocation here,
      // just accessing the TLS components is enough to test for recursion
      // during lazy TLS allocation.
      void *ptr2 = std::malloc(oom_size);
      if (ptr2) std::free(ptr2);
    }

    if (sleep_dist(gen) > 5) {
      std::this_thread::sleep_for(std::chrono::milliseconds(sleep_dist(gen) % 3));
    }
  }
}

}  // namespace

TEST(JemallocHookStress, ThreadChurnRecursion) {
#if USE_JEMALLOC
  memgraph::memory::SetHooks();
#endif

  // Set a high limit so we don't actually throw, we just want to exercise the hooks
  memgraph::utils::total_memory_tracker.SetHardLimit(1024LL * 1024 * 1024 * 10);  // 10GB

  static constexpr int kNumThreads = 50;
  static constexpr int kIterations = 1000;

  std::vector<std::thread> thread_pool;
  thread_pool.reserve(kNumThreads);

  for (int i = 0; i < kNumThreads; ++i) {
    thread_pool.emplace_back(ThreadFunc, kIterations);
  }

  for (auto &thread_item : thread_pool) {
    thread_item.join();
  }
}

TEST(JemallocHookStress, OOMPathRecursion) {
#if USE_JEMALLOC
  memgraph::memory::SetHooks();
#endif

  // Set a low limit to trigger "return false" in Alloc
  memgraph::utils::total_memory_tracker.SetHardLimit(1024);

  auto run_oom = []() {
    for (int i = 0; i < 100; ++i) {
      memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler enabler;
      // This should return nullptr because of the limit
      void *ptr = std::malloc(1024ULL * 1024);
      if (ptr) std::free(ptr);
    }
  };

  std::vector<std::thread> oom_threads;
  oom_threads.reserve(20);
  for (int i = 0; i < 20; ++i) {
    oom_threads.emplace_back(run_oom);
  }

  for (auto &oom_thread : oom_threads) {
    oom_thread.join();
  }
}
