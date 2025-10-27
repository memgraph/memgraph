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

#include <benchmark/benchmark.h>
#include <atomic>
#include <random>
#include <thread>
#include <vector>

#include "utils/memory.hpp"

using namespace memgraph::utils;

// Benchmark fixture for memory resources
class MemoryResourceFixture : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &) override {
    // Reset random number generator for consistent results
    rng_.seed(42);
  }

  void TearDown(const benchmark::State &) override {
    // Cleanup code if needed
  }

  // Generate random allocation parameters
  std::pair<size_t, size_t> GetRandomAllocation() {
    size_t size = size_dist_(rng_);
    size_t alignment = 1U << alignment_dist_(rng_);
    return {size, alignment};
  }

 private:
  std::mt19937 rng_{42};
  std::uniform_int_distribution<size_t> size_dist_{1, 1024};
  std::uniform_int_distribution<size_t> alignment_dist_{0, 8};
};

// Single-threaded allocation benchmark
BENCHMARK_F(MemoryResourceFixture, MonotonicBufferResource_SingleThread)(benchmark::State &state) {
  MonotonicBufferResource mem(1024 * 1024);  // 1MB initial size

  for (auto _ : state) {
    void *ptr = mem.allocate(64, 8);
    benchmark::DoNotOptimize(ptr);
  }

  mem.Release();
}

BENCHMARK_F(MemoryResourceFixture, ThreadSafeMonotonicBufferResource_SingleThread)(benchmark::State &state) {
  ThreadSafeMonotonicBufferResource mem(1024 * 1024);  // 1MB initial size

  for (auto _ : state) {
    void *ptr = mem.allocate(64, 8);
    benchmark::DoNotOptimize(ptr);
  }

  mem.Release();
}

BENCHMARK_F(MemoryResourceFixture, MonotonicBufferResource_SingleThread_TinyAllocations)(benchmark::State &state) {
  MonotonicBufferResource mem(1024 * 1024);  // 1MB initial size

  for (auto _ : state) {
    void *ptr = mem.allocate(4, 2);
    benchmark::DoNotOptimize(ptr);
  }

  mem.Release();
}

BENCHMARK_F(MemoryResourceFixture, ThreadSafeMonotonicBufferResource_SingleThread_TinyAllocations)
(benchmark::State &state) {
  ThreadSafeMonotonicBufferResource mem(1024 * 1024);  // 1MB initial size

  for (auto _ : state) {
    void *ptr = mem.allocate(4, 2);
    benchmark::DoNotOptimize(ptr);
  }

  mem.Release();
}

BENCHMARK_F(MemoryResourceFixture, MonotonicBufferResource_SingleThread_LargeAllocations)(benchmark::State &state) {
  MonotonicBufferResource mem(1024 * 1024);  // 1MB initial size

  for (auto _ : state) {
    void *ptr = mem.allocate(1024, 16);
    benchmark::DoNotOptimize(ptr);
  }

  mem.Release();
}

BENCHMARK_F(MemoryResourceFixture, ThreadSafeMonotonicBufferResource_SingleThread_LargeAllocations)
(benchmark::State &state) {
  ThreadSafeMonotonicBufferResource mem(1024 * 1024);  // 1MB initial size

  for (auto _ : state) {
    void *ptr = mem.allocate(1024, 16);
    benchmark::DoNotOptimize(ptr);
  }

  mem.Release();
}

BENCHMARK_F(MemoryResourceFixture, MonotonicBufferResource_SingleThread_RandomAllocations)(benchmark::State &state) {
  MonotonicBufferResource mem(1024 * 1024);  // 1MB initial size

  for (auto _ : state) {
    auto [size, alignment] = GetRandomAllocation();
    void *ptr = mem.allocate(size, alignment);
    benchmark::DoNotOptimize(ptr);
  }

  mem.Release();
}

BENCHMARK_F(MemoryResourceFixture, ThreadSafeMonotonicBufferResource_SingleThread_RandomAllocations)
(benchmark::State &state) {
  ThreadSafeMonotonicBufferResource mem(1024 * 1024);  // 1MB initial size

  for (auto _ : state) {
    auto [size, alignment] = GetRandomAllocation();
    void *ptr = mem.allocate(size, alignment);
    benchmark::DoNotOptimize(ptr);
  }

  mem.Release();
}

// Mixed workload benchmark (allocations of different sizes)
static void BM_MixedAllocations(benchmark::State &state) {
  ThreadSafeMonotonicBufferResource mem(1024 * 1024);
  const int num_threads = state.range(0);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> size_dist(8, 1024);
  std::uniform_int_distribution<> align_dist(1, 64);

  for (auto _ : state) {
    std::vector<std::thread> threads;
    std::atomic<size_t> total_allocations{0};

    for (int i = 0; i < num_threads; ++i) {
      threads.emplace_back([&mem, &total_allocations, &gen, &size_dist, &align_dist]() {
        for (int j = 0; j < 500; ++j) {
          size_t size = size_dist(gen);
          size_t alignment = 1 << (align_dist(gen) % 7);  // Power of 2 alignment

          void *ptr = mem.allocate(size, alignment);
          benchmark::DoNotOptimize(ptr);
          total_allocations.fetch_add(1, std::memory_order_relaxed);
        }
      });
    }

    for (auto &thread : threads) {
      thread.join();
    }

    benchmark::DoNotOptimize(total_allocations.load());
  }

  mem.Release();
}

BENCHMARK(BM_MixedAllocations)->RangeMultiplier(2)->Range(1, 8)->UseRealTime()->Unit(benchmark::kMicrosecond);

// Contention benchmark - high contention scenario
static void BM_HighContention(benchmark::State &state) {
  ThreadSafeMonotonicBufferResource mem(1024);  // Small buffer to force contention
  const int num_threads = state.range(0);

  for (auto _ : state) {
    std::vector<std::thread> threads;
    std::atomic<size_t> total_allocations{0};

    for (int i = 0; i < num_threads; ++i) {
      threads.emplace_back([&mem, &total_allocations]() {
        for (int j = 0; j < 100; ++j) {
          // Allocate large blocks to quickly exhaust the small buffer
          void *ptr = mem.allocate(512, 8);
          benchmark::DoNotOptimize(ptr);
          total_allocations.fetch_add(1, std::memory_order_relaxed);

          // Small delay to increase contention
          std::this_thread::sleep_for(std::chrono::microseconds(1));
        }
      });
    }

    for (auto &thread : threads) {
      thread.join();
    }

    benchmark::DoNotOptimize(total_allocations.load());
  }

  mem.Release();
}

BENCHMARK(BM_HighContention)->RangeMultiplier(2)->Range(2, 16)->UseRealTime()->Unit(benchmark::kMicrosecond);

// Release performance benchmark
static void BM_ReleasePerformance(benchmark::State &state) {
  ThreadSafeMonotonicBufferResource mem(1024 * 1024);
  const int num_allocations = state.range(0);

  // Pre-allocate memory
  std::vector<void *> ptrs;
  ptrs.reserve(num_allocations);

  for (int i = 0; i < num_allocations; ++i) {
    ptrs.push_back(mem.allocate(64, 8));
  }

  for (auto _ : state) {
    mem.Release();
    benchmark::DoNotOptimize(mem);
  }
}

BENCHMARK(BM_ReleasePerformance)->RangeMultiplier(10)->Range(100, 10000)->UseRealTime()->Unit(benchmark::kMicrosecond);

BENCHMARK_MAIN();
