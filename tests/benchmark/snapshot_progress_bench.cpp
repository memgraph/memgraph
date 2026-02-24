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

#include <benchmark/benchmark.h>

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include "storage/v2/snapshot_progress.hpp"

// Simulates work done per item in the snapshot loop (cheap stand-in for real I/O)
static void SimulateItemWork() {
  // ~50ns of arithmetic to approximate lightweight per-item overhead
  volatile uint64_t sink = 0;
  for (int i = 0; i < 10; ++i) {
    sink += i * 3;
  }
}

// Baseline: no progress tracking at all
static void BM_NoProgress(benchmark::State &state) {
  const auto n_items = static_cast<uint64_t>(state.range(0));
  for (auto _ : state) {
    uint64_t count = 0;
    for (uint64_t i = 0; i < n_items; ++i) {
      SimulateItemWork();
      ++count;
    }
    benchmark::DoNotOptimize(count);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(n_items));
}

// Per-item atomic increment (original implementation)
static void BM_PerItemAtomic(benchmark::State &state) {
  const auto n_items = static_cast<uint64_t>(state.range(0));
  memgraph::storage::SnapshotProgress progress;
  progress.SetPhase(memgraph::storage::SnapshotProgress::Phase::VERTICES, n_items);
  for (auto _ : state) {
    progress.items_done.store(0, std::memory_order_relaxed);
    uint64_t count = 0;
    for (uint64_t i = 0; i < n_items; ++i) {
      SimulateItemWork();
      ++count;
      progress.IncrementDone();
    }
    benchmark::DoNotOptimize(count);
    benchmark::DoNotOptimize(progress.items_done.load(std::memory_order_relaxed));
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(n_items));
}

// Batched atomic increment (new implementation)
static void BM_BatchedAtomic(benchmark::State &state) {
  const auto n_items = static_cast<uint64_t>(state.range(0));
  memgraph::storage::SnapshotProgress progress;
  progress.SetPhase(memgraph::storage::SnapshotProgress::Phase::VERTICES, n_items);
  for (auto _ : state) {
    progress.items_done.store(0, std::memory_order_relaxed);
    uint64_t count = 0;
    {
      memgraph::storage::BatchedProgressCounter batch_counter(&progress);
      for (uint64_t i = 0; i < n_items; ++i) {
        SimulateItemWork();
        ++count;
        batch_counter.Increment();
      }
    }  // destructor flushes remainder
    benchmark::DoNotOptimize(count);
    benchmark::DoNotOptimize(progress.items_done.load(std::memory_order_relaxed));
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(n_items));
}

// Multi-threaded: per-item atomic (contention scenario)
static void BM_PerItemAtomic_MT(benchmark::State &state) {
  const auto n_items = static_cast<uint64_t>(state.range(0));
  const auto n_threads = static_cast<unsigned>(state.range(1));
  memgraph::storage::SnapshotProgress progress;
  progress.SetPhase(memgraph::storage::SnapshotProgress::Phase::VERTICES, n_items);

  for (auto _ : state) {
    progress.items_done.store(0, std::memory_order_relaxed);
    const auto items_per_thread = n_items / n_threads;
    std::vector<std::thread> threads;
    threads.reserve(n_threads);
    for (unsigned t = 0; t < n_threads; ++t) {
      threads.emplace_back([&progress, items_per_thread] {
        for (uint64_t i = 0; i < items_per_thread; ++i) {
          SimulateItemWork();
          progress.IncrementDone();
        }
      });
    }
    for (auto &t : threads) t.join();
    benchmark::DoNotOptimize(progress.items_done.load(std::memory_order_relaxed));
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(n_items));
}

// Multi-threaded: batched atomic (contention scenario)
static void BM_BatchedAtomic_MT(benchmark::State &state) {
  const auto n_items = static_cast<uint64_t>(state.range(0));
  const auto n_threads = static_cast<unsigned>(state.range(1));
  memgraph::storage::SnapshotProgress progress;
  progress.SetPhase(memgraph::storage::SnapshotProgress::Phase::VERTICES, n_items);

  for (auto _ : state) {
    progress.items_done.store(0, std::memory_order_relaxed);
    const auto items_per_thread = n_items / n_threads;
    std::vector<std::thread> threads;
    threads.reserve(n_threads);
    for (unsigned t = 0; t < n_threads; ++t) {
      threads.emplace_back([&progress, items_per_thread] {
        memgraph::storage::BatchedProgressCounter batch_counter(&progress);
        for (uint64_t i = 0; i < items_per_thread; ++i) {
          SimulateItemWork();
          batch_counter.Increment();
        }
      });
    }
    for (auto &t : threads) t.join();
    benchmark::DoNotOptimize(progress.items_done.load(std::memory_order_relaxed));
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(n_items));
}

// Single-threaded benchmarks: 100K and 1M items
BENCHMARK(BM_NoProgress)->Arg(100000)->Arg(1000000);
BENCHMARK(BM_PerItemAtomic)->Arg(100000)->Arg(1000000);
BENCHMARK(BM_BatchedAtomic)->Arg(100000)->Arg(1000000);

// Multi-threaded benchmarks: 1M items, 4 and 8 threads
BENCHMARK(BM_PerItemAtomic_MT)->Args({1000000, 4})->Args({1000000, 8});
BENCHMARK(BM_BatchedAtomic_MT)->Args({1000000, 4})->Args({1000000, 8});

BENCHMARK_MAIN();
