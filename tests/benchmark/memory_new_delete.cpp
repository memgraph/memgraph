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

// Measures the per-allocation cost of the global operator new, which every
// allocation in the process pays.

#include <benchmark/benchmark.h>

#include <cstddef>
#include <string>
#include <vector>

#include "utils/memory_tracker.hpp"

namespace {

// Allocate and free the same block. The allocator returns it from the thread
// cache, so the measurement is dominated by the fixed cost of a new/delete pair.
void NewDeleteHot(benchmark::State &state) {
  const auto size = static_cast<std::size_t>(state.range(0));
  for (auto _ : state) {
    auto *p = ::operator new(size);
    benchmark::DoNotOptimize(p);
    ::operator delete(p, size);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(NewDeleteHot)->Arg(8)->Arg(64)->Arg(512)->Arg(4096);

// Hold many blocks live before releasing them, so allocations are served from
// the arena rather than repeatedly reusing one cached block.
void NewDeleteBatch(benchmark::State &state) {
  static constexpr std::size_t kBatch = 1024;
  const auto size = static_cast<std::size_t>(state.range(0));
  std::vector<void *> ptrs(kBatch, nullptr);
  for (auto _ : state) {
    for (auto &p : ptrs) {
      p = ::operator new(size);
      benchmark::DoNotOptimize(p);
    }
    for (auto *p : ptrs) {
      ::operator delete(p, size);
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kBatch);
}

BENCHMARK(NewDeleteBatch)->Arg(8)->Arg(64)->Arg(512);

// A container workload, where each allocation is surrounded by the work a caller
// actually does with the memory.
void StringVectorChurn(benchmark::State &state) {
  static constexpr std::size_t kCount = 256;
  for (auto _ : state) {
    std::vector<std::string> strings;
    strings.reserve(kCount);
    for (std::size_t i = 0; i < kCount; ++i) {
      strings.emplace_back(64, static_cast<char>('a' + (i % 26)));
    }
    benchmark::DoNotOptimize(strings.data());
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kCount);
}

BENCHMARK(StringVectorChurn);

// The scope on its own, with no allocation, to separate its cost from the
// allocator's.
void RefusalHandledScopeOnly(benchmark::State &state) {
  for (auto _ : state) {
    const memgraph::utils::MemoryTracker::RefusalHandledScope refusal_handled;
    benchmark::DoNotOptimize(memgraph::utils::MemoryTracker::IsRefusalHandled());
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(RefusalHandledScopeOnly);

// The same allocations with a per-thread limit armed, which is the state a query
// runs under.
void NewDeleteHotWithEnabler(benchmark::State &state) {
  const memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler enabler;
  const auto size = static_cast<std::size_t>(state.range(0));
  for (auto _ : state) {
    auto *p = ::operator new(size);
    benchmark::DoNotOptimize(p);
    ::operator delete(p, size);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(NewDeleteHotWithEnabler)->Arg(8)->Arg(64)->Arg(512);

}  // namespace

BENCHMARK_MAIN();
