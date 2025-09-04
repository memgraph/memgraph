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

#include "planner/core/union_find.hpp"

#include <vector>

#include <benchmark/benchmark.h>

constexpr auto kRangeLow = 64;
constexpr auto kRangeHigh = 1024 * 4;

namespace {

using memgraph::planner::core::UnionFind;
using memgraph::planner::core::UnionFindContext;

void BM_UnionFind_MakeSet(benchmark::State &state) {
  UnionFind uf;
  for (auto _ : state) {  // NOLINT(clang-analyzer-deadcode.DeadStores)
    uf.Clear();
    for (int i = 0; i < state.range(0); ++i) {
      benchmark::DoNotOptimize(uf.MakeSet());
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * state.range(0));
  state.SetComplexityN(state.range(0));
}

void BM_UnionFind_Find(benchmark::State &state) {
  UnionFind uf;
  std::vector<uint32_t> ids;

  // Setup: create sets
  ids.reserve(state.range(0));
  for (int i = 0; i < state.range(0); ++i) {
    ids.push_back(uf.MakeSet());
  }

  // Create some unions to make find work harder
  for (int i = 0; i < state.range(0) / 2; ++i) {
    uf.UnionSets(ids[i], ids[i + state.range(0) / 2]);
  }

  for (auto _ : state) {  // NOLINT(clang-analyzer-deadcode.DeadStores)
    for (auto id : ids) {
      benchmark::DoNotOptimize(uf.Find(id));
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * ids.size()));
  state.SetComplexityN(state.range(0));
}

void BM_UnionFind_Find_OnAChain(benchmark::State &state) {
  UnionFind uf;
  std::vector<uint32_t> ids;

  // Setup: create a chain of unions
  ids.reserve(state.range(0));
  for (int i = 0; i < state.range(0); ++i) {
    ids.push_back(uf.MakeSet());
  }

  for (size_t i = 0; i < (state.range(0) - 1); ++i) {
    uf.UnionSets(ids[i], ids[i + 1]);
  }

  for (auto _ : state) {  // NOLINT(clang-analyzer-deadcode.DeadStores)
    // Benchmark find on the first element (should trigger path halving)
    for (int i = 0; i < 1000; ++i) {
      benchmark::DoNotOptimize(uf.Find(ids[0]));
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * 1000);
  state.SetComplexityN(state.range(0));
}

void BM_UnionFind_Union(benchmark::State &state) {
  UnionFind uf;
  std::vector<uint32_t> ids;
  for (auto _ : state) {  // NOLINT(clang-analyzer-deadcode.DeadStores)
    state.PauseTiming();
    uf.Clear();
    ids.clear();

    // Setup: create sets to union
    ids.reserve(state.range(0));
    for (int i = 0; i < state.range(0); ++i) {
      ids.push_back(uf.MakeSet());
    }
    state.ResumeTiming();

    // Perform unions
    for (size_t i = 0; i < (state.range(0) - 1); ++i) {
      benchmark::DoNotOptimize(uf.UnionSets(ids[i], ids[i + 1]));
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * ids.size()));
  state.SetComplexityN(state.range(0));
}

void BM_UnionFind_BulkUnion(benchmark::State &state) {
  UnionFind uf;
  std::vector<uint32_t> ids;
  UnionFindContext ctx;

  auto size = state.range(0);
  for (auto _ : state) {  // NOLINT(clang-analyzer-deadcode.DeadStores)
    state.PauseTiming();
    uf.Clear();
    ids.clear();

    ids.reserve(size);
    for (int64_t i = 0; i < size; ++i) {
      ids.push_back(uf.MakeSet());
    }

    state.ResumeTiming();

    // Bulk union
    uf.UnionSets(ids, ctx);

    benchmark::DoNotOptimize(uf);
  }

  state.SetComplexityN(size);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * size);
}

}  // namespace

BENCHMARK(BM_UnionFind_MakeSet)->Range(kRangeLow, kRangeHigh)->Complexity();
BENCHMARK(BM_UnionFind_Find)->Range(kRangeLow, kRangeHigh)->Complexity();
BENCHMARK(BM_UnionFind_Find_OnAChain)->Range(kRangeLow, kRangeHigh)->Complexity();
BENCHMARK(BM_UnionFind_Union)->Range(kRangeLow, kRangeHigh)->Complexity();
BENCHMARK(BM_UnionFind_BulkUnion)->Range(kRangeLow, kRangeHigh)->Complexity();

BENCHMARK_MAIN();
