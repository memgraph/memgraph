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
#include <vector>
#include "planner/core/egraph.hpp"
#include "planner/core/processing_context.hpp"

using namespace memgraph::planner::core;

enum class BenchSymbol : uint32_t {
  A = 0,
  B = 1,
  C = 2,
  D = 3,
  E = 4,
  F = 10,
  G = 11,
  H = 12,
  Plus = 13,
  Mul = 14,
};

struct BenchAnalysis {};

static void BM_EGraphRebuild_SmallGraph(benchmark::State &state) {
  for (auto _ : state) {
    EGraph<BenchSymbol, BenchAnalysis> egraph;
    ProcessingContext<BenchSymbol> ctx;
    std::vector<EClassId> created_ids;

    // Create leaf nodes
    for (int i = 0; i < 10; ++i) {
      auto sym = static_cast<BenchSymbol>(i % 5);
      auto id = egraph.emplace(sym, i);
      created_ids.push_back(id);
    }

    // Create compound nodes
    for (int i = 0; i < 5; ++i) {
      auto sym = static_cast<BenchSymbol>(10 + (i % 5));
      std::vector<EClassId> children = {created_ids[i % created_ids.size()], created_ids[(i + 1) % created_ids.size()]};
      auto id = egraph.emplace(sym, memgraph::utils::small_vector<EClassId>(children.begin(), children.end()));
      created_ids.push_back(id);
    }

    // Perform merges
    for (int i = 0; i < 10; ++i) {
      auto id1 = created_ids[i % created_ids.size()];
      auto id2 = created_ids[(i + 3) % created_ids.size()];
      egraph.merge(id1, id2);
    }

    // Benchmark the rebuild operation
    egraph.rebuild(ctx);
    benchmark::ClobberMemory();
  }
}

static void BM_EGraphRebuild_MediumGraph(benchmark::State &state) {
  for (auto _ : state) {
    EGraph<BenchSymbol, BenchAnalysis> egraph;
    ProcessingContext<BenchSymbol> ctx;
    std::vector<EClassId> created_ids;

    // Create more leaf nodes
    for (int i = 0; i < 50; ++i) {
      auto sym = static_cast<BenchSymbol>(i % 5);
      auto id = egraph.emplace(sym, i);
      created_ids.push_back(id);
    }

    // Create compound nodes with varying children
    for (int i = 0; i < 25; ++i) {
      auto sym = static_cast<BenchSymbol>(10 + (i % 5));
      std::vector<EClassId> children;
      int num_children = 2 + (i % 2);
      for (int j = 0; j < num_children; ++j) {
        children.push_back(created_ids[(i + j) % created_ids.size()]);
      }
      auto id = egraph.emplace(sym, memgraph::utils::small_vector<EClassId>(children.begin(), children.end()));
      created_ids.push_back(id);
    }

    // Perform more merges to create complex patterns
    for (int i = 0; i < 50; ++i) {
      auto id1 = created_ids[i % created_ids.size()];
      auto id2 = created_ids[(i + 7) % created_ids.size()];
      egraph.merge(id1, id2);
    }

    // Benchmark the rebuild operation
    egraph.rebuild(ctx);
    benchmark::ClobberMemory();
  }
}

static void BM_EGraphRebuild_LargeGraph(benchmark::State &state) {
  for (auto _ : state) {
    EGraph<BenchSymbol, BenchAnalysis> egraph;
    ProcessingContext<BenchSymbol> ctx;
    std::vector<EClassId> created_ids;

    // Create many leaf nodes
    for (int i = 0; i < 100; ++i) {
      auto sym = static_cast<BenchSymbol>(i % 5);
      auto id = egraph.emplace(sym, i);
      created_ids.push_back(id);
    }

    // Create compound nodes
    for (int i = 0; i < 50; ++i) {
      auto sym = static_cast<BenchSymbol>(10 + (i % 5));
      std::vector<EClassId> children;
      int num_children = 2 + (i % 3);
      for (int j = 0; j < num_children; ++j) {
        children.push_back(created_ids[(i + j * 7) % created_ids.size()]);
      }
      auto id = egraph.emplace(sym, memgraph::utils::small_vector<EClassId>(children.begin(), children.end()));
      created_ids.push_back(id);
    }

    // Perform many merges - this is likely where the performance issue manifests
    for (int i = 0; i < 100; ++i) {
      auto id1 = created_ids[i % created_ids.size()];
      auto id2 = created_ids[(i + 13) % created_ids.size()];
      egraph.merge(id1, id2);
    }

    // Benchmark the rebuild operation - this calls repair_hashcons -> canonicalize
    egraph.rebuild(ctx);
    benchmark::ClobberMemory();
  }
}

// Benchmark just the merge operations without rebuild
static void BM_EGraphMerge_Only(benchmark::State &state) {
  EGraph<BenchSymbol, BenchAnalysis> egraph;
  ProcessingContext<BenchSymbol> ctx;
  std::vector<EClassId> created_ids;

  // Pre-populate with nodes
  for (int i = 0; i < 100; ++i) {
    auto sym = static_cast<BenchSymbol>(i % 5);
    auto id = egraph.emplace(sym, i);
    created_ids.push_back(id);
  }

  for (int i = 0; i < 50; ++i) {
    auto sym = static_cast<BenchSymbol>(10 + (i % 5));
    std::vector<EClassId> children = {created_ids[i % created_ids.size()], created_ids[(i + 1) % created_ids.size()]};
    auto id = egraph.emplace(sym, memgraph::utils::small_vector<EClassId>(children.begin(), children.end()));
    created_ids.push_back(id);
  }

  for (auto _ : state) {
    // Reset egraph state for consistent benchmarking
    state.PauseTiming();
    EGraph<BenchSymbol, BenchAnalysis> fresh_egraph = egraph;
    state.ResumeTiming();

    // Benchmark just the merge operations
    for (int i = 0; i < 50; ++i) {
      auto id1 = created_ids[i % created_ids.size()];
      auto id2 = created_ids[(i + 7) % created_ids.size()];
      benchmark::DoNotOptimize(fresh_egraph.merge(id1, id2));
    }
  }
}

// Test idempotent rebuild performance
static void BM_EGraphRebuild_Idempotent(benchmark::State &state) {
  // Setup a complex egraph once
  EGraph<BenchSymbol, BenchAnalysis> egraph;
  ProcessingContext<BenchSymbol> ctx;
  std::vector<EClassId> created_ids;

  for (int i = 0; i < 50; ++i) {
    auto sym = static_cast<BenchSymbol>(i % 5);
    auto id = egraph.emplace(sym, i);
    created_ids.push_back(id);
  }

  for (int i = 0; i < 25; ++i) {
    auto sym = static_cast<BenchSymbol>(10 + (i % 5));
    std::vector<EClassId> children = {created_ids[i % created_ids.size()], created_ids[(i + 1) % created_ids.size()]};
    auto id = egraph.emplace(sym, memgraph::utils::small_vector<EClassId>(children.begin(), children.end()));
    created_ids.push_back(id);
  }

  for (int i = 0; i < 30; ++i) {
    auto id1 = created_ids[i % created_ids.size()];
    auto id2 = created_ids[(i + 5) % created_ids.size()];
    egraph.merge(id1, id2);
  }

  // Do initial rebuild
  egraph.rebuild(ctx);

  // Benchmark subsequent rebuilds (should be fast if idempotent)
  for (auto _ : state) {
    egraph.rebuild(ctx);
    benchmark::ClobberMemory();
  }
}

BENCHMARK(BM_EGraphRebuild_SmallGraph);
BENCHMARK(BM_EGraphRebuild_MediumGraph);
BENCHMARK(BM_EGraphRebuild_LargeGraph);
BENCHMARK(BM_EGraphMerge_Only);
BENCHMARK(BM_EGraphRebuild_Idempotent);

BENCHMARK_MAIN();
