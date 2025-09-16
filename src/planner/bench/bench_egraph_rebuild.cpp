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
#include <random>
#include <vector>
#include "planner/core/egraph.hpp"
#include "planner/core/processing_context.hpp"

using namespace memgraph::planner::core;

enum class TestSymbol : uint32_t { A, B, C, D, X, Y, Plus, Mul, Sub, Div, F, F2, G, H, Var, Const, Node0 = 1000 };

struct NoAnalysis {};

using TestEGraph = EGraph<TestSymbol, NoAnalysis>;
using TestContext = ProcessingContext<TestSymbol>;

// Benchmark leaf node creation (expected to be cheap)
static void BM_CreateLeafNodes(benchmark::State &state) {
  TestEGraph egraph;
  TestContext ctx;

  for (auto _ : state) {
    egraph.clear();

    // Create many leaf nodes with different disambiguators
    for (int i = 0; i < state.range(0); ++i) {
      benchmark::DoNotOptimize(egraph.emplace(TestSymbol::Var, i));
    }
  }

  state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BM_CreateLeafNodes)->Range(100, 10000)->Unit(benchmark::kMicrosecond);

// Benchmark composite node creation (expected to be cheap)
static void BM_CreateCompositeNodes(benchmark::State &state) {
  TestEGraph egraph;
  TestContext ctx;

  for (auto _ : state) {
    egraph.clear();

    // Create some leaf nodes first
    std::vector<EClassId> leaves;
    for (int i = 0; i < 10; ++i) {
      leaves.push_back(egraph.emplace(TestSymbol::Var, i));
    }

    // Create composite nodes using the leaves
    for (int i = 0; i < state.range(0); ++i) {
      auto left = leaves[i % leaves.size()];
      auto right = leaves[(i + 1) % leaves.size()];
      benchmark::DoNotOptimize(egraph.emplace(TestSymbol::Plus, {left, right}));
    }
  }

  state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BM_CreateCompositeNodes)->Range(100, 10000)->Unit(benchmark::kMicrosecond);

// Benchmark merging operations (expected to be cheap)
static void BM_MergeOperations(benchmark::State &state) {
  TestEGraph egraph;
  TestContext ctx;

  for (auto _ : state) {
    egraph.clear();

    // Create pairs of equivalent expressions to merge
    std::vector<std::pair<EClassId, EClassId>> merge_pairs;
    for (int i = 0; i < state.range(0); ++i) {
      auto a = egraph.emplace(TestSymbol::Var, i);
      auto b = egraph.emplace(TestSymbol::Var, i + 1000);  // Different but will be merged
      merge_pairs.emplace_back(a, b);
    }

    state.ResumeTiming();

    // Perform merges
    for (const auto &[a, b] : merge_pairs) {
      benchmark::DoNotOptimize(egraph.merge(a, b));
    }

    state.PauseTiming();
  }

  state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BM_MergeOperations)->Range(100, 5000)->Unit(benchmark::kMicrosecond);

// Benchmark rebuild without congruence (baseline)
static void BM_RebuildNoCongruence(benchmark::State &state) {
  TestEGraph egraph;
  TestContext ctx;

  for (auto _ : state) {
    egraph.clear();

    // Create nodes and perform merges to populate worklist
    std::vector<EClassId> nodes;
    for (int i = 0; i < state.range(0); ++i) {
      nodes.push_back(egraph.emplace(TestSymbol::Var, i));
    }

    // Merge some nodes to create worklist entries
    for (int i = 0; i < state.range(0) / 2; ++i) {
      egraph.merge(nodes[i], nodes[i + state.range(0) / 2]);
    }

    state.ResumeTiming();

    // Rebuild to process worklist
    egraph.rebuild(ctx);

    state.PauseTiming();
  }

  state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BM_RebuildNoCongruence)->Range(100, 2000)->Unit(benchmark::kMicrosecond);

// Benchmark rebuild with congruence updates
static void BM_RebuildWithCongruence(benchmark::State &state) {
  TestEGraph egraph;
  TestContext ctx;

  for (auto _ : state) {
    egraph.clear();

    // Create a structure that will have congruences after merging
    std::vector<EClassId> vars;
    std::vector<EClassId> exprs;

    // Create variables
    for (int i = 0; i < state.range(0); ++i) {
      vars.push_back(egraph.emplace(TestSymbol::Var, i));
    }

    // Create expressions that will become congruent
    for (int i = 0; i < state.range(0) / 2; ++i) {
      auto expr1 = egraph.emplace(TestSymbol::Plus, {vars[i], vars[i + 1]});
      auto expr2 = egraph.emplace(TestSymbol::Plus, {vars[i + state.range(0) / 2], vars[i + 1 + state.range(0) / 2]});
      exprs.push_back(expr1);
      exprs.push_back(expr2);
    }

    // Merge variables to create congruences
    for (int i = 0; i < state.range(0) / 2; ++i) {
      egraph.merge(vars[i], vars[i + state.range(0) / 2]);
      egraph.merge(vars[i + 1], vars[i + 1 + state.range(0) / 2]);
    }

    state.ResumeTiming();

    // Rebuild to detect and process congruences
    egraph.rebuild(ctx);

    state.PauseTiming();
  }

  state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BM_RebuildWithCongruence)->Range(50, 1000)->Unit(benchmark::kMicrosecond);

// Benchmark complex congruence scenario
static void BM_CongruenceHeavyWorkload(benchmark::State &state) {
  TestEGraph egraph;
  TestContext ctx;

  for (auto _ : state) {
    egraph.clear();

    // Create a deep expression tree that will have many congruences
    std::vector<EClassId> level0;

    // Level 0: variables
    for (int i = 0; i < state.range(0); ++i) {
      level0.push_back(egraph.emplace(TestSymbol::Var, i));
    }

    // Level 1: pairs
    std::vector<EClassId> level1;
    for (int i = 0; i < state.range(0) / 2; ++i) {
      level1.push_back(egraph.emplace(TestSymbol::Plus, {level0[i * 2], level0[i * 2 + 1]}));
    }

    // Level 2: pairs of pairs
    std::vector<EClassId> level2;
    for (int i = 0; i < state.range(0) / 4; ++i) {
      level2.push_back(egraph.emplace(TestSymbol::Mul, {level1[i * 2], level1[i * 2 + 1]}));
    }

    // Create duplicate structure
    std::vector<EClassId> dup_level0;
    for (int i = 0; i < state.range(0); ++i) {
      dup_level0.push_back(egraph.emplace(TestSymbol::Var, i + 10000));
    }

    std::vector<EClassId> dup_level1;
    for (int i = 0; i < state.range(0) / 2; ++i) {
      dup_level1.push_back(egraph.emplace(TestSymbol::Plus, {dup_level0[i * 2], dup_level0[i * 2 + 1]}));
    }

    std::vector<EClassId> dup_level2;
    for (int i = 0; i < state.range(0) / 4; ++i) {
      dup_level2.push_back(egraph.emplace(TestSymbol::Mul, {dup_level1[i * 2], dup_level1[i * 2 + 1]}));
    }

    // Merge corresponding variables to trigger cascading congruences
    for (int i = 0; i < state.range(0); ++i) {
      egraph.merge(level0[i], dup_level0[i]);
    }

    state.ResumeTiming();

    // This should trigger many congruence updates
    egraph.rebuild(ctx);

    state.PauseTiming();
  }

  state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BM_CongruenceHeavyWorkload)->Range(16, 256)->Unit(benchmark::kMicrosecond);

// Memory pressure benchmark
static void BM_MemoryPressure(benchmark::State &state) {
  for (auto _ : state) {
    TestEGraph egraph;
    TestContext ctx;

    // Create large e-graph
    std::vector<EClassId> nodes;
    for (int i = 0; i < state.range(0); ++i) {
      if (i < 1000) {
        nodes.push_back(egraph.emplace(TestSymbol::Var, i));
      } else {
        auto left = nodes[(i - 1000) % (i - 1000 + 1)];
        auto right = nodes[(i - 999) % (i - 999 + 1)];
        nodes.push_back(egraph.emplace(TestSymbol::Plus, {left, right}));
      }
    }

    // Perform many merges
    for (int i = 0; i < state.range(0) / 10; ++i) {
      egraph.merge(nodes[i * 10], nodes[i * 10 + 5]);
    }

    benchmark::DoNotOptimize(egraph.rebuild(ctx));

    // Force memory measurement
    benchmark::DoNotOptimize(egraph.num_nodes());
    benchmark::DoNotOptimize(egraph.num_classes());
  }

  state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BM_MemoryPressure)->Range(1000, 20000)->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();
