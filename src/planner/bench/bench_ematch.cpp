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
#include <random>
#include <vector>

#include "planner/pattern/matcher.hpp"

import memgraph.planner.core.eids;

using namespace memgraph::planner::core;

enum class Op : uint8_t { Add, Mul, Neg, Var, Const, F };

struct NoAnalysis {};

using TestEGraph = EGraph<Op, NoAnalysis>;
using TestPattern = Pattern<Op>;
using TestEMatcher = EMatcher<Op, NoAnalysis>;
using TestMatches = std::vector<PatternMatch>;

// Pattern variable constants for benchmarks
constexpr PatternVar kVarX{0};
constexpr PatternVar kVarY{1};

// Helper: Build a wide e-graph with N independent binary expressions
// Creates: Var(0), Var(1), ..., Var(N-1), then Add(Var(0), Var(1)), Add(Var(2), Var(3)), ...
static void BuildWideEGraph(TestEGraph &egraph, int64_t num_adds) {
  std::vector<EClassId> vars;
  vars.reserve(num_adds * 2);

  for (int64_t i = 0; i < num_adds * 2; ++i) {
    auto v = egraph.emplace(Op::Var, static_cast<uint64_t>(i));
    vars.push_back(v.eclass_id);
  }

  for (int64_t i = 0; i < num_adds; ++i) {
    egraph.emplace(Op::Add, {vars[i * 2], vars[i * 2 + 1]});
  }
}

// Helper: Build a deep e-graph with chain of nested expressions
// Creates: Neg(Neg(Neg(...Neg(Var(0))...))) with depth levels
static auto BuildDeepEGraph(TestEGraph &egraph, int64_t depth) -> EClassId {
  auto current = egraph.emplace(Op::Var, 0).eclass_id;

  for (int64_t i = 0; i < depth; ++i) {
    current = egraph.emplace(Op::Neg, {current}).eclass_id;
  }

  return current;
}

// Helper: Build a pattern with nested Neg operations
static auto BuildNestedNegPattern(int depth) -> TestPattern {
  auto builder = TestPattern::Builder{};
  auto x = builder.var(kVarX);
  auto current = x;

  for (int i = 0; i < depth; ++i) {
    current = builder.sym(Op::Neg, {current});
  }

  return std::move(builder).build();
}

// ============================================================================
// Benchmark: Index Building (via rebuild)
// ============================================================================

// Measure time to build symbol index on e-graphs of varying sizes
static void BM_EMatcher_BuildIndex(benchmark::State &state) {
  auto num_nodes = state.range(0);

  TestEGraph egraph;
  BuildWideEGraph(egraph, num_nodes);

  TestEMatcher ematcher(egraph);

  for (auto _ : state) {
    ematcher.rebuild_index();
    benchmark::ClobberMemory();
  }

  state.SetItemsProcessed(state.iterations() * num_nodes);
  state.counters["nodes"] = static_cast<double>(egraph.num_nodes());
  state.counters["classes"] = static_cast<double>(egraph.num_classes());
}

BENCHMARK(BM_EMatcher_BuildIndex)->Range(100, 50'000)->Unit(benchmark::kMicrosecond);

// ============================================================================
// Benchmark: Simple Pattern Matching (varying e-graph size)
// ============================================================================

// Match Add(?x, ?y) pattern against e-graphs of varying sizes
static void BM_EMatcher_MatchSimplePattern(benchmark::State &state) {
  auto num_adds = state.range(0);

  TestEGraph egraph;
  BuildWideEGraph(egraph, num_adds);

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  // Pattern: Add(?x, ?y)
  auto pattern = TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}});

  TestMatches matches;
  for (auto _ : state) {
    ematcher.match_into(pattern, ctx, matches);
    benchmark::DoNotOptimize(matches);
  }

  state.SetItemsProcessed(state.iterations() * num_adds);
  state.counters["expected_matches"] = static_cast<double>(num_adds);
}

BENCHMARK(BM_EMatcher_MatchSimplePattern)->Range(100, 50'000)->Unit(benchmark::kMicrosecond);

// ============================================================================
// Benchmark: Deep Pattern Matching (varying pattern depth)
// ============================================================================

// Match Neg(Neg(...Neg(?x)...)) pattern of varying depths (fresh context each time)
static void BM_EMatcher_MatchDeepPattern(benchmark::State &state) {
  auto depth = state.range(0);

  TestEGraph egraph;
  BuildDeepEGraph(egraph, depth);

  TestEMatcher ematcher(egraph);

  auto pattern = BuildNestedNegPattern(static_cast<int>(depth));

  for (auto _ : state) {
    EMatchContext ctx;  // Fresh context each iteration
    TestMatches matches;
    ematcher.match_into(pattern, ctx, matches);
    benchmark::DoNotOptimize(matches);
  }

  state.SetItemsProcessed(state.iterations());
  state.counters["depth"] = static_cast<double>(depth);
}

BENCHMARK(BM_EMatcher_MatchDeepPattern)->Range(1, 100)->Unit(benchmark::kMicrosecond);

// Match Neg(Neg(...Neg(?x)...)) pattern with reusable context
static void BM_EMatcher_MatchDeepPatternWithContext(benchmark::State &state) {
  auto depth = state.range(0);

  TestEGraph egraph;
  BuildDeepEGraph(egraph, depth);

  TestEMatcher ematcher(egraph);

  auto pattern = BuildNestedNegPattern(static_cast<int>(depth));

  // Create reusable context outside the benchmark loop
  EMatchContext ctx;
  TestMatches matches;

  for (auto _ : state) {
    ctx.clear();  // Reset buffers but keep capacity
    ematcher.match_into(pattern, ctx, matches);
    benchmark::DoNotOptimize(matches);
  }

  state.SetItemsProcessed(state.iterations());
  state.counters["depth"] = static_cast<double>(depth);
}

BENCHMARK(BM_EMatcher_MatchDeepPatternWithContext)->Range(1, 100)->Unit(benchmark::kMicrosecond);

// ============================================================================
// Benchmark: Variable Consistency (same variable used multiple times)
// ============================================================================

// Match Add(?x, ?x) - requires checking variable consistency
static void BM_EMatcher_MatchSameVariable(benchmark::State &state) {
  auto num_adds = state.range(0);

  TestEGraph egraph;

  // Create expressions where some have same children, some different
  std::vector<EClassId> vars;
  for (int64_t i = 0; i < num_adds; ++i) {
    vars.push_back(egraph.emplace(Op::Var, static_cast<uint64_t>(i)).eclass_id);
  }

  // Half with same children (will match), half with different (won't match)
  for (int64_t i = 0; i < num_adds / 2; ++i) {
    egraph.emplace(Op::Add, {vars[i], vars[i]});  // Add(x, x)
  }
  for (int64_t i = 0; i < num_adds / 2; ++i) {
    auto j = (i + 1) % (num_adds / 2);
    if (i != j) {
      egraph.emplace(Op::Add, {vars[i], vars[j]});  // Add(x, y) where x != y
    }
  }

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  // Pattern: Add(?x, ?x) - same variable twice
  auto pattern = TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarX}});

  TestMatches matches;
  for (auto _ : state) {
    ematcher.match_into(pattern, ctx, matches);
    benchmark::DoNotOptimize(matches);
  }

  state.SetItemsProcessed(state.iterations() * num_adds);
  state.counters["expected_matches"] = static_cast<double>(num_adds / 2);
}

BENCHMARK(BM_EMatcher_MatchSameVariable)->Range(100, 10'000)->Unit(benchmark::kMicrosecond);

// ============================================================================
// Benchmark: Multiple E-nodes per E-class (after merges)
// ============================================================================

// E-graph with merged e-classes (multiple e-nodes per class)
static void BM_EMatcher_MatchMergedEGraph(benchmark::State &state) {
  auto num_vars = state.range(0);

  TestEGraph egraph;
  ProcessingContext<Op> pctx;

  // Create variables and expressions
  std::vector<EClassId> vars;
  for (int64_t i = 0; i < num_vars; ++i) {
    vars.push_back(egraph.emplace(Op::Var, static_cast<uint64_t>(i)).eclass_id);
  }

  // Create Add and Mul expressions with same operands
  std::vector<EClassId> adds, muls;
  for (int64_t i = 0; i < num_vars - 1; ++i) {
    adds.push_back(egraph.emplace(Op::Add, {vars[i], vars[i + 1]}).eclass_id);
    muls.push_back(egraph.emplace(Op::Mul, {vars[i], vars[i + 1]}).eclass_id);
  }

  // Merge Add and Mul e-classes (simulating discovered equivalences)
  for (size_t i = 0; i < adds.size(); ++i) {
    egraph.merge(adds[i], muls[i]);
  }
  egraph.rebuild(pctx);

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  // Pattern: Add(?x, ?y) - will match merged e-classes
  auto pattern = TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}});

  TestMatches matches;
  for (auto _ : state) {
    ematcher.match_into(pattern, ctx, matches);
    benchmark::DoNotOptimize(matches);
  }

  state.SetItemsProcessed(state.iterations() * num_vars);
}

BENCHMARK(BM_EMatcher_MatchMergedEGraph)->Range(10, 1'000)->Unit(benchmark::kMicrosecond);

// ============================================================================
// Benchmark: Incremental Index Update
// ============================================================================

// Measure cost of incremental vs full rebuild
static void BM_EMatcher_IncrementalUpdate(benchmark::State &state) {
  auto initial_size = state.range(0);
  auto increment_size = 10;  // Add 10 new e-classes per iteration

  TestEGraph egraph;
  BuildWideEGraph(egraph, initial_size);

  TestEMatcher ematcher(egraph);

  for (auto _ : state) {
    state.PauseTiming();
    // Add new nodes
    std::vector<EClassId> new_eclasses;
    for (int i = 0; i < increment_size; ++i) {
      auto v1 = egraph.emplace(Op::Var, static_cast<uint64_t>(initial_size * 2 + i * 2)).eclass_id;
      auto v2 = egraph.emplace(Op::Var, static_cast<uint64_t>(initial_size * 2 + i * 2 + 1)).eclass_id;
      auto add = egraph.emplace(Op::Add, {v1, v2}).eclass_id;
      new_eclasses.push_back(v1);
      new_eclasses.push_back(v2);
      new_eclasses.push_back(add);
    }
    state.ResumeTiming();

    ematcher.rebuild_index(new_eclasses);
  }

  state.SetItemsProcessed(state.iterations() * increment_size);
  state.counters["initial_size"] = static_cast<double>(initial_size);
}

BENCHMARK(BM_EMatcher_IncrementalUpdate)->Range(1'000, 50'000)->Unit(benchmark::kMicrosecond);

// ============================================================================
// Benchmark: Worst Case - Many Potential Matches, Few Actual
// ============================================================================

// Pattern requires specific structure that few e-nodes have
static void BM_EMatcher_SelectivePattern(benchmark::State &state) {
  auto num_nodes = state.range(0);

  TestEGraph egraph;

  // Create many Var nodes
  std::vector<EClassId> vars;
  for (int64_t i = 0; i < num_nodes; ++i) {
    vars.push_back(egraph.emplace(Op::Var, static_cast<uint64_t>(i)).eclass_id);
  }

  // Create many Add nodes
  for (int64_t i = 0; i < num_nodes - 1; ++i) {
    egraph.emplace(Op::Add, {vars[i], vars[i + 1]});
  }

  // Create only ONE Neg node (at the end)
  auto neg = egraph.emplace(Op::Neg, {vars[0]}).eclass_id;
  egraph.emplace(Op::Add, {neg, vars[1]});

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  // Pattern: Add(Neg(?x), ?y) - only matches the single Add(Neg(...), ...) we created
  auto pattern = TestPattern::build(Op::Add, {Sym(Op::Neg, Var{kVarX}), Var{kVarY}});

  TestMatches matches;
  for (auto _ : state) {
    ematcher.match_into(pattern, ctx, matches);
    benchmark::DoNotOptimize(matches);
  }

  state.SetItemsProcessed(state.iterations());
  state.counters["total_adds"] = static_cast<double>(num_nodes);
  state.counters["expected_matches"] = 1.0;
}

BENCHMARK(BM_EMatcher_SelectivePattern)->Range(100, 10'000)->Unit(benchmark::kMicrosecond);

// ============================================================================
// Benchmark: Multiple Pattern Matching with Shared Context
// ============================================================================

// Match multiple patterns using the same reusable context
static void BM_EMatcher_MultiplePatternsSameContext(benchmark::State &state) {
  auto num_patterns = state.range(0);

  TestEGraph egraph;
  BuildWideEGraph(egraph, 100);  // Fixed size e-graph

  TestEMatcher ematcher(egraph);

  // Create patterns of varying depth
  std::vector<TestPattern> patterns;
  patterns.reserve(num_patterns);
  for (int i = 0; i < num_patterns; ++i) {
    patterns.push_back(BuildNestedNegPattern(i % 10 + 1));  // Depths 1-10
  }

  // Create reusable context
  EMatchContext ctx;
  TestMatches matches;

  for (auto _ : state) {
    size_t total_matches = 0;
    for (auto const &pattern : patterns) {
      ctx.clear();
      ematcher.match_into(pattern, ctx, matches);
      total_matches += matches.size();
    }
    benchmark::DoNotOptimize(total_matches);
  }

  state.SetItemsProcessed(state.iterations() * num_patterns);
}

BENCHMARK(BM_EMatcher_MultiplePatternsSameContext)->Range(10, 100)->Unit(benchmark::kMicrosecond);

// Compare: multiple patterns without shared context (fresh context each time)
static void BM_EMatcher_MultiplePatternsNoContext(benchmark::State &state) {
  auto num_patterns = state.range(0);

  TestEGraph egraph;
  BuildWideEGraph(egraph, 100);  // Fixed size e-graph

  TestEMatcher ematcher(egraph);

  // Create patterns of varying depth
  std::vector<TestPattern> patterns;
  patterns.reserve(num_patterns);
  for (int i = 0; i < num_patterns; ++i) {
    patterns.push_back(BuildNestedNegPattern(i % 10 + 1));  // Depths 1-10
  }

  for (auto _ : state) {
    size_t total_matches = 0;
    for (auto const &pattern : patterns) {
      EMatchContext ctx;  // Fresh context - allocates fresh each time
      TestMatches matches;
      ematcher.match_into(pattern, ctx, matches);
      total_matches += matches.size();
    }
    benchmark::DoNotOptimize(total_matches);
  }

  state.SetItemsProcessed(state.iterations() * num_patterns);
}

BENCHMARK(BM_EMatcher_MultiplePatternsNoContext)->Range(10, 100)->Unit(benchmark::kMicrosecond);
