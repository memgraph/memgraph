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

#include "bench_common.hpp"

#include <array>
#include <optional>

using namespace memgraph::planner::bench;
using namespace memgraph::planner::bench::ranges;

// ============================================================================
// Index Building
// ============================================================================
//
// Measures: Cost of building the symbol index from scratch.
// Why it matters: Index rebuild happens after every e-graph modification.
// Variables: graph_size - affects index size and build time.

class IndexBuildFixture : public MatcherFixtureBase {
 protected:
  int64_t graph_size_ = 0;

  void SetUp(const benchmark::State &state) override {
    graph_size_ = state.range(0);
    SetupGraphAndMatcher([this](TestEGraph &g) { BuildIndependentAdds(g, graph_size_); });
  }
};

BENCHMARK_DEFINE_F(IndexBuildFixture, FullRebuild)(benchmark::State &state) {
  for (auto _ : state) {
    matcher_->rebuild_index();
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * graph_size_);
}

BENCHMARK_REGISTER_F(IndexBuildFixture, FullRebuild)
    ->Args({kSmall})
    ->Args({kMedium})
    ->Args({kLarge})
    ->Args({kMassive})
    ->ArgNames({"size"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Incremental Index Update
// ============================================================================
//
// Measures: Cost of updating index after adding new nodes (vs full rebuild).
// Why it matters: Incremental updates should be faster than full rebuilds.
// Variables: initial_size (existing graph), increment_size (nodes added).

class IncrementalUpdateFixture : public MatcherFixtureBase {
 protected:
  int64_t initial_size_ = 0;
  int64_t increment_size_ = 0;

  void SetUp(const benchmark::State &state) override {
    initial_size_ = state.range(0);
    increment_size_ = state.range(1);
    SetupGraphAndMatcher([this](TestEGraph &g) { BuildIndependentAdds(g, initial_size_); });
  }
};

BENCHMARK_DEFINE_F(IncrementalUpdateFixture, Update)(benchmark::State &state) {
  int64_t var_counter = initial_size_ * 2;
  for (auto _ : state) {
    state.PauseTiming();
    std::vector<EClassId> added_eclasses;
    for (int64_t i = 0; i < increment_size_; ++i) {
      auto var1 = egraph_.emplace(Op::Var, static_cast<uint64_t>(var_counter++)).eclass_id;
      auto var2 = egraph_.emplace(Op::Var, static_cast<uint64_t>(var_counter++)).eclass_id;
      auto add = egraph_.emplace(Op::Add, {var1, var2}).eclass_id;
      added_eclasses.append_range(std::array{var1, var2, add});
    }
    state.ResumeTiming();
    matcher_->rebuild_index(added_eclasses);
  }
  state.SetItemsProcessed(state.iterations() * increment_size_);
}

BENCHMARK_REGISTER_F(IncrementalUpdateFixture, Update)
    ->ArgsProduct({{kMedium, kLarge, kMassive}, {10, 50, 100}})
    ->ArgNames({"init", "incr"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Simple Pattern: Add(?x, ?y)
// ============================================================================
//
// Measures: Basic pattern matching throughput with two distinct variables.
// Why it matters: Baseline for pattern matching performance.
// Variables: graph_size (number of potential matches), context_mode (reuse).

class SimplePatternFixture : public MatcherFixtureBase {
 protected:
  std::optional<TestPattern> pattern_;
  int64_t graph_size_ = 0;
  int64_t context_mode_ = 0;

  void SetUp(const benchmark::State &state) override {
    graph_size_ = state.range(0);
    context_mode_ = state.range(1);
    SetupGraphAndMatcher([this](TestEGraph &g) { BuildIndependentAdds(g, graph_size_); });
    pattern_.emplace(PatternAdd());
  }
};

BENCHMARK_DEFINE_F(SimplePatternFixture, Match)(benchmark::State &state) {
  BenchmarkWithMatchContext(state, context_mode_, match_context_, [&](EMatchContext &ctx) {
    matcher_->match_into(*pattern_, ctx, matches_);
    benchmark::DoNotOptimize(matches_);
  });
  state.SetItemsProcessed(state.iterations() * graph_size_);
}

BENCHMARK_REGISTER_F(SimplePatternFixture, Match)
    ->ArgsProduct({{kSmall, kMedium, kLarge, kMassive}, {kFreshCtx, kReusedCtx}})
    ->ArgNames({"size", "ctx"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Deep Pattern: Neg(Neg(...Neg(?x)...))
// ============================================================================
//
// Measures: Cost of matching deeply nested patterns (chain of Neg nodes).
// Why it matters: Reveals O(n²) scaling issue - depth n pattern tries n
//   candidates, each doing O(k) work. See TODO.txt for details.
// Variables: pattern_depth (chain length), context_mode.

class DeepPatternFixture : public MatcherFixtureBase {
 protected:
  std::optional<TestPattern> pattern_;
  int64_t pattern_depth_ = 0;
  int64_t context_mode_ = 0;

  void SetUp(const benchmark::State &state) override {
    pattern_depth_ = state.range(0);
    context_mode_ = state.range(1);
    SetupGraphAndMatcher([this](TestEGraph &g) { BuildNegChain(g, pattern_depth_); });
    pattern_.emplace(PatternNestedNeg(static_cast<int>(pattern_depth_)));
  }
};

BENCHMARK_DEFINE_F(DeepPatternFixture, Match)(benchmark::State &state) {
  BenchmarkWithMatchContext(state, context_mode_, match_context_, [&](EMatchContext &ctx) {
    matcher_->match_into(*pattern_, ctx, matches_);
    benchmark::DoNotOptimize(matches_);
  });
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(DeepPatternFixture, Match)
    ->ArgsProduct({{1, 5, 10, 50, 100}, {kFreshCtx, kReusedCtx}})
    ->ArgNames({"depth", "ctx"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Same Variable: Add(?x, ?x)
// ============================================================================
//
// Measures: Cost of patterns where the same variable appears twice.
// Why it matters: Tests variable equality checking during matching.
// Variables: graph_size (half matches Add(x,x), half Add(x,y)).

class SameVariableFixture : public MatcherFixtureBase {
 protected:
  std::optional<TestPattern> pattern_;
  int64_t graph_size_ = 0;
  int64_t context_mode_ = 0;

  void SetUp(const benchmark::State &state) override {
    graph_size_ = state.range(0);
    context_mode_ = state.range(1);
    SetupGraphAndMatcher([this](TestEGraph &g) { BuildMixedAdds(g, graph_size_); });
    pattern_.emplace(PatternAddSameVar());
  }
};

BENCHMARK_DEFINE_F(SameVariableFixture, Match)(benchmark::State &state) {
  BenchmarkWithMatchContext(state, context_mode_, match_context_, [&](EMatchContext &ctx) {
    matcher_->match_into(*pattern_, ctx, matches_);
    benchmark::DoNotOptimize(matches_);
  });
  state.SetItemsProcessed(state.iterations() * graph_size_);
}

BENCHMARK_REGISTER_F(SameVariableFixture, Match)
    ->ArgsProduct({{kSmall, kMedium, kLarge}, {kFreshCtx, kReusedCtx}})
    ->ArgNames({"size", "ctx"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Merged E-Graph (multiple e-nodes per class)
// ============================================================================
//
// Measures: Matching cost when e-classes contain multiple e-nodes.
// Why it matters: After rewrite rules merge equivalences, e-classes grow.
//   Matcher must handle multiple representations per e-class.
// Variables: graph_size (Add and Mul nodes merged pairwise).

class MergedEGraphFixture : public MatcherFixtureBase {
 protected:
  std::optional<TestPattern> pattern_;
  int64_t graph_size_ = 0;
  int64_t context_mode_ = 0;

  void SetUp(const benchmark::State &state) override {
    graph_size_ = state.range(0);
    context_mode_ = state.range(1);
    SetupGraphAndMatcher([this](TestEGraph &g) { BuildMergedAddMul(g, graph_size_); });
    pattern_.emplace(PatternAdd());
  }
};

BENCHMARK_DEFINE_F(MergedEGraphFixture, Match)(benchmark::State &state) {
  BenchmarkWithMatchContext(state, context_mode_, match_context_, [&](EMatchContext &ctx) {
    matcher_->match_into(*pattern_, ctx, matches_);
    benchmark::DoNotOptimize(matches_);
  });
  state.SetItemsProcessed(state.iterations() * graph_size_);
}

BENCHMARK_REGISTER_F(MergedEGraphFixture, Match)
    ->ArgsProduct({{kSmall, kMedium, kLarge}, {kFreshCtx, kReusedCtx}})
    ->ArgNames({"size", "ctx"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Selective Pattern: Add(Neg(?x), ?y) - few matches among many nodes
// ============================================================================
//
// Measures: Cost when pattern has low selectivity (few matches, many nodes).
// Why it matters: Tests index lookup efficiency - should skip non-matching
//   nodes quickly without iterating entire graph.
// Variables: graph_size (many Adds, only ONE Neg to match).

class SelectivePatternFixture : public MatcherFixtureBase {
 protected:
  std::optional<TestPattern> pattern_;
  int64_t graph_size_ = 0;
  int64_t context_mode_ = 0;

  void SetUp(const benchmark::State &state) override {
    graph_size_ = state.range(0);
    context_mode_ = state.range(1);
    SetupGraphAndMatcher([this](TestEGraph &g) { BuildAddsWithOneNeg(g, graph_size_); });
    pattern_.emplace(PatternSelective());
  }
};

BENCHMARK_DEFINE_F(SelectivePatternFixture, Match)(benchmark::State &state) {
  BenchmarkWithMatchContext(state, context_mode_, match_context_, [&](EMatchContext &ctx) {
    matcher_->match_into(*pattern_, ctx, matches_);
    benchmark::DoNotOptimize(matches_);
  });
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(SelectivePatternFixture, Match)
    ->ArgsProduct({{kSmall, kMedium, kLarge}, {kFreshCtx, kReusedCtx}})
    ->ArgNames({"size", "ctx"})
    ->Unit(benchmark::kMicrosecond);
