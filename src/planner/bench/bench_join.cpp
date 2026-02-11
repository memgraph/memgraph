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

using namespace memgraph::planner::bench;
using namespace memgraph::planner::bench::ranges;

// ============================================================================
// Join Strategies
// ============================================================================
//
// Compares join strategies for multi-pattern rules:
// - HashJoinShared: Patterns share variables, O(n) matches via hash-join
// - HashJoinWide: Few shared x values, O(n²/k) matches where k = unique x's
// - Cartesian: No shared variables, O(n²) matches (worst case)

// Measures: Hash-join with 1:1 variable sharing (Add and Mul share same x,y).
// Expected: O(n) matches, linear scaling.

class HashJoinSharedFixture : public RewriterFixtureBase {
 protected:
  int64_t num_nodes_ = 0;
  TestRewriteRule rule_ = RuleMergeAddMul();

  void SetUp(const benchmark::State &state) override {
    num_nodes_ = state.range(0);
    ResetEGraph();
    BuildAddMulPairs(egraph_, num_nodes_);
    CreateMatcher();
  }
};

BENCHMARK_DEFINE_F(HashJoinSharedFixture, Match)(benchmark::State &state) {
  for (auto _ : state) {
    rewrite_context_.clear_new_eclasses();
    auto rewrites = rule_.apply(egraph_, *matcher_, rewrite_context_);
    benchmark::DoNotOptimize(rewrites);
  }
  state.SetItemsProcessed(state.iterations() * num_nodes_);
}

BENCHMARK_REGISTER_F(HashJoinSharedFixture, Match)
    ->Args({kSmall})
    ->Args({kMedium})
    ->Args({kLarge})
    ->Args({kXLarge})
    ->ArgNames({"nodes"})
    ->Unit(benchmark::kMicrosecond);

// Measures: Hash-join with many-to-many sharing (n nodes share k x values).
// Expected: O(n²/k) matches, super-linear growth.

class HashJoinWideFixture : public RewriterFixtureBase {
 protected:
  int64_t num_nodes_ = 0;
  int64_t num_unique_x_ = 0;
  TestRewriteRule rule_ = RuleWideJoin();

  void SetUp(const benchmark::State &state) override {
    num_nodes_ = state.range(0);
    num_unique_x_ = std::max(int64_t{1}, num_nodes_ / 10);
    ResetEGraph();
    BuildAddMulFewSharedX(egraph_, num_nodes_, num_unique_x_);
    CreateMatcher();
  }
};

BENCHMARK_DEFINE_F(HashJoinWideFixture, Match)(benchmark::State &state) {
  for (auto _ : state) {
    rewrite_context_.clear_new_eclasses();
    auto rewrites = rule_.apply(egraph_, *matcher_, rewrite_context_);
    benchmark::DoNotOptimize(rewrites);
  }
  auto matches_per_x = num_nodes_ / num_unique_x_;
  state.SetItemsProcessed(state.iterations() * matches_per_x * matches_per_x * num_unique_x_);
}

BENCHMARK_REGISTER_F(HashJoinWideFixture, Match)
    ->Args({kSmall})
    ->Args({kMedium})
    ->Args({500})
    ->ArgNames({"nodes"})
    ->Unit(benchmark::kMicrosecond);

// Measures: Cartesian product join (patterns have no shared variables).
// Expected: O(n²) matches, quadratic scaling - shows worst-case join cost.

class CartesianJoinFixture : public RewriterFixtureBase {
 protected:
  int64_t num_nodes_ = 0;
  TestRewriteRule rule_ = RuleCartesian();

  void SetUp(const benchmark::State &state) override {
    num_nodes_ = state.range(0);
    ResetEGraph();
    BuildAddNegDisjoint(egraph_, num_nodes_);
    CreateMatcher();
  }
};

BENCHMARK_DEFINE_F(CartesianJoinFixture, Match)(benchmark::State &state) {
  for (auto _ : state) {
    rewrite_context_.clear_new_eclasses();
    auto rewrites = rule_.apply(egraph_, *matcher_, rewrite_context_);
    benchmark::DoNotOptimize(rewrites);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(CartesianJoinFixture, Match)
    ->Args({kSmall})
    ->Args({kMedium})
    ->Args({500})
    ->ArgNames({"nodes"})
    ->Unit(benchmark::kMicrosecond);
