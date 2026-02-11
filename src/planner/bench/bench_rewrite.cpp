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

#include <vector>

using namespace memgraph::planner::bench;
using namespace memgraph::planner::bench::ranges;

// ============================================================================
// Saturation (double-neg chains)
// ============================================================================
//
// Measures: End-to-end equality saturation with double negation elimination.
// Why it matters: Tests the complete rewrite loop - matching, rule application,
//   e-graph rebuild, and termination detection.
// Variables: num_chains (independent chains), chain_depth (Neg nesting).

class SaturationFixture : public RewriterFixtureBase {
 protected:
  TestRuleSet rules_;
  int64_t num_chains_ = 0;
  int64_t chain_depth_ = 0;

  void SetUp(const benchmark::State &state) override {
    num_chains_ = state.range(0);
    chain_depth_ = state.range(1);
    rules_ = TestRuleSet::Build(RuleDoubleNeg());
    SetupGraphAndRewriter([this](TestEGraph &g) { BuildNegChains(g, num_chains_, chain_depth_); }, rules_);
  }
};

BENCHMARK_DEFINE_F(SaturationFixture, Saturate)(benchmark::State &state) {
  for (auto _ : state) {
    state.PauseTiming();
    SetupGraphAndRewriter([this](TestEGraph &g) { BuildNegChains(g, num_chains_, chain_depth_); }, rules_);
    state.ResumeTiming();
    auto result = rewriter_->saturate(RewriteConfig::Unlimited());
    benchmark::DoNotOptimize(result);
  }
  state.SetItemsProcessed(state.iterations() * num_chains_);
}

BENCHMARK_REGISTER_F(SaturationFixture, Saturate)
    ->ArgsProduct({{kSmall, kMedium, kLarge}, {2, 4, 8, 16}})
    ->ArgNames({"chains", "depth"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Many Rules (scaling)
// ============================================================================
//
// Measures: Saturation cost as number of rules increases.
// Why it matters: Real optimizers have many rules; need linear scaling.
// Variables: num_rules (rules in the ruleset).

class ManyRulesFixture : public RewriterFixtureBase {
 protected:
  TestRuleSet rules_;
  int64_t num_rules_ = 0;

  void SetUp(const benchmark::State &state) override {
    num_rules_ = state.range(0);
    auto builder = TestRuleSet::Builder{};
    for (int64_t i = 0; i < num_rules_; ++i) {
      builder.add_rule(RuleNoOp());
    }
    rules_ = std::move(builder).build();
    SetupGraphAndRewriter([](TestEGraph &g) { BuildIndependentAdds(g, kMedium); }, rules_);
  }
};

BENCHMARK_DEFINE_F(ManyRulesFixture, Saturate)(benchmark::State &state) {
  RewriteConfig config;
  config.max_iterations = 100;
  for (auto _ : state) {
    auto result = rewriter_->saturate(config);
    benchmark::DoNotOptimize(result);
  }
  state.SetItemsProcessed(state.iterations() * num_rules_);
}

BENCHMARK_REGISTER_F(ManyRulesFixture, Saturate)
    ->Args({1})
    ->Args({10})
    ->Args({50})
    ->Args({100})
    ->ArgNames({"rules"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Saturation Loop Overhead
// ============================================================================
//
// Measures: Per-iteration overhead of the saturation loop itself.
// Why it matters: Even with no-op rules, loop has bookkeeping costs.
// Variables: max_iterations (iterations before termination).

class SaturationLoopFixture : public RewriterFixtureBase {
 protected:
  TestRuleSet rules_;
  int64_t max_iterations_ = 0;

  void SetUp(const benchmark::State &state) override {
    max_iterations_ = state.range(0);
    rules_ = TestRuleSet::Build(RuleNoOp());
    SetupGraphAndRewriter([](TestEGraph &g) { BuildIndependentAdds(g, kMedium); }, rules_);
  }
};

BENCHMARK_DEFINE_F(SaturationLoopFixture, Overhead)(benchmark::State &state) {
  RewriteConfig config;
  config.max_iterations = static_cast<std::size_t>(max_iterations_);
  for (auto _ : state) {
    auto result = rewriter_->saturate(config);
    benchmark::DoNotOptimize(result);
  }
  state.SetItemsProcessed(state.iterations() * max_iterations_);
}

BENCHMARK_REGISTER_F(SaturationLoopFixture, Overhead)
    ->Args({1})
    ->Args({10})
    ->Args({100})
    ->Args({1000})
    ->ArgNames({"max_iter"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Single Iteration
// ============================================================================
//
// Measures: Cost of one saturation iteration (all rules applied once).
// Why it matters: Isolates per-iteration cost without loop overhead.
// Variables: num_chains (graph size).

class SingleIterationFixture : public RewriterFixtureBase {
 protected:
  TestRuleSet rules_;
  int64_t num_chains_ = 0;

  void SetUp(const benchmark::State &state) override {
    num_chains_ = state.range(0);
    rules_ = TestRuleSet::Build(RuleDoubleNeg());
    SetupGraphAndRewriter([this](TestEGraph &g) { BuildNegChains(g, num_chains_, 2); }, rules_);
  }
};

BENCHMARK_DEFINE_F(SingleIterationFixture, Once)(benchmark::State &state) {
  for (auto _ : state) {
    auto rewrites = rewriter_->iterate_once();
    benchmark::DoNotOptimize(rewrites);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(SingleIterationFixture, Once)
    ->Args({kSmall})
    ->Args({kMedium})
    ->Args({kLarge})
    ->Args({kHuge})
    ->ArgNames({"chains"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Realistic End-to-End
// ============================================================================
//
// Measures: Complete saturation with multiple rule types on mixed graph.
// Why it matters: Simulates real optimizer with diverse rewrites.
// Variables: graph_size (variables, with 1/4 double-negs, 1/4 Add/Mul pairs).

class RealisticFixture : public benchmark::Fixture {
 protected:
  TestRuleSet rules_;
  int64_t graph_size_ = 0;

  void SetUp(const benchmark::State &state) override {
    graph_size_ = state.range(0);
    rules_ = TestRuleSet::Build(RuleDoubleNeg(), RuleMergeAddMul());
  }
};

BENCHMARK_DEFINE_F(RealisticFixture, Saturate)(benchmark::State &state) {
  for (auto _ : state) {
    state.PauseTiming();
    TestEGraph egraph;
    std::vector<EClassId> variables;
    for (int64_t i = 0; i < graph_size_; ++i) {
      variables.push_back(egraph.emplace(Op::Var, static_cast<uint64_t>(i)).eclass_id);
    }
    // Create some double negations
    for (int64_t i = 0; i < graph_size_ / 4; ++i) {
      auto neg1 = egraph.emplace(Op::Neg, {variables[static_cast<std::size_t>(i)]}).eclass_id;
      egraph.emplace(Op::Neg, {neg1});
    }
    // Create some Add/Mul pairs
    for (int64_t i = 0; i < graph_size_ / 4; ++i) {
      auto idx = static_cast<std::size_t>(graph_size_ / 2 + i);
      if (idx + 1 < variables.size()) {
        egraph.emplace(Op::Add, {variables[idx], variables[idx + 1]});
        egraph.emplace(Op::Mul, {variables[idx], variables[idx + 1]});
      }
    }
    TestRewriter rewriter(egraph, rules_);
    state.ResumeTiming();
    auto result = rewriter.saturate(RewriteConfig::Default());
    benchmark::DoNotOptimize(result);
  }
}

BENCHMARK_REGISTER_F(RealisticFixture, Saturate)
    ->Args({kMedium})
    ->Args({kLarge})
    ->Args({kXLarge})
    ->Args({kHuge})
    ->ArgNames({"size"})
    ->Unit(benchmark::kMicrosecond);
