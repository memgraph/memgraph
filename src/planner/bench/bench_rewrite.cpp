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

#include "bench_rewrite.hpp"

using namespace memgraph::planner::bench;

// ============================================================================
// Saturation (double-neg chains)
// ============================================================================
//
// Full saturation loop: matching, rule application, e-graph rebuild, termination detection.

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
    ->Name("Rewrite/Saturate/DoubleNeg")
    ->ArgsProduct({{kSmall, kMedium, kLarge}, {2, 4, 8, 16}})
    ->ArgNames({"chains", "depth"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Many Rules (scaling)
// ============================================================================

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
    ->Name("Rewrite/Saturate/ManyNoop")
    ->Args({1})
    ->Args({10})
    ->Args({50})
    ->Args({100})
    ->ArgNames({"rules"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Saturation Loop Overhead (no-op rule)
// ============================================================================

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
    ->Name("Rewrite/Saturate/LoopOverhead")
    ->Args({1})
    ->Args({10})
    ->Args({100})
    ->Args({1000})
    ->ArgNames({"max_iter"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Single Iteration (one pass, all rules)
// ============================================================================

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
    ->Name("Rewrite/IterateOnce/DoubleNeg")
    ->Args({kSmall})
    ->Args({kMedium})
    ->Args({kLarge})
    ->Args({kHuge})
    ->ArgNames({"chains"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Realistic End-to-End (mixed graph, multiple rule types)
// ============================================================================
//
// graph_size variables, with graph_size/4 double-neg chains (depth 2) and
// graph_size/4 Add/Mul pairs reusing the variables.

class RealisticFixture : public RewriterFixtureBase {
 protected:
  using benchmark::Fixture::SetUp;
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
    SetupGraphAndRewriter(
        [this](TestEGraph &g) {
          BuildNegChains(g, graph_size_ / 4, 2);
          BuildAddMulPairs(g, graph_size_ / 4);
        },
        rules_);
    state.ResumeTiming();
    auto result = rewriter_->saturate(RewriteConfig::Default());
    benchmark::DoNotOptimize(result);
  }
}

BENCHMARK_REGISTER_F(RealisticFixture, Saturate)
    ->Name("Rewrite/Saturate/Realistic")
    ->Args({kMedium})
    ->Args({kLarge})
    ->Args({kXLarge})
    ->Args({kHuge})
    ->ArgNames({"size"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// VM Match+Apply (single-pattern rule on double-neg chains)
// ============================================================================
//
// Items processed = num_chains * (chain_depth - 1) - the actual number of
// double-neg matches per chain. For depth=2 this equals num_chains; at higher
// depths the metric stays comparable across the sweep.

class RewriteFixture : public MatcherFixtureBase {
 protected:
  using benchmark::Fixture::SetUp;
  TestVMExecutor vm_executor_{egraph_};
  TestRewriteContext ctx_{egraph_};
  TestRewriteRule rule_ = RuleDoubleNeg();
  int64_t num_chains_ = 0;
  int64_t chain_depth_ = 0;

  void SetUp(const benchmark::State &state) override {
    num_chains_ = state.range(0);
    chain_depth_ = state.range(1);
    SetupGraphAndMatcher([this](TestEGraph &g) { BuildNegChains(g, num_chains_, chain_depth_); });
    vm_executor_ = TestVMExecutor(egraph_);
  }
};

BENCHMARK_DEFINE_F(RewriteFixture, Apply)(benchmark::State &state) {
  for (auto _ : state) {
    rule_.match(*matcher_, vm_executor_, ctx_.matcher_ctx());
    auto rewrites = rule_.apply(ctx_.rule_ctx(), ctx_.matcher_ctx());
    benchmark::DoNotOptimize(rewrites);
  }
  state.SetItemsProcessed(state.iterations() * num_chains_ * (chain_depth_ - 1));
}

BENCHMARK_REGISTER_F(RewriteFixture, Apply)
    ->Name("Rewrite/MatchApply/DoubleNeg")
    ->ArgsProduct({{kSmall, kMedium, kLarge}, {2, 4, 8}})
    ->Args({kXLarge, 2})
    ->Args({kHuge, 2})
    ->ArgNames({"chains", "depth"})
    ->Unit(benchmark::kMicrosecond);
