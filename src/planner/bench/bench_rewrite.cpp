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

#include <vector>

#include "planner/core/rewrite.hpp"

using namespace memgraph::planner::core;

enum class Op : uint8_t { Add, Mul, Neg, Var, Const };

struct NoAnalysis {};

using TestEGraph = EGraph<Op, NoAnalysis>;
using TestPattern = Pattern<Op>;
using TestRewriteRule = RewriteRule<Op>;
using TestRewriter = Rewriter<Op, NoAnalysis>;

// ============================================================================
// Helper Functions
// ============================================================================

// Build pattern for Neg(Neg(?x))
static auto MakeDoubleNegPattern() -> TestPattern {
  auto builder = TestPattern::Builder{};
  auto x = builder.var(0);
  auto neg_x = builder.sym(Op::Neg, {x});
  auto neg_neg_x = builder.sym(Op::Neg, {neg_x});
  return std::move(builder).build(neg_neg_x);
}

// Build pattern for Add(?x, ?y)
static auto MakeAddPattern() -> TestPattern {
  auto builder = TestPattern::Builder{};
  auto x = builder.var(0);
  auto y = builder.var(1);
  auto add = builder.sym(Op::Add, {x, y});
  return std::move(builder).build(add);
}

// Build pattern for Mul(?x, ?y)
static auto MakeMulPattern() -> TestPattern {
  auto builder = TestPattern::Builder{};
  auto x = builder.var(0);
  auto y = builder.var(1);
  auto mul = builder.sym(Op::Mul, {x, y});
  return std::move(builder).build(mul);
}

// Create double negation elimination rule: Neg(Neg(?x)) -> ?x
static auto MakeDoubleNegRule() -> TestRewriteRule {
  auto builder = TestRewriteRule::Builder{};
  builder.pattern(MakeDoubleNegPattern()).apply<NoAnalysis>([](auto &egraph, auto matches, auto &) -> std::size_t {
    std::size_t count = 0;
    for (auto const &match : matches) {
      auto x = match.subst.at(PatternVar{0});
      auto matched_eclass = match.pattern_roots[0];
      if (egraph.find(matched_eclass) != egraph.find(x)) {
        egraph.merge(matched_eclass, x);
        count++;
      }
    }
    return count;
  });
  return std::move(builder).build("double_negation");
}

// Create a no-op rule that matches but doesn't rewrite (for overhead testing)
static auto MakeNoOpAddRule() -> TestRewriteRule {
  auto builder = TestRewriteRule::Builder{};
  builder.pattern(MakeAddPattern()).apply<NoAnalysis>([](auto &, auto, auto &) -> std::size_t {
    return 0;  // Match but don't rewrite
  });
  return std::move(builder).build("noop_add");
}

// Create a two-pattern rule that merges Add and Mul with same operands
static auto MakeTwoPatternMergeRule() -> TestRewriteRule {
  auto builder = TestRewriteRule::Builder{};
  builder.pattern(MakeAddPattern(), "add")
      .pattern(MakeMulPattern(), "mul")
      .apply<NoAnalysis>([](auto &egraph, auto matches, auto &) -> std::size_t {
        std::size_t count = 0;
        for (auto const &match : matches) {
          auto add_eclass = match.pattern_roots[0];
          auto mul_eclass = match.pattern_roots[1];
          if (egraph.find(add_eclass) != egraph.find(mul_eclass)) {
            egraph.merge(add_eclass, mul_eclass);
            count++;
          }
        }
        return count;
      });
  return std::move(builder).build("merge_add_mul");
}

// Build e-graph with many double negations for repeated rewrites
static void BuildDoubleNegChains(TestEGraph &egraph, int64_t num_chains, int64_t chain_depth) {
  for (int64_t i = 0; i < num_chains; ++i) {
    auto v = egraph.emplace(Op::Var, static_cast<uint64_t>(i));
    auto current = v.current_eclassid;
    for (int64_t j = 0; j < chain_depth; ++j) {
      auto neg = egraph.emplace(Op::Neg, {current});
      current = neg.current_eclassid;
    }
  }
}

// Build e-graph with Add and Mul nodes sharing operands
static void BuildAddMulPairs(TestEGraph &egraph, int64_t num_pairs) {
  for (int64_t i = 0; i < num_pairs; ++i) {
    auto x = egraph.emplace(Op::Var, static_cast<uint64_t>(i * 2));
    auto y = egraph.emplace(Op::Var, static_cast<uint64_t>(i * 2 + 1));
    egraph.emplace(Op::Add, {x.current_eclassid, y.current_eclassid});
    egraph.emplace(Op::Mul, {x.current_eclassid, y.current_eclassid});
  }
}

// ============================================================================
// Benchmark Area 4: Dirty E-class Tracking / Matcher Rebuild
// ============================================================================
// Tests the cost of full matcher.rebuild() after each iteration.
// We want to see if incremental rebuild would help.

// Scenario: Many iterations with small numbers of rewrites per iteration
// This stresses the rebuild-per-iteration overhead
static void BM_Rewrite_ManyIterationsFewRewrites(benchmark::State &state) {
  auto num_chains = state.range(0);

  for (auto _ : state) {
    state.PauseTiming();
    TestEGraph egraph;
    // Create chains of depth 2 (each chain has one Neg(Neg(x)) to rewrite)
    BuildDoubleNegChains(egraph, num_chains, 2);
    TestRewriter rewriter(egraph);
    rewriter.add_rule(MakeDoubleNegRule());
    state.ResumeTiming();

    auto result = rewriter.saturate(RewriteConfig::Unlimited());
    benchmark::DoNotOptimize(result);
  }

  state.SetItemsProcessed(state.iterations() * num_chains);
  state.counters["chains"] = static_cast<double>(num_chains);
}

BENCHMARK(BM_Rewrite_ManyIterationsFewRewrites)->Range(10, 10'000)->Unit(benchmark::kMicrosecond);

// Scenario: Deep chains cause multiple iterations to reach saturation
// Each iteration does a small amount of work, but rebuild is called each time
static void BM_Rewrite_DeepChainsSaturation(benchmark::State &state) {
  auto chain_depth = state.range(0);

  for (auto _ : state) {
    state.PauseTiming();
    TestEGraph egraph;
    // 10 chains of variable depth
    BuildDoubleNegChains(egraph, 10, chain_depth);
    TestRewriter rewriter(egraph);
    rewriter.add_rule(MakeDoubleNegRule());
    state.ResumeTiming();

    auto result = rewriter.saturate(RewriteConfig::Unlimited());
    benchmark::DoNotOptimize(result);
    benchmark::ClobberMemory();
  }

  state.counters["depth"] = static_cast<double>(chain_depth);
}

BENCHMARK(BM_Rewrite_DeepChainsSaturation)->Range(4, 64)->Unit(benchmark::kMicrosecond);

// Isolated: Just measure matcher.rebuild() cost after merges
static void BM_Rewrite_MatcherRebuildAfterMerges(benchmark::State &state) {
  auto num_merges = state.range(0);

  TestEGraph egraph;
  ProcessingContext<Op> pctx;

  // Create nodes to merge
  std::vector<EClassId> vars;
  for (int64_t i = 0; i < num_merges * 2; ++i) {
    auto v = egraph.emplace(Op::Var, static_cast<uint64_t>(i));
    vars.push_back(v.current_eclassid);
  }

  // Create structure
  for (int64_t i = 0; i < num_merges; ++i) {
    egraph.emplace(Op::Add, {vars[i * 2], vars[i * 2 + 1]});
  }

  EMatcher<Op, NoAnalysis> matcher(egraph);

  for (auto _ : state) {
    state.PauseTiming();
    // Perform some merges to dirty the e-graph
    for (int64_t i = 0; i < std::min(num_merges / 10, int64_t{10}); ++i) {
      auto idx = i * 2;
      if (idx + 1 < static_cast<int64_t>(vars.size())) {
        egraph.merge(vars[idx], vars[idx + 1]);
      }
    }
    egraph.rebuild(pctx);
    state.ResumeTiming();

    // This is what we're measuring - full rebuild of matcher index
    matcher.rebuild();
    benchmark::ClobberMemory();
  }

  state.SetItemsProcessed(state.iterations());
  state.counters["eclasses"] = static_cast<double>(egraph.num_classes());
  state.counters["enodes"] = static_cast<double>(egraph.num_nodes());
}

BENCHMARK(BM_Rewrite_MatcherRebuildAfterMerges)->Range(100, 10'000)->Unit(benchmark::kMicrosecond);

// ============================================================================
// Benchmark Area 5: Type Erasure (std::any_cast)
// ============================================================================
// Tests the cost of any_cast<ApplyFn<Analysis>> per rule application.

// Scenario: Many rules, each applied once per iteration
// This stresses the any_cast overhead per rule
static void BM_Rewrite_ManyRulesAnyCast(benchmark::State &state) {
  auto num_rules = state.range(0);

  TestEGraph egraph;
  // Small e-graph with some Add nodes
  for (int64_t i = 0; i < 100; ++i) {
    auto x = egraph.emplace(Op::Var, static_cast<uint64_t>(i * 2));
    auto y = egraph.emplace(Op::Var, static_cast<uint64_t>(i * 2 + 1));
    egraph.emplace(Op::Add, {x.current_eclassid, y.current_eclassid});
  }

  TestRewriter rewriter(egraph);

  // Add many no-op rules (they match but don't rewrite, so we can iterate many times)
  for (int64_t i = 0; i < num_rules; ++i) {
    rewriter.add_rule(MakeNoOpAddRule());
  }

  RewriteConfig config;
  config.max_iterations = 100;  // Many iterations to stress the any_cast path

  for (auto _ : state) {
    auto result = rewriter.saturate(config);
    benchmark::DoNotOptimize(result);
  }

  // Each iteration applies num_rules rules, each requiring an any_cast
  state.SetItemsProcessed(state.iterations() * num_rules);
  state.counters["rules"] = static_cast<double>(num_rules);
}

BENCHMARK(BM_Rewrite_ManyRulesAnyCast)->Range(1, 100)->Unit(benchmark::kMicrosecond);

// Isolated: Direct measurement of apply() call overhead
static void BM_Rewrite_SingleRuleApplyOverhead(benchmark::State &state) {
  auto num_matches = state.range(0);

  TestEGraph egraph;

  // Create many Add nodes for matching
  for (int64_t i = 0; i < num_matches; ++i) {
    auto x = egraph.emplace(Op::Var, static_cast<uint64_t>(i * 2));
    auto y = egraph.emplace(Op::Var, static_cast<uint64_t>(i * 2 + 1));
    egraph.emplace(Op::Add, {x.current_eclassid, y.current_eclassid});
  }

  auto rule = MakeNoOpAddRule();
  EMatcher<Op, NoAnalysis> matcher(egraph);
  EMatchContext match_ctx;
  ProcessingContext<Op> proc_ctx;
  UnifiedMatchBuffers unified_buffers;

  for (auto _ : state) {
    // Measure the rule.apply() call which includes the any_cast
    auto rewrites = rule.apply<NoAnalysis>(egraph, matcher, match_ctx, proc_ctx, unified_buffers);
    benchmark::DoNotOptimize(rewrites);
  }

  state.SetItemsProcessed(state.iterations());
  state.counters["matches"] = static_cast<double>(num_matches);
}

BENCHMARK(BM_Rewrite_SingleRuleApplyOverhead)->Range(10, 10'000)->Unit(benchmark::kMicrosecond);

// ============================================================================
// Benchmark Area 6: Memory Allocation in apply() for Multi-Pattern Rules
// ============================================================================
// Tests the cost of allocating std::vector<UnifiedMatch> per pattern.

// Scenario: Two-pattern rule with many matches - allocates new_unified per pattern
static void BM_Rewrite_TwoPatternRuleAllocation(benchmark::State &state) {
  auto num_pairs = state.range(0);

  for (auto _ : state) {
    state.PauseTiming();
    TestEGraph egraph;
    BuildAddMulPairs(egraph, num_pairs);
    TestRewriter rewriter(egraph);
    rewriter.add_rule(MakeTwoPatternMergeRule());
    state.ResumeTiming();

    auto result = rewriter.saturate(RewriteConfig::Unlimited());
    benchmark::DoNotOptimize(result);
  }

  state.SetItemsProcessed(state.iterations() * num_pairs);
  state.counters["pairs"] = static_cast<double>(num_pairs);
}

BENCHMARK(BM_Rewrite_TwoPatternRuleAllocation)->Range(10, 5'000)->Unit(benchmark::kMicrosecond);

// Scenario: Three-pattern rule (more allocations per apply)
static void BM_Rewrite_ThreePatternRuleAllocation(benchmark::State &state) {
  auto num_triples = state.range(0);

  // Create a three-pattern rule: Add(?x,?y), Mul(?x,?y), Neg(?x)
  auto builder = TestRewriteRule::Builder{};
  builder.pattern(MakeAddPattern(), "add").pattern(MakeMulPattern(), "mul");

  // Third pattern: Neg(?x) - shares variable with first two
  auto neg_builder = TestPattern::Builder{};
  auto nx = neg_builder.var(0);
  auto neg = neg_builder.sym(Op::Neg, {nx});
  builder.pattern(std::move(neg_builder).build(neg), "neg");

  builder.apply<NoAnalysis>([](auto &, auto matches, auto &) -> std::size_t {
    // Just count matches, don't actually merge
    return matches.size() > 0 ? 0 : 0;
  });

  auto three_pattern_rule = std::move(builder).build("three_pattern");

  for (auto _ : state) {
    state.PauseTiming();
    TestEGraph egraph;

    // Create nodes that match all three patterns
    for (int64_t i = 0; i < num_triples; ++i) {
      auto x = egraph.emplace(Op::Var, static_cast<uint64_t>(i * 2));
      auto y = egraph.emplace(Op::Var, static_cast<uint64_t>(i * 2 + 1));
      egraph.emplace(Op::Add, {x.current_eclassid, y.current_eclassid});
      egraph.emplace(Op::Mul, {x.current_eclassid, y.current_eclassid});
      egraph.emplace(Op::Neg, {x.current_eclassid});
    }

    TestRewriter rewriter(egraph);
    rewriter.add_rule(TestRewriteRule{three_pattern_rule});  // Copy the rule
    state.ResumeTiming();

    auto result = rewriter.saturate(RewriteConfig::Unlimited());
    benchmark::DoNotOptimize(result);
  }

  state.SetItemsProcessed(state.iterations() * num_triples);
  state.counters["triples"] = static_cast<double>(num_triples);
}

BENCHMARK(BM_Rewrite_ThreePatternRuleAllocation)->Range(10, 2'000)->Unit(benchmark::kMicrosecond);

// Isolated: Measure just the join/allocation portion with constrained matching
static void BM_Rewrite_MultiPatternJoinOverhead(benchmark::State &state) {
  auto num_matches = state.range(0);

  TestEGraph egraph;
  BuildAddMulPairs(egraph, num_matches);

  auto rule = MakeTwoPatternMergeRule();
  EMatcher<Op, NoAnalysis> matcher(egraph);
  EMatchContext match_ctx;
  ProcessingContext<Op> proc_ctx;
  UnifiedMatchBuffers unified_buffers;

  for (auto _ : state) {
    // Measure the multi-pattern join in apply()
    auto rewrites = rule.apply<NoAnalysis>(egraph, matcher, match_ctx, proc_ctx, unified_buffers);
    benchmark::DoNotOptimize(rewrites);
  }

  state.SetItemsProcessed(state.iterations());
  state.counters["matches_per_pattern"] = static_cast<double>(num_matches);
}

BENCHMARK(BM_Rewrite_MultiPatternJoinOverhead)->Range(10, 5'000)->Unit(benchmark::kMicrosecond);

// ============================================================================
// Comparative Benchmarks
// ============================================================================

// Compare: Single iteration vs full saturation (shows per-iteration overhead)
static void BM_Rewrite_SingleIteration(benchmark::State &state) {
  auto num_chains = state.range(0);

  TestEGraph egraph;
  BuildDoubleNegChains(egraph, num_chains, 2);
  TestRewriter rewriter(egraph);
  rewriter.add_rule(MakeDoubleNegRule());

  for (auto _ : state) {
    auto rewrites = rewriter.apply_once();
    benchmark::DoNotOptimize(rewrites);
  }

  state.SetItemsProcessed(state.iterations());
  state.counters["chains"] = static_cast<double>(num_chains);
}

BENCHMARK(BM_Rewrite_SingleIteration)->Range(10, 10'000)->Unit(benchmark::kMicrosecond);

// End-to-end: Realistic saturation workload
static void BM_Rewrite_RealisticSaturation(benchmark::State &state) {
  auto graph_size = state.range(0);

  for (auto _ : state) {
    state.PauseTiming();
    TestEGraph egraph;

    // Mix of different node types
    std::vector<EClassId> vars;
    for (int64_t i = 0; i < graph_size; ++i) {
      auto v = egraph.emplace(Op::Var, static_cast<uint64_t>(i));
      vars.push_back(v.current_eclassid);
    }

    // Create some double negations
    for (int64_t i = 0; i < graph_size / 4; ++i) {
      auto neg1 = egraph.emplace(Op::Neg, {vars[i]});
      egraph.emplace(Op::Neg, {neg1.current_eclassid});
    }

    // Create some Add/Mul pairs
    for (int64_t i = 0; i < graph_size / 4; ++i) {
      auto idx = graph_size / 2 + i;
      if (idx + 1 < static_cast<int64_t>(vars.size())) {
        egraph.emplace(Op::Add, {vars[idx], vars[idx + 1]});
        egraph.emplace(Op::Mul, {vars[idx], vars[idx + 1]});
      }
    }

    TestRewriter rewriter(egraph);
    rewriter.add_rule(MakeDoubleNegRule());
    rewriter.add_rule(MakeTwoPatternMergeRule());
    state.ResumeTiming();

    auto result = rewriter.saturate(RewriteConfig::Default());
    benchmark::DoNotOptimize(result);
  }

  state.counters["size"] = static_cast<double>(graph_size);
}

BENCHMARK(BM_Rewrite_RealisticSaturation)->Range(100, 10'000)->Unit(benchmark::kMicrosecond);
