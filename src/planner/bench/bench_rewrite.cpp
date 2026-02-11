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

#include "planner/rewrite/rewriter.hpp"

using namespace memgraph::planner::core;

enum class Op : uint8_t { Add, Mul, Neg, Var, Const };

struct NoAnalysis {};

using TestEGraph = EGraph<Op, NoAnalysis>;
using TestPattern = Pattern<Op>;
using TestRewriteRule = RewriteRule<Op, NoAnalysis>;
using TestRuleSet = RuleSet<Op, NoAnalysis>;
using TestRewriter = Rewriter<Op, NoAnalysis>;
using TestRuleContext = RuleContext<Op, NoAnalysis>;
using TestRewriteContext = RewriteContext;

// ============================================================================
// Named Constants
// ============================================================================

// Standard pattern variable constants for benchmarks
constexpr PatternVar kVarX{0};
constexpr PatternVar kVarY{1};

// Explicit root bindings for patterns
constexpr PatternVar kVarDoubleNegRoot{10};
constexpr PatternVar kVarAddRoot{11};
constexpr PatternVar kVarMulRoot{12};

// ============================================================================
// Helper Functions
// ============================================================================

// Build pattern for Neg(Neg(?x)) with explicit root binding
static auto MakeDoubleNegPattern() -> TestPattern {
  return TestPattern::build(Op::Neg, {Sym(Op::Neg, Var{kVarX})}, kVarDoubleNegRoot);
}

// Build pattern for Add(?x, ?y)
static auto MakeAddPattern() -> TestPattern { return TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}}); }

// Build pattern for Mul(?x, ?y)
static auto MakeMulPattern() -> TestPattern { return TestPattern::build(Op::Mul, {Var{kVarX}, Var{kVarY}}); }

// Create double negation elimination rule: Neg(Neg(?x)) -> ?x
static auto MakeDoubleNegRule() -> TestRewriteRule {
  return TestRewriteRule::Builder{"double_negation"}
      .pattern(MakeDoubleNegPattern())
      .apply([](TestRuleContext &ctx, Match const &match) { ctx.merge(match[kVarDoubleNegRoot], match[kVarX]); });
}

// Create a no-op rule that matches but doesn't rewrite (for overhead testing)
static auto MakeNoOpAddRule() -> TestRewriteRule {
  return TestRewriteRule::Builder{"noop_add"}.pattern(MakeAddPattern()).apply([](TestRuleContext &, Match const &) {});
}

// Create a two-pattern rule that merges Add and Mul with same operands
// For multi-pattern rules, we use explicit root bindings to access each pattern's root
// Uses kVarAddRoot and kVarMulRoot defined at file scope
static auto MakeTwoPatternMergeRule() -> TestRewriteRule {
  return TestRewriteRule::Builder{"merge_add_mul"}
      .pattern(TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}}, kVarAddRoot), "add")
      .pattern(TestPattern::build(Op::Mul, {Var{kVarX}, Var{kVarY}}, kVarMulRoot), "mul")
      .apply([](TestRuleContext &ctx, Match const &match) { ctx.merge(match[kVarAddRoot], match[kVarMulRoot]); });
}

// Build e-graph with many double negations for repeated rewrites
static void BuildDoubleNegChains(TestEGraph &egraph, int64_t num_chains, int64_t chain_depth) {
  for (int64_t i = 0; i < num_chains; ++i) {
    auto v = egraph.emplace(Op::Var, static_cast<uint64_t>(i));
    auto current = v.eclass_id;
    for (int64_t j = 0; j < chain_depth; ++j) {
      auto neg = egraph.emplace(Op::Neg, {current});
      current = neg.eclass_id;
    }
  }
}

// Build e-graph with Add and Mul nodes sharing operands
static void BuildAddMulPairs(TestEGraph &egraph, int64_t num_pairs) {
  for (int64_t i = 0; i < num_pairs; ++i) {
    auto x = egraph.emplace(Op::Var, static_cast<uint64_t>(i * 2));
    auto y = egraph.emplace(Op::Var, static_cast<uint64_t>(i * 2 + 1));
    egraph.emplace(Op::Add, {x.eclass_id, y.eclass_id});
    egraph.emplace(Op::Mul, {x.eclass_id, y.eclass_id});
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

  auto rules = TestRuleSet::Build(MakeDoubleNegRule());

  for (auto _ : state) {
    state.PauseTiming();
    TestEGraph egraph;
    // Create chains of depth 2 (each chain has one Neg(Neg(x)) to rewrite)
    BuildDoubleNegChains(egraph, num_chains, 2);
    TestRewriter rewriter(egraph, rules);
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

  auto rules = TestRuleSet::Build(MakeDoubleNegRule());

  for (auto _ : state) {
    state.PauseTiming();
    TestEGraph egraph;
    // 10 chains of variable depth
    BuildDoubleNegChains(egraph, 10, chain_depth);
    TestRewriter rewriter(egraph, rules);
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
    vars.push_back(v.eclass_id);
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
    matcher.rebuild_index();
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
    auto x = egraph.emplace(Op::Var, static_cast<uint64_t>(i * 2)).eclass_id;
    auto y = egraph.emplace(Op::Var, static_cast<uint64_t>(i * 2 + 1)).eclass_id;
    egraph.emplace(Op::Add, {x, y});
  }

  // Add many no-op rules (they match but don't rewrite, so we can iterate many times)
  auto rules_builder = TestRuleSet::Builder{};
  for (int64_t i = 0; i < num_rules; ++i) {
    rules_builder.add_rule(MakeNoOpAddRule());
  }
  auto rules = std::move(rules_builder).build();

  TestRewriter rewriter(egraph, std::move(rules));

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
    auto x = egraph.emplace(Op::Var, static_cast<uint64_t>(i * 2)).eclass_id;
    auto y = egraph.emplace(Op::Var, static_cast<uint64_t>(i * 2 + 1)).eclass_id;
    egraph.emplace(Op::Add, {x, y});
  }

  auto rule = MakeNoOpAddRule();
  EMatcher<Op, NoAnalysis> matcher(egraph);
  TestRewriteContext ctx;

  for (auto _ : state) {
    ctx.clear_new_eclasses();
    // Measure the rule.apply() call which includes the any_cast
    auto rewrites = rule.apply(egraph, matcher, ctx);
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

  auto rules = TestRuleSet::Build(MakeTwoPatternMergeRule());

  for (auto _ : state) {
    state.PauseTiming();
    TestEGraph egraph;
    BuildAddMulPairs(egraph, num_pairs);
    TestRewriter rewriter(egraph, rules);
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
  auto three_pattern_rule = TestRewriteRule::Builder{"three_pattern"}
                                .pattern(MakeAddPattern(), "add")
                                .pattern(MakeMulPattern(), "mul")
                                .pattern(TestPattern::build(Op::Neg, {Var{kVarX}}), "neg")
                                .apply([](TestRuleContext &, Match const &) {});
  auto rules = TestRuleSet::Build(std::move(three_pattern_rule));

  for (auto _ : state) {
    state.PauseTiming();
    TestEGraph egraph;

    // Create nodes that match all three patterns
    for (int64_t i = 0; i < num_triples; ++i) {
      auto x = egraph.emplace(Op::Var, static_cast<uint64_t>(i * 2)).eclass_id;
      auto y = egraph.emplace(Op::Var, static_cast<uint64_t>(i * 2 + 1)).eclass_id;
      egraph.emplace(Op::Add, {x, y});
      egraph.emplace(Op::Mul, {x, y});
      egraph.emplace(Op::Neg, {x});
    }

    TestRewriter rewriter(egraph, rules);
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
  TestRewriteContext ctx;

  for (auto _ : state) {
    ctx.clear_new_eclasses();
    // Measure the multi-pattern join in apply()
    auto rewrites = rule.apply(egraph, matcher, ctx);
    benchmark::DoNotOptimize(rewrites);
  }

  state.SetItemsProcessed(state.iterations());
  state.counters["matches_per_pattern"] = static_cast<double>(num_matches);
}

BENCHMARK(BM_Rewrite_MultiPatternJoinOverhead)->Range(10, 5'000)->Unit(benchmark::kMicrosecond);

// Benchmark 3-pattern chain join: Add(?x,?y) + Mul(?x,?z) + Neg(?x)
// Each pattern uses different op, ensuring disjoint matching
// Tests join performance at stride=3 with varying match counts
static void BM_Rewrite_ThreePatternJoin(benchmark::State &state) {
  auto num_matches = state.range(0);

  // Three patterns sharing ?x: Add(?x,?y), Mul(?x,?z), Neg(?x)
  constexpr PatternVar kVarZ{2};
  constexpr PatternVar kAddRoot{10};
  constexpr PatternVar kMulRoot{11};
  constexpr PatternVar kNegRoot{12};

  auto build_rule = [&]() {
    return TestRewriteRule::Builder{"three_pattern"}
        .pattern(TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}}, kAddRoot), "add")
        .pattern(TestPattern::build(Op::Mul, {Var{kVarX}, Var{kVarZ}}, kMulRoot), "mul")
        .pattern(TestPattern::build(Op::Neg, {Var{kVarX}}, kNegRoot), "neg")
        .apply([](TestRuleContext &, Match const &) {});
  };
  auto rule = build_rule();

  for (auto _ : state) {
    state.PauseTiming();
    TestEGraph egraph;

    // Create match sets: Add(x,y), Mul(x,z), Neg(x) for each unique x
    for (int64_t m = 0; m < num_matches; ++m) {
      auto x = egraph.emplace(Op::Var, static_cast<uint64_t>(m)).eclass_id;
      auto y = egraph.emplace(Op::Var, static_cast<uint64_t>(10000 + m)).eclass_id;
      auto z = egraph.emplace(Op::Var, static_cast<uint64_t>(20000 + m)).eclass_id;
      egraph.emplace(Op::Add, {x, y});
      egraph.emplace(Op::Mul, {x, z});
      egraph.emplace(Op::Neg, {x});
    }

    EMatcher<Op, NoAnalysis> matcher(egraph);
    TestRewriteContext ctx;
    state.ResumeTiming();

    ctx.clear_new_eclasses();
    auto rewrites = rule.apply(egraph, matcher, ctx);
    benchmark::DoNotOptimize(rewrites);
  }

  state.SetItemsProcessed(state.iterations() * num_matches);
  state.counters["matches"] = static_cast<double>(num_matches);
  state.counters["stride"] = 3.0;
}

BENCHMARK(BM_Rewrite_ThreePatternJoin)->Range(100, 10'000)->Unit(benchmark::kMicrosecond);

// ============================================================================
// Comparative Benchmarks
// ============================================================================

// Compare: Single iteration vs full saturation (shows per-iteration overhead)
static void BM_Rewrite_SingleIteration(benchmark::State &state) {
  auto num_chains = state.range(0);

  TestEGraph egraph;
  BuildDoubleNegChains(egraph, num_chains, 2);
  auto rules = TestRuleSet::Build(MakeDoubleNegRule());
  TestRewriter rewriter(egraph, std::move(rules));

  for (auto _ : state) {
    auto rewrites = rewriter.iterate_once();
    benchmark::DoNotOptimize(rewrites);
  }

  state.SetItemsProcessed(state.iterations());
  state.counters["chains"] = static_cast<double>(num_chains);
}

BENCHMARK(BM_Rewrite_SingleIteration)->Range(10, 10'000)->Unit(benchmark::kMicrosecond);

// End-to-end: Realistic saturation workload
static void BM_Rewrite_RealisticSaturation(benchmark::State &state) {
  auto graph_size = state.range(0);

  auto rules = TestRuleSet::Build(MakeDoubleNegRule(), MakeTwoPatternMergeRule());

  for (auto _ : state) {
    state.PauseTiming();
    TestEGraph egraph;

    // Mix of different node types
    std::vector<EClassId> vars;
    for (int64_t i = 0; i < graph_size; ++i) {
      vars.push_back(egraph.emplace(Op::Var, static_cast<uint64_t>(i)).eclass_id);
    }

    // Create some double negations
    for (int64_t i = 0; i < graph_size / 4; ++i) {
      auto neg1 = egraph.emplace(Op::Neg, {vars[i]}).eclass_id;
      egraph.emplace(Op::Neg, {neg1});
    }

    // Create some Add/Mul pairs
    for (int64_t i = 0; i < graph_size / 4; ++i) {
      auto idx = graph_size / 2 + i;
      if (idx + 1 < static_cast<int64_t>(vars.size())) {
        egraph.emplace(Op::Add, {vars[idx], vars[idx + 1]});
        egraph.emplace(Op::Mul, {vars[idx], vars[idx + 1]});
      }
    }

    TestRewriter rewriter(egraph, rules);
    state.ResumeTiming();

    auto result = rewriter.saturate(RewriteConfig::Default());
    benchmark::DoNotOptimize(result);
  }

  state.counters["size"] = static_cast<double>(graph_size);
}

BENCHMARK(BM_Rewrite_RealisticSaturation)->Range(100, 10'000)->Unit(benchmark::kMicrosecond);

// ============================================================================
// Hash-Join vs Cartesian Product Benchmarks
// ============================================================================
// These benchmarks demonstrate the benefit of hash-join for multi-pattern rules
// when patterns share variables (enabling join) vs when they don't (Cartesian).

// Scenario: Two patterns sharing variables - hash-join applies
// Add(?x, ?y) and Mul(?x, ?y) share both variables, enabling efficient join
static void BM_Rewrite_HashJoinSharedVars(benchmark::State &state) {
  auto num_pairs = state.range(0);

  for (auto _ : state) {
    state.PauseTiming();
    TestEGraph egraph;
    BuildAddMulPairs(egraph, num_pairs);

    // Rule with shared variables (hash-join applies)
    auto rule = MakeTwoPatternMergeRule();
    EMatcher<Op, NoAnalysis> matcher(egraph);
    TestRewriteContext ctx;
    state.ResumeTiming();

    ctx.clear_new_eclasses();
    auto rewrites = rule.apply(egraph, matcher, ctx);
    benchmark::DoNotOptimize(rewrites);
  }

  state.SetItemsProcessed(state.iterations() * num_pairs);
  state.counters["pairs"] = static_cast<double>(num_pairs);
}

BENCHMARK(BM_Rewrite_HashJoinSharedVars)->Range(10, 5'000)->Unit(benchmark::kMicrosecond);

// Scenario: Wide hash join - many-to-many matches (output >> input)
// Add(?x, ?y) and Mul(?x, ?z) share only ?x, with few unique x values
// This creates fan-out: each unique x has multiple Adds and Muls
static void BM_Rewrite_HashJoinWide(benchmark::State &state) {
  auto num_nodes = state.range(0);
  auto num_unique_x = std::max(int64_t{1}, num_nodes / 10);  // 10 nodes per unique x

  // Rule: Add(?x, ?y) and Mul(?x, ?z) - shares only ?x
  constexpr PatternVar kVarZ{2};
  constexpr PatternVar kMulRoot{11};

  auto wide_rule = TestRewriteRule::Builder{"wide_join"}
                       .pattern(TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}}, kVarAddRoot), "add")
                       .pattern(TestPattern::build(Op::Mul, {Var{kVarX}, Var{kVarZ}}, kMulRoot), "mul")
                       .apply([](TestRuleContext &, Match const &) {});

  for (auto _ : state) {
    state.PauseTiming();
    TestEGraph egraph;

    // Create shared x variables
    std::vector<EClassId> x_vars;
    x_vars.reserve(static_cast<std::size_t>(num_unique_x));
    for (int64_t i = 0; i < num_unique_x; ++i) {
      x_vars.push_back(egraph.emplace(Op::Var, static_cast<uint64_t>(i)).eclass_id);
    }

    // Create Add nodes with cycling x values, unique y values
    for (int64_t i = 0; i < num_nodes; ++i) {
      auto x = x_vars[static_cast<std::size_t>(i % num_unique_x)];
      auto y = egraph.emplace(Op::Var, static_cast<uint64_t>(1000 + i)).eclass_id;
      egraph.emplace(Op::Add, {x, y});
    }

    // Create Mul nodes with cycling x values, unique z values
    for (int64_t i = 0; i < num_nodes; ++i) {
      auto x = x_vars[static_cast<std::size_t>(i % num_unique_x)];
      auto z = egraph.emplace(Op::Var, static_cast<uint64_t>(2000 + i)).eclass_id;
      egraph.emplace(Op::Mul, {x, z});
    }

    EMatcher<Op, NoAnalysis> matcher(egraph);
    TestRewriteContext ctx;
    state.ResumeTiming();

    ctx.clear_new_eclasses();
    auto rewrites = wide_rule.apply(egraph, matcher, ctx);
    benchmark::DoNotOptimize(rewrites);
  }

  // Expected matches: (num_nodes/num_unique_x)^2 * num_unique_x = num_nodes^2 / num_unique_x
  auto nodes_per_x = num_nodes / num_unique_x;
  auto expected_matches = nodes_per_x * nodes_per_x * num_unique_x;
  state.SetItemsProcessed(state.iterations() * expected_matches);
  state.counters["nodes"] = static_cast<double>(num_nodes);
  state.counters["unique_x"] = static_cast<double>(num_unique_x);
  state.counters["expected_matches"] = static_cast<double>(expected_matches);
}

// Smaller range due to O(n²/k) output growth where k=unique_x
BENCHMARK(BM_Rewrite_HashJoinWide)->Range(10, 500)->Unit(benchmark::kMicrosecond);

// Scenario: Two patterns with NO shared variables - Cartesian product
// Add(?x, ?y) and Neg(?z) have disjoint variables, causing Cartesian product
static void BM_Rewrite_CartesianNoSharedVars(benchmark::State &state) {
  auto num_nodes = state.range(0);

  // Create a rule with disjoint variables
  constexpr PatternVar kVarZ{2};

  auto cartesian_rule = TestRewriteRule::Builder{"cartesian_disjoint"}
                            .pattern(TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}}, kVarAddRoot), "add")
                            .pattern(TestPattern::build(Op::Neg, {Var{kVarZ}}), "neg")  // No join possible
                            .apply([](TestRuleContext &, Match const &) {});

  for (auto _ : state) {
    state.PauseTiming();
    TestEGraph egraph;

    // Create Add nodes and separate Neg nodes
    for (int64_t i = 0; i < num_nodes; ++i) {
      auto x = egraph.emplace(Op::Var, static_cast<uint64_t>(i * 2)).eclass_id;
      auto y = egraph.emplace(Op::Var, static_cast<uint64_t>(i * 2 + 1)).eclass_id;
      egraph.emplace(Op::Add, {x, y});
      egraph.emplace(Op::Neg, {x});
    }

    EMatcher<Op, NoAnalysis> matcher(egraph);
    TestRewriteContext ctx;
    state.ResumeTiming();

    ctx.clear_new_eclasses();
    auto rewrites = cartesian_rule.apply(egraph, matcher, ctx);
    benchmark::DoNotOptimize(rewrites);
  }

  // Cartesian product: num_nodes Add matches × num_nodes Neg matches
  state.SetItemsProcessed(state.iterations());
  state.counters["nodes"] = static_cast<double>(num_nodes);
  state.counters["expected_matches"] = static_cast<double>(num_nodes * num_nodes);
}

// Use smaller range because Cartesian product grows as O(n²)
BENCHMARK(BM_Rewrite_CartesianNoSharedVars)->Range(10, 500)->Unit(benchmark::kMicrosecond);

// ============================================================================
// Saturation Loop Overhead Benchmarks
// ============================================================================
// These benchmarks isolate the per-iteration overhead of the saturation loop.

// Measure overhead of saturation loop with no rewrites happening
// (rules match but return 0 rewrites, so iteration continues until max)
static void BM_Rewrite_SaturationLoopOverhead(benchmark::State &state) {
  auto max_iterations = state.range(0);

  TestEGraph egraph;
  // Small e-graph
  for (int64_t i = 0; i < 100; ++i) {
    auto x = egraph.emplace(Op::Var, static_cast<uint64_t>(i * 2)).eclass_id;
    auto y = egraph.emplace(Op::Var, static_cast<uint64_t>(i * 2 + 1)).eclass_id;
    egraph.emplace(Op::Add, {x, y});
  }

  auto rules = TestRuleSet::Build(MakeNoOpAddRule());  // Matches but doesn't rewrite
  TestRewriter rewriter(egraph, std::move(rules));

  RewriteConfig config;
  config.max_iterations = static_cast<std::size_t>(max_iterations);

  for (auto _ : state) {
    auto result = rewriter.saturate(config);
    benchmark::DoNotOptimize(result);
  }

  state.SetItemsProcessed(state.iterations() * max_iterations);
  state.counters["iterations"] = static_cast<double>(max_iterations);
}

BENCHMARK(BM_Rewrite_SaturationLoopOverhead)->Range(1, 1'000)->Unit(benchmark::kMicrosecond);

// Measure saturation with incremental rebuild vs full rebuild
// This tests the incremental index update path
static void BM_Rewrite_SaturationWithIncrementalRebuild(benchmark::State &state) {
  auto num_chains = state.range(0);

  auto rules = TestRuleSet::Build(MakeDoubleNegRule());

  for (auto _ : state) {
    state.PauseTiming();
    TestEGraph egraph;
    // Deep chains to require multiple iterations
    BuildDoubleNegChains(egraph, num_chains, 8);
    TestRewriter rewriter(egraph, rules);
    state.ResumeTiming();

    // Saturate with default config (uses incremental rebuild)
    auto result = rewriter.saturate(RewriteConfig::Unlimited());
    benchmark::DoNotOptimize(result);
  }

  state.SetItemsProcessed(state.iterations() * num_chains);
  state.counters["chains"] = static_cast<double>(num_chains);
}

BENCHMARK(BM_Rewrite_SaturationWithIncrementalRebuild)->Range(10, 1'000)->Unit(benchmark::kMicrosecond);
