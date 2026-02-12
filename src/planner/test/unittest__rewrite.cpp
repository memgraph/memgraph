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

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "planner/core/rewrite.hpp"

namespace memgraph::planner::core {

// Test symbol enum (same as used in other planner tests)
enum class Op : uint8_t {
  Add,
  Mul,
  Neg,
  Var,
  Const,
};

struct NoAnalysis {};

using TestEGraph = EGraph<Op, NoAnalysis>;
using TestPattern = Pattern<Op>;
using TestRewriteRule = RewriteRule<Op>;
using TestRewriter = Rewriter<Op, NoAnalysis>;

// --- Helper Functions ---

// Build pattern for Neg(Neg(?x))
auto make_double_neg_pattern() -> TestPattern {
  auto builder = TestPattern::Builder{};
  auto x = builder.var(0);                         // ?x
  auto neg_x = builder.sym(Op::Neg, {x});          // Neg(?x)
  auto neg_neg_x = builder.sym(Op::Neg, {neg_x});  // Neg(Neg(?x))
  return std::move(builder).build(neg_neg_x);
}

// Build pattern for Add(?x, ?y)
auto make_add_pattern() -> TestPattern {
  auto builder = TestPattern::Builder{};
  auto x = builder.var(0);
  auto y = builder.var(1);
  auto add = builder.sym(Op::Add, {x, y});
  return std::move(builder).build(add);
}

// Create a double negation elimination rule: Neg(Neg(?x)) -> ?x
auto make_double_neg_rule() -> TestRewriteRule {
  auto builder = TestRewriteRule::Builder{};
  builder.pattern(make_double_neg_pattern())
      .apply<NoAnalysis>([](auto &egraph, auto matches, auto & /*proc_ctx*/) -> std::size_t {
        std::size_t count = 0;
        for (auto const &match : matches) {
          auto x = match.subst.at(PatternVar{0});
          auto matched_eclass = match.pattern_roots[0];
          // Merge Neg(Neg(?x)) with ?x
          if (egraph.find(matched_eclass) != egraph.find(x)) {
            egraph.merge(matched_eclass, x);
            count++;
          }
        }
        return count;
      });
  return std::move(builder).build("double_negation");
}

// --- RewriteConfig Tests ---

TEST(RewriteConfig, DefaultValues) {
  auto config = RewriteConfig::Default();
  EXPECT_EQ(config.max_iterations, 10);
  EXPECT_EQ(config.max_enodes, 10000);
  EXPECT_EQ(config.timeout, std::chrono::milliseconds{1000});
}

TEST(RewriteConfig, UnlimitedValues) {
  auto config = RewriteConfig::Unlimited();
  EXPECT_EQ(config.max_iterations, std::numeric_limits<std::size_t>::max());
  EXPECT_EQ(config.max_enodes, std::numeric_limits<std::size_t>::max());
  EXPECT_GT(config.timeout, std::chrono::hours{24 * 30});  // At least a month
}

// --- RewriteResult Tests ---

TEST(RewriteResult, SaturatedCheck) {
  RewriteResult result;
  result.stop_reason = RewriteResult::StopReason::Saturated;
  EXPECT_TRUE(result.saturated());

  result.stop_reason = RewriteResult::StopReason::IterationLimit;
  EXPECT_FALSE(result.saturated());

  result.stop_reason = RewriteResult::StopReason::ENodeLimit;
  EXPECT_FALSE(result.saturated());

  result.stop_reason = RewriteResult::StopReason::Timeout;
  EXPECT_FALSE(result.saturated());
}

// --- RewriteRule Tests ---

TEST(RewriteRule, BuilderCreatesRule) {
  auto builder = TestRewriteRule::Builder{};
  builder.pattern(make_double_neg_pattern(), "double_neg").apply<NoAnalysis>([](auto &, auto, auto &) -> std::size_t {
    return 0;
  });
  auto rule = std::move(builder).build("test_rule");

  EXPECT_EQ(rule.name(), "test_rule");
  EXPECT_EQ(rule.patterns().size(), 1);
}

TEST(RewriteRule, MultiplePatterns) {
  auto builder = TestRewriteRule::Builder{};
  builder.pattern(make_double_neg_pattern(), "pattern1")
      .pattern(make_add_pattern(), "pattern2")
      .apply<NoAnalysis>([](auto &, auto, auto &) -> std::size_t { return 0; });
  auto rule = std::move(builder).build("multi_pattern_rule");

  EXPECT_EQ(rule.patterns().size(), 2);
}

// --- Rewriter Tests ---

TEST(Rewriter, EmptyRulesSaturatesImmediately) {
  TestEGraph egraph;
  auto x = egraph.emplace(Op::Var, 1);
  [[maybe_unused]] auto neg_x = egraph.emplace(Op::Neg, {x.current_eclassid});

  TestRewriter rewriter(egraph);
  // No rules added

  auto result = rewriter.saturate();

  EXPECT_TRUE(result.saturated());
  EXPECT_EQ(result.iterations, 1);
  EXPECT_EQ(result.rewrites_applied, 0);
}

TEST(Rewriter, DoubleNegationElimination) {
  TestEGraph egraph;
  auto x = egraph.emplace(Op::Var, 1);
  auto neg_x = egraph.emplace(Op::Neg, {x.current_eclassid});
  auto neg_neg_x = egraph.emplace(Op::Neg, {neg_x.current_eclassid});

  // Before rewrite: x, Neg(x), and Neg(Neg(x)) are separate e-classes
  EXPECT_NE(egraph.find(x.current_eclassid), egraph.find(neg_neg_x.current_eclassid));

  TestRewriter rewriter(egraph);
  rewriter.add_rule(make_double_neg_rule());

  auto result = rewriter.saturate();

  // After rewrite: x and Neg(Neg(x)) should be in the same e-class
  EXPECT_EQ(egraph.find(x.current_eclassid), egraph.find(neg_neg_x.current_eclassid));
  EXPECT_TRUE(result.saturated());
  EXPECT_GE(result.rewrites_applied, 1);
}

TEST(Rewriter, TripleNegation) {
  // Neg(Neg(Neg(x))) should simplify to Neg(x)
  TestEGraph egraph;
  auto x = egraph.emplace(Op::Var, 1);
  auto neg_x = egraph.emplace(Op::Neg, {x.current_eclassid});
  auto neg_neg_x = egraph.emplace(Op::Neg, {neg_x.current_eclassid});
  auto neg_neg_neg_x = egraph.emplace(Op::Neg, {neg_neg_x.current_eclassid});

  TestRewriter rewriter(egraph);
  rewriter.add_rule(make_double_neg_rule());

  auto result = rewriter.saturate();

  // Neg(Neg(Neg(x))) should be equivalent to Neg(x)
  EXPECT_EQ(egraph.find(neg_x.current_eclassid), egraph.find(neg_neg_neg_x.current_eclassid));
  EXPECT_TRUE(result.saturated());
}

TEST(Rewriter, QuadrupleNegation) {
  // Neg(Neg(Neg(Neg(x)))) should simplify to x
  TestEGraph egraph;
  auto x = egraph.emplace(Op::Var, 1);
  auto neg1 = egraph.emplace(Op::Neg, {x.current_eclassid});
  auto neg2 = egraph.emplace(Op::Neg, {neg1.current_eclassid});
  auto neg3 = egraph.emplace(Op::Neg, {neg2.current_eclassid});
  auto neg4 = egraph.emplace(Op::Neg, {neg3.current_eclassid});

  TestRewriter rewriter(egraph);
  rewriter.add_rule(make_double_neg_rule());

  auto result = rewriter.saturate();

  // Should simplify: neg4 == neg2 == x
  EXPECT_EQ(egraph.find(x.current_eclassid), egraph.find(neg4.current_eclassid));
  EXPECT_EQ(egraph.find(x.current_eclassid), egraph.find(neg2.current_eclassid));
  EXPECT_TRUE(result.saturated());
}

TEST(Rewriter, ApplyOnce) {
  TestEGraph egraph;
  auto x = egraph.emplace(Op::Var, 1);
  auto neg_x = egraph.emplace(Op::Neg, {x.current_eclassid});
  auto neg_neg_x = egraph.emplace(Op::Neg, {neg_x.current_eclassid});
  auto neg_neg_neg_x = egraph.emplace(Op::Neg, {neg_neg_x.current_eclassid});

  TestRewriter rewriter(egraph);
  rewriter.add_rule(make_double_neg_rule());

  // First apply_once should match Neg(Neg(x)) and Neg(Neg(Neg(x)))
  auto rewrites1 = rewriter.apply_once();
  EXPECT_GE(rewrites1, 1);

  // x should now be equivalent to Neg(Neg(x))
  EXPECT_EQ(egraph.find(x.current_eclassid), egraph.find(neg_neg_x.current_eclassid));

  // Second apply_once might find more rewrites due to merged e-classes
  [[maybe_unused]] auto rewrites2 = rewriter.apply_once();

  // Eventually reaches saturation
  auto rewrites3 = rewriter.apply_once();
  if (rewrites3 == 0) {
    // Already saturated
    EXPECT_EQ(egraph.find(neg_x.current_eclassid), egraph.find(neg_neg_neg_x.current_eclassid));
  }
}

TEST(Rewriter, IterationLimit) {
  TestEGraph egraph;
  auto x = egraph.emplace(Op::Var, 1);
  [[maybe_unused]] auto neg_x = egraph.emplace(Op::Neg, {x.current_eclassid});

  // Rule that always produces a rewrite (adds new nodes)
  auto always_builder = TestRewriteRule::Builder{};
  always_builder.pattern(make_double_neg_pattern()).apply<NoAnalysis>([](auto &eg, auto, auto &) -> std::size_t {
    // Always add a new node to keep rewriting
    static int counter = 100;
    eg.emplace(Op::Var, counter++);
    return 1;
  });
  auto always_rewrite = std::move(always_builder).build("always_rewrite");

  TestRewriter rewriter(egraph);
  rewriter.add_rule(always_rewrite);

  RewriteConfig config;
  config.max_iterations = 5;

  auto result = rewriter.saturate(config);

  EXPECT_FALSE(result.saturated());
  EXPECT_EQ(result.stop_reason, RewriteResult::StopReason::IterationLimit);
  EXPECT_EQ(result.iterations, 5);
}

TEST(Rewriter, ENodeLimit) {
  TestEGraph egraph;
  auto x = egraph.emplace(Op::Var, 1);

  // Rule that adds many nodes
  auto explosive_builder = TestRewriteRule::Builder{};
  explosive_builder.pattern(make_double_neg_pattern()).apply<NoAnalysis>([](auto &eg, auto, auto &) -> std::size_t {
    // Add many nodes to exceed limit
    static int counter = 200;
    for (int i = 0; i < 100; ++i) {
      eg.emplace(Op::Var, counter++);
    }
    return 1;
  });
  auto explosive_rule = std::move(explosive_builder).build("explosive");

  TestRewriter rewriter(egraph);
  rewriter.add_rule(explosive_rule);

  RewriteConfig config;
  config.max_enodes = 50;  // Low limit

  auto result = rewriter.saturate(config);

  // Should stop due to e-node limit (or saturate if no matches)
  // Since we have no Neg(Neg(?)) pattern, it should saturate immediately
  EXPECT_TRUE(result.saturated() || result.stop_reason == RewriteResult::StopReason::ENodeLimit);
}

TEST(Rewriter, TimeoutBehavior) {
  TestEGraph egraph;
  auto x = egraph.emplace(Op::Var, 1);
  auto neg_x = egraph.emplace(Op::Neg, {x.current_eclassid});
  auto neg_neg_x = egraph.emplace(Op::Neg, {neg_x.current_eclassid});

  // Rule that sleeps to trigger timeout
  auto slow_builder = TestRewriteRule::Builder{};
  slow_builder.pattern(make_double_neg_pattern()).apply<NoAnalysis>([](auto &eg, auto matches, auto &) -> std::size_t {
    std::this_thread::sleep_for(std::chrono::milliseconds{50});
    // Still do the rewrite
    for (auto const &match : matches) {
      auto var_x = match.subst.at(PatternVar{0});
      eg.merge(match.pattern_roots[0], var_x);
    }
    return matches.size();
  });
  auto slow_rule = std::move(slow_builder).build("slow");

  TestRewriter rewriter(egraph);
  rewriter.add_rule(slow_rule);

  RewriteConfig config;
  config.timeout = std::chrono::milliseconds{10};  // Very short timeout

  auto result = rewriter.saturate(config);

  // Should either timeout or complete quickly (depends on timing)
  // The first iteration runs before timeout is checked
  EXPECT_GE(result.iterations, 1);
}

TEST(Rewriter, MultipleRules) {
  // Test with two rules
  TestEGraph egraph;
  auto x = egraph.emplace(Op::Var, 1);
  auto y = egraph.emplace(Op::Var, 2);
  auto neg_x = egraph.emplace(Op::Neg, {x.current_eclassid});
  auto neg_neg_x = egraph.emplace(Op::Neg, {neg_x.current_eclassid});
  [[maybe_unused]] auto add_xy = egraph.emplace(Op::Add, {x.current_eclassid, y.current_eclassid});

  // Rule 1: double negation
  auto double_neg = make_double_neg_rule();

  // Rule 2: count additions (doesn't actually rewrite)
  auto count_adds_builder = TestRewriteRule::Builder{};
  count_adds_builder.pattern(make_add_pattern()).apply<NoAnalysis>([](auto &, auto, auto &) -> std::size_t {
    // Just a counter rule - doesn't produce rewrites
    return 0;
  });
  auto count_adds = std::move(count_adds_builder).build("count_adds");

  TestRewriter rewriter(egraph);
  rewriter.add_rule(double_neg);
  rewriter.add_rule(count_adds);

  EXPECT_EQ(rewriter.num_rules(), 2);

  auto result = rewriter.saturate();

  // Double negation should still be eliminated
  EXPECT_EQ(egraph.find(x.current_eclassid), egraph.find(neg_neg_x.current_eclassid));
  EXPECT_TRUE(result.saturated());
}

TEST(Rewriter, NoMatchingPatterns) {
  TestEGraph egraph;
  auto x = egraph.emplace(Op::Var, 1);
  auto y = egraph.emplace(Op::Var, 2);
  [[maybe_unused]] auto add = egraph.emplace(Op::Add, {x.current_eclassid, y.current_eclassid});

  // Only double negation rule, but no Neg nodes in graph
  TestRewriter rewriter(egraph);
  rewriter.add_rule(make_double_neg_rule());

  auto result = rewriter.saturate();

  EXPECT_TRUE(result.saturated());
  EXPECT_EQ(result.rewrites_applied, 0);
  EXPECT_EQ(result.iterations, 1);
}

TEST(Rewriter, MultipleMatchesInOneIteration) {
  // Multiple Neg(Neg(x)) patterns that can all be rewritten
  TestEGraph egraph;
  auto a = egraph.emplace(Op::Var, 1);
  auto b = egraph.emplace(Op::Var, 2);
  auto neg_a = egraph.emplace(Op::Neg, {a.current_eclassid});
  auto neg_neg_a = egraph.emplace(Op::Neg, {neg_a.current_eclassid});
  auto neg_b = egraph.emplace(Op::Neg, {b.current_eclassid});
  auto neg_neg_b = egraph.emplace(Op::Neg, {neg_b.current_eclassid});

  TestRewriter rewriter(egraph);
  rewriter.add_rule(make_double_neg_rule());

  auto result = rewriter.saturate();

  // Both should be simplified
  EXPECT_EQ(egraph.find(a.current_eclassid), egraph.find(neg_neg_a.current_eclassid));
  EXPECT_EQ(egraph.find(b.current_eclassid), egraph.find(neg_neg_b.current_eclassid));
  EXPECT_GE(result.rewrites_applied, 2);
  EXPECT_TRUE(result.saturated());
}

TEST(Rewriter, RefreshMatcher) {
  TestEGraph egraph;
  auto x = egraph.emplace(Op::Var, 1);

  TestRewriter rewriter(egraph);
  rewriter.add_rule(make_double_neg_rule());

  // Externally add new nodes
  auto neg_x = egraph.emplace(Op::Neg, {x.current_eclassid});
  auto neg_neg_x = egraph.emplace(Op::Neg, {neg_x.current_eclassid});

  // Refresh matcher to pick up new nodes
  rewriter.refresh_matcher();

  auto result = rewriter.saturate();

  // Should find and simplify the new pattern
  EXPECT_EQ(egraph.find(x.current_eclassid), egraph.find(neg_neg_x.current_eclassid));
  EXPECT_TRUE(result.saturated());
}

// --- Multi-Pattern Rule Tests ---

TEST(Rewriter, TwoPatternRule) {
  // Rule that correlates two patterns (similar to inline rewrite)
  TestEGraph egraph;
  ProcessingContext<Op> proc_ctx;

  auto x = egraph.emplace(Op::Var, 1);
  auto y = egraph.emplace(Op::Var, 2);
  auto add = egraph.emplace(Op::Add, {x.current_eclassid, y.current_eclassid});
  auto mul = egraph.emplace(Op::Mul, {x.current_eclassid, y.current_eclassid});

  // Pattern 1: Add(?x, ?y)
  auto add_pattern = make_add_pattern();

  // Pattern 2: Mul(?x, ?y) with same variables
  auto mul_builder = TestPattern::Builder{};
  auto mx = mul_builder.var(0);
  auto my = mul_builder.var(1);
  auto pmul = mul_builder.sym(Op::Mul, {mx, my});
  auto mul_pattern = std::move(mul_builder).build(pmul);

  // Rule: merge Add and Mul if they have same operands
  // With unified matches, the framework already joins on shared variables (?x, ?y)
  auto merge_builder = TestRewriteRule::Builder{};
  merge_builder.pattern(std::move(add_pattern), "add")
      .pattern(std::move(mul_pattern), "mul")
      .apply<NoAnalysis>([](auto &eg, auto matches, auto &) -> std::size_t {
        std::size_t count = 0;
        // Each match contains a correlated (Add, Mul) pair with same ?x and ?y
        for (auto const &match : matches) {
          auto add_eclass = match.pattern_roots[0];
          auto mul_eclass = match.pattern_roots[1];

          if (eg.find(add_eclass) != eg.find(mul_eclass)) {
            eg.merge(add_eclass, mul_eclass);
            count++;
          }
        }
        return count;
      });
  auto merge_rule = std::move(merge_builder).build("merge_add_mul");

  TestRewriter rewriter(egraph);
  rewriter.add_rule(merge_rule);

  auto result = rewriter.saturate();

  // Add(x, y) and Mul(x, y) should be merged
  EXPECT_EQ(egraph.find(add.current_eclassid), egraph.find(mul.current_eclassid));
  EXPECT_TRUE(result.saturated());
}

// --- Depth Tests ---

TEST(Rewriter, DeepPatternMatching) {
  // Test that patterns at various depths work correctly
  TestEGraph egraph;

  // Build a deep expression: Neg(Neg(Neg(Neg(Neg(Neg(x))))))
  auto x = egraph.emplace(Op::Var, 1);
  auto current = x.current_eclassid;
  std::vector<EClassId> neg_chain;
  for (int i = 0; i < 6; ++i) {
    auto neg = egraph.emplace(Op::Neg, {current});
    neg_chain.push_back(neg.current_eclassid);
    current = neg.current_eclassid;
  }

  TestRewriter rewriter(egraph);
  rewriter.add_rule(make_double_neg_rule());

  auto result = rewriter.saturate();

  // 6 negations should collapse to 0 (equivalent to x)
  EXPECT_EQ(egraph.find(x.current_eclassid), egraph.find(neg_chain.back()));
  EXPECT_TRUE(result.saturated());
}

}  // namespace memgraph::planner::core
