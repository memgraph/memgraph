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

#include "test_rewriter_fixture.hpp"
#include "test_rules.hpp"

namespace memgraph::planner::core {

using namespace test;

// --- Saturation ---

class ChainedNegationTest : public RewriterTest, public ::testing::WithParamInterface<int> {};

TEST_P(ChainedNegationTest, CollapsesToCorrectEClass) {
  // Neg(Neg(?x)) -> ?x collapses chains of negations:
  //   - Even depth: Neg^(2n)(x)   -> x
  //   - Odd depth:  Neg^(2n+1)(x) -> Neg(x)
  //
  //   After saturation, two separate e-classes remain:
  //
  //   ┌───────────────────────────┐
  //   │ x ≡ Neg^2(x) ≡ Neg^4(x)   │  (even e-class)
  //   └─────────────┬─────────────┘
  //                 │
  //   ┌─────────────┴─────────────┐
  //   │ Neg(x) ≡ Neg^3(x) ≡ ...   │  (odd e-class)
  //   └───────────────────────────┘
  //
  // Risk: Saturation loop terminates early or rules don't fire iteratively.
  use_rules(make_double_neg_rule());

  int depth = GetParam();
  auto x = leaf(Op::Var, 1);
  auto neg_x = node(Op::Neg, x);
  auto chain_top = x;
  for (int i = 0; i < depth; ++i) {
    chain_top = node(Op::Neg, chain_top);
  }
  rebuild_index();

  // Before: each Neg^k(x) is in its own e-class
  EXPECT_EQ(egraph.num_classes(), static_cast<size_t>(depth + 1));

  saturate();

  // After: collapsed to exactly 2 e-classes (even and odd)
  // with 3 live e-nodes: x, Neg(even_class), Neg(odd_class)
  EXPECT_EQ(egraph.num_classes(), 2u);
  EXPECT_EQ(egraph.num_live_nodes(), 3u);

  auto expected = depth % 2 == 0 ? x : neg_x;
  EXPECT_EQ(egraph.find(expected), egraph.find(chain_top));
  EXPECT_NE(egraph.find(x), egraph.find(neg_x)) << "Even and odd chains must stay separate";
  expect_saturated();
  expect_rewrites(depth - 1);  // Neg^2..Neg^depth each match once
}

INSTANTIATE_TEST_SUITE_P(Depths, ChainedNegationTest, ::testing::Values(2, 3, 4, 5, 6, 7, 8),
                         [](auto const &info) { return "depth_" + std::to_string(info.param); });

TEST_F(RewriterTest, MultipleRules_FireIndependently) {
  // Two rules with disjoint patterns both fire in the same saturation.
  //
  //   ┌────────────┐       ┌────────────┐
  //   │ Neg(Neg(x))│       │   F(y,y)   │
  //   └─────┬──────┘       └─────┬──────┘
  //         │                   / \
  //   ┌─────┴──────┐      ┌────┴───┴────┐
  //   │   Neg(x)   │      │      y      │
  //   └─────┬──────┘      └─────────────┘
  //         │
  //   ┌─────┴──────┐
  //   │     x      │
  //   └────────────┘
  //
  //   Rule 1: Neg(Neg(?x)) -> ?x  (double negation - mathematically valid)
  //   Rule 2: F(?x, ?x) -> ?x     (synthetic idempotent rule)
  //
  // Risk: Rules could interfere, skip, or only one fires.
  use_rules(make_double_neg_rule(), make_idempotent_f_rule());

  auto x = leaf(Op::Var, 1);
  auto y = leaf(Op::Var, 2);
  auto neg_neg_x = node(Op::Neg, node(Op::Neg, x));
  auto f_yy = node(Op::F, y, y);
  rebuild_index();

  saturate();

  EXPECT_EQ(egraph.find(x), egraph.find(neg_neg_x));
  EXPECT_EQ(egraph.find(y), egraph.find(f_yy));
  expect_saturated();
  expect_rewrites(2);
}

TEST_F(RewriterTest, DisjointMatches_AllProcessed) {
  // Two independent subgraphs both match the same rule pattern.
  //
  //   Neg(Neg(a))     Neg(Neg(b))
  //       ↓               ↓
  //       a               b
  //
  // Risk: Only one match processed, or matches interfere.
  use_rules(make_double_neg_rule());

  auto a = leaf(Op::Var, 1);
  auto b = leaf(Op::Var, 2);
  auto neg_neg_a = node(Op::Neg, node(Op::Neg, a));
  auto neg_neg_b = node(Op::Neg, node(Op::Neg, b));
  rebuild_index();

  saturate();

  EXPECT_EQ(egraph.find(a), egraph.find(neg_neg_a));
  EXPECT_EQ(egraph.find(b), egraph.find(neg_neg_b));
  expect_rewrites(2);
}

// --- Termination Limits ---

TEST_F(RewriterTest, IterationLimitReached_StopsRewriting) {
  // A rule that always produces changes will never saturate.
  // The iteration limit prevents infinite loops.
  //
  // Risk: Without iteration limit, non-terminating rules loop forever.

  auto x = leaf(Op::Var, 1);
  node(Op::Neg, node(Op::Neg, x));

  auto always_rewrite = TestRewriteRule::Builder{"always_rewrite"}
                            .pattern(make_double_neg_pattern())
                            .apply([counter = 2](auto &ctx, [[maybe_unused]] Match const &match) mutable {
                              auto new_node = ctx.emplace(Op::Var, counter++);
                              auto another_node = ctx.emplace(Op::Var, counter++);
                              ctx.merge(new_node.eclass_id, another_node.eclass_id);
                            });

  rebuild_index();
  use_rules(always_rewrite);
  saturate({.max_iterations = 5});

  expect_not_saturated();
  expect_stop_reason(RewriteResult::StopReason::IterationLimit);
  expect_iterations(5);
}

TEST_F(RewriterTest, ENodeLimitReached_StopsRewriting) {
  // A rule that creates many nodes causes exponential growth.
  // The e-node limit prevents out-of-memory.
  //
  //   Growth: 1 node → 11 nodes → 121 nodes → ...
  //
  // Risk: Without e-node limit, explosive rules exhaust memory.

  leaf(Op::Var, 1);

  auto explosive_rule = TestRewriteRule::Builder{"explosive"}
                            .pattern(TestPattern::build(Op::Var, kVarRoot))
                            .apply([counter = 2](TestRuleContext &ctx, Match const &match) mutable {
                              for (int i = 0; i < 10; ++i) {
                                auto n = ctx.emplace(Op::Var, counter++);
                                ctx.merge(n.eclass_id, match[kVarRoot]);
                              }
                            });

  rebuild_index();
  use_rules(explosive_rule);
  saturate({.max_iterations = 100, .max_enodes = 50});

  expect_not_saturated();
  expect_stop_reason(RewriteResult::StopReason::ENodeLimit);
}

// --- Multi-Pattern Joins ---

TEST_F(RewriterTest, TwoPatterns_JoinOnSharedVariables) {
  // Two patterns with shared variables (?x, ?y) are joined.
  // Only matches where both patterns bind the SAME e-classes fire.
  //
  //   ┌────────────┐       ┌────────────┐
  //   │  F(x, y)   │       │  F2(x, y)  │
  //   └─────┬──────┘       └─────┬──────┘
  //        / \                  / \
  //   ┌───┴───┴───┐        ┌───┴───┴───┐
  //   │  x     y  │        │  x     y  │  ← same x, y
  //   └───────────┘        └───────────┘
  //
  //   Patterns: F(?x, ?y), F2(?x, ?y)
  //   Join condition: ?x and ?y must match same e-classes in both patterns.
  //
  // Risk: Join logic broken; patterns matched independently (Cartesian).
  auto merge_rule =
      TestRewriteRule::Builder{"merge_f_f2"}
          .pattern(TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}}, kVarRootA), "f")
          .pattern(TestPattern::build(Op::F2, {Var{kVarX}, Var{kVarY}}, kVarRootB), "f2")
          .apply([](TestRuleContext &ctx, Match const &match) { ctx.merge(match[kVarRootA], match[kVarRootB]); });
  use_rules(merge_rule);

  auto x = leaf(Op::Var, 1);
  auto y = leaf(Op::Var, 2);
  auto f_xy = node(Op::F, x, y);
  auto f2_xy = node(Op::F2, x, y);
  rebuild_index();

  saturate();

  EXPECT_EQ(egraph.find(f_xy), egraph.find(f2_xy));
  expect_saturated();
  expect_rewrites(1);
}

TEST_F(RewriterTest, ThreePatternChain_TransitiveVariableBinding) {
  // Three patterns form a chain: P1(?x) - P2(?x,?y) - P3(?y)
  // Variables propagate transitively through the middle pattern.
  //
  //   ┌────────┐     ┌────────────┐     ┌────────┐
  //   │  F(a)  │     │  F2(a,b)   │     │  F(b)  │
  //   └───┬────┘     └─────┬──────┘     └───┬────┘
  //       │               / \               │
  //   ┌───┴───┐      ┌───┴───┴───┐     ┌───┴───┐
  //   │   a   │      │   a   b   │     │   b   │
  //   └───────┘      └───────────┘     └───────┘
  //
  //   Patterns: F(?x), F2(?x,?y), F(?y)
  //   P1 and P3 don't share variables directly, but P2 bridges them.
  //
  // Risk: Transitive binding broken; chain patterns fail to join.
  use_rules(make_chain_join_rule());

  auto a = leaf(Op::Var, 1);
  auto b = leaf(Op::Var, 2);
  auto f_a = node(Op::F, a);
  auto f2_ab = node(Op::F2, a, b);
  auto f_b = node(Op::F, b);
  rebuild_index();

  EXPECT_NE(egraph.find(f_a), egraph.find(f_b));

  saturate();

  EXPECT_EQ(egraph.find(f_a), egraph.find(f_b));
  EXPECT_NE(egraph.find(f2_ab), egraph.find(f_a));
  expect_saturated();
  expect_rewrites(1);
}

TEST_F(RewriterTest, ThreePatternChain_IncompleteChainDoesNotFire) {
  // Same patterns as above, but the graph breaks the chain.
  // F2(a,c) exists, but F(b) has b≠c, so ?y can't bind consistently.
  //
  //   ┌────────┐     ┌────────────┐     ┌────────┐
  //   │  F(a)  │     │  F2(a,c)   │     │  F(b)  │
  //   └───┬────┘     └─────┬──────┘     └───┬────┘
  //       │               / \               │
  //   ┌───┴───┐      ┌───┴───┴───┐     ┌───┴───┐
  //   │   a   │      │   a   c   │     │   b   │  ← c ≠ b, chain broken
  //   └───────┘      └───────────┘     └───────┘
  //
  //   Patterns: F(?x), F2(?x,?y), F(?y)
  //   No binding exists where ?y = c AND ?y = b.
  //
  // Risk: Incomplete chains incorrectly fire (join logic broken).
  use_rules(make_chain_join_rule());

  auto a = leaf(Op::Var, 1);
  auto b = leaf(Op::Var, 2);
  auto c = leaf(Op::Var, 3);
  auto f_a = node(Op::F, a);
  node(Op::F2, a, c);
  auto f_b = node(Op::F, b);
  rebuild_index();

  EXPECT_NE(egraph.find(f_a), egraph.find(f_b));

  saturate();

  EXPECT_NE(egraph.find(f_a), egraph.find(f_b));
  expect_rewrites(0);
  expect_saturated();
}

TEST_F(RewriterTest, MultiPattern_RequiresAllPatternsMatch) {
  // Multi-pattern rule requires ALL patterns to match for the rule to fire.
  auto rule = TestRewriteRule::Builder{"needs_both"}
                  .pattern(TestPattern::build(Op::F, {Var{kVarX}}))
                  .pattern(TestPattern::build(Op::F2, {Var{kVarX}, Var{kVarY}}))
                  .apply([](TestRuleContext &, Match const &) {});
  use_rules(rule);

  node(Op::F, leaf(Op::Var, 1));  // Only F exists, not F2
  rebuild_index();

  saturate();

  expect_rewrites(0);
}

// --- Node Creation ---

TEST_F(RewriterTest, Emplace_CreatesNewNodes) {
  // Rules can create new nodes using ctx.emplace().
  //
  //   Before:          After:
  //   ┌─────┐          ┌─────────────────┐
  //   │  x  │    →     │   x ≡ F(x,x)    │  (same e-class)
  //   └─────┘          └─────────────────┘

  auto create_f_rule = TestRewriteRule::Builder{"create_f"}
                           .pattern(TestPattern::build(Op::Var, kVarRoot))
                           .apply([](TestRuleContext &ctx, Match const &match) {
                             auto var_eclass = match[kVarRoot];
                             auto new_f = ctx.emplace(Op::F, utils::small_vector{var_eclass, var_eclass});
                             ctx.merge(new_f.eclass_id, var_eclass);
                           });
  use_rules(create_f_rule);

  auto x = leaf(Op::Var, 1);
  rebuild_index();

  EXPECT_EQ(egraph.eclass(egraph.find(x)).nodes().size(), 1);  // Var

  rewriter().iterate_once();

  EXPECT_EQ(egraph.eclass(egraph.find(x)).nodes().size(), 2);  // Var and F
}

TEST_F(RewriterTest, EmplacedNodes_Matchable) {
  // Nodes created in iteration N must be matchable in iteration N+1.
  auto create_f = TestRewriteRule::Builder{"create_f"}
                      .pattern(TestPattern::build(Op::Var, kVarX))
                      .pattern(TestPattern::build(Op::Var, kVarY))
                      .apply([](TestRuleContext &ctx, Match const &match) {
                        auto vx = match[kVarX];
                        auto vy = match[kVarY];
                        if (ctx.find(vx) != ctx.find(vy)) {
                          ctx.emplace(Op::F, utils::small_vector{vx, vy});
                        }
                      });

  auto wrap_f2 = TestRewriteRule::Builder{"wrap_f2"}
                     .pattern(TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}}, kVarRootA))
                     .apply([](TestRuleContext &ctx, Match const &match) {
                       ctx.emplace(Op::F2, utils::small_vector{match[kVarRootA]});
                     });
  use_rules(create_f, wrap_f2);

  leaf(Op::Var, 1);
  leaf(Op::Var, 2);
  rebuild_index();

  rewriter().iterate_once();  // Creates F nodes
  rewriter().iterate_once();  // Creates F2 nodes wrapping the new Fs

  std::size_t f_count = 0;
  std::size_t f2_count = 0;
  for (auto const &[id, eclass] : egraph.canonical_classes()) {
    for (auto enode_id : eclass.nodes()) {
      auto const &enode = egraph.get_enode(enode_id);
      if (enode.symbol() == Op::F) ++f_count;
      if (enode.symbol() == Op::F2) ++f2_count;
    }
  }
  EXPECT_EQ(f_count, 2);  // F(1,2) and F(2,1)
  EXPECT_EQ(f2_count, 2);
}

TEST_F(RewriterTest, RebuildIndex_IndexesNewNodes) {
  // Nodes added after index build are not matchable until rebuild.
  use_rules(make_double_neg_rule());
  rebuild_index();  // Index is empty

  auto x = leaf(Op::Var, 1);
  auto neg_neg_x = node(Op::Neg, node(Op::Neg, x));

  // Without rebuild: nodes not in index, no matches
  saturate();
  expect_rewrites(0);
  EXPECT_NE(egraph.find(x), egraph.find(neg_neg_x));

  // After rebuild: nodes indexed, rule fires
  rebuild_index();
  saturate();
  expect_rewrites(1);
  EXPECT_EQ(egraph.find(x), egraph.find(neg_neg_x));
}

// --- Congruence and Merge Effects ---

TEST_F(RewriterTest, Congruence_PropagatesAfterMerge) {
  // When a rule merges a≡b, parent nodes F(a) and F(b) should also merge
  // due to e-graph congruence closure.
  //
  //   Before:                       After merge(a,b):
  //   ┌────┐  ┌────┐                ┌──────────┐
  //   │F(a)│  │F(b)│                │F(a)≡F(b) │  ← congruence
  //   └─┬──┘  └─┬──┘      →         └────┬─────┘
  //     │       │                        │
  //   ┌─┴─┐   ┌─┴─┐                 ┌────┴────┐
  //   │ a │   │ b │                 │  a ≡ b  │
  //   └───┘   └───┘                 └─────────┘
  //
  // Risk: Congruence closure not triggered after rewrite-induced merge.
  use_rules(make_merge_vars_rule());

  auto a = leaf(Op::Var, 1);
  auto b = leaf(Op::Var, 2);
  auto f_a = node(Op::F, a);
  auto f_b = node(Op::F, b);
  rebuild_index();

  EXPECT_NE(egraph.find(f_a), egraph.find(f_b));

  saturate();

  EXPECT_EQ(egraph.find(a), egraph.find(b));
  EXPECT_EQ(egraph.find(f_a), egraph.find(f_b));  // Congruence propagated
}

TEST_F(RewriterTest, EmplaceExisting_ReturnsSameEClass) {
  // Emplacing a node that already exists returns the existing e-class
  // with did_insert=false (hash-consing property).
  //
  // Risk: Duplicate nodes created, breaking e-graph invariants.

  auto x = leaf(Op::Var, 1);
  auto f_x = node(Op::F, x);
  rebuild_index();

  std::size_t emplace_count = 0;
  bool saw_existing = false;

  auto try_duplicate = TestRewriteRule::Builder{"try_duplicate"}
                           .pattern(TestPattern::build(Op::Var, kVarX))
                           .apply([&](TestRuleContext &ctx, Match const &match) {
                             // Try to create F(x) which already exists
                             auto [eclass_id, _, did_insert] = ctx.emplace(Op::F, utils::small_vector{match[kVarX]});
                             ++emplace_count;
                             if (!did_insert) {
                               saw_existing = true;
                               EXPECT_EQ(eclass_id, f_x);
                             }
                           });
  use_rules(try_duplicate);

  rewriter().iterate_once();

  EXPECT_EQ(emplace_count, 1);
  EXPECT_TRUE(saw_existing);
  expect_rewrites(0);
}

TEST_F(RewriterTest, MergeUnlocksNewMatches) {
  // A pattern requiring equal children F(?x, ?x) doesn't match F(a, b) initially.
  // After merging a≡b, the pattern now matches.
  //
  //   Before:                       After merge(a,b):
  //   ┌──────────┐                  ┌──────────┐
  //   │ F(a, b)  │                  │ F(a, b)  │  ← now matches F(?x,?x)
  //   └────┬─────┘       →          └────┬─────┘
  //       / \                           / \
  //   ┌──┴┐ ┌┴──┐                   ┌───┴──┴───┐
  //   │ a │ │ b │                   │  a ≡ b   │
  //   └───┘ └───┘                   └──────────┘
  //
  // Risk: Saturation doesn't re-check patterns after merges.

  // First rule merges all Var nodes, second fires when children become equal
  use_rules(make_merge_vars_rule(), make_idempotent_f_rule());

  auto a = leaf(Op::Var, 1);
  auto b = leaf(Op::Var, 2);
  auto f_ab = node(Op::F, a, b);
  rebuild_index();

  saturate();

  // After merge_vars fires: a≡b
  // Then idempotent fires: F(a,b) now matches F(?x,?x) and merges with a
  EXPECT_EQ(egraph.find(a), egraph.find(b));     // becasue of merge_vars
  EXPECT_EQ(egraph.find(f_ab), egraph.find(a));  // because of idempotent_f
  expect_saturated();
  expect_rewrites(2);
  expect_iterations(2);
}

// --- Edge Cases ---

TEST_F(RewriterTest, EmptyRuleset_Saturates) {
  leaf(Op::Var, 1);
  rebuild_index();
  use_rules();
  saturate();
  expect_saturated();
  expect_iterations(1);
  expect_rewrites(0);
}

TEST_F(RewriterTest, NoMatches_Saturates) {
  node(Op::F, leaf(Op::Var, 1));
  rebuild_index();
  use_rules(make_double_neg_rule());  // Matches Neg(Neg(?x)), not F
  saturate();
  expect_saturated();
  expect_iterations(1);
  expect_rewrites(0);
}

TEST_F(RewriterTest, ZeroPatternRule_NeverFires) {
  leaf(Op::Var, 1);
  rebuild_index();
  use_rules(TestRewriteRule::Builder{"empty"}.apply([](TestRuleContext &, Match const &) {}));
  EXPECT_EQ(rewriter().iterate_once(), 0);
}

TEST_F(RewriterTest, PerRuleStatistics_Tracked) {
  node(Op::Neg, node(Op::Neg, leaf(Op::Var, 1)));
  rebuild_index();
  auto rule = make_double_neg_rule();
  use_rules(rule);
  saturate();
  expect_rule_rewrites(rule, 1);
  expect_iterations(2);
  expect_rewrites(1);
}

}  // namespace memgraph::planner::core
