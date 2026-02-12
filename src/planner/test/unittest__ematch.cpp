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

#include "planner/core/ematch.hpp"

namespace memgraph::planner::core {

// Test symbol enum (same as used in other planner tests)
enum class Op : uint8_t {
  Add,
  Mul,
  Neg,
  Var,
  Const,
  F,
};

struct NoAnalysis {};

using TestEGraph = EGraph<Op, NoAnalysis>;
using TestPattern = Pattern<Op>;
using TestEMatcher = EMatcher<Op, NoAnalysis>;

// Helper to create a pattern: ?x
auto make_var_pattern(uint32_t var_id) -> TestPattern {
  auto builder = TestPattern::Builder{};
  auto x = builder.var(var_id);
  return std::move(builder).build(x);
}

// Helper to create a pattern: Op()
auto make_leaf_pattern(Op op) -> TestPattern {
  auto builder = TestPattern::Builder{};
  auto s = builder.sym(op);
  return std::move(builder).build(s);
}

// Helper to create a pattern: Op(?x)
auto make_unary_pattern(Op op, uint32_t var_id) -> TestPattern {
  auto builder = TestPattern::Builder{};
  auto x = builder.var(var_id);
  auto s = builder.sym(op, {x});
  return std::move(builder).build(s);
}

// Helper to create a pattern: Op(?x, ?y)
auto make_binary_pattern(Op op, uint32_t var_x, uint32_t var_y) -> TestPattern {
  auto builder = TestPattern::Builder{};
  auto x = builder.var(var_x);
  auto y = builder.var(var_y);
  auto s = builder.sym(op, {x, y});
  return std::move(builder).build(s);
}

// --- Variable Pattern Matching Tests ---

TEST(EMatcher, MatchVariablePatternMatchesAll) {
  TestEGraph egraph;
  [[maybe_unused]] auto x = egraph.emplace(Op::Var, 1);
  [[maybe_unused]] auto y = egraph.emplace(Op::Var, 2);

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  auto pattern = make_var_pattern(0);
  auto matches = ematcher.match(pattern, ctx);

  // Variable pattern should match all e-classes
  EXPECT_EQ(matches.size(), 2);
}

TEST(EMatcher, MatchVariablePatternBindsCorrectly) {
  TestEGraph egraph;
  auto x = egraph.emplace(Op::Var, 1);

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  auto pattern = make_var_pattern(42);
  auto matches = ematcher.match(pattern, ctx);

  ASSERT_EQ(matches.size(), 1);
  EXPECT_EQ(matches[0].matched_eclass, x.current_eclassid);
  EXPECT_EQ(matches[0].subst.size(), 1);
  EXPECT_EQ(matches[0].subst.at(PatternVar{42}), x.current_eclassid);
}

// --- Leaf Symbol Matching Tests ---

TEST(EMatcher, MatchLeafSymbolPattern) {
  TestEGraph egraph;
  [[maybe_unused]] auto c1 = egraph.emplace(Op::Const, 1);
  [[maybe_unused]] auto c2 = egraph.emplace(Op::Const, 2);
  [[maybe_unused]] auto v = egraph.emplace(Op::Var, 1);

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  auto pattern = make_leaf_pattern(Op::Const);
  auto matches = ematcher.match(pattern, ctx);

  // Should match both Const nodes
  EXPECT_EQ(matches.size(), 2);
}

TEST(EMatcher, MatchLeafSymbolNoMatches) {
  TestEGraph egraph;
  [[maybe_unused]] auto v = egraph.emplace(Op::Var, 1);

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  auto pattern = make_leaf_pattern(Op::Const);
  auto matches = ematcher.match(pattern, ctx);

  EXPECT_TRUE(matches.empty());
}

// --- Unary Pattern Matching Tests ---

TEST(EMatcher, MatchUnaryPattern) {
  TestEGraph egraph;
  auto x = egraph.emplace(Op::Var, 1);
  auto neg_x = egraph.emplace(Op::Neg, {x.current_eclassid});

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  auto pattern = make_unary_pattern(Op::Neg, 0);
  auto matches = ematcher.match(pattern, ctx);

  ASSERT_EQ(matches.size(), 1);
  EXPECT_EQ(matches[0].matched_eclass, neg_x.current_eclassid);
  EXPECT_EQ(matches[0].subst.at(PatternVar{0}), x.current_eclassid);
}

TEST(EMatcher, MatchUnaryPatternNoMatch) {
  TestEGraph egraph;
  auto x = egraph.emplace(Op::Var, 1);
  auto y = egraph.emplace(Op::Var, 2);
  [[maybe_unused]] auto add = egraph.emplace(Op::Add, {x.current_eclassid, y.current_eclassid});

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  auto pattern = make_unary_pattern(Op::Neg, 0);
  auto matches = ematcher.match(pattern, ctx);

  EXPECT_TRUE(matches.empty());
}

// --- Binary Pattern Matching Tests ---

TEST(EMatcher, MatchBinaryPattern) {
  TestEGraph egraph;
  auto x = egraph.emplace(Op::Var, 1);
  auto y = egraph.emplace(Op::Var, 2);
  auto add = egraph.emplace(Op::Add, {x.current_eclassid, y.current_eclassid});

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  auto pattern = make_binary_pattern(Op::Add, 0, 1);
  auto matches = ematcher.match(pattern, ctx);

  ASSERT_EQ(matches.size(), 1);
  EXPECT_EQ(matches[0].matched_eclass, add.current_eclassid);
  EXPECT_EQ(matches[0].subst.at(PatternVar{0}), x.current_eclassid);
  EXPECT_EQ(matches[0].subst.at(PatternVar{1}), y.current_eclassid);
}

TEST(EMatcher, MatchBinaryPatternMultipleMatches) {
  TestEGraph egraph;
  auto a = egraph.emplace(Op::Var, 1);
  auto b = egraph.emplace(Op::Var, 2);
  auto c = egraph.emplace(Op::Var, 3);
  [[maybe_unused]] auto add1 = egraph.emplace(Op::Add, {a.current_eclassid, b.current_eclassid});
  [[maybe_unused]] auto add2 = egraph.emplace(Op::Add, {b.current_eclassid, c.current_eclassid});

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  auto pattern = make_binary_pattern(Op::Add, 0, 1);
  auto matches = ematcher.match(pattern, ctx);

  EXPECT_EQ(matches.size(), 2);
}

// --- Same Variable Matching (Variable Consistency) Tests ---

TEST(EMatcher, MatchSameVariableTwice) {
  // Pattern: Add(?x, ?x) should only match when both children are same e-class
  TestEGraph egraph;
  auto a = egraph.emplace(Op::Var, 1);
  auto b = egraph.emplace(Op::Var, 2);
  auto add_aa = egraph.emplace(Op::Add, {a.current_eclassid, a.current_eclassid});
  [[maybe_unused]] auto add_ab = egraph.emplace(Op::Add, {a.current_eclassid, b.current_eclassid});

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  // Pattern: Add(?x, ?x)
  auto builder = TestPattern::Builder{};
  auto x = builder.var(0);
  auto add = builder.sym(Op::Add, {x, x});
  auto pattern = std::move(builder).build(add);

  auto matches = ematcher.match(pattern, ctx);

  // Only Add(a, a) should match, not Add(a, b)
  ASSERT_EQ(matches.size(), 1);
  EXPECT_EQ(matches[0].matched_eclass, add_aa.current_eclassid);
  EXPECT_EQ(matches[0].subst.at(PatternVar{0}), a.current_eclassid);
}

TEST(EMatcher, MatchSameVariableAfterMerge) {
  // After merging a and b, Add(?x, ?x) should also match Add(a, b)
  TestEGraph egraph;
  ProcessingContext<Op> pctx;

  auto a = egraph.emplace(Op::Var, 1);
  auto b = egraph.emplace(Op::Var, 2);
  [[maybe_unused]] auto add_ab = egraph.emplace(Op::Add, {a.current_eclassid, b.current_eclassid});

  // Merge a and b
  egraph.merge(a.current_eclassid, b.current_eclassid);
  egraph.rebuild(pctx);

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  // Pattern: Add(?x, ?x)
  auto builder = TestPattern::Builder{};
  auto x = builder.var(0);
  auto add = builder.sym(Op::Add, {x, x});
  auto pattern = std::move(builder).build(add);

  auto matches = ematcher.match(pattern, ctx);

  // Now Add(a, b) should match since a == b
  EXPECT_EQ(matches.size(), 1);
}

// --- Nested Pattern Matching Tests ---

TEST(EMatcher, MatchNestedPattern) {
  // Pattern: Add(Neg(?x), ?y)
  TestEGraph egraph;
  auto x = egraph.emplace(Op::Var, 1);
  auto y = egraph.emplace(Op::Var, 2);
  auto neg_x = egraph.emplace(Op::Neg, {x.current_eclassid});
  auto add = egraph.emplace(Op::Add, {neg_x.current_eclassid, y.current_eclassid});

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  auto builder = TestPattern::Builder{};
  auto px = builder.var(0);
  auto pneg = builder.sym(Op::Neg, {px});
  auto py = builder.var(1);
  auto padd = builder.sym(Op::Add, {pneg, py});
  auto pattern = std::move(builder).build(padd);

  auto matches = ematcher.match(pattern, ctx);

  ASSERT_EQ(matches.size(), 1);
  EXPECT_EQ(matches[0].matched_eclass, add.current_eclassid);
  EXPECT_EQ(matches[0].subst.at(PatternVar{0}), x.current_eclassid);
  EXPECT_EQ(matches[0].subst.at(PatternVar{1}), y.current_eclassid);
}

TEST(EMatcher, MatchDeeplyNestedPattern) {
  // Pattern: Add(Mul(Neg(?x), ?y), ?z)
  TestEGraph egraph;
  auto x = egraph.emplace(Op::Var, 1);
  auto y = egraph.emplace(Op::Var, 2);
  auto z = egraph.emplace(Op::Var, 3);
  auto neg_x = egraph.emplace(Op::Neg, {x.current_eclassid});
  auto mul = egraph.emplace(Op::Mul, {neg_x.current_eclassid, y.current_eclassid});
  auto add = egraph.emplace(Op::Add, {mul.current_eclassid, z.current_eclassid});

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  auto builder = TestPattern::Builder{};
  auto px = builder.var(0);
  auto pneg = builder.sym(Op::Neg, {px});
  auto py = builder.var(1);
  auto pmul = builder.sym(Op::Mul, {pneg, py});
  auto pz = builder.var(2);
  auto padd = builder.sym(Op::Add, {pmul, pz});
  auto pattern = std::move(builder).build(padd);

  auto matches = ematcher.match(pattern, ctx);

  ASSERT_EQ(matches.size(), 1);
  EXPECT_EQ(matches[0].matched_eclass, add.current_eclassid);
  EXPECT_EQ(matches[0].subst.at(PatternVar{0}), x.current_eclassid);
  EXPECT_EQ(matches[0].subst.at(PatternVar{1}), y.current_eclassid);
  EXPECT_EQ(matches[0].subst.at(PatternVar{2}), z.current_eclassid);
}

// --- Incremental Rebuild Tests ---

TEST(EMatcher, IncrementalRebuildFindsNewNodes) {
  TestEGraph egraph;
  auto x = egraph.emplace(Op::Var, 1);

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  // Initially only Var pattern matches
  auto var_pattern = make_leaf_pattern(Op::Var);
  EXPECT_EQ(ematcher.match(var_pattern, ctx).size(), 1);

  auto const_pattern = make_leaf_pattern(Op::Const);
  EXPECT_TRUE(ematcher.match(const_pattern, ctx).empty());

  // Add new Const node
  auto c = egraph.emplace(Op::Const, 42);

  // Before rebuild, Const pattern still doesn't match
  EXPECT_TRUE(ematcher.match(const_pattern, ctx).empty());

  // Incremental rebuild with new e-class
  std::vector<EClassId> new_eclasses = {c.current_eclassid};
  ematcher.rebuild(new_eclasses);

  // Now Const pattern matches
  auto const_matches = ematcher.match(const_pattern, ctx);
  ASSERT_EQ(const_matches.size(), 1);
  EXPECT_EQ(const_matches[0].matched_eclass, c.current_eclassid);
}

TEST(EMatcher, FullRebuildAfterMerge) {
  TestEGraph egraph;
  ProcessingContext<Op> pctx;

  auto a = egraph.emplace(Op::Var, 1);
  auto b = egraph.emplace(Op::Var, 2);
  [[maybe_unused]] auto add = egraph.emplace(Op::Add, {a.current_eclassid, b.current_eclassid});

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  // Pattern: Add(?x, ?x) - doesn't match initially
  auto builder = TestPattern::Builder{};
  auto x = builder.var(0);
  auto padd = builder.sym(Op::Add, {x, x});
  auto pattern = std::move(builder).build(padd);

  EXPECT_TRUE(ematcher.match(pattern, ctx).empty());

  // Merge a and b
  egraph.merge(a.current_eclassid, b.current_eclassid);
  egraph.rebuild(pctx);

  // Full rebuild to pick up merge
  ematcher.rebuild();

  // Now should match
  EXPECT_EQ(ematcher.match(pattern, ctx).size(), 1);
}

// --- Multiple E-nodes in Same E-class Tests ---

TEST(EMatcher, MatchWithMergedEClasses) {
  // After merge, an e-class might have multiple e-nodes, pattern should match any
  TestEGraph egraph;
  ProcessingContext<Op> pctx;

  auto x = egraph.emplace(Op::Var, 1);
  auto y = egraph.emplace(Op::Var, 2);
  auto add_xy = egraph.emplace(Op::Add, {x.current_eclassid, y.current_eclassid});
  auto mul_xy = egraph.emplace(Op::Mul, {x.current_eclassid, y.current_eclassid});

  // Merge Add and Mul e-classes (pretend they're equivalent)
  [[maybe_unused]] auto merged = egraph.merge(add_xy.current_eclassid, mul_xy.current_eclassid);
  egraph.rebuild(pctx);

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  // Pattern for Add should still match
  auto add_pattern = make_binary_pattern(Op::Add, 0, 1);
  auto add_matches = ematcher.match(add_pattern, ctx);
  EXPECT_EQ(add_matches.size(), 1);

  // Pattern for Mul should also match (same e-class)
  auto mul_pattern = make_binary_pattern(Op::Mul, 0, 1);
  auto mul_matches = ematcher.match(mul_pattern, ctx);
  EXPECT_EQ(mul_matches.size(), 1);

  // Both should match the same merged e-class
  EXPECT_EQ(add_matches[0].matched_eclass, mul_matches[0].matched_eclass);
}

// --- Arity Mismatch Tests ---

TEST(EMatcher, NoMatchOnArityMismatch) {
  TestEGraph egraph;
  auto x = egraph.emplace(Op::Var, 1);
  auto y = egraph.emplace(Op::Var, 2);
  auto z = egraph.emplace(Op::Var, 3);

  // Create 3-ary F(x, y, z)
  [[maybe_unused]] auto f3 = egraph.emplace(Op::F, {x.current_eclassid, y.current_eclassid, z.current_eclassid});

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  // Pattern: F(?a, ?b) - 2-ary
  auto builder = TestPattern::Builder{};
  auto pa = builder.var(0);
  auto pb = builder.var(1);
  auto pf = builder.sym(Op::F, {pa, pb});
  auto pattern = std::move(builder).build(pf);

  auto matches = ematcher.match(pattern, ctx);

  // Should not match because arity is different
  EXPECT_TRUE(matches.empty());
}

// --- Complex Scenario Tests ---

TEST(EMatcher, ComplexExpressionGraph) {
  // Build: (a + b) * (c + d)
  TestEGraph egraph;
  auto a = egraph.emplace(Op::Var, 1);
  auto b = egraph.emplace(Op::Var, 2);
  auto c = egraph.emplace(Op::Var, 3);
  auto d = egraph.emplace(Op::Var, 4);
  auto add1 = egraph.emplace(Op::Add, {a.current_eclassid, b.current_eclassid});
  auto add2 = egraph.emplace(Op::Add, {c.current_eclassid, d.current_eclassid});
  auto mul = egraph.emplace(Op::Mul, {add1.current_eclassid, add2.current_eclassid});

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  // Pattern: Mul(Add(?x, ?y), ?z)
  auto builder = TestPattern::Builder{};
  auto px = builder.var(0);
  auto py = builder.var(1);
  auto padd = builder.sym(Op::Add, {px, py});
  auto pz = builder.var(2);
  auto pmul = builder.sym(Op::Mul, {padd, pz});
  auto pattern = std::move(builder).build(pmul);

  auto matches = ematcher.match(pattern, ctx);

  // Should match with first Add
  ASSERT_EQ(matches.size(), 1);
  EXPECT_EQ(matches[0].matched_eclass, mul.current_eclassid);
}

TEST(EMatcher, MultipleMatchesSameExpression) {
  // Pattern that can match in multiple ways within same expression
  // Build: Add(x, Add(y, z))
  TestEGraph egraph;
  auto x = egraph.emplace(Op::Var, 1);
  auto y = egraph.emplace(Op::Var, 2);
  auto z = egraph.emplace(Op::Var, 3);
  auto inner_add = egraph.emplace(Op::Add, {y.current_eclassid, z.current_eclassid});
  [[maybe_unused]] auto outer_add = egraph.emplace(Op::Add, {x.current_eclassid, inner_add.current_eclassid});

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  // Pattern: Add(?a, ?b) - should match both Add nodes
  auto pattern = make_binary_pattern(Op::Add, 0, 1);
  auto matches = ematcher.match(pattern, ctx);

  EXPECT_EQ(matches.size(), 2);
}

// --- Same Variable at Different Depths Tests ---

TEST(EMatcher, SameVariableAtDifferentDepths) {
  // Pattern: Add(?x, Add(?x, ?y)) - ?x appears at depth 1 and depth 2
  // Should only match when the first child equals the first child of the inner Add

  TestEGraph egraph;
  auto a = egraph.emplace(Op::Var, 1);
  auto b = egraph.emplace(Op::Var, 2);
  auto c = egraph.emplace(Op::Var, 3);

  // Build: Add(a, Add(a, b)) - should match with ?x=a, ?y=b
  auto inner_match = egraph.emplace(Op::Add, {a.current_eclassid, b.current_eclassid});
  auto outer_match = egraph.emplace(Op::Add, {a.current_eclassid, inner_match.current_eclassid});

  // Build: Add(a, Add(c, b)) - should NOT match (a != c)
  auto inner_nomatch = egraph.emplace(Op::Add, {c.current_eclassid, b.current_eclassid});
  [[maybe_unused]] auto outer_nomatch = egraph.emplace(Op::Add, {a.current_eclassid, inner_nomatch.current_eclassid});

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  // Pattern: Add(?x, Add(?x, ?y))
  auto builder = TestPattern::Builder{};
  auto px = builder.var(0);   // ?x
  auto py = builder.var(1);   // ?y
  auto px2 = builder.var(0);  // ?x again (same id!)
  auto inner = builder.sym(Op::Add, {px2, py});
  auto outer = builder.sym(Op::Add, {px, inner});
  auto pattern = std::move(builder).build(outer);

  auto matches = ematcher.match(pattern, ctx);

  // Only outer_match should match
  ASSERT_EQ(matches.size(), 1);
  EXPECT_EQ(matches[0].matched_eclass, outer_match.current_eclassid);
  EXPECT_EQ(matches[0].subst.at(PatternVar{0}), a.current_eclassid);  // ?x = a
  EXPECT_EQ(matches[0].subst.at(PatternVar{1}), b.current_eclassid);  // ?y = b
}

TEST(EMatcher, SameVariableAtDifferentDepthsAfterMerge) {
  // After merging a and c, Add(a, Add(c, b)) should also match Add(?x, Add(?x, ?y))
  TestEGraph egraph;
  ProcessingContext<Op> pctx;

  auto a = egraph.emplace(Op::Var, 1);
  auto b = egraph.emplace(Op::Var, 2);
  auto c = egraph.emplace(Op::Var, 3);

  // Build: Add(a, Add(c, b)) - initially won't match
  auto inner = egraph.emplace(Op::Add, {c.current_eclassid, b.current_eclassid});
  auto outer = egraph.emplace(Op::Add, {a.current_eclassid, inner.current_eclassid});

  // Merge a and c
  egraph.merge(a.current_eclassid, c.current_eclassid);
  egraph.rebuild(pctx);

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  // Pattern: Add(?x, Add(?x, ?y))
  auto builder = TestPattern::Builder{};
  auto px = builder.var(0);
  auto py = builder.var(1);
  auto px2 = builder.var(0);  // same ?x
  auto pinner = builder.sym(Op::Add, {px2, py});
  auto pouter = builder.sym(Op::Add, {px, pinner});
  auto pattern = std::move(builder).build(pouter);

  auto matches = ematcher.match(pattern, ctx);

  // Now should match because a == c
  ASSERT_EQ(matches.size(), 1);
  EXPECT_EQ(matches[0].matched_eclass, egraph.find(outer.current_eclassid));
}

TEST(EMatcher, SameVariableThreeOccurrences) {
  // Pattern: Add(?x, Add(?x, ?x)) - ?x appears three times at different depths
  TestEGraph egraph;
  auto a = egraph.emplace(Op::Var, 1);
  auto b = egraph.emplace(Op::Var, 2);

  // Build: Add(a, Add(a, a)) - should match
  auto inner_aaa = egraph.emplace(Op::Add, {a.current_eclassid, a.current_eclassid});
  auto outer_aaa = egraph.emplace(Op::Add, {a.current_eclassid, inner_aaa.current_eclassid});

  // Build: Add(a, Add(a, b)) - should NOT match (third ?x != b)
  auto inner_aab = egraph.emplace(Op::Add, {a.current_eclassid, b.current_eclassid});
  [[maybe_unused]] auto outer_aab = egraph.emplace(Op::Add, {a.current_eclassid, inner_aab.current_eclassid});

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  // Pattern: Add(?x, Add(?x, ?x))
  auto builder = TestPattern::Builder{};
  auto px1 = builder.var(0);
  auto px2 = builder.var(0);
  auto px3 = builder.var(0);
  auto inner = builder.sym(Op::Add, {px2, px3});
  auto outer = builder.sym(Op::Add, {px1, inner});
  auto pattern = std::move(builder).build(outer);

  auto matches = ematcher.match(pattern, ctx);

  // Only Add(a, Add(a, a)) should match
  ASSERT_EQ(matches.size(), 1);
  EXPECT_EQ(matches[0].matched_eclass, outer_aaa.current_eclassid);
  EXPECT_EQ(matches[0].subst.at(PatternVar{0}), a.current_eclassid);
}

// --- Context Reuse Tests ---

TEST(EMatcher, ContextReuseAcrossMultipleMatches) {
  TestEGraph egraph;
  auto a = egraph.emplace(Op::Var, 1);
  auto b = egraph.emplace(Op::Var, 2);
  [[maybe_unused]] auto add = egraph.emplace(Op::Add, {a.current_eclassid, b.current_eclassid});
  [[maybe_unused]] auto mul = egraph.emplace(Op::Mul, {a.current_eclassid, b.current_eclassid});

  TestEMatcher ematcher(egraph);
  EMatchContext ctx;

  // Match multiple patterns with same context
  auto add_pattern = make_binary_pattern(Op::Add, 0, 1);
  auto mul_pattern = make_binary_pattern(Op::Mul, 0, 1);
  auto var_pattern = make_var_pattern(0);

  auto add_matches = ematcher.match(add_pattern, ctx);
  auto mul_matches = ematcher.match(mul_pattern, ctx);
  auto var_matches = ematcher.match(var_pattern, ctx);

  EXPECT_EQ(add_matches.size(), 1);
  EXPECT_EQ(mul_matches.size(), 1);
  EXPECT_EQ(var_matches.size(), 4);  // 4 e-classes total
}

}  // namespace memgraph::planner::core
