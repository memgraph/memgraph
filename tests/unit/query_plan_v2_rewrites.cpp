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

#include "query/plan_v2/egraph.hpp"
#include "query/plan_v2/egraph_internal.hpp"
#include "query/plan_v2/rewrites.hpp"

namespace memgraph::query::plan::v2 {
namespace {

class InlineRewriteTest : public ::testing::Test {
 protected:
  egraph eg_;
};

// Test: Simple inline rewrite
// Bind(Once, sym, Literal) + Identifier(sym) -> Identifier merged with Literal
TEST_F(InlineRewriteTest, SimpleInline) {
  // Create: Bind(Once, sym_0, Literal(42))
  auto once = eg_.MakeOnce();
  auto sym = eg_.MakeSymbol(0, "x");
  auto literal = eg_.MakeLiteral(storage::ExternalPropertyValue{42});
  [[maybe_unused]] auto bind = eg_.MakeBind(once, sym, literal);

  // Create: Identifier(sym_0)
  auto ident = eg_.MakeIdentifier(sym);

  // Get internal e-graph for inspection
  auto &impl = internal::get_impl(eg_);
  auto &core_egraph = impl.egraph_;

  // Before rewrite: ident and literal are in different e-classes
  auto ident_core = internal::to_core_id(ident);
  auto literal_core = internal::to_core_id(literal);
  EXPECT_NE(core_egraph.find(ident_core), core_egraph.find(literal_core));

  // Apply rewrite
  auto merges = ApplyInlineRewrite(eg_);

  // Should have merged one pair
  EXPECT_EQ(merges, 1);

  // After rewrite: ident and literal should be in same e-class
  EXPECT_EQ(core_egraph.find(ident_core), core_egraph.find(literal_core));
}

// Test: Multiple identifiers referencing same binding
TEST_F(InlineRewriteTest, MultipleIdentifiersSameBinding) {
  // Create: Bind(Once, sym_0, Literal(42))
  auto once = eg_.MakeOnce();
  auto sym = eg_.MakeSymbol(0, "x");
  auto literal = eg_.MakeLiteral(storage::ExternalPropertyValue{42});
  [[maybe_unused]] auto bind = eg_.MakeBind(once, sym, literal);

  // Create two Identifiers referencing same sym
  auto ident1 = eg_.MakeIdentifier(sym);
  auto ident2 = eg_.MakeIdentifier(sym);

  auto &impl = internal::get_impl(eg_);
  auto &core_egraph = impl.egraph_;

  auto ident1_core = internal::to_core_id(ident1);
  auto ident2_core = internal::to_core_id(ident2);
  auto literal_core = internal::to_core_id(literal);

  // Before: all different e-classes
  EXPECT_NE(core_egraph.find(ident1_core), core_egraph.find(literal_core));
  EXPECT_NE(core_egraph.find(ident2_core), core_egraph.find(literal_core));

  // Apply rewrite
  auto merges = ApplyInlineRewrite(eg_);

  // Should merge both identifiers with the literal
  // Note: ident1 and ident2 might already be in same e-class due to hash-consing
  // so we check they all end up in the same e-class as literal
  EXPECT_GE(merges, 1);

  // After: all in same e-class
  EXPECT_EQ(core_egraph.find(ident1_core), core_egraph.find(literal_core));
  EXPECT_EQ(core_egraph.find(ident2_core), core_egraph.find(literal_core));
}

// Test: No rewrite when no binding exists
TEST_F(InlineRewriteTest, NoBindingNoRewrite) {
  // Create just an Identifier without corresponding Bind
  auto sym = eg_.MakeSymbol(0, "x");
  [[maybe_unused]] auto ident = eg_.MakeIdentifier(sym);

  // Apply rewrite - should do nothing
  auto merges = ApplyInlineRewrite(eg_);
  EXPECT_EQ(merges, 0);
}

// Test: Different symbols don't interfere
TEST_F(InlineRewriteTest, DifferentSymbolsIndependent) {
  // Create: Bind(Once, sym_0, Literal(1))
  auto once1 = eg_.MakeOnce();
  auto sym0 = eg_.MakeSymbol(0, "x");
  auto literal1 = eg_.MakeLiteral(storage::ExternalPropertyValue{1});
  [[maybe_unused]] auto bind1 = eg_.MakeBind(once1, sym0, literal1);

  // Create: Bind(Once, sym_1, Literal(2))
  auto once2 = eg_.MakeOnce();
  auto sym1 = eg_.MakeSymbol(1, "y");
  auto literal2 = eg_.MakeLiteral(storage::ExternalPropertyValue{2});
  [[maybe_unused]] auto bind2 = eg_.MakeBind(once2, sym1, literal2);

  // Create: Identifier(sym_0) and Identifier(sym_1)
  auto ident0 = eg_.MakeIdentifier(sym0);
  auto ident1 = eg_.MakeIdentifier(sym1);

  auto &impl = internal::get_impl(eg_);
  auto &core_egraph = impl.egraph_;

  // Apply rewrite
  auto merges = ApplyInlineRewrite(eg_);
  EXPECT_EQ(merges, 2);

  // Verify correct merging
  auto ident0_core = internal::to_core_id(ident0);
  auto ident1_core = internal::to_core_id(ident1);
  auto literal1_core = internal::to_core_id(literal1);
  auto literal2_core = internal::to_core_id(literal2);

  // ident0 should be merged with literal1 (value 1)
  EXPECT_EQ(core_egraph.find(ident0_core), core_egraph.find(literal1_core));

  // ident1 should be merged with literal2 (value 2)
  EXPECT_EQ(core_egraph.find(ident1_core), core_egraph.find(literal2_core));

  // But literal1 and literal2 should NOT be merged (different values)
  EXPECT_NE(core_egraph.find(literal1_core), core_egraph.find(literal2_core));
}

// Test: Chained bindings
// x = 1, y = x => Identifier(y) should be equivalent to Literal(1)
TEST_F(InlineRewriteTest, ChainedBindings) {
  // Create: Bind(Once, sym_x, Literal(1))
  auto once = eg_.MakeOnce();
  auto sym_x = eg_.MakeSymbol(0, "x");
  auto literal = eg_.MakeLiteral(storage::ExternalPropertyValue{1});
  auto bind_x = eg_.MakeBind(once, sym_x, literal);

  // Create: Identifier(sym_x) - this is what we'll bind to y
  auto ident_x = eg_.MakeIdentifier(sym_x);

  // Create: Bind(bind_x, sym_y, Identifier(sym_x))
  auto sym_y = eg_.MakeSymbol(1, "y");
  [[maybe_unused]] auto bind_y = eg_.MakeBind(bind_x, sym_y, ident_x);

  // Create: Identifier(sym_y)
  auto ident_y = eg_.MakeIdentifier(sym_y);

  auto &impl = internal::get_impl(eg_);
  auto &core_egraph = impl.egraph_;

  // Apply rewrite multiple times to propagate
  auto result = ApplyAllRewrites(eg_, RewriteConfig{.max_iterations = 5});
  EXPECT_GE(result.rewrites_applied, 2);

  // After full propagation:
  // - ident_x should be merged with literal (from first binding)
  // - ident_y should be merged with ident_x (from second binding)
  // - Therefore ident_y should also be in same e-class as literal

  auto ident_y_core = internal::to_core_id(ident_y);
  auto literal_core = internal::to_core_id(literal);

  EXPECT_EQ(core_egraph.find(ident_y_core), core_egraph.find(literal_core));
}

// Test: Iteration limit stops rewriting
TEST_F(InlineRewriteTest, IterationLimit) {
  // Create a chain of bindings that requires multiple iterations
  // x = 1, y = x, z = y => need 2 iterations to propagate to z
  auto once = eg_.MakeOnce();
  auto sym_x = eg_.MakeSymbol(0, "x");
  auto literal = eg_.MakeLiteral(storage::ExternalPropertyValue{1});
  auto bind_x = eg_.MakeBind(once, sym_x, literal);

  auto ident_x = eg_.MakeIdentifier(sym_x);
  auto sym_y = eg_.MakeSymbol(1, "y");
  auto bind_y = eg_.MakeBind(bind_x, sym_y, ident_x);

  auto ident_y = eg_.MakeIdentifier(sym_y);
  auto sym_z = eg_.MakeSymbol(2, "z");
  [[maybe_unused]] auto bind_z = eg_.MakeBind(bind_y, sym_z, ident_y);

  [[maybe_unused]] auto ident_z = eg_.MakeIdentifier(sym_z);

  // With max_iterations=1, should stop early
  auto result = ApplyAllRewrites(eg_, RewriteConfig{.max_iterations = 1});
  EXPECT_EQ(result.stop_reason, RewriteResult::StopReason::IterationLimit);
  EXPECT_EQ(result.iterations, 1);
  EXPECT_GE(result.rewrites_applied, 1);
}

// Test: Default config reaches saturation
TEST_F(InlineRewriteTest, DefaultConfig) {
  // Create simple binding
  auto once = eg_.MakeOnce();
  auto sym = eg_.MakeSymbol(0, "x");
  auto literal = eg_.MakeLiteral(storage::ExternalPropertyValue{42});
  [[maybe_unused]] auto bind = eg_.MakeBind(once, sym, literal);
  [[maybe_unused]] auto ident = eg_.MakeIdentifier(sym);

  // Default config should saturate
  auto result = ApplyAllRewrites(eg_, RewriteConfig::Default());
  EXPECT_TRUE(result.saturated());
  EXPECT_EQ(result.rewrites_applied, 1);
}

// Test: Unlimited config reaches saturation (no limits hit)
TEST_F(InlineRewriteTest, UnlimitedConfig) {
  // Create simple binding
  auto once = eg_.MakeOnce();
  auto sym = eg_.MakeSymbol(0, "x");
  auto literal = eg_.MakeLiteral(storage::ExternalPropertyValue{42});
  [[maybe_unused]] auto bind = eg_.MakeBind(once, sym, literal);
  [[maybe_unused]] auto ident = eg_.MakeIdentifier(sym);

  // Unlimited config should saturate
  auto result = ApplyAllRewrites(eg_, RewriteConfig::Unlimited());
  EXPECT_TRUE(result.saturated());
  EXPECT_EQ(result.rewrites_applied, 1);
}

// Test: ApplyAllRewrites reaches fixed point
TEST_F(InlineRewriteTest, FixedPoint) {
  // Create simple binding
  auto once = eg_.MakeOnce();
  auto sym = eg_.MakeSymbol(0, "x");
  auto literal = eg_.MakeLiteral(storage::ExternalPropertyValue{42});
  [[maybe_unused]] auto bind = eg_.MakeBind(once, sym, literal);
  [[maybe_unused]] auto ident = eg_.MakeIdentifier(sym);

  // First call should do work
  auto result1 = ApplyAllRewrites(eg_);
  EXPECT_EQ(result1.rewrites_applied, 1);
  EXPECT_TRUE(result1.saturated());

  // Second call should be at fixed point
  auto result2 = ApplyAllRewrites(eg_);
  EXPECT_EQ(result2.rewrites_applied, 0);
  EXPECT_TRUE(result2.saturated());
}

// =======================================================================
// Operator construction tests
// =======================================================================

class OperatorConstructionTest : public ::testing::Test {
 protected:
  egraph eg_;
};

// Binary operators produce distinct eclasses for different operands
TEST_F(OperatorConstructionTest, BinaryOperatorsDistinct) {
  auto lit1 = eg_.MakeLiteral(storage::ExternalPropertyValue{1});
  auto lit2 = eg_.MakeLiteral(storage::ExternalPropertyValue{2});
  auto lit3 = eg_.MakeLiteral(storage::ExternalPropertyValue{3});

  auto add12 = eg_.MakeAdd(lit1, lit2);
  auto add13 = eg_.MakeAdd(lit1, lit3);
  auto sub12 = eg_.MakeSub(lit1, lit2);

  auto &impl = internal::get_impl(eg_);
  auto &core = impl.egraph_;

  // Different operands or different operator → different eclasses
  EXPECT_NE(core.find(internal::to_core_id(add12)), core.find(internal::to_core_id(add13)));
  EXPECT_NE(core.find(internal::to_core_id(add12)), core.find(internal::to_core_id(sub12)));
}

// Hash-consing: same operator + same children → same eclass
TEST_F(OperatorConstructionTest, HashConsing) {
  auto lit1 = eg_.MakeLiteral(storage::ExternalPropertyValue{1});
  auto lit2 = eg_.MakeLiteral(storage::ExternalPropertyValue{2});

  auto add_a = eg_.MakeAdd(lit1, lit2);
  auto add_b = eg_.MakeAdd(lit1, lit2);

  auto &impl = internal::get_impl(eg_);
  auto &core = impl.egraph_;

  // Same operation, same operands → same eclass
  EXPECT_EQ(core.find(internal::to_core_id(add_a)), core.find(internal::to_core_id(add_b)));
}

// Operand order matters: Add(1,2) != Add(2,1)
TEST_F(OperatorConstructionTest, OperandOrderMatters) {
  auto lit1 = eg_.MakeLiteral(storage::ExternalPropertyValue{1});
  auto lit2 = eg_.MakeLiteral(storage::ExternalPropertyValue{2});

  auto add12 = eg_.MakeAdd(lit1, lit2);
  auto add21 = eg_.MakeAdd(lit2, lit1);

  auto &impl = internal::get_impl(eg_);
  auto &core = impl.egraph_;

  EXPECT_NE(core.find(internal::to_core_id(add12)), core.find(internal::to_core_id(add21)));
}

// Unary operators
TEST_F(OperatorConstructionTest, UnaryOperators) {
  auto lit = eg_.MakeLiteral(storage::ExternalPropertyValue{5});

  auto neg = eg_.MakeUnaryMinus(lit);
  auto pos = eg_.MakeUnaryPlus(lit);
  auto not_op = eg_.MakeNot(lit);

  auto &impl = internal::get_impl(eg_);
  auto &core = impl.egraph_;

  // Different unary operators → different eclasses
  EXPECT_NE(core.find(internal::to_core_id(neg)), core.find(internal::to_core_id(pos)));
  EXPECT_NE(core.find(internal::to_core_id(neg)), core.find(internal::to_core_id(not_op)));
}

// Nested operators create correct tree
TEST_F(OperatorConstructionTest, NestedOperators) {
  auto lit1 = eg_.MakeLiteral(storage::ExternalPropertyValue{1});
  auto lit2 = eg_.MakeLiteral(storage::ExternalPropertyValue{2});
  auto lit3 = eg_.MakeLiteral(storage::ExternalPropertyValue{3});

  // (1 + 2) * 3
  auto add = eg_.MakeAdd(lit1, lit2);
  auto mul = eg_.MakeMul(add, lit3);

  auto &impl = internal::get_impl(eg_);
  auto &core = impl.egraph_;

  // All distinct
  EXPECT_NE(core.find(internal::to_core_id(add)), core.find(internal::to_core_id(mul)));
  EXPECT_NE(core.find(internal::to_core_id(lit1)), core.find(internal::to_core_id(add)));
  EXPECT_NE(core.find(internal::to_core_id(lit3)), core.find(internal::to_core_id(mul)));
}

// All comparison operators produce distinct eclasses
TEST_F(OperatorConstructionTest, ComparisonOperatorsDistinct) {
  auto lit1 = eg_.MakeLiteral(storage::ExternalPropertyValue{1});
  auto lit2 = eg_.MakeLiteral(storage::ExternalPropertyValue{2});

  auto eq = eg_.MakeEq(lit1, lit2);
  auto neq = eg_.MakeNeq(lit1, lit2);
  auto lt = eg_.MakeLt(lit1, lit2);
  auto lte = eg_.MakeLte(lit1, lit2);
  auto gt = eg_.MakeGt(lit1, lit2);
  auto gte = eg_.MakeGte(lit1, lit2);

  auto &impl = internal::get_impl(eg_);
  auto &core = impl.egraph_;

  auto ids = std::vector{eq, neq, lt, lte, gt, gte};
  for (std::size_t i = 0; i < ids.size(); ++i) {
    for (std::size_t j = i + 1; j < ids.size(); ++j) {
      EXPECT_NE(core.find(internal::to_core_id(ids[i])), core.find(internal::to_core_id(ids[j])))
          << "Operators at index " << i << " and " << j << " should be distinct";
    }
  }
}

// Boolean operators produce distinct eclasses
TEST_F(OperatorConstructionTest, BooleanOperatorsDistinct) {
  auto t = eg_.MakeLiteral(storage::ExternalPropertyValue{true});
  auto f = eg_.MakeLiteral(storage::ExternalPropertyValue{false});

  auto and_op = eg_.MakeAnd(t, f);
  auto or_op = eg_.MakeOr(t, f);
  auto xor_op = eg_.MakeXor(t, f);

  auto &impl = internal::get_impl(eg_);
  auto &core = impl.egraph_;

  EXPECT_NE(core.find(internal::to_core_id(and_op)), core.find(internal::to_core_id(or_op)));
  EXPECT_NE(core.find(internal::to_core_id(and_op)), core.find(internal::to_core_id(xor_op)));
  EXPECT_NE(core.find(internal::to_core_id(or_op)), core.find(internal::to_core_id(xor_op)));
}

// =======================================================================
// Inline rewrite through operators
// =======================================================================

class InlineThroughOperatorTest : public ::testing::Test {
 protected:
  egraph eg_;
};

// Bind(Once, sym, Literal(1)) + Add(Identifier(sym), Literal(2))
// → Identifier should be merged with Literal(1)
TEST_F(InlineThroughOperatorTest, InlineThroughAdd) {
  auto once = eg_.MakeOnce();
  auto sym = eg_.MakeSymbol(0, "a");
  auto lit1 = eg_.MakeLiteral(storage::ExternalPropertyValue{1});
  [[maybe_unused]] auto bind = eg_.MakeBind(once, sym, lit1);

  auto ident = eg_.MakeIdentifier(sym);
  auto lit2 = eg_.MakeLiteral(storage::ExternalPropertyValue{2});
  [[maybe_unused]] auto add = eg_.MakeAdd(ident, lit2);

  auto &impl = internal::get_impl(eg_);
  auto &core = impl.egraph_;

  auto result = ApplyAllRewrites(eg_);
  EXPECT_GE(result.rewrites_applied, 1);
  EXPECT_TRUE(result.saturated());

  // Identifier(sym) should be merged with Literal(1)
  EXPECT_EQ(core.find(internal::to_core_id(ident)), core.find(internal::to_core_id(lit1)));
}

// Inline through unary minus: Bind(Once, sym, Lit(5)) + UnaryMinus(Identifier(sym))
TEST_F(InlineThroughOperatorTest, InlineThroughUnaryMinus) {
  auto once = eg_.MakeOnce();
  auto sym = eg_.MakeSymbol(0, "a");
  auto lit = eg_.MakeLiteral(storage::ExternalPropertyValue{5});
  [[maybe_unused]] auto bind = eg_.MakeBind(once, sym, lit);

  auto ident = eg_.MakeIdentifier(sym);
  [[maybe_unused]] auto neg = eg_.MakeUnaryMinus(ident);

  auto &impl = internal::get_impl(eg_);
  auto &core = impl.egraph_;

  auto result = ApplyAllRewrites(eg_);
  EXPECT_GE(result.rewrites_applied, 1);
  EXPECT_TRUE(result.saturated());

  EXPECT_EQ(core.find(internal::to_core_id(ident)), core.find(internal::to_core_id(lit)));
}

// Inline same variable used twice: Add(Identifier(sym), Identifier(sym))
TEST_F(InlineThroughOperatorTest, InlineSameVarBothSides) {
  auto once = eg_.MakeOnce();
  auto sym = eg_.MakeSymbol(0, "a");
  auto lit = eg_.MakeLiteral(storage::ExternalPropertyValue{1});
  [[maybe_unused]] auto bind = eg_.MakeBind(once, sym, lit);

  auto ident = eg_.MakeIdentifier(sym);
  [[maybe_unused]] auto add = eg_.MakeAdd(ident, ident);

  auto &impl = internal::get_impl(eg_);
  auto &core = impl.egraph_;

  auto result = ApplyAllRewrites(eg_);
  EXPECT_GE(result.rewrites_applied, 1);
  EXPECT_TRUE(result.saturated());

  // Identifier merged with literal
  EXPECT_EQ(core.find(internal::to_core_id(ident)), core.find(internal::to_core_id(lit)));
}

// Inline through comparison: Bind + Lt(Identifier, Literal)
TEST_F(InlineThroughOperatorTest, InlineThroughComparison) {
  auto once = eg_.MakeOnce();
  auto sym = eg_.MakeSymbol(0, "a");
  auto lit1 = eg_.MakeLiteral(storage::ExternalPropertyValue{1});
  [[maybe_unused]] auto bind = eg_.MakeBind(once, sym, lit1);

  auto ident = eg_.MakeIdentifier(sym);
  auto lit2 = eg_.MakeLiteral(storage::ExternalPropertyValue{2});
  [[maybe_unused]] auto lt = eg_.MakeLt(ident, lit2);

  auto &impl = internal::get_impl(eg_);
  auto &core = impl.egraph_;

  auto result = ApplyAllRewrites(eg_);
  EXPECT_GE(result.rewrites_applied, 1);
  EXPECT_TRUE(result.saturated());

  EXPECT_EQ(core.find(internal::to_core_id(ident)), core.find(internal::to_core_id(lit1)));
}

// Inline through boolean: Bind + And(Identifier, Literal)
TEST_F(InlineThroughOperatorTest, InlineThroughBoolean) {
  auto once = eg_.MakeOnce();
  auto sym = eg_.MakeSymbol(0, "a");
  auto lit_t = eg_.MakeLiteral(storage::ExternalPropertyValue{true});
  [[maybe_unused]] auto bind = eg_.MakeBind(once, sym, lit_t);

  auto ident = eg_.MakeIdentifier(sym);
  auto lit_f = eg_.MakeLiteral(storage::ExternalPropertyValue{false});
  [[maybe_unused]] auto and_op = eg_.MakeAnd(ident, lit_f);

  auto &impl = internal::get_impl(eg_);
  auto &core = impl.egraph_;

  auto result = ApplyAllRewrites(eg_);
  EXPECT_GE(result.rewrites_applied, 1);
  EXPECT_TRUE(result.saturated());

  EXPECT_EQ(core.find(internal::to_core_id(ident)), core.find(internal::to_core_id(lit_t)));
}

// Nested: Bind(Once, sym, Lit(1)) + Mul(Add(Identifier(sym), Lit(2)), Lit(3))
TEST_F(InlineThroughOperatorTest, InlineThroughNestedOperators) {
  auto once = eg_.MakeOnce();
  auto sym = eg_.MakeSymbol(0, "a");
  auto lit1 = eg_.MakeLiteral(storage::ExternalPropertyValue{1});
  [[maybe_unused]] auto bind = eg_.MakeBind(once, sym, lit1);

  auto ident = eg_.MakeIdentifier(sym);
  auto lit2 = eg_.MakeLiteral(storage::ExternalPropertyValue{2});
  auto lit3 = eg_.MakeLiteral(storage::ExternalPropertyValue{3});
  auto add = eg_.MakeAdd(ident, lit2);
  [[maybe_unused]] auto mul = eg_.MakeMul(add, lit3);

  auto &impl = internal::get_impl(eg_);
  auto &core = impl.egraph_;

  auto result = ApplyAllRewrites(eg_);
  EXPECT_GE(result.rewrites_applied, 1);
  EXPECT_TRUE(result.saturated());

  // Identifier should be merged with Literal(1) even when deeply nested
  EXPECT_EQ(core.find(internal::to_core_id(ident)), core.find(internal::to_core_id(lit1)));
}

}  // namespace
}  // namespace memgraph::query::plan::v2
