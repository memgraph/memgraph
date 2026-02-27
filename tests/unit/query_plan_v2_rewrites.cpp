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
  auto bind = eg_.MakeBind(once, sym, literal);
  (void)bind;

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
  auto bind = eg_.MakeBind(once, sym, literal);
  (void)bind;

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
  auto ident = eg_.MakeIdentifier(sym);
  (void)ident;

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
  auto bind1 = eg_.MakeBind(once1, sym0, literal1);
  (void)bind1;

  // Create: Bind(Once, sym_1, Literal(2))
  auto once2 = eg_.MakeOnce();
  auto sym1 = eg_.MakeSymbol(1, "y");
  auto literal2 = eg_.MakeLiteral(storage::ExternalPropertyValue{2});
  auto bind2 = eg_.MakeBind(once2, sym1, literal2);
  (void)bind2;

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
  auto bind_y = eg_.MakeBind(bind_x, sym_y, ident_x);
  (void)bind_y;

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
  auto bind_z = eg_.MakeBind(bind_y, sym_z, ident_y);
  (void)bind_z;

  auto ident_z = eg_.MakeIdentifier(sym_z);
  (void)ident_z;

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
  auto bind = eg_.MakeBind(once, sym, literal);
  (void)bind;
  auto ident = eg_.MakeIdentifier(sym);
  (void)ident;

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
  auto bind = eg_.MakeBind(once, sym, literal);
  (void)bind;
  auto ident = eg_.MakeIdentifier(sym);
  (void)ident;

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
  auto bind = eg_.MakeBind(once, sym, literal);
  (void)bind;
  auto ident = eg_.MakeIdentifier(sym);
  (void)ident;

  // First call should do work
  auto result1 = ApplyAllRewrites(eg_);
  EXPECT_EQ(result1.rewrites_applied, 1);
  EXPECT_TRUE(result1.saturated());

  // Second call should be at fixed point
  auto result2 = ApplyAllRewrites(eg_);
  EXPECT_EQ(result2.rewrites_applied, 0);
  EXPECT_TRUE(result2.saturated());
}

}  // namespace
}  // namespace memgraph::query::plan::v2
