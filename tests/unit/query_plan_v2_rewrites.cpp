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

// Rewrite-engine tests for plan_v2: e-graph construction (hash-consing,
// distinctness) and the inline rewrite's saturation / configuration behaviour.
//
// Inclusion criterion: a test here pins a distinct rewrite-engine property.
// The inline rule is a single structural match on Identifier(sym), so it is
// operator-agnostic; one "inline through a parent expression" guard suffices
// rather than one per operator.

#include <gtest/gtest.h>

#include <utility>

#include "query/plan_v2/egraph/egraph.hpp"
#include "query/plan_v2/egraph/egraph_internal.hpp"
#include "query/plan_v2/rewrite/rewrites.hpp"

namespace memgraph::query::plan::v2 {
namespace {

class EgraphTestBase : public ::testing::Test {
 protected:
  egraph eg_;

  auto &Core() { return impl_of(eg_).graph.core(); }

  [[nodiscard]] auto Find(eclass e) { return Core().find(to_core(e)); }

  void ExpectDistinct(eclass a, eclass b) { EXPECT_NE(Find(a), Find(b)); }

  void ExpectSame(eclass a, eclass b) { EXPECT_EQ(Find(a), Find(b)); }

  void ExpectAllDistinct(std::vector<eclass> const &ops) {
    for (size_t i = 0; i < ops.size(); ++i)
      for (size_t j = i + 1; j < ops.size(); ++j) ExpectDistinct(ops[i], ops[j]);
  }
};

class InlineRewriteTest : public EgraphTestBase {};

// Test: Simple inline rewrite
// Bind(Once, sym, Literal) + Identifier(sym) -> Identifier merged with Literal
TEST_F(InlineRewriteTest, SimpleInline) {
  auto once = eg_.MakeOnce();
  auto sym = eg_.MakeSymbol(0, "x");
  auto literal = eg_.MakeLiteral(storage::ExternalPropertyValue{42});
  [[maybe_unused]] auto bind = eg_.MakeBind(once, sym, literal);
  auto ident = eg_.MakeIdentifier(sym);

  ExpectDistinct(ident, literal);
  EXPECT_EQ(ApplyInlineRewrite(eg_), 1);
  ExpectSame(ident, literal);
}

// Test: Multiple identifiers referencing same binding
TEST_F(InlineRewriteTest, MultipleIdentifiersSameBinding) {
  auto once = eg_.MakeOnce();
  auto sym = eg_.MakeSymbol(0, "x");
  auto literal = eg_.MakeLiteral(storage::ExternalPropertyValue{42});
  [[maybe_unused]] auto bind = eg_.MakeBind(once, sym, literal);

  auto ident1 = eg_.MakeIdentifier(sym);
  auto ident2 = eg_.MakeIdentifier(sym);

  ExpectDistinct(ident1, literal);
  ExpectDistinct(ident2, literal);

  // Both MakeIdentifier(sym) calls hash-cons to the same Identifier e-class, so
  // there is one identifier to merge with the literal: exactly one merge.
  auto merges = ApplyInlineRewrite(eg_);
  EXPECT_EQ(merges, 1);

  ExpectSame(ident1, literal);
  ExpectSame(ident2, literal);
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
  auto once1 = eg_.MakeOnce();
  auto sym0 = eg_.MakeSymbol(0, "x");
  auto literal1 = eg_.MakeLiteral(storage::ExternalPropertyValue{1});
  [[maybe_unused]] auto bind1 = eg_.MakeBind(once1, sym0, literal1);

  auto once2 = eg_.MakeOnce();
  auto sym1 = eg_.MakeSymbol(1, "y");
  auto literal2 = eg_.MakeLiteral(storage::ExternalPropertyValue{2});
  [[maybe_unused]] auto bind2 = eg_.MakeBind(once2, sym1, literal2);

  auto ident0 = eg_.MakeIdentifier(sym0);
  auto ident1 = eg_.MakeIdentifier(sym1);

  EXPECT_EQ(ApplyInlineRewrite(eg_), 2);

  ExpectSame(ident0, literal1);
  ExpectSame(ident1, literal2);
  ExpectDistinct(literal1, literal2);
}

// Test: Chained bindings
// x = 1, y = x => Identifier(y) should be equivalent to Literal(1)
TEST_F(InlineRewriteTest, ChainedBindings) {
  auto once = eg_.MakeOnce();
  auto sym_x = eg_.MakeSymbol(0, "x");
  auto literal = eg_.MakeLiteral(storage::ExternalPropertyValue{1});
  auto bind_x = eg_.MakeBind(once, sym_x, literal);

  auto ident_x = eg_.MakeIdentifier(sym_x);

  auto sym_y = eg_.MakeSymbol(1, "y");
  [[maybe_unused]] auto bind_y = eg_.MakeBind(bind_x, sym_y, ident_x);

  auto ident_y = eg_.MakeIdentifier(sym_y);

  auto result = ApplyAllRewrites(eg_, RewriteConfig{.max_iterations = 5});
  EXPECT_EQ(result.rewrites_applied, 2);

  ExpectSame(ident_y, literal);
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
  // Lower bound on purpose: the point is that the single iteration did
  // productive work before the cap stopped it, not the exact per-pass count.
  EXPECT_GE(result.rewrites_applied, 1);
}

// Both config presets saturate a trivial binding in exactly one rewrite. The
// presets differ only in their limits, which don't bind on a one-rewrite egraph
// (limit-binding behaviour is covered by IterationLimit).
TEST_F(InlineRewriteTest, ConfigPresetsSaturateTrivialBinding) {
  auto saturate_trivial = [](RewriteConfig config) {
    egraph eg;
    auto once = eg.MakeOnce();
    auto sym = eg.MakeSymbol(0, "x");
    auto literal = eg.MakeLiteral(storage::ExternalPropertyValue{42});
    (void)eg.MakeBind(once, sym, literal);
    (void)eg.MakeIdentifier(sym);
    return ApplyAllRewrites(eg, config);
  };

  for (auto const &[label, config] :
       {std::pair{"Default", RewriteConfig::Default()}, std::pair{"Unlimited", RewriteConfig::Unlimited()}}) {
    auto result = saturate_trivial(config);
    EXPECT_TRUE(result.saturated()) << label;
    EXPECT_EQ(result.rewrites_applied, 1) << label;
  }
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

class OperatorConstructionTest : public EgraphTestBase {};

// Binary operators produce distinct eclasses for different operands
TEST_F(OperatorConstructionTest, BinaryOperatorsDistinct) {
  auto lit1 = eg_.MakeLiteral(storage::ExternalPropertyValue{1});
  auto lit2 = eg_.MakeLiteral(storage::ExternalPropertyValue{2});
  auto lit3 = eg_.MakeLiteral(storage::ExternalPropertyValue{3});

  ExpectAllDistinct({eg_.MakeAdd(lit1, lit2), eg_.MakeAdd(lit1, lit3), eg_.MakeSub(lit1, lit2)});
}

// Hash-consing: same operator + same children → same eclass
TEST_F(OperatorConstructionTest, HashConsing) {
  auto lit1 = eg_.MakeLiteral(storage::ExternalPropertyValue{1});
  auto lit2 = eg_.MakeLiteral(storage::ExternalPropertyValue{2});

  ExpectSame(eg_.MakeAdd(lit1, lit2), eg_.MakeAdd(lit1, lit2));
}

// Operand order matters: Add(1,2) != Add(2,1)
TEST_F(OperatorConstructionTest, OperandOrderMatters) {
  auto lit1 = eg_.MakeLiteral(storage::ExternalPropertyValue{1});
  auto lit2 = eg_.MakeLiteral(storage::ExternalPropertyValue{2});

  ExpectDistinct(eg_.MakeAdd(lit1, lit2), eg_.MakeAdd(lit2, lit1));
}

// The Make* wrappers all forward to the same hash-consing keyed on (symbol,
// children), so distinctness across operator families / arities is the one
// e-graph invariant - covered above and in the typed-egraph tests - rather than
// a separate property per family. One binary case representative suffices here.

// =======================================================================
// Inline rewrite through operators
// =======================================================================

class InlineThroughOperatorTest : public EgraphTestBase {};

// The inline rule matches Identifier(sym) directly and is agnostic to the
// operator that holds it, so one case guards that inlining still fires when
// the Identifier sits inside a parent expression (and that the now-dead Bind
// is left behind for elimination).  Per-operator variety is unnecessary here;
// round-trip reconstruction of each operator symbol is covered at the converter
// layer.
TEST_F(InlineThroughOperatorTest, InlineThroughAdd) {
  auto once = eg_.MakeOnce();
  auto sym = eg_.MakeSymbol(0, "a");
  auto lit1 = eg_.MakeLiteral(storage::ExternalPropertyValue{1});
  [[maybe_unused]] auto bind = eg_.MakeBind(once, sym, lit1);

  auto ident = eg_.MakeIdentifier(sym);
  auto lit2 = eg_.MakeLiteral(storage::ExternalPropertyValue{2});
  [[maybe_unused]] auto add = eg_.MakeAdd(ident, lit2);

  auto result = ApplyAllRewrites(eg_);
  EXPECT_EQ(result.rewrites_applied, 1);
  EXPECT_TRUE(result.saturated());
  ExpectSame(ident, lit1);
}

}  // namespace
}  // namespace memgraph::query::plan::v2
