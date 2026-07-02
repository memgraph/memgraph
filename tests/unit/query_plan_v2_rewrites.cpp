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

#include <cmath>
#include <cstddef>
#include <optional>
#include <set>
#include "query/plan_v2/egraph/egraph.hpp"
#include "query/plan_v2/egraph/egraph_internal.hpp"
#include "query/plan_v2/resolve/analysis.hpp"
#include "query/plan_v2/rewrite/fold.hpp"
#include "query/plan_v2/rewrite/rewrites.hpp"
#include "query/plan_v2/rewrite/rewrites_internal.hpp"

namespace memgraph::query::plan::v2 {
namespace {

using storage::ExternalPropertyValue;

// ==========================================================================
// Pure constant-fold evaluator
// ==========================================================================

TEST(FoldConstant, AddsTwoInts) {
  ExternalPropertyValue const operands[] = {ExternalPropertyValue{int64_t{1}}, ExternalPropertyValue{int64_t{1}}};
  auto const result = FoldConstant(symbol::Add, operands);
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsInt());
  EXPECT_EQ(result->ValueInt(), 2);
}

TEST(FoldConstant, ComparisonYieldsBool) {
  ExternalPropertyValue const operands[] = {ExternalPropertyValue{int64_t{1}}, ExternalPropertyValue{int64_t{2}}};
  auto const result = FoldConstant(symbol::Lt, operands);
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsBool());
  EXPECT_TRUE(result->ValueBool());
}

TEST(FoldConstant, UnaryMinusNegates) {
  ExternalPropertyValue const operands[] = {ExternalPropertyValue{int64_t{5}}};
  auto const result = FoldConstant(symbol::UnaryMinus, operands);
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsInt());
  EXPECT_EQ(result->ValueInt(), -5);
}

TEST(FoldConstant, DivisionByZeroDeclines) {
  ExternalPropertyValue const operands[] = {ExternalPropertyValue{int64_t{1}}, ExternalPropertyValue{int64_t{0}}};
  EXPECT_FALSE(FoldConstant(symbol::Div, operands).has_value());
}

TEST(FoldConstant, NonNumericArithmeticDeclines) {
  ExternalPropertyValue const operands[] = {ExternalPropertyValue{std::string_view{"a"}},
                                            ExternalPropertyValue{std::string_view{"b"}}};
  EXPECT_FALSE(FoldConstant(symbol::Mul, operands).has_value());
}

// Mixed-type arithmetic keeps Cypher's result type: int + double is double.
// The result is a different constant from Int{3} under constant identity.
TEST(FoldConstant, MixedIntDoubleAddYieldsDouble) {
  ExternalPropertyValue const operands[] = {ExternalPropertyValue{1.0}, ExternalPropertyValue{int64_t{2}}};
  auto const result = FoldConstant(symbol::Add, operands);
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsDouble());
  EXPECT_EQ(result->ValueDouble(), 3.0);
}

// Cypher's three-valued logic: a determining operand wins over null, an
// undetermined combination stays null.
TEST(FoldConstant, AndOfFalseAndNullIsFalse) {
  ExternalPropertyValue const operands[] = {ExternalPropertyValue{false}, ExternalPropertyValue{}};
  auto const result = FoldConstant(symbol::And, operands);
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsBool());
  EXPECT_FALSE(result->ValueBool());
}

TEST(FoldConstant, OrOfTrueAndNullIsTrue) {
  ExternalPropertyValue const operands[] = {ExternalPropertyValue{true}, ExternalPropertyValue{}};
  auto const result = FoldConstant(symbol::Or, operands);
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsBool());
  EXPECT_TRUE(result->ValueBool());
}

TEST(FoldConstant, AndOfTrueAndNullIsNull) {
  ExternalPropertyValue const operands[] = {ExternalPropertyValue{true}, ExternalPropertyValue{}};
  auto const result = FoldConstant(symbol::And, operands);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsNull());
}

// Or is asymmetric with And: a false operand can't determine the result, so
// `false OR null` stays null (mirror of AndOfTrueAndNullIsNull).
TEST(FoldConstant, OrOfFalseAndNullIsNull) {
  ExternalPropertyValue const operands[] = {ExternalPropertyValue{false}, ExternalPropertyValue{}};
  auto const result = FoldConstant(symbol::Or, operands);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsNull());
}

// Xor never short-circuits (unlike And/Or, no operand can determine it alone),
// so any null operand yields null.
TEST(FoldConstant, XorWithNullIsNull) {
  ExternalPropertyValue const operands[] = {ExternalPropertyValue{true}, ExternalPropertyValue{}};
  auto const result = FoldConstant(symbol::Xor, operands);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsNull());
}

// A list-valued result is not a scalar constant: Add over two lists concatenates
// (a List), so ToConstant declines and the fold leaves the expression alone.
TEST(FoldConstant, ListResultDeclines) {
  auto const list = ExternalPropertyValue{ExternalPropertyValue::list_t{ExternalPropertyValue{int64_t{1}}}};
  ExternalPropertyValue const operands[] = {list, list};
  EXPECT_FALSE(FoldConstant(symbol::Add, operands).has_value());
}

// Null propagates through arithmetic and unary operators as a folded null
// constant, not as a declined fold.
TEST(FoldConstant, AddOfNullIsNull) {
  ExternalPropertyValue const operands[] = {ExternalPropertyValue{int64_t{5}}, ExternalPropertyValue{}};
  auto const result = FoldConstant(symbol::Add, operands);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsNull());
}

TEST(FoldConstant, UnaryMinusOfNullIsNull) {
  ExternalPropertyValue const operands[] = {ExternalPropertyValue{}};
  auto const result = FoldConstant(symbol::UnaryMinus, operands);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsNull());
}

TEST(FoldConstant, NotOfNullIsNull) {
  ExternalPropertyValue const operands[] = {ExternalPropertyValue{}};
  auto const result = FoldConstant(symbol::Not, operands);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsNull());
}

// Double division never throws: 0.0/0.0 folds to a NaN constant per IEEE.
TEST(FoldConstant, ZeroOverZeroFoldsToNaN) {
  ExternalPropertyValue const operands[] = {ExternalPropertyValue{0.0}, ExternalPropertyValue{0.0}};
  auto const result = FoldConstant(symbol::Div, operands);
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsDouble());
  EXPECT_TRUE(std::isnan(result->ValueDouble()));
}

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

  auto ConstantOf(eclass e) -> std::optional<storage::ExternalPropertyValue> {
    if (auto const *expr = Core().eclass(Find(e)).analysis().expression()) return expr->known_constant_value;
    return std::nullopt;
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
  auto add = eg_.MakeAdd(ident, lit2);

  // Inlining merges the Identifier into the bound literal; once both operands
  // are constant the fold rule collapses Add(1, 2) to 3. The salient property
  // here is that inlining reaches an Identifier nested in a parent operator.
  auto result = ApplyAllRewrites(eg_);
  EXPECT_TRUE(result.saturated());
  ExpectSame(ident, lit1);
  // The fold then fires through the inlined operand: Add(1, 2) collapses to 3.
  auto const folded = ConstantOf(add);
  ASSERT_TRUE(folded.has_value());
  ASSERT_TRUE(folded->IsInt());
  EXPECT_EQ(folded->ValueInt(), 3);
}

// =======================================================================
// Constant folding (fact-gated rewrite)
// =======================================================================

class ConstantFoldTest : public EgraphTestBase {};

// Tracer: a binary op over two constant operands folds to their value.
TEST_F(ConstantFoldTest, FoldsAddOfTwoLiterals) {
  auto one = eg_.MakeLiteral(ExternalPropertyValue{int64_t{1}});
  auto sum = eg_.MakeAdd(one, one);
  auto two = eg_.MakeLiteral(ExternalPropertyValue{int64_t{2}});

  ExpectDistinct(sum, two);
  ApplyAllRewrites(eg_);
  ExpectSame(sum, two);
  // The merge alone can't tell a wrongly-typed fold result apart; pin the
  // folded value and its type too.
  auto const folded = ConstantOf(sum);
  ASSERT_TRUE(folded.has_value());
  ASSERT_TRUE(folded->IsInt());
  EXPECT_EQ(folded->ValueInt(), 2);
}

// The fold result keeps its Cypher type: 1.0 + 2 folds to Double{3.0}, a
// different constant from the Int{3} literal - the two must not merge.
TEST_F(ConstantFoldTest, FoldPreservesResultType) {
  auto one_double = eg_.MakeLiteral(ExternalPropertyValue{1.0});
  auto two_int = eg_.MakeLiteral(ExternalPropertyValue{int64_t{2}});
  auto sum = eg_.MakeAdd(one_double, two_int);
  auto three_int = eg_.MakeLiteral(ExternalPropertyValue{int64_t{3}});

  ApplyAllRewrites(eg_);

  auto const folded = ConstantOf(sum);
  ASSERT_TRUE(folded.has_value());
  ASSERT_TRUE(folded->IsDouble());
  EXPECT_EQ(folded->ValueDouble(), 3.0);
  ExpectDistinct(sum, three_int);
}

// A NaN-producing fold interns cleanly: the expression carries a NaN constant.
TEST_F(ConstantFoldTest, FoldsZeroOverZeroToNaNConstant) {
  auto zero = eg_.MakeLiteral(ExternalPropertyValue{0.0});
  auto div = eg_.MakeDiv(zero, zero);

  ApplyAllRewrites(eg_);

  auto const folded = ConstantOf(div);
  ASSERT_TRUE(folded.has_value());
  ASSERT_TRUE(folded->IsDouble());
  EXPECT_TRUE(std::isnan(folded->ValueDouble()));
}

// Folding cascades under saturation: 1+1+1+1 collapses innermost-out to 4.
TEST_F(ConstantFoldTest, CascadesNestedAdds) {
  auto one = eg_.MakeLiteral(ExternalPropertyValue{int64_t{1}});
  auto s2 = eg_.MakeAdd(one, one);
  auto s3 = eg_.MakeAdd(s2, one);
  auto s4 = eg_.MakeAdd(s3, one);
  auto four = eg_.MakeLiteral(ExternalPropertyValue{int64_t{4}});

  ApplyAllRewrites(eg_);
  ExpectSame(s4, four);
}

// Regression: `-(0.0)` folds to `-0.0`, which must intern distinctly from the
// pre-existing `+0.0` literal - the sign is observable (`1.0/x` flips ±inf).
TEST_F(ConstantFoldTest, NegativeZeroDoesNotCollapseIntoPositiveZero) {
  auto pos_zero = eg_.MakeLiteral(ExternalPropertyValue{0.0});
  auto neg = eg_.MakeUnaryMinus(pos_zero);

  ApplyAllRewrites(eg_);

  auto const folded = ConstantOf(neg);
  ASSERT_TRUE(folded.has_value());
  ASSERT_TRUE(folded->IsDouble());
  EXPECT_EQ(folded->ValueDouble(), 0.0);             // equal under IEEE ==
  EXPECT_TRUE(std::signbit(folded->ValueDouble()));  // but negative zero
  ExpectDistinct(neg, pos_zero);
}

// A non-constant operand blocks the fold: the expression keeps no constant.
TEST_F(ConstantFoldTest, DoesNotFoldNonConstantOperand) {
  auto param = eg_.MakeParameterLookup(0);
  auto one = eg_.MakeLiteral(ExternalPropertyValue{int64_t{1}});
  auto sum = eg_.MakeAdd(param, one);

  ApplyAllRewrites(eg_);
  EXPECT_FALSE(ConstantOf(sum).has_value());
}

// The rule wires the non-arithmetic operator families too: a comparison over
// constants folds to a boolean constant.
TEST_F(ConstantFoldTest, FoldsComparisonToBool) {
  auto one = eg_.MakeLiteral(ExternalPropertyValue{int64_t{1}});
  auto two = eg_.MakeLiteral(ExternalPropertyValue{int64_t{2}});
  auto lt = eg_.MakeLt(one, two);

  ApplyAllRewrites(eg_);
  auto const folded = ConstantOf(lt);
  ASSERT_TRUE(folded.has_value());
  ASSERT_TRUE(folded->IsBool());
  EXPECT_TRUE(folded->ValueBool());
}

// The rule latch keeps a long saturation sparse: an alternating Add/Mul chain of
// constants folds to a single constant, and across the whole saturation only the
// Add and Mul fold rules ever fire - the other binary and unary fold rules never
// do. (The two do not strictly alternate one-per-pass: a fold's merge updates
// analysis immediately, so once an Add folds the Mul above it folds later in the
// same pass; both stay active until the chain is exhausted.)
TEST(RuleLatchAlternation, OnlyTheTwoChainRulesEverFire) {
  egraph eg;
  auto const two = eg.MakeLiteral(ExternalPropertyValue{int64_t{2}});
  eclass cur = eg.MakeLiteral(ExternalPropertyValue{int64_t{1}});
  for (int level = 0; level < 6; ++level) {
    cur = (level % 2 == 0) ? eg.MakeAdd(cur, two) : eg.MakeMul(cur, two);
  }

  planner::core::rewrite::Rewriter rewriter{impl_of(eg).graph, DefaultRules()};
  auto const result = rewriter.saturate(planner::core::rewrite::RewriteConfig::Unlimited(),
                                        planner::core::rewrite::ArmingMode::Latched);
  ASSERT_TRUE(result.saturated());

  std::set<std::size_t> const fired_rules = [&] {
    std::set<std::size_t> fired;
    for (std::size_t i = 0; i < result.rewrites_per_rule.size(); ++i) {
      if (result.rewrites_per_rule[i] > 0) fired.insert(i);
    }
    return fired;
  }();
  EXPECT_EQ(fired_rules.size(), 2U) << "only the Add and Mul fold rules fire; the other ~17 rules never do";

  auto const *const folded = impl_of(eg).graph.core().analysis_of(to_core(cur)).expression();
  ASSERT_NE(folded, nullptr);
  EXPECT_TRUE(folded->known_constant_value.has_value()) << "the chain folds to a single constant";
}

}  // namespace
}  // namespace memgraph::query::plan::v2
