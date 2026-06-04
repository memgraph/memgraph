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

// Cardinality estimation tests for plan_v2.
//
// The builtin estimator does plan-time type/size analysis, not cardinality
// estimation: it reports that an expression is statically known to produce a
// list of a known size (e.g. range(0, 5) -> a 6-element list).  Cardinality -
// the number of rows an operator pulls - is an operator-level concept.  A known
// list size becomes a known cardinality only at Unwind, which emits one row per
// element; Output (scalar RETURN) collapses to one row regardless.
// BuiltinEstimator: range(int, int) literal size deduction, default otherwise.
// Unwind/Output: size -> row-cardinality propagation, end to end.

#include <gtest/gtest.h>

#include "query/exceptions.hpp"
#include "query/plan/operator.hpp"
#include "query/plan_v2/cost/builtin_estimator.hpp"
#include "query/plan_v2/cost/cardinality_estimator.hpp"
#include "query/plan_v2/egraph/builtin_functions.hpp"
#include "query/plan_v2/egraph/egraph.hpp"
#include "query/plan_v2/egraph/egraph_internal.hpp"
#include "query/plan_v2/frontend/egraph_converter.hpp"
#include "query/plan_v2/test_support/literals.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::query::plan::v2 {
namespace {

using EClassId = planner::core::EClassId;

auto CoreOf(egraph const &eg) -> EGraph const & { return impl_of(eg).graph.core(); }

auto AsCoreId(eclass e) -> EClassId { return EClassId{e.value_of()}; }

auto EstimateBuiltin(egraph &eg, eclass fn, std::vector<EClassId> args) -> double {
  BuiltinEstimator estimator{eg};
  auto const &core_eg = CoreOf(eg);
  auto const fn_eclass = core_eg.find(EClassId{fn.value_of()});
  auto const fn_enode_id = core_eg.eclass(fn_eclass).nodes()[0];
  auto const &fn_enode = core_eg.get_enode(fn_enode_id);
  return estimator.Estimate(fn_enode, args);
}

// ============================================================================
// BuiltinKind classification
// ============================================================================

TEST(BuiltinKindClassifier, RangeIsRecognised) {
  EXPECT_EQ(BuiltinKindFor("range"), BuiltinKind::Range);
  EXPECT_EQ(BuiltinKindFor("RANGE"), BuiltinKind::Range);
  EXPECT_EQ(BuiltinKindFor("Range"), BuiltinKind::Range);
}

TEST(BuiltinKindClassifier, UnknownFallback) {
  EXPECT_EQ(BuiltinKindFor("toString"), BuiltinKind::Unknown);
  EXPECT_EQ(BuiltinKindFor(""), BuiltinKind::Unknown);
  EXPECT_EQ(BuiltinKindFor("rang"), BuiltinKind::Unknown);
}

// ============================================================================
// BuiltinEstimator: Range cardinality
// ============================================================================

TEST(BuiltinEstimator, RangeWithIntLiteralsReturnsCount) {
  egraph eg;
  auto a = IntLit(eg, 0);
  auto b = IntLit(eg, 5);
  EXPECT_DOUBLE_EQ(EstimateBuiltin(eg, eg.MakeFunction("range", {a, b}), {AsCoreId(a), AsCoreId(b)}), 6.0);
}

TEST(BuiltinEstimator, RangeWithReversedBoundsClampsAtZero) {
  egraph eg;
  auto a = IntLit(eg, 5);
  auto b = IntLit(eg, 0);
  EXPECT_DOUBLE_EQ(EstimateBuiltin(eg, eg.MakeFunction("range", {a, b}), {AsCoreId(a), AsCoreId(b)}), 0.0);
}

TEST(BuiltinEstimator, RangeWithParameterFallsBackToDefault) {
  egraph eg;
  auto a = IntLit(eg, 0);
  auto b = eg.MakeParameterLookup(0);
  EXPECT_DOUBLE_EQ(EstimateBuiltin(eg, eg.MakeFunction("range", {a, b}), {AsCoreId(a), AsCoreId(b)}), kDefaultListSize);
}

TEST(BuiltinEstimator, UnknownFunctionFallsBackToDefault) {
  egraph eg;
  auto a = IntLit(eg, 0);
  EXPECT_DOUBLE_EQ(EstimateBuiltin(eg, eg.MakeFunction("unknown_func", {a}), {AsCoreId(a)}), kDefaultListSize);
}

// ============================================================================
// Unwind cost composition end-to-end
// ============================================================================

TEST(UnwindCostShape, ProducesUnwindOperator) {
  // UNWIND range(0, 5) AS x RETURN x : x is referenced, so the binding is kept
  // and the row pipe is a v1 Unwind with cardinality 6 (the range length).
  egraph eg;
  auto once = eg.MakeOnce();
  auto x_sym = eg.MakeSymbol(0, "x");
  auto a = eg.MakeLiteral(storage::ExternalPropertyValue{int64_t{0}});
  auto b = eg.MakeLiteral(storage::ExternalPropertyValue{int64_t{5}});
  auto range = eg.MakeFunction("range", {a, b});
  auto unwind = eg.MakeUnwind(once, x_sym, range);

  auto out_sym = eg.MakeSymbol(1, "x");
  auto x_ref = eg.MakeIdentifier(x_sym);
  auto named_output = eg.MakeNamedOutput("x", out_sym, x_ref);
  auto root = eg.MakeOutput(unwind, {named_output});

  auto ctx = QueryPlannerContext{};
  auto result = ConvertToLogicalOperator(eg, root, ctx);

  ASSERT_NE(result.plan, nullptr);

  auto const &produce = dynamic_cast<plan::Produce const &>(*result.plan);
  ASSERT_NE(produce.input(), nullptr);
  auto const *unwind_op = dynamic_cast<plan::Unwind const *>(produce.input().get());
  ASSERT_NE(unwind_op, nullptr) << "Top input must be a v1 Unwind";
  EXPECT_EQ(unwind_op->output_symbol_.name(), "x");
  ASSERT_NE(unwind_op->input(), nullptr);
  EXPECT_NE(dynamic_cast<plan::Once const *>(unwind_op->input().get()), nullptr);

  EXPECT_GT(result.cost, 0.0);
  EXPECT_LT(result.cost, 1e9);
  EXPECT_DOUBLE_EQ(result.cardinality, 6.0);
}

// ============================================================================
// Dead-Unwind: elide the binding when the sym is unused and the list length
// is statically known.
// ============================================================================

// Build a constant list literal e-class; its known_list_length seeds the
// dead-Unwind gate.
auto ListLit(egraph &eg, std::initializer_list<int64_t> values) -> eclass {
  std::vector<storage::ExternalPropertyValue> elements;
  for (auto v : values) elements.emplace_back(int64_t{v});
  return eg.MakeLiteral(storage::ExternalPropertyValue(std::move(elements)));
}

TEST(DeadUnwind, UnreferencedSymOverConstListBuildsCardinalityScale) {
  // UNWIND [1, 2, 3] AS x RETURN 2 : x is referenced nowhere and the list
  // length is known, so the binding is wasted work.
  egraph eg;
  auto once = eg.MakeOnce();
  auto x_sym = eg.MakeSymbol(0, "x");
  auto list = ListLit(eg, {1, 2, 3});
  auto unwind = eg.MakeUnwind(once, x_sym, list);

  auto r_sym = eg.MakeSymbol(1, "r");
  auto two = eg.MakeLiteral(storage::ExternalPropertyValue{int64_t{2}});
  auto named_output = eg.MakeNamedOutput("r", r_sym, two);
  auto root = eg.MakeOutput(unwind, {named_output});

  auto ctx = QueryPlannerContext{};
  auto result = ConvertToLogicalOperator(eg, root, ctx);

  ASSERT_NE(result.plan, nullptr);
  auto const &produce = dynamic_cast<plan::Produce const &>(*result.plan);
  auto const *card_scale = dynamic_cast<plan::CardinalityScale const *>(produce.input().get());
  ASSERT_NE(card_scale, nullptr) << "Unreferenced sym over a known-length list must build CardinalityScale";
  EXPECT_NE(dynamic_cast<plan::Once const *>(card_scale->input().get()), nullptr);
  EXPECT_DOUBLE_EQ(result.cardinality, 3.0);
}

TEST(DeadUnwind, UnreferencedSymOverRangeBuildsCardinalityScale) {
  // UNWIND range(0, 5) AS x RETURN 1 : x is unused and range's length is
  // provable from its int-literal bounds, so the binding elides to a
  // CardinalityScale of cardinality 6.
  egraph eg;
  auto once = eg.MakeOnce();
  auto x_sym = eg.MakeSymbol(0, "x");
  auto a = eg.MakeLiteral(storage::ExternalPropertyValue{int64_t{0}});
  auto b = eg.MakeLiteral(storage::ExternalPropertyValue{int64_t{5}});
  auto range = eg.MakeFunction("range", {a, b});
  auto unwind = eg.MakeUnwind(once, x_sym, range);

  auto r_sym = eg.MakeSymbol(1, "r");
  auto one = eg.MakeLiteral(storage::ExternalPropertyValue{int64_t{1}});
  auto named_output = eg.MakeNamedOutput("r", r_sym, one);
  auto root = eg.MakeOutput(unwind, {named_output});

  auto ctx = QueryPlannerContext{};
  auto result = ConvertToLogicalOperator(eg, root, ctx);

  ASSERT_NE(result.plan, nullptr);
  auto const &produce = dynamic_cast<plan::Produce const &>(*result.plan);
  ASSERT_NE(dynamic_cast<plan::CardinalityScale const *>(produce.input().get()), nullptr)
      << "Unreferenced sym over a provable-length range must build CardinalityScale";
  EXPECT_DOUBLE_EQ(result.cardinality, 6.0);
}

TEST(DeadUnwind, ReferencedSymStaysUnwind) {
  // UNWIND [1, 2, 3] AS x RETURN x : x is referenced, so the binding is needed
  // and the dead alt is not eligible even though the list length is known.
  egraph eg;
  auto once = eg.MakeOnce();
  auto x_sym = eg.MakeSymbol(0, "x");
  auto list = ListLit(eg, {1, 2, 3});
  auto unwind = eg.MakeUnwind(once, x_sym, list);

  auto out_sym = eg.MakeSymbol(1, "x");
  auto x_ref = eg.MakeIdentifier(x_sym);
  auto named_output = eg.MakeNamedOutput("x", out_sym, x_ref);
  auto root = eg.MakeOutput(unwind, {named_output});

  auto ctx = QueryPlannerContext{};
  auto result = ConvertToLogicalOperator(eg, root, ctx);

  ASSERT_NE(result.plan, nullptr);
  auto const &produce = dynamic_cast<plan::Produce const &>(*result.plan);
  EXPECT_NE(dynamic_cast<plan::Unwind const *>(produce.input().get()), nullptr)
      << "A referenced sym must keep the Unwind binding";
  EXPECT_EQ(dynamic_cast<plan::CardinalityScale const *>(produce.input().get()), nullptr);
}

// ============================================================================
// Output cardinality contract
// ============================================================================

TEST(OutputCardinality, ScalarReturnIsOneRowEvenWhenValueIsList) {
  egraph eg;
  auto once = eg.MakeOnce();
  auto a = eg.MakeLiteral(storage::ExternalPropertyValue{int64_t{0}});
  auto b = eg.MakeLiteral(storage::ExternalPropertyValue{int64_t{5}});
  auto range = eg.MakeFunction("range", {a, b});
  auto r_sym = eg.MakeSymbol(0, "r");
  auto named_output = eg.MakeNamedOutput("r", r_sym, range);
  auto root = eg.MakeOutput(once, {named_output});

  auto ctx = QueryPlannerContext{};
  auto result = ConvertToLogicalOperator(eg, root, ctx);

  ASSERT_NE(result.plan, nullptr);
  EXPECT_DOUBLE_EQ(result.cardinality, 1.0);
}

}  // namespace
}  // namespace memgraph::query::plan::v2
