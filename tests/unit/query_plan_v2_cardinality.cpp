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
// A builtin's statically-known list size (e.g. range(0, 5) -> 6) is carried as
// a known_list_length analysis fact, seeded at make time; the cost model reads
// it for the Function's cardinality and falls back to the estimator only for
// the unknown case.  Cardinality - the number of rows an operator pulls - is an
// operator-level concept: a known list size becomes a known cardinality only at
// Unwind, which emits one row per element; Output (scalar RETURN) collapses to
// one row regardless.
// Function: fact-driven cardinality, estimator default otherwise.
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
#include "query/plan_v2/test_support/cost.hpp"
#include "query/plan_v2/test_support/literals.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::query::plan::v2 {
namespace {

// The cardinality the cost model derives for a Function e-class: a known list
// length read from the analysis fact when present (range over constant bounds),
// else the estimator's default for the unknown case.
auto FunctionCardinality(egraph &eg, eclass fn) -> double {
  test::CostHarness const h{eg, fn};
  auto const alts = h.alts(fn);
  EXPECT_EQ(alts.size(), 1U);
  return alts.front().cardinality;
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
// Function cardinality: read from the known_list_length fact, estimator default
// otherwise.
// ============================================================================

TEST(FunctionCardinality, RangeOverConstantBoundsReadsKnownLength) {
  egraph eg;
  auto fn = eg.MakeFunction("range", {IntLit(eg, 0), IntLit(eg, 5)}, /*is_pure=*/true);
  EXPECT_DOUBLE_EQ(FunctionCardinality(eg, fn), 6.0);
}

TEST(FunctionCardinality, RangeWithReversedBoundsIsEmpty) {
  egraph eg;
  auto fn = eg.MakeFunction("range", {IntLit(eg, 5), IntLit(eg, 0)}, /*is_pure=*/true);
  EXPECT_DOUBLE_EQ(FunctionCardinality(eg, fn), 0.0);
}

TEST(FunctionCardinality, RangeWithNonConstantBoundFallsBackToDefault) {
  egraph eg;
  auto fn = eg.MakeFunction("range", {IntLit(eg, 0), eg.MakeParameterLookup(0)}, /*is_pure=*/true);
  EXPECT_DOUBLE_EQ(FunctionCardinality(eg, fn), kDefaultListSize);
}

TEST(FunctionCardinality, RangeWithIntegralDoubleBoundFallsBackToDefault) {
  // range() rejects non-integer bounds at runtime (throws), so an integral
  // double like 5.0 must NOT seed a known length - otherwise a dead Unwind over
  // it would silently succeed where the real range() errors. Falls back to the
  // estimator default, keeping the alive (throwing) Unwind.
  egraph eg;
  auto const dbl = [&](double v) { return eg.MakeLiteral(storage::ExternalPropertyValue{v}); };
  auto fn = eg.MakeFunction("range", {dbl(0.0), dbl(5.0)}, /*is_pure=*/true);
  EXPECT_DOUBLE_EQ(FunctionCardinality(eg, fn), kDefaultListSize);
}

TEST(FunctionCardinality, UnknownFunctionFallsBackToDefault) {
  egraph eg;
  auto fn = eg.MakeFunction("unknown_func", {IntLit(eg, 0)}, /*is_pure=*/false);
  EXPECT_DOUBLE_EQ(FunctionCardinality(eg, fn), kDefaultListSize);
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
  auto range = eg.MakeFunction("range", {a, b}, /*is_pure=*/true);
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
  auto range = eg.MakeFunction("range", {a, b}, /*is_pure=*/true);
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
  auto range = eg.MakeFunction("range", {a, b}, /*is_pure=*/true);
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
