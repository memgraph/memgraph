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

// Unit tests for the plan_v2 analysis framework: constant-identity equality,
// the ExpressionAnalysis lattice merge, that merge through the real EGraph, and
// the make half that seeds a new e-class's analysis arm and facts.

#include <gtest/gtest.h>

#include <limits>

#include "query/exceptions.hpp"
#include "query/plan_v2/egraph/egraph.hpp"
#include "query/plan_v2/egraph/egraph_internal.hpp"
#include "query/plan_v2/egraph/symbol.hpp"
#include "query/plan_v2/resolve/analysis.hpp"
#include "query/plan_v2/resolve/constant_identity.hpp"
#include "storage/v2/property_value.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::query::plan::v2 {
namespace {

using storage::ExternalPropertyValue;

TEST(ConstantIdentity, ScalarIntsAgree) {
  ConstantIdentityEq same;
  EXPECT_TRUE(same(ExternalPropertyValue{int64_t{5}}, ExternalPropertyValue{int64_t{5}}));
}

TEST(ConstantIdentity, ScalarIntsDisagree) {
  ConstantIdentityEq same;
  EXPECT_FALSE(same(ExternalPropertyValue{int64_t{5}}, ExternalPropertyValue{int64_t{6}}));
}

TEST(ConstantIdentity, IntAndDoubleAreDistinctConstants) {
  ConstantIdentityEq same;
  EXPECT_FALSE(same(ExternalPropertyValue{int64_t{1}}, ExternalPropertyValue{1.0}));
}

TEST(ConstantIdentity, NaNEqualsNaN) {
  ConstantIdentityEq same;
  auto const nan = std::numeric_limits<double>::quiet_NaN();
  EXPECT_TRUE(same(ExternalPropertyValue{nan}, ExternalPropertyValue{nan}));
}

TEST(ConstantIdentity, NullEqualsNull) {
  ConstantIdentityEq same;
  EXPECT_TRUE(same(ExternalPropertyValue{}, ExternalPropertyValue{}));
}

TEST(ConstantIdentity, ListWithNaNElementIsSame) {
  ConstantIdentityEq same;
  auto const nan = std::numeric_limits<double>::quiet_NaN();
  auto list_with_nan = [&] { return ExternalPropertyValue{ExternalPropertyValue::list_t{ExternalPropertyValue{nan}}}; };
  EXPECT_TRUE(same(list_with_nan(), list_with_nan()));
}

TEST(ConstantIdentity, ListsWithCrossTypeElementsAreDistinct) {
  ConstantIdentityEq same;
  auto int_list = ExternalPropertyValue{ExternalPropertyValue::list_t{ExternalPropertyValue{int64_t{1}}}};
  auto double_list = ExternalPropertyValue{ExternalPropertyValue::list_t{ExternalPropertyValue{1.0}}};
  EXPECT_FALSE(same(int_list, double_list));
}

TEST(ConstantIdentity, MapWithNaNValueIsSame) {
  ConstantIdentityEq same;
  auto const nan = std::numeric_limits<double>::quiet_NaN();
  auto map_with_nan = [&] {
    return ExternalPropertyValue{ExternalPropertyValue::map_t{{"k", ExternalPropertyValue{nan}}}};
  };
  EXPECT_TRUE(same(map_with_nan(), map_with_nan()));
}

// --- ExpressionAnalysis lattice merge ---------------------------------------

TEST(AnalysisMerge, OneSidedConstantIsTaken) {
  analysis lhs{ExpressionAnalysis{}};
  analysis rhs{ExpressionAnalysis{.known_constant_value = ExternalPropertyValue{int64_t{7}}}};
  lhs.merge(rhs);
  auto const &e = std::get<ExpressionAnalysis>(lhs);
  ASSERT_TRUE(e.known_constant_value.has_value());
  EXPECT_TRUE(ConstantIdentityEq{}(*e.known_constant_value, ExternalPropertyValue{int64_t{7}}));
}

TEST(AnalysisMerge, ConflictingConstantsThrow) {
  analysis lhs{ExpressionAnalysis{.known_constant_value = ExternalPropertyValue{int64_t{5}}}};
  analysis rhs{ExpressionAnalysis{.known_constant_value = ExternalPropertyValue{int64_t{6}}}};
  EXPECT_THROW(lhs.merge(rhs), PlannerBug);
}

TEST(AnalysisMerge, CrossKindThrows) {
  analysis expr{ExpressionAnalysis{}};
  analysis op{OperatorAnalysis{}};
  EXPECT_THROW(expr.merge(op), PlannerBug);
}

TEST(AnalysisMerge, AgreeingConstantsAreKept) {
  analysis lhs{ExpressionAnalysis{.known_constant_value = ExternalPropertyValue{int64_t{42}}}};
  analysis rhs{ExpressionAnalysis{.known_constant_value = ExternalPropertyValue{int64_t{42}}}};
  EXPECT_NO_THROW(lhs.merge(rhs));
  auto const &e = std::get<ExpressionAnalysis>(lhs);
  ASSERT_TRUE(e.known_constant_value.has_value());
  EXPECT_TRUE(ConstantIdentityEq{}(*e.known_constant_value, ExternalPropertyValue{int64_t{42}}));
}

TEST(AnalysisMerge, OneSidedListLengthIsTaken) {
  analysis lhs{ExpressionAnalysis{}};
  analysis rhs{ExpressionAnalysis{.known_list_length = std::size_t{3}}};
  lhs.merge(rhs);
  auto const &e = std::get<ExpressionAnalysis>(lhs);
  ASSERT_TRUE(e.known_list_length.has_value());
  EXPECT_EQ(*e.known_list_length, 3U);
}

TEST(AnalysisMerge, ConflictingListLengthsThrow) {
  analysis lhs{ExpressionAnalysis{.known_list_length = std::size_t{3}}};
  analysis rhs{ExpressionAnalysis{.known_list_length = std::size_t{4}}};
  EXPECT_THROW(lhs.merge(rhs), PlannerBug);
}

// --- Merge through the real EGraph ------------------------------------------
// The seam between core::EClass and the analysis lattice: a seeded e-class
// carries its arm and facts into the e-graph, and EGraph::merge combines them.

using planner::core::EClassId;
using planner::core::EGraph;

TEST(EGraphAnalysisMerge, CrossKindThrowsThroughMerge) {
  EGraph<symbol, analysis> g;
  auto const expr = g.emplace(symbol::Literal, 0, analysis{ExpressionAnalysis{}});
  auto const op = g.emplace(symbol::Once, 0, analysis{OperatorAnalysis{}});
  EXPECT_THROW(g.merge(expr.eclass_id, op.eclass_id), PlannerBug);
}

TEST(EGraphAnalysisMerge, ConflictingConstantsThrowThroughMerge) {
  EGraph<symbol, analysis> g;
  auto const a = g.emplace(
      symbol::Literal, 0, analysis{ExpressionAnalysis{.known_constant_value = ExternalPropertyValue{int64_t{5}}}});
  auto const b = g.emplace(
      symbol::Literal, 1, analysis{ExpressionAnalysis{.known_constant_value = ExternalPropertyValue{int64_t{6}}}});
  EXPECT_THROW(g.merge(a.eclass_id, b.eclass_id), PlannerBug);
}

TEST(EGraphAnalysisMerge, AgreeingConstantSurvivesMerge) {
  EGraph<symbol, analysis> g;
  auto const a = g.emplace(
      symbol::Literal, 0, analysis{ExpressionAnalysis{.known_constant_value = ExternalPropertyValue{int64_t{42}}}});
  auto const b = g.emplace(
      symbol::Literal, 1, analysis{ExpressionAnalysis{.known_constant_value = ExternalPropertyValue{int64_t{42}}}});
  auto const merged = g.merge(a.eclass_id, b.eclass_id);
  auto const &e = std::get<ExpressionAnalysis>(g.eclass(merged.eclass_id).analysis());
  ASSERT_TRUE(e.known_constant_value.has_value());
  EXPECT_TRUE(ConstantIdentityEq{}(*e.known_constant_value, ExternalPropertyValue{int64_t{42}}));
}

// --- Make seeds the analysis arm (and Literal's constant) -------------------
// The make half: each symbol's make() attaches the arm for its kind, and
// Literal additionally carries its value as known_constant_value.

auto AnalysisOf(egraph const &eg, eclass e) -> analysis const & {
  auto const &core = impl_of(eg).graph.core();
  return core.eclass(core.find(EClassId{e.value_of()})).analysis();
}

TEST(MakeSeedsAnalysis, LiteralCarriesConstant) {
  egraph eg;
  auto const lit = eg.MakeLiteral(ExternalPropertyValue{int64_t{7}});
  auto const &e = std::get<ExpressionAnalysis>(AnalysisOf(eg, lit));
  ASSERT_TRUE(e.known_constant_value.has_value());
  EXPECT_TRUE(ConstantIdentityEq{}(*e.known_constant_value, ExternalPropertyValue{int64_t{7}}));
}

TEST(MakeSeedsAnalysis, ScalarLiteralHasNoListLength) {
  egraph eg;
  auto const lit = eg.MakeLiteral(ExternalPropertyValue{int64_t{7}});
  auto const &e = std::get<ExpressionAnalysis>(AnalysisOf(eg, lit));
  EXPECT_FALSE(e.known_list_length.has_value());
}

TEST(MakeSeedsAnalysis, ListLiteralCarriesLength) {
  egraph eg;
  auto const list = ExternalPropertyValue{ExternalPropertyValue::list_t{
      ExternalPropertyValue{int64_t{1}}, ExternalPropertyValue{int64_t{2}}, ExternalPropertyValue{int64_t{3}}}};
  auto const lit = eg.MakeLiteral(list);
  auto const &e = std::get<ExpressionAnalysis>(AnalysisOf(eg, lit));
  ASSERT_TRUE(e.known_list_length.has_value());
  EXPECT_EQ(*e.known_list_length, 3U);
}

TEST(MakeSeedsAnalysis, OperatorSymbolGetsOperatorArm) {
  egraph eg;
  EXPECT_TRUE(std::holds_alternative<OperatorAnalysis>(AnalysisOf(eg, eg.MakeOnce())));
}

TEST(MakeSeedsAnalysis, SymbolGetsSymbolArm) {
  egraph eg;
  EXPECT_TRUE(std::holds_alternative<SymbolAnalysis>(AnalysisOf(eg, eg.MakeSymbol(0, "x"))));
}

TEST(MakeSeedsAnalysis, NamedOutputIsExpressionArm) {
  egraph eg;
  auto const sym = eg.MakeSymbol(0, "x");
  auto const expr = eg.MakeLiteral(ExternalPropertyValue{int64_t{1}});
  EXPECT_TRUE(std::holds_alternative<ExpressionAnalysis>(AnalysisOf(eg, eg.MakeNamedOutput("x", sym, expr))));
}

}  // namespace
}  // namespace memgraph::query::plan::v2
