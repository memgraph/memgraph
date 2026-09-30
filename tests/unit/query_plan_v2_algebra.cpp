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

// Pure-algebra tests for plan_v2: the Bind AliveRequired formula and Alternative
// dominance.  These do not construct an egraph or invoke the planner - they
// exercise the required-set composition, the generic dominance_compare fold,
// and the real Alternative dimension directions directly.
//
// Inclusion criterion: a test here pins a plan_v2 algebra property.  The
// underlying VariableSet container primitives are tested separately; behaviour
// changes here shift the cost-model and resolver together and are cheaper to
// catch at the algebra level than via the full pipeline.

#include <compare>

#include <gtest/gtest.h>

#include "query/plan_v2/egraph/alternative.hpp"
#include "query/plan_v2/test_support/variable_set.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::query::plan::v2 {
namespace {

using EClassId = planner::core::EClassId;
using ENodeId = planner::core::ENodeId;

auto Cmp(Alternative const &a, Alternative const &b) {
  return planner::core::extract::dominance_compare<AlternativeDim_Cost,
                                                   AlternativeDim_Cardinality,
                                                   AlternativeDim_Required,
                                                   AlternativeDim_Introduces>(a, b);
}

auto MakeAlt(double cost, double cardinality, std::initializer_list<uint16_t> required, uint32_t enode_id)
    -> Alternative {
  return {.cost = cost, .cardinality = cardinality, .required = MakeSet(required), .enode_id = ENodeId{enode_id}};
}

struct AltDominanceCase {
  std::string_view name;
  Alternative a;
  Alternative b;
  std::partial_ordering expected;
};

class AltDominanceTest : public testing::TestWithParam<AltDominanceCase> {};

TEST_P(AltDominanceTest, Verify) {
  auto const &tc = GetParam();
  EXPECT_EQ(Cmp(tc.a, tc.b), tc.expected);
}

std::string AltDominanceCaseName(const testing::TestParamInfo<AltDominanceCase> &info) {
  return std::string(info.param.name);
}

// ============================================================================
// Bind algebra: AliveRequired set algebra
// ============================================================================

struct AliveRequiredCase {
  std::string_view name;
  VariableSet input;
  uint16_t sym;
  VariableSet expr_demands;
  VariableSet expected;

  AliveRequiredCase(std::string_view name_, std::initializer_list<uint16_t> input_ids, uint16_t sym_id,
                    std::initializer_list<uint16_t> expr_ids, std::initializer_list<uint16_t> expected_ids)
      : name(name_),
        input(MakeSet(input_ids)),
        sym(sym_id),
        expr_demands(MakeSet(expr_ids)),
        expected(MakeSet(expected_ids)) {}
};

class AliveRequiredTest : public testing::TestWithParam<AliveRequiredCase> {};

TEST_P(AliveRequiredTest, Verify) {
  auto const &tc = GetParam();
  EXPECT_EQ(tc.input.difference_bit(tc.sym).set_union(tc.expr_demands), tc.expected);
}

std::string AliveRequiredCaseName(const testing::TestParamInfo<AliveRequiredCase> &info) {
  return std::string(info.param.name);
}

INSTANTIATE_TEST_SUITE_P(
    BindAlgebra_AliveRequired, AliveRequiredTest,
    testing::Values(AliveRequiredCase{"RemovesSymFromInput", {1, 2, 3}, 2, {}, {1, 3}},
                    AliveRequiredCase{"AddsExprDemandsToOutput", {1, 2}, 2, {4, 5}, {1, 4, 5}},
                    AliveRequiredCase{"ExprDemandOverlapsInputProducesUnion", {1, 2, 3}, 2, {3, 4}, {1, 3, 4}},
                    AliveRequiredCase{"ExprDemandReintroducesSym", {1, 2}, 2, {2}, {1, 2}},
                    AliveRequiredCase{"EmptyInputAndExprYieldsEmpty", {}, 2, {}, {}},
                    AliveRequiredCase{"SymIsMinElementOfInput_NoDemands", {1, 2, 3}, 1, {}, {2, 3}},
                    AliveRequiredCase{"SymIsMinElementOfInput_WithDemands", {1, 2, 3}, 1, {4}, {2, 3, 4}},
                    AliveRequiredCase{"SymIsMaxElementOfInput_NoDemands", {1, 2, 3}, 3, {}, {1, 2}},
                    AliveRequiredCase{"SymIsMaxElementOfInput_WithDemands", {1, 2, 3}, 3, {4}, {1, 2, 4}},
                    AliveRequiredCase{"SymIsSoleElementOfInput_NoDemands", {2}, 2, {}, {}},
                    AliveRequiredCase{"SymIsSoleElementOfInput_WithDemands", {2}, 2, {1, 3}, {1, 3}}),
    AliveRequiredCaseName);

INSTANTIATE_TEST_SUITE_P(AltDominance, AltDominanceTest,
                         testing::Values(AltDominanceCase{"LowerCostDominatesWhenCardinalityAndRequiredEqual",
                                                          MakeAlt(1.0, 6.0, {1}, 0),
                                                          MakeAlt(5.0, 6.0, {1}, 1),
                                                          std::partial_ordering::greater},
                                         AltDominanceCase{"EqualOnAllAxesIsEquivalent",
                                                          MakeAlt(5.0, 6.0, {1}, 0),
                                                          MakeAlt(5.0, 6.0, {1}, 1),
                                                          std::partial_ordering::equivalent},
                                         AltDominanceCase{"LowerCostHigherCardinalityIsUnordered",
                                                          MakeAlt(1.0, 1000.0, {1}, 0),
                                                          MakeAlt(5.0, 6.0, {1}, 1),
                                                          std::partial_ordering::unordered},
                                         AltDominanceCase{"SmallerRequiredDominatesWhenCostAndCardinalityEqual",
                                                          MakeAlt(5.0, 6.0, {1}, 0),
                                                          MakeAlt(5.0, 6.0, {1, 2}, 1),
                                                          std::partial_ordering::greater},
                                         AltDominanceCase{"LowerCardinalityDominates_Forward",
                                                          MakeAlt(5.0, 6.0, {1}, 0),
                                                          MakeAlt(5.0, 1000.0, {1}, 1),
                                                          std::partial_ordering::greater},
                                         AltDominanceCase{"LowerCardinalityDominates_Reverse",
                                                          MakeAlt(5.0, 1000.0, {1}, 0),
                                                          MakeAlt(5.0, 6.0, {1}, 1),
                                                          std::partial_ordering::less}),
                         AltDominanceCaseName);

// ============================================================================
// dominance_compare fold machinery (generic, dimension-agnostic)
// ============================================================================

// A minimal two-dimension alternative exercises the generic dominance_compare
// fold independently of Alternative's real dimension set: agreement on a dim,
// disagreement across dims (unordered), all-equal (equivalent), and a single
// deciding dim when the other ties.  These dims compare raw (no "lower is
// better" reinterpretation), so `less` means the first argument is smaller.
struct Pair2 {
  double x;
  double y;
};

struct Dim2X {
  static auto compare(Pair2 const &a, Pair2 const &b) -> std::partial_ordering { return a.x <=> b.x; }
};

struct Dim2Y {
  static auto compare(Pair2 const &a, Pair2 const &b) -> std::partial_ordering { return a.y <=> b.y; }
};

auto Cmp2(Pair2 const &a, Pair2 const &b) { return planner::core::extract::dominance_compare<Dim2X, Dim2Y>(a, b); }

TEST(DominanceFold, AgreementOnBothDimsTakesThatDirection) {
  EXPECT_EQ(Cmp2({1.0, 1.0}, {2.0, 2.0}), std::partial_ordering::less);
  EXPECT_EQ(Cmp2({2.0, 2.0}, {1.0, 1.0}), std::partial_ordering::greater);
}

TEST(DominanceFold, DisagreementAcrossDimsIsUnordered) {
  EXPECT_EQ(Cmp2({1.0, 2.0}, {2.0, 1.0}), std::partial_ordering::unordered);
}

TEST(DominanceFold, EqualOnAllDimsIsEquivalent) {
  EXPECT_EQ(Cmp2({1.0, 1.0}, {1.0, 1.0}), std::partial_ordering::equivalent);
}

TEST(DominanceFold, SingleDecidingDimWhenOtherTies) {
  EXPECT_EQ(Cmp2({1.0, 1.0}, {1.0, 2.0}), std::partial_ordering::less);
}

}  // namespace
}  // namespace memgraph::query::plan::v2
