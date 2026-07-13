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
// the ExpressionAnalysis merge, that merge through the real EGraph, and
// the make half that seeds a new e-class's analysis arm and facts.

#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <utility>

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

using planner::core::EClassId;
using planner::core::EGraph;
using storage::ExternalPropertyValue;

auto SingletonList(ExternalPropertyValue v) -> ExternalPropertyValue {
  return ExternalPropertyValue{ExternalPropertyValue::list_t{std::move(v)}};
}

auto SingletonMap(ExternalPropertyValue v) -> ExternalPropertyValue {
  return ExternalPropertyValue{ExternalPropertyValue::map_t{{"k", std::move(v)}}};
}

// Assert `a` is the expression arm carrying `expected` as its known constant,
// compared under constant identity (type-then-structure).
void ExpectConstant(analysis const &a, ExternalPropertyValue const &expected) {
  auto const *e = a.expression();
  ASSERT_NE(e, nullptr);
  ASSERT_TRUE(e->known_constant_value.has_value());
  EXPECT_TRUE(ConstantIdentityEq{}(*e->known_constant_value, expected));
}

// --- Constant identity: equality and its hash companion ----------------------
//
// Only cases where constant identity diverges from ExternalPropertyValue's own
// == / std::hash earn a test; equal-scalar or null-null cases would just
// re-check PropertyValue. The divergences: no numeric coercion (Int is not
// Double), NaN is reflexive, and both rules recurse through List/Map.

class ConstantIdentityTest : public ::testing::Test {
 protected:
  ConstantIdentityEq eq_;
  ConstantIdentityHash hash_;
};

TEST_F(ConstantIdentityTest, IntAndDoubleAreDistinctConstants) {
  EXPECT_FALSE(eq_(ExternalPropertyValue{int64_t{1}}, ExternalPropertyValue{1.0}));
}

TEST_F(ConstantIdentityTest, NaNEqualsNaN) {
  auto const nan = std::numeric_limits<double>::quiet_NaN();
  EXPECT_TRUE(eq_(ExternalPropertyValue{nan}, ExternalPropertyValue{nan}));
}

TEST_F(ConstantIdentityTest, ListWithNaNElementIsSame) {
  auto const nan = std::numeric_limits<double>::quiet_NaN();
  EXPECT_TRUE(eq_(SingletonList(ExternalPropertyValue{nan}), SingletonList(ExternalPropertyValue{nan})));
}

TEST_F(ConstantIdentityTest, ListsWithCrossTypeElementsAreDistinct) {
  EXPECT_FALSE(eq_(SingletonList(ExternalPropertyValue{int64_t{1}}), SingletonList(ExternalPropertyValue{1.0})));
}

TEST_F(ConstantIdentityTest, MapWithNaNValueIsSame) {
  auto const nan = std::numeric_limits<double>::quiet_NaN();
  EXPECT_TRUE(eq_(SingletonMap(ExternalPropertyValue{nan}), SingletonMap(ExternalPropertyValue{nan})));
}

// The hash must follow the equality: distinct under Eq may collide, but equal
// under Eq must hash alike. The two interesting cases are the ones where
// std::hash<ExternalPropertyValue> disagrees with constant identity.
TEST_F(ConstantIdentityTest, HashSeparatesIntFromDouble) {
  EXPECT_NE(hash_(ExternalPropertyValue{int64_t{1}}), hash_(ExternalPropertyValue{1.0}));
  // An int64 past 2^53 rounds when coerced to double; the type tag must keep it
  // from colliding with that double.
  auto const big = int64_t{(int64_t{1} << 53) + 1};
  EXPECT_NE(hash_(ExternalPropertyValue{big}), hash_(ExternalPropertyValue{static_cast<double>(big)}));
}

TEST_F(ConstantIdentityTest, HashUnifiesNaNs) {
  // Different NaN bit patterns are one constant under Eq, so one hash.
  EXPECT_EQ(hash_(ExternalPropertyValue{std::numeric_limits<double>::quiet_NaN()}),
            hash_(ExternalPropertyValue{std::nan("0x1234")}));
}

TEST_F(ConstantIdentityTest, HashUnifiesNaNsInsideLists) {
  EXPECT_EQ(hash_(SingletonList(ExternalPropertyValue{std::numeric_limits<double>::quiet_NaN()})),
            hash_(SingletonList(ExternalPropertyValue{std::nan("0x1234")})));
}

// --- ExpressionAnalysis merge -----------------------------------------------

TEST(AnalysisMerge, OneSidedConstantIsTaken) {
  analysis lhs{ExpressionAnalysis{}};
  analysis rhs{ExpressionAnalysis{.known_constant_value = ExternalPropertyValue{int64_t{7}}}};
  lhs.merge(rhs);
  ExpectConstant(lhs, ExternalPropertyValue{int64_t{7}});
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
  ExpectConstant(lhs, ExternalPropertyValue{int64_t{42}});
}

TEST(AnalysisMerge, OneSidedListLengthIsTaken) {
  analysis lhs{ExpressionAnalysis{}};
  analysis rhs{ExpressionAnalysis{.known_list_length = std::size_t{3}}};
  lhs.merge(rhs);
  auto const *e = lhs.expression();
  ASSERT_NE(e, nullptr);
  ASSERT_TRUE(e->known_list_length.has_value());
  EXPECT_EQ(*e->known_list_length, 3U);
}

TEST(AnalysisMerge, ConflictingListLengthsThrow) {
  analysis lhs{ExpressionAnalysis{.known_list_length = std::size_t{3}}};
  analysis rhs{ExpressionAnalysis{.known_list_length = std::size_t{4}}};
  EXPECT_THROW(lhs.merge(rhs), PlannerBug);
}

// --- Merge through the real EGraph ------------------------------------------
// The seam between core::EClass and the e-class analysis: a seeded e-class
// carries its arm and facts into the e-graph, and EGraph::merge combines them.

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
  ExpectConstant(g.eclass(merged.eclass_id).analysis(), ExternalPropertyValue{int64_t{42}});
}

// --- Seed-purity check on hash-cons hits ------------------------------------
// EGraph::emplace enforces the SymbolMakeTraits contract: a seed must be a pure
// function of e-node identity, so re-inserting the same identity with a
// conflicting seed merges the seeds on the hash-cons hit and throws.

TEST(EGraphSeedPurity, ConflictingSeedOnHashConsHitThrows) {
  EGraph<symbol, analysis> g;
  g.emplace(
      symbol::Literal, 0, analysis{ExpressionAnalysis{.known_constant_value = ExternalPropertyValue{int64_t{5}}}});
  // Same identity (Literal#0), different constant: the hash-cons hit runs the
  // seed-purity merge, which throws on the contradiction.
  EXPECT_THROW(
      g.emplace(
          symbol::Literal, 0, analysis{ExpressionAnalysis{.known_constant_value = ExternalPropertyValue{int64_t{6}}}}),
      PlannerBug);
}

TEST(EGraphSeedPurity, AgreeingSeedOnHashConsHitReusesClass) {
  EGraph<symbol, analysis> g;
  auto const a = g.emplace(
      symbol::Literal, 0, analysis{ExpressionAnalysis{.known_constant_value = ExternalPropertyValue{int64_t{7}}}});
  // Same identity and the same seed: the check is a no-op and the class is reused.
  auto const b = g.emplace(
      symbol::Literal, 0, analysis{ExpressionAnalysis{.known_constant_value = ExternalPropertyValue{int64_t{7}}}});
  EXPECT_EQ(a.eclass_id, b.eclass_id);
  EXPECT_FALSE(b.did_insert);
}

// --- Make seeds the analysis arm (and Literal's constant) -------------------
// The make half: each symbol's make() attaches the arm for its kind, and
// Literal additionally carries its value as known_constant_value. The literal
// store is keyed on ConstantIdentityHash/Eq, so a literal's e-class always
// carries the fact for exactly the value it was interned as.

class MakeAnalysis : public ::testing::Test {
 protected:
  egraph eg_;

  auto Arm(eclass e) -> analysis const & {
    auto const &core = impl_of(eg_).graph.core();
    return core.eclass(core.find(EClassId{e.value_of()})).analysis();
  }

  auto Expression(eclass e) -> ExpressionAnalysis const * { return Arm(e).expression(); }
};

TEST_F(MakeAnalysis, LiteralCarriesConstant) {
  auto const lit = eg_.MakeLiteral(ExternalPropertyValue{int64_t{7}});
  ExpectConstant(Arm(lit), ExternalPropertyValue{int64_t{7}});
}

TEST_F(MakeAnalysis, ScalarLiteralHasNoListLength) {
  auto const *e = Expression(eg_.MakeLiteral(ExternalPropertyValue{int64_t{7}}));
  ASSERT_NE(e, nullptr);
  EXPECT_FALSE(e->known_list_length.has_value());
}

TEST_F(MakeAnalysis, ListLiteralCarriesLength) {
  auto const list = ExternalPropertyValue{ExternalPropertyValue::list_t{
      ExternalPropertyValue{int64_t{1}}, ExternalPropertyValue{int64_t{2}}, ExternalPropertyValue{int64_t{3}}}};
  auto const *e = Expression(eg_.MakeLiteral(list));
  ASSERT_NE(e, nullptr);
  ASSERT_TRUE(e->known_list_length.has_value());
  EXPECT_EQ(*e->known_list_length, 3U);
}

TEST_F(MakeAnalysis, OperatorSymbolGetsOperatorArm) {
  EXPECT_TRUE(std::holds_alternative<OperatorAnalysis>(Arm(eg_.MakeOnce())));
}

TEST_F(MakeAnalysis, SymbolGetsSymbolArm) {
  EXPECT_TRUE(std::holds_alternative<SymbolAnalysis>(Arm(eg_.MakeSymbol(0, "x"))));
}

TEST_F(MakeAnalysis, NamedOutputIsExpressionArm) {
  auto const sym = eg_.MakeSymbol(0, "x");
  auto const expr = eg_.MakeLiteral(ExternalPropertyValue{int64_t{1}});
  EXPECT_TRUE(std::holds_alternative<ExpressionAnalysis>(Arm(eg_.MakeNamedOutput("x", sym, expr))));
}

TEST_F(MakeAnalysis, IntAndDoubleInternDistinctly) {
  auto const as_int = eg_.MakeLiteral(ExternalPropertyValue{int64_t{1}});
  auto const as_double = eg_.MakeLiteral(ExternalPropertyValue{1.0});
  // Numeric coercion must not fuse them: each keeps its own typed constant.
  EXPECT_NE(as_int.value_of(), as_double.value_of());

  auto const *int_facts = Expression(as_int);
  ASSERT_NE(int_facts, nullptr);
  ASSERT_TRUE(int_facts->known_constant_value.has_value());
  EXPECT_TRUE(int_facts->known_constant_value->IsInt());

  auto const *double_facts = Expression(as_double);
  ASSERT_NE(double_facts, nullptr);
  ASSERT_TRUE(double_facts->known_constant_value.has_value());
  EXPECT_TRUE(double_facts->known_constant_value->IsDouble());
}

TEST_F(MakeAnalysis, NaNInternsOnce) {
  auto const a = eg_.MakeLiteral(ExternalPropertyValue{std::numeric_limits<double>::quiet_NaN()});
  auto const b = eg_.MakeLiteral(ExternalPropertyValue{std::nan("0x1234")});
  EXPECT_EQ(a.value_of(), b.value_of());
}

}  // namespace
}  // namespace memgraph::query::plan::v2
