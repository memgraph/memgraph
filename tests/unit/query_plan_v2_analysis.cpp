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

// Unit tests for the plan_v2 analysis framework: constant-identity equality
// and the ExpressionAnalysis lattice merge.

#include <gtest/gtest.h>

#include <limits>

#include "query/exceptions.hpp"
#include "query/plan_v2/resolve/analysis.hpp"
#include "query/plan_v2/resolve/constant_identity.hpp"
#include "storage/v2/property_value.hpp"

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

}  // namespace
}  // namespace memgraph::query::plan::v2
