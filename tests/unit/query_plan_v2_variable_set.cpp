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

// Unit tests for the VariableSet container primitives (test / difference_bit /
// set_union), each exercised independently.  VariableSet is plain set algebra;
// the plan_v2 domain meaning built on top of it (the Bind AliveRequired
// formula, Alternative dominance) is tested at the algebra level.

#include <gtest/gtest.h>

#include "query/plan_v2/test_support/variable_set.hpp"

namespace memgraph::query::plan::v2 {
namespace {

// ----------------------------------------------------------------------------
// test(bit): membership query
// ----------------------------------------------------------------------------

TEST(VariableSet, TestReportsMembership) {
  auto const s = MakeSet({1, 2, 3});
  EXPECT_TRUE(s.test(2));
  EXPECT_TRUE(s.test(1));
  EXPECT_TRUE(s.test(3));
}

TEST(VariableSet, TestRejectsAbsentBit) {
  auto const s = MakeSet({1, 2, 3});
  EXPECT_FALSE(s.test(4));
  EXPECT_FALSE(s.test(0));
}

TEST(VariableSet, TestOnEmptyIsAlwaysFalse) {
  auto const s = MakeSet({});
  EXPECT_FALSE(s.test(1));
}

// ----------------------------------------------------------------------------
// difference_bit(bit): drop a single element
// ----------------------------------------------------------------------------

TEST(VariableSet, DifferenceBitRemovesPresentElement) {
  EXPECT_EQ(MakeSet({1, 2, 3}).difference_bit(2), MakeSet({1, 3}));
}

TEST(VariableSet, DifferenceBitOfAbsentElementIsNoOp) {
  EXPECT_EQ(MakeSet({1, 2, 3}).difference_bit(4), MakeSet({1, 2, 3}));
}

TEST(VariableSet, DifferenceBitOnEmptyStaysEmpty) { EXPECT_EQ(MakeSet({}).difference_bit(1), MakeSet({})); }

TEST(VariableSet, DifferenceBitOfSoleElementYieldsEmpty) { EXPECT_EQ(MakeSet({2}).difference_bit(2), MakeSet({})); }

// ----------------------------------------------------------------------------
// set_union(other): combine two sets
// ----------------------------------------------------------------------------

TEST(VariableSet, SetUnionCombinesDisjoint) {
  EXPECT_EQ(MakeSet({1, 2}).set_union(MakeSet({3, 4})), MakeSet({1, 2, 3, 4}));
}

TEST(VariableSet, SetUnionOfOverlappingDeduplicates) {
  EXPECT_EQ(MakeSet({1, 2, 3}).set_union(MakeSet({2, 3, 4})), MakeSet({1, 2, 3, 4}));
}

TEST(VariableSet, SetUnionWithEmptyIsIdentity) {
  EXPECT_EQ(MakeSet({1, 2}).set_union(MakeSet({})), MakeSet({1, 2}));
  EXPECT_EQ(MakeSet({}).set_union(MakeSet({1, 2})), MakeSet({1, 2}));
}

}  // namespace
}  // namespace memgraph::query::plan::v2
