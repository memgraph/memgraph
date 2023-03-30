// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <cmath>
#include <limits>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "utils/math.hpp"

TEST(UtilsMath, Log2) {
  for (uint64_t i = 1; i < 1000000; ++i) {
    ASSERT_EQ(memgraph::utils::Log2(i), static_cast<uint64_t>(log2(i)));
  }
}

TEST(UtilsMath, EqualFloat) {
  ASSERT_TRUE(memgraph::utils::ApproxEqualDecimal(0.2f, 0.2f));
  ASSERT_TRUE(memgraph::utils::ApproxEqualDecimal(0.2f, 0.199999999999f));
  ASSERT_TRUE(memgraph::utils::ApproxEqualDecimal(0.2f, 0.200000000001f));
  ASSERT_FALSE(memgraph::utils::ApproxEqualDecimal(0.2f, 0.19995f));
}

TEST(UtilsMath, EqualDouble) {
  ASSERT_TRUE(memgraph::utils::ApproxEqualDecimal(0.2, 0.2));
  ASSERT_TRUE(memgraph::utils::ApproxEqualDecimal(0.2, 0.19999999999999999999));
  ASSERT_TRUE(memgraph::utils::ApproxEqualDecimal(0.2, 0.20000000000000000001));
  ASSERT_FALSE(memgraph::utils::ApproxEqualDecimal(0.2, 0.19995));
}

TEST(UtilsMath, LessThan) {
  ASSERT_TRUE(memgraph::utils::LessThanDecimal(0.2, 0.3));
  ASSERT_TRUE(memgraph::utils::LessThanDecimal(0.2, 0.20001));
}

TEST(UtilsMath, ChiSquared) {
  ASSERT_EQ(std::numeric_limits<double>::max(), memgraph::utils::ChiSquaredValue(2.0, 0.0));
  ASSERT_DOUBLE_EQ(0.0, memgraph::utils::ChiSquaredValue(2.0, 2.0));
  ASSERT_DOUBLE_EQ(1.0, memgraph::utils::ChiSquaredValue(2.0, 1.0));
  ASSERT_DOUBLE_EQ(1. / 3., memgraph::utils::ChiSquaredValue(4.0, 3.0));
}
