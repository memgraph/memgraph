// Copyright 2022 Memgraph Ltd.
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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "utils/math.hpp"

TEST(UtilsMath, Log2) {
  for (uint64_t i = 1; i < 1000000; ++i) {
    ASSERT_EQ(memgraph::utils::Log2(i), static_cast<uint64_t>(log2(i)));
  }
}

TEST(UtilsMath, IdenticalPositiveValues) {
  const auto epsilon = memgraph::utils::GetEpsilon();
  const auto a = 1.0;
  const auto b = 1.0;

  EXPECT_TRUE(memgraph::utils::CompareDouble(a, b, epsilon) == 0);
}

TEST(UtilsMath, IdenticalNegativeValues) {
  const auto epsilon = memgraph::utils::GetEpsilon();
  const auto a = -1.0;
  const auto b = -1.0;

  EXPECT_TRUE(memgraph::utils::CompareDouble(a, b, epsilon) == 0);
}

TEST(UtilsMath, EpsilonTooSmallSoExpectingDeath) {
  const auto epsilon = memgraph::utils::GetEpsilon() / 10;

  EXPECT_DEATH(memgraph::utils::CompareDouble(0.0, 0.0, epsilon), "");
}

TEST(UtilsMath, FirstValueMuchSmallerThanSecond) {
  const auto a = 1.0;
  const auto b = 2.0;

  EXPECT_TRUE(memgraph::utils::CompareDouble(a, b) < 0);
}

TEST(UtilsMath, FirstValueMuchGreaterThanSecond) {
  const auto a = 2.0;
  const auto b = 1.0;

  EXPECT_TRUE(memgraph::utils::CompareDouble(a, b) > 0);
}

TEST(UtilsMath, FirstValueSmallerThanButEqualToSecond) {
  const auto epsilon = memgraph::utils::GetEpsilon();
  const auto a = 1.0;
  const auto b = a + epsilon / 2;

  EXPECT_TRUE(memgraph::utils::CompareDouble(a, b) == 0);
}

TEST(UtilsMath, FirstValueGreaterThanButEqualToSecond) {
  const auto epsilon = memgraph::utils::GetEpsilon();
  const auto a = 2.0;
  const auto b = a - epsilon / 2;

  EXPECT_TRUE(memgraph::utils::CompareDouble(a, b) == 0);
}

TEST(UtilsMath, FirstValueSmallerThanAndNotEqualToSecond) {
  const auto epsilon = memgraph::utils::GetEpsilon();
  const auto a = 1.0;
  const auto b = a + 2 * epsilon;

  EXPECT_TRUE(memgraph::utils::CompareDouble(a, b) < 0);
}

TEST(UtilsMath, FirstValueGreaterThanAndNotEqualToSecond) {
  const auto epsilon = memgraph::utils::GetEpsilon();
  const auto a = 2.0;
  const auto b = a - 2 * epsilon;

  EXPECT_TRUE(memgraph::utils::CompareDouble(a, b) > 0);
}

TEST(UtilsMath, IsStrictlyGreaterBigDistance) {
  const auto a = 2.0;
  const auto b = 1.0;

  EXPECT_TRUE(memgraph::utils::IsStrictlyGreater(a, b));
}

TEST(UtilsMath, IsStrictlyGreaterWithSmallDistance) {
  const auto a = 1.0;
  const auto b = 1.0 - 2 * memgraph::utils::GetEpsilon();

  EXPECT_TRUE(memgraph::utils::IsStrictlyGreater(a, b));
}

TEST(UtilsMath, IsStrictlyGreaterTooSmallDistance) {
  const auto a = 1.0;
  const auto b = 1.0 - memgraph::utils::GetEpsilon() / 2;

  EXPECT_TRUE(!memgraph::utils::IsStrictlyGreater(a, b));
}

TEST(UtilsMath, IsGreaterOrEqualWithSmallDistance) {
  const auto a = 1.0;
  const auto b = 1.0 - memgraph::utils::GetEpsilon() / 2;

  EXPECT_TRUE(memgraph::utils::IsGreaterOrEqual(a, b));
}

TEST(UtilsMath, IsStrictlyLowerWithBigDistance) {
  const auto a = 1.0;
  const auto b = 2.0;

  EXPECT_TRUE(memgraph::utils::IsStrictlyLower(a, b));
}

TEST(UtilsMath, IsStrictlyLowerSmallDistance) {
  const auto a = 1.0 - 2 * memgraph::utils::GetEpsilon();
  const auto b = 1.0;

  EXPECT_TRUE(memgraph::utils::IsStrictlyLower(a, b));
}

TEST(UtilsMath, IsStrictlyLowerWithTooSmallDistance) {
  const auto a = 1.0 - memgraph::utils::GetEpsilon() / 2;
  const auto b = 1.0;

  EXPECT_FALSE(memgraph::utils::IsStrictlyLower(a, b));
}

TEST(UtilsMath, IsLowerOrEqualWithSmallDistance) {
  const auto a = 1.0 - memgraph::utils::GetEpsilon() / 2;
  const auto b = 1.0;

  EXPECT_TRUE(memgraph::utils::IsLowerOrEqual(a, b));
}
