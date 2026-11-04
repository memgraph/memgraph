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

#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string_view>

#include <gtest/gtest.h>

#include "algorithm/bitwise.hpp"

namespace {

constexpr auto kMin = std::numeric_limits<int64_t>::min();
constexpr auto kMax = std::numeric_limits<int64_t>::max();

std::optional<int64_t> Op(std::optional<int64_t> a, std::optional<std::string_view> op, std::optional<int64_t> b) {
  return Bitwise::Op(a, op, b);
}

}  // namespace

TEST(Bitwise, AndOrXor) {
  EXPECT_EQ(Op(60, "&", 13), 12);
  EXPECT_EQ(Op(60, "AND", 13), 12);
  EXPECT_EQ(Op(60, "|", 13), 61);
  EXPECT_EQ(Op(60, "OR", 13), 61);
  EXPECT_EQ(Op(60, "^", 13), 49);
  EXPECT_EQ(Op(60, "XOR", 13), 49);
  EXPECT_EQ(Op(kMin, "&", -1), kMin);
  EXPECT_EQ(Op(kMin, "|", kMax), -1);
}

TEST(Bitwise, OperatorIsCaseInsensitive) {
  EXPECT_EQ(Op(60, "and", 13), 12);
  EXPECT_EQ(Op(60, "And", 13), 12);
  EXPECT_EQ(Op(60, "Left Shift", 2), 240);
  EXPECT_EQ(Op(-1, "Right Shift Unsigned", 60), 15);
}

TEST(Bitwise, NotIgnoresTheRightOperand) {
  EXPECT_EQ(Op(60, "~", 0), -61);
  EXPECT_EQ(Op(60, "NOT", 12345), -61);
  EXPECT_EQ(Op(kMin, "~", 0), kMax);
}

// The "~" comparison is case-sensitive, so only the literal "~" may omit the right operand (intentional, for
// compatibility).
TEST(Bitwise, OnlyTildeAcceptsANullRightOperand) {
  EXPECT_EQ(Op(60, "~", std::nullopt), -61);
  EXPECT_EQ(Op(60, "NOT", std::nullopt), std::nullopt);
  EXPECT_EQ(Op(60, "not", std::nullopt), std::nullopt);
}

TEST(Bitwise, NullOrEmptyArgumentsYieldNull) {
  EXPECT_EQ(Op(std::nullopt, "&", 13), std::nullopt);
  EXPECT_EQ(Op(std::nullopt, "~", 0), std::nullopt);
  EXPECT_EQ(Op(60, std::nullopt, 13), std::nullopt);
  EXPECT_EQ(Op(60, "", 13), std::nullopt);
  EXPECT_EQ(Op(60, "&", std::nullopt), std::nullopt);
}

TEST(Bitwise, ShiftLeft) {
  EXPECT_EQ(Op(60, "<<", 2), 240);
  EXPECT_EQ(Op(60, "LEFT SHIFT", 2), 240);
  EXPECT_EQ(Op(1, "<<", 63), kMin);
  EXPECT_EQ(Op(kMax, "<<", 1), -2);
  EXPECT_EQ(Op(-1, "<<", 0), -1);
}

TEST(Bitwise, ShiftRightKeepsTheSign) {
  EXPECT_EQ(Op(60, ">>", 2), 15);
  EXPECT_EQ(Op(60, "RIGHT SHIFT", 2), 15);
  EXPECT_EQ(Op(-60, ">>", 2), -15);
  EXPECT_EQ(Op(kMin, ">>", 63), -1);
}

TEST(Bitwise, ShiftRightUnsignedFillsWithZeros) {
  EXPECT_EQ(Op(-1, ">>>", 60), 15);
  EXPECT_EQ(Op(-1, "RIGHT SHIFT UNSIGNED", 60), 15);
  EXPECT_EQ(Op(kMin, ">>>", 63), 1);
  EXPECT_EQ(Op(-1, ">>>", 0), -1);
  EXPECT_EQ(Op(60, ">>>", 2), 15);
}

// Java uses only the six low bits of a long's shift distance.
TEST(Bitwise, ShiftDistanceIsTakenModulo64) {
  EXPECT_EQ(Op(1, "<<", 64), 1);
  EXPECT_EQ(Op(1, "<<", 65), 2);
  EXPECT_EQ(Op(1, "<<", -1), kMin);
  EXPECT_EQ(Op(kMin, ">>", 64), kMin);
  EXPECT_EQ(Op(kMin, ">>>", -1), 1);
  EXPECT_EQ(Op(1, "<<", kMin), 1);
}

TEST(Bitwise, UnknownOperatorThrows) {
  EXPECT_THROW(Op(60, "NAND", 13), std::invalid_argument);
  EXPECT_THROW(Op(60, "&&", 13), std::invalid_argument);
  EXPECT_THROW(Op(60, "right  shift", 2), std::invalid_argument);
  EXPECT_THROW(Op(60, " and", 13), std::invalid_argument);

  try {
    Op(60, "NAND", 13);
    FAIL() << "expected std::invalid_argument";
  } catch (const std::invalid_argument &e) {
    EXPECT_STREQ(e.what(), "Invalid bitwise operator : 'NAND'");
  }
}

// The null checks run first, so an unknown operator with a null right operand is not an error.
TEST(Bitwise, NullRightOperandWinsOverAnUnknownOperator) { EXPECT_EQ(Op(60, "NAND", std::nullopt), std::nullopt); }

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
