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

#include <bitset>
#include <stdexcept>
#include <string>
#include <string_view>

#include "gtest/gtest.h"

#include "utils/codepoint_set.hpp"

namespace {

using memgraph::utils::CodepointSet;

char32_t Codepoint(std::size_t pos) { return static_cast<char32_t>(pos); }

TEST(CodepointSet, ReadsAStringTheWayBitsetDoes) {
  constexpr std::string_view kBits = "0000000000000000000000000000000000000000000000000000000000000101";
  constexpr CodepointSet<64> kSet{kBits};
  const std::bitset<64> reference{std::string{kBits}};
  for (std::size_t pos = 0; pos != 64; ++pos) {
    EXPECT_EQ(kSet.Contains(Codepoint(pos)), reference[pos]) << "codepoint " << pos;
  }
  // The trailing character names codepoint 0, so this set holds 0 and 2.
  EXPECT_TRUE(kSet.Contains(Codepoint(0)));
  EXPECT_FALSE(kSet.Contains(Codepoint(1)));
  EXPECT_TRUE(kSet.Contains(Codepoint(2)));
}

TEST(CodepointSet, HoldsCodepointsOnEitherSideOfAWordBoundary) {
  std::string bits(128, '0');
  bits[127] = '1';       // codepoint 0, the last bit of the last word
  bits[127 - 63] = '1';  // codepoint 63, the first bit of the last word
  bits[127 - 64] = '1';  // codepoint 64, the last bit of the first word
  bits[0] = '1';         // codepoint 127
  const CodepointSet<128> set{bits};
  EXPECT_TRUE(set.Contains(Codepoint(0)));
  EXPECT_TRUE(set.Contains(Codepoint(63)));
  EXPECT_TRUE(set.Contains(Codepoint(64)));
  EXPECT_TRUE(set.Contains(Codepoint(127)));
  EXPECT_FALSE(set.Contains(Codepoint(1)));
  EXPECT_FALSE(set.Contains(Codepoint(62)));
  EXPECT_FALSE(set.Contains(Codepoint(65)));
}

TEST(CodepointSet, ExcludesCodepointsAtOrAboveItsSize) {
  const CodepointSet<64> set{std::string(64, '1')};
  EXPECT_TRUE(set.Contains(Codepoint(63)));
  EXPECT_FALSE(set.Contains(Codepoint(64)));
  EXPECT_FALSE(set.Contains(U'\U0010FFFF'));
}

TEST(CodepointSet, ComparesByTheCodepointsItHolds) {
  std::string holds_zero(64, '0');
  holds_zero[63] = '1';
  EXPECT_EQ(CodepointSet<64>{std::string(64, '0')}, CodepointSet<64>{std::string(64, '0')});
  EXPECT_NE(CodepointSet<64>{std::string(64, '0')}, CodepointSet<64>{holds_zero});
}

// Reached while initialising a constant, each of these rejections is a build
// failure instead; at run time it is an exception.
TEST(CodepointSet, RejectsAStringThatIsNotOneCharacterPerCodepoint) {
  EXPECT_THROW((CodepointSet<64>{std::string(63, '0')}), std::invalid_argument);
  EXPECT_THROW((CodepointSet<64>{std::string(65, '0')}), std::invalid_argument);
  EXPECT_THROW((CodepointSet<64>{std::string_view{""}}), std::invalid_argument);
}

TEST(CodepointSet, RejectsACharacterThatIsNotABit) {
  EXPECT_THROW((CodepointSet<64>{std::string(63, '0') + "2"}), std::invalid_argument);
  EXPECT_THROW((CodepointSet<64>{std::string(63, '0') + " "}), std::invalid_argument);
  EXPECT_THROW((CodepointSet<64>{"1" + std::string(62, '0') + "x"}), std::invalid_argument);
}

}  // namespace
