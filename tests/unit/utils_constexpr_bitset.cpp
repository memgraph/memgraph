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

#include "utils/constexpr_bitset.hpp"

namespace {

using memgraph::utils::ConstexprBitset;

TEST(ConstexprBitset, ReadsAStringTheWayBitsetDoes) {
  constexpr std::string_view kBits = "0000000000000000000000000000000000000000000000000000000000000101";
  constexpr ConstexprBitset<64> kSet{kBits};
  const std::bitset<64> reference{std::string{kBits}};
  for (std::size_t pos = 0; pos != 64; ++pos) {
    EXPECT_EQ(kSet.Contains(pos), reference[pos]) << "index " << pos;
  }
  // The trailing character names index 0, so this set holds 0 and 2.
  EXPECT_TRUE(kSet.Contains(0));
  EXPECT_FALSE(kSet.Contains(1));
  EXPECT_TRUE(kSet.Contains(2));
}

TEST(ConstexprBitset, HoldsMembersOnEitherSideOfAWordBoundary) {
  std::string bits(128, '0');
  bits[127] = '1';       // index 0, the last bit of the last word
  bits[127 - 63] = '1';  // index 63, the first bit of the last word
  bits[127 - 64] = '1';  // index 64, the last bit of the first word
  bits[0] = '1';         // index 127
  const ConstexprBitset<128> set{bits};
  EXPECT_TRUE(set.Contains(0));
  EXPECT_TRUE(set.Contains(63));
  EXPECT_TRUE(set.Contains(64));
  EXPECT_TRUE(set.Contains(127));
  EXPECT_FALSE(set.Contains(1));
  EXPECT_FALSE(set.Contains(62));
  EXPECT_FALSE(set.Contains(65));
}

TEST(ConstexprBitset, ExcludesIndicesAtOrAboveItsSize) {
  const ConstexprBitset<64> set{std::string(64, '1')};
  EXPECT_TRUE(set.Contains(63));
  EXPECT_FALSE(set.Contains(64));
  EXPECT_FALSE(set.Contains(std::size_t{0x10FFFF}));
}

TEST(ConstexprBitset, ComparesByTheMembersItHolds) {
  std::string holds_zero(64, '0');
  holds_zero[63] = '1';
  EXPECT_EQ(ConstexprBitset<64>{std::string(64, '0')}, ConstexprBitset<64>{std::string(64, '0')});
  EXPECT_NE(ConstexprBitset<64>{std::string(64, '0')}, ConstexprBitset<64>{holds_zero});
}

// Reached while initialising a constant, each of these rejections is a build
// failure instead; at run time it is an exception.
TEST(ConstexprBitset, RejectsAStringThatIsNotOneCharacterPerBit) {
  EXPECT_THROW((ConstexprBitset<64>{std::string(63, '0')}), std::invalid_argument);
  EXPECT_THROW((ConstexprBitset<64>{std::string(65, '0')}), std::invalid_argument);
  EXPECT_THROW((ConstexprBitset<64>{std::string_view{""}}), std::invalid_argument);
}

TEST(ConstexprBitset, RejectsACharacterThatIsNotABit) {
  EXPECT_THROW((ConstexprBitset<64>{std::string(63, '0') + "2"}), std::invalid_argument);
  EXPECT_THROW((ConstexprBitset<64>{std::string(63, '0') + " "}), std::invalid_argument);
  EXPECT_THROW((ConstexprBitset<64>{"1" + std::string(62, '0') + "x"}), std::invalid_argument);
}

}  // namespace
