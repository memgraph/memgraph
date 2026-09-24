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

// The arithmetic both layers place a pair of numbers by, asked of the numbers
// themselves.
//
// A stored value and a query value are held differently and compared by
// separate code, and each has its own tests for reaching this. What those
// cannot state without a value wrapped around it is the arithmetic, which is
// the part the two must never differ on: a pair placed one way by a filter and
// another by an index is a row one returns and the other does not.
//
// Nothing here links a layer, so a failure names the arithmetic rather than
// whichever caller happened to reach it.

#include <cmath>
#include <compare>
#include <cstdint>
#include <limits>

#include <gtest/gtest.h>

#include "value_order/numbers.hpp"

namespace {

using memgraph::value_order::AnIntegerCanHold;
using memgraph::value_order::CompareDoublesNaNLast;
using memgraph::value_order::PlaceIntegerAgainstDouble;
using memgraph::value_order::PlaceIntegerAgainstDoubleNaNLast;
using memgraph::value_order::ReversedOrder;

/// The largest integer every smaller one of which is exactly a double. Past it
/// the doubles stop being spaced one apart, which is where reading an integer
/// as a double stops telling two of them apart.
constexpr auto kWidestExact = int64_t{1} << 53;

constexpr auto kNaN = std::numeric_limits<double>::quiet_NaN();
constexpr auto kInfinity = std::numeric_limits<double>::infinity();

}  // namespace

TEST(PlacesAnIntegerAgainstADouble, ReadsEachAtItsOwnWidth) {
  // The pair the usual arithmetic conversions cannot tell apart: two integers
  // one apart that reach the same double. Placing them through it would hold
  // both equal to that double while telling them apart from each other, which
  // leaves a sort without a strict weak ordering.
  auto const reached = static_cast<double>(kWidestExact);

  EXPECT_TRUE(std::is_eq(PlaceIntegerAgainstDouble(kWidestExact, reached)));
  EXPECT_TRUE(std::is_gt(PlaceIntegerAgainstDouble(kWidestExact + 1, reached)));
}

TEST(PlacesAnIntegerAgainstADouble, PlacesOneEitherSideOfTheFractionBesideIt) {
  EXPECT_TRUE(std::is_lt(PlaceIntegerAgainstDouble(2, 2.5)));
  EXPECT_TRUE(std::is_gt(PlaceIntegerAgainstDouble(3, 2.5)));
  EXPECT_TRUE(std::is_lt(PlaceIntegerAgainstDouble(-3, -2.5)));
  EXPECT_TRUE(std::is_gt(PlaceIntegerAgainstDouble(-2, -2.5)));
}

TEST(PlacesAnIntegerAgainstADouble, SettlesTheRangeBeforeConverting) {
  // Turning a double outside the integer range into one is undefined rather
  // than merely inexact, so the range is answered before the conversion.
  constexpr auto widest = std::numeric_limits<int64_t>::max();
  constexpr auto narrowest = std::numeric_limits<int64_t>::min();

  EXPECT_TRUE(std::is_lt(PlaceIntegerAgainstDouble(widest, 1e300)));
  EXPECT_TRUE(std::is_gt(PlaceIntegerAgainstDouble(narrowest, -1e300)));
  EXPECT_TRUE(std::is_lt(PlaceIntegerAgainstDouble(widest, kInfinity)));
  EXPECT_TRUE(std::is_gt(PlaceIntegerAgainstDouble(narrowest, -kInfinity)));
}

TEST(PlacesAnIntegerAgainstADouble, PlacesTheEdgesOfTheIntegerRange) {
  // The smallest integer is exactly a double and the largest is not, so the
  // two edges are not each other's mirror and each is asked for.
  constexpr auto narrowest = std::numeric_limits<int64_t>::min();
  auto const as_double = static_cast<double>(narrowest);

  EXPECT_TRUE(std::is_eq(PlaceIntegerAgainstDouble(narrowest, as_double)));
  EXPECT_TRUE(std::is_gt(PlaceIntegerAgainstDouble(narrowest + 1, as_double)));

  // One past the largest integer is exactly a double, and every integer is
  // below it.
  EXPECT_TRUE(std::is_lt(PlaceIntegerAgainstDouble(std::numeric_limits<int64_t>::max(), -as_double)));
}

TEST(PlacesAnIntegerAgainstADouble, LeavesOnlyANaNUnplaced) {
  EXPECT_EQ(PlaceIntegerAgainstDouble(0, kNaN), std::partial_ordering::unordered);
  EXPECT_EQ(PlaceIntegerAgainstDouble(0, -kNaN), std::partial_ordering::unordered);

  EXPECT_NE(PlaceIntegerAgainstDouble(0, 0.0), std::partial_ordering::unordered);
  EXPECT_NE(PlaceIntegerAgainstDouble(0, kInfinity), std::partial_ordering::unordered);
}

TEST(PlacesAnIntegerAgainstADouble, ReadsTheWholePartAndThenWhatFollowsIt) {
  // A shared whole part is decided by what the double carries past it, and
  // truncation takes the double's own sign either side of zero.
  EXPECT_TRUE(std::is_lt(PlaceIntegerAgainstDouble(0, 0.5)));
  EXPECT_TRUE(std::is_gt(PlaceIntegerAgainstDouble(0, -0.5)));
  EXPECT_TRUE(std::is_eq(PlaceIntegerAgainstDouble(0, 0.0)));
  EXPECT_TRUE(std::is_eq(PlaceIntegerAgainstDouble(0, -0.0)));
}

TEST(PlacesADoubleWithANaNLast, PutsANaNAfterEveryNumberAndBesideAnotherNaN) {
  EXPECT_EQ(CompareDoublesNaNLast(kNaN, 0.0), std::weak_ordering::greater);
  EXPECT_EQ(CompareDoublesNaNLast(0.0, kNaN), std::weak_ordering::less);
  EXPECT_EQ(CompareDoublesNaNLast(kInfinity, kNaN), std::weak_ordering::less);

  // More than one bit pattern spells a NaN, and this order holds them alike.
  EXPECT_EQ(CompareDoublesNaNLast(kNaN, -kNaN), std::weak_ordering::equivalent);
  EXPECT_EQ(CompareDoublesNaNLast(std::nan(""), kNaN), std::weak_ordering::equivalent);
}

TEST(PlacesADoubleWithANaNLast, LeavesEveryOtherPairWhereTheNumbersAre) {
  EXPECT_EQ(CompareDoublesNaNLast(1.0, 2.0), std::weak_ordering::less);
  EXPECT_EQ(CompareDoublesNaNLast(2.0, 1.0), std::weak_ordering::greater);
  EXPECT_EQ(CompareDoublesNaNLast(1.0, 1.0), std::weak_ordering::equivalent);

  // Zero has two spellings and they name one number.
  EXPECT_EQ(CompareDoublesNaNLast(0.0, -0.0), std::weak_ordering::equivalent);
}

TEST(PlacesAnIntegerAgainstADoubleWithANaNLast, AgreesWithTheOpenPlacementAndPutsANaNLast) {
  EXPECT_EQ(PlaceIntegerAgainstDoubleNaNLast(2, 2.5), std::weak_ordering::less);
  EXPECT_EQ(PlaceIntegerAgainstDoubleNaNLast(3, 2.5), std::weak_ordering::greater);
  EXPECT_EQ(PlaceIntegerAgainstDoubleNaNLast(2, 2.0), std::weak_ordering::equivalent);

  // The one pair the open placement leaves unplaced, and every number is below it.
  EXPECT_EQ(PlaceIntegerAgainstDoubleNaNLast(0, kNaN), std::weak_ordering::less);
  EXPECT_EQ(PlaceIntegerAgainstDoubleNaNLast(std::numeric_limits<int64_t>::max(), kNaN), std::weak_ordering::less);
}

TEST(ReadsAnOrderFromTheOtherSide, SwapsTheTwoEndsAndLeavesTheRest) {
  EXPECT_EQ(ReversedOrder(std::partial_ordering::less), std::partial_ordering::greater);
  EXPECT_EQ(ReversedOrder(std::partial_ordering::greater), std::partial_ordering::less);
  EXPECT_EQ(ReversedOrder(std::partial_ordering::equivalent), std::partial_ordering::equivalent);
  EXPECT_EQ(ReversedOrder(std::partial_ordering::unordered), std::partial_ordering::unordered);

  EXPECT_EQ(ReversedOrder(std::weak_ordering::less), std::weak_ordering::greater);
  EXPECT_EQ(ReversedOrder(std::weak_ordering::greater), std::weak_ordering::less);
  EXPECT_EQ(ReversedOrder(std::weak_ordering::equivalent), std::weak_ordering::equivalent);
}

TEST(ReadsAnOrderFromTheOtherSide, AnswersAPairTheSameWayFromEitherEnd) {
  auto const both_ways = [](int64_t whole, double other) {
    EXPECT_EQ(ReversedOrder(PlaceIntegerAgainstDouble(whole, other)),
              ReversedOrder(ReversedOrder(ReversedOrder(PlaceIntegerAgainstDouble(whole, other)))))
        << "reversing an order three times is reversing it once";
  };
  both_ways(2, 2.5);
  both_ways(kWidestExact + 1, static_cast<double>(kWidestExact));
  both_ways(0, kNaN);
}

TEST(AsksWhetherAnIntegerCanHoldADouble, FencesTheRangeAtThePowerOfTwoEitherSide) {
  EXPECT_TRUE(AnIntegerCanHold(0.0));
  EXPECT_TRUE(AnIntegerCanHold(static_cast<double>(kWidestExact)));

  // The smallest integer is exactly a double, so the range holds it. One past
  // the largest is exactly a double too, and the range stops below it.
  auto const narrowest = static_cast<double>(std::numeric_limits<int64_t>::min());
  EXPECT_TRUE(AnIntegerCanHold(narrowest));
  EXPECT_FALSE(AnIntegerCanHold(-narrowest));
  EXPECT_FALSE(AnIntegerCanHold(std::nextafter(narrowest, -kInfinity)));

  EXPECT_FALSE(AnIntegerCanHold(1e300));
  EXPECT_FALSE(AnIntegerCanHold(kInfinity));
  EXPECT_FALSE(AnIntegerCanHold(-kInfinity));
  EXPECT_FALSE(AnIntegerCanHold(kNaN));
}
