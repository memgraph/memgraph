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

// How far a range may read an index: the stretch of the stored order a comparison against one
// value can answer over, asked by name rather than through a scan.

#include <gtest/gtest.h>

#include <array>
#include <limits>
#include <vector>

#include "storage/v2/property_constants.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/property_value_utils.hpp"
#include "storage/v2/temporal.hpp"

using namespace memgraph::storage;

namespace {

constexpr TemporalType kEveryTemporalKind[] = {
    TemporalType::Date, TemporalType::LocalTime, TemporalType::LocalDateTime, TemporalType::Duration};

PropertyValue Temporal(TemporalType kind, int64_t microseconds) {
  return PropertyValue(TemporalData{kind, microseconds});
}

/// Whether the stretch around `bound_value` holds `value`, which is what a scan fenced to that
/// stretch hands back.
bool WithinStretchOf(PropertyValue const &value, PropertyValue const &bound_value) {
  auto const lower = LowerBoundComparableWith(bound_value);
  auto const upper = UpperBoundComparableWith(bound_value);
  return IsValueIncludedByLowerBound(value, lower) && IsValueIncludedByUpperBound(value, upper);
}

}  // namespace

TEST(ComparableBounds, HoldsEveryValueOfTheSameTemporalKind) {
  for (auto const kind : kEveryTemporalKind) {
    auto const bound = Temporal(kind, 0);
    EXPECT_TRUE(WithinStretchOf(Temporal(kind, std::numeric_limits<int64_t>::min()), bound));
    EXPECT_TRUE(WithinStretchOf(Temporal(kind, -1), bound));
    EXPECT_TRUE(WithinStretchOf(Temporal(kind, 0), bound));
    EXPECT_TRUE(WithinStretchOf(Temporal(kind, std::numeric_limits<int64_t>::max()), bound));
  }
}

TEST(ComparableBounds, HoldsNoValueOfAnotherTemporalKind) {
  // The four share one stored type, so nothing but the stretch keeps them apart.
  for (auto const bound_kind : kEveryTemporalKind) {
    for (auto const value_kind : kEveryTemporalKind) {
      if (value_kind == bound_kind) continue;
      EXPECT_FALSE(WithinStretchOf(Temporal(value_kind, 0), Temporal(bound_kind, 0)))
          << "bound kind " << static_cast<unsigned>(bound_kind) << ", value kind " << static_cast<unsigned>(value_kind);
    }
  }
}

TEST(ComparableBounds, HoldsNoValueOfAnotherType) {
  auto const date = Temporal(TemporalType::Date, 0);
  EXPECT_FALSE(WithinStretchOf(PropertyValue(int64_t{0}), date));
  EXPECT_FALSE(WithinStretchOf(PropertyValue("a"), date));
  EXPECT_FALSE(WithinStretchOf(kSmallestZonedTemporalData, date));
  EXPECT_FALSE(WithinStretchOf(kSmallestEnum, date));

  // The last of the four ends where the stored type does, so what follows it is still outside.
  auto const duration = Temporal(TemporalType::Duration, 0);
  EXPECT_FALSE(WithinStretchOf(kSmallestZonedTemporalData, duration));
  EXPECT_FALSE(WithinStretchOf(kSmallestEnum, duration));
}

TEST(ComparableBounds, LeavesEveryOtherTypeOneStretch) {
  // A type that keeps one stretch answers exactly as the type-level question does, so nothing
  // but the temporal kinds is narrowed.
  for (auto const &value : {PropertyValue(true),
                            PropertyValue(int64_t{7}),
                            PropertyValue(2.5),
                            PropertyValue("a"),
                            kSmallestZonedTemporalData,
                            kSmallestEnum,
                            kSmallestPoint2d,
                            kSmallestPoint3d}) {
    EXPECT_EQ(LowerBoundComparableWith(value), LowerBoundForType(value.type()));
    EXPECT_EQ(UpperBoundComparableWith(value), UpperBoundForType(value.type()));
  }
}

TEST(TypeBands, FenceAScanAboveEveryValueItCouldHold) {
  // A scan with no upper bound of its own is fenced at kLargestProperty, so a
  // value sorting above it is handed back by no such scan. A NaN is placed after
  // every number, which a point's coordinates are, so a point holding one is the
  // value most likely to escape the fence.
  auto const nan = std::numeric_limits<double>::quiet_NaN();

  for (auto const &value : {kSmallestProperty,
                            kSmallestBool,
                            kSmallestNumber,
                            kSmallestString,
                            kSmallestList,
                            kSmallestMap,
                            kSmallestTemporalData,
                            kSmallestZonedTemporalData,
                            kSmallestEnum,
                            kSmallestPoint2d,
                            kSmallestPoint3d,
                            PropertyValue(std::numeric_limits<double>::infinity()),
                            PropertyValue(nan),
                            PropertyValue(Point2d{CoordinateReferenceSystem::Cartesian_2d, nan, nan}),
                            PropertyValue(Point3d{CoordinateReferenceSystem::Cartesian_3d, nan, nan, nan}),
                            PropertyValue(Point3d{CoordinateReferenceSystem::Cartesian_3d, 1.0, nan, 2.0})}) {
    EXPECT_FALSE(kLargestProperty < value) << "a value sorts above the fence a bounded scan stops at";
  }

  // And the fence is above rather than equal to an ordinary value, so it does
  // not exclude one.
  EXPECT_TRUE(kSmallestProperty < kLargestProperty);
  EXPECT_TRUE(kSmallestPoint3d < kLargestProperty);
}

TEST(ComparableBounds, HoldsNoNaNInTheStretchAroundANumber) {
  // A NaN is placed above every number so a sorted container can hold one, and
  // that puts it inside the stretch a comparison against a number would read.
  // Every comparison against a NaN is false, so a scan fenced to that stretch
  // has to stop below it.
  auto const nan = PropertyValue(std::numeric_limits<double>::quiet_NaN());

  EXPECT_FALSE(WithinStretchOf(nan, PropertyValue(0.0)));
  EXPECT_FALSE(WithinStretchOf(nan, PropertyValue(int64_t{7})));
  EXPECT_FALSE(WithinStretchOf(nan, kSmallestNumber));

  // Every number the stretch did hold, it still holds.
  for (auto const &value : {PropertyValue(0.0),
                            PropertyValue(int64_t{7}),
                            kSmallestNumber,
                            PropertyValue(std::numeric_limits<double>::infinity()),
                            PropertyValue(std::numeric_limits<int64_t>::max())}) {
    EXPECT_TRUE(WithinStretchOf(value, PropertyValue(0.0)));
  }
}

/// Every stored type, with the smallest value each one holds.
///
/// Listed rather than derived so that a type added to the value has to be given
/// a place here before these properties can be checked of it.
struct TypeAndSmallest {
  PropertyValueType type;
  PropertyValue smallest;
};

std::vector<TypeAndSmallest> EveryTypeAndItsSmallest() {
  return {
      {PropertyValueType::Bool, kSmallestBool},
      {PropertyValueType::Int, kSmallestNumber},
      {PropertyValueType::Double, kSmallestNumber},
      {PropertyValueType::String, kSmallestString},
      {PropertyValueType::List, kSmallestList},
      {PropertyValueType::Map, kSmallestMap},
      {PropertyValueType::TemporalData, kSmallestTemporalData},
      {PropertyValueType::ZonedTemporalData, kSmallestZonedTemporalData},
      {PropertyValueType::Enum, kSmallestEnum},
      {PropertyValueType::Point2d, kSmallestPoint2d},
      {PropertyValueType::Point3d, kSmallestPoint3d},
  };
}

TEST(TypeBands, HoldTheSmallestValueOfTheirOwnType) {
  // The band a range is fenced to has to hold every value of that type. The
  // smallest is the one a bound is most likely to fall the wrong side of.
  for (auto const &[type, smallest] : EveryTypeAndItsSmallest()) {
    EXPECT_TRUE(IsValueIncludedByLowerBound(smallest, LowerBoundForType(type)))
        << "type " << static_cast<unsigned>(type) << " excludes its own smallest value";
    EXPECT_TRUE(IsValueIncludedByUpperBound(smallest, UpperBoundForType(type)))
        << "type " << static_cast<unsigned>(type);
  }
}

TEST(TypeBands, RunOneAfterAnotherWithNoGapAndNoOverlap) {
  // Each band ends where the next begins, so every value falls in exactly one.
  // Written as a property rather than a table because the table is the thing
  // under test.
  auto const every = EveryTypeAndItsSmallest();
  for (auto const &[type, smallest] : every) {
    for (auto const &[other_type, other_smallest] : every) {
      if (type == other_type) continue;
      // Int and Double share one band, being ordered against each other.
      auto const numbers = std::array{PropertyValueType::Int, PropertyValueType::Double};
      if (std::ranges::contains(numbers, type) && std::ranges::contains(numbers, other_type)) continue;

      auto const within = IsValueIncludedByLowerBound(other_smallest, LowerBoundForType(type)) &&
                          IsValueIncludedByUpperBound(other_smallest, UpperBoundForType(type));
      EXPECT_FALSE(within) << "the band for type " << static_cast<unsigned>(type) << " holds the smallest value of "
                           << static_cast<unsigned>(other_type);
    }
  }
}

TEST(HoldsANaN, FindsANaNWhereverAValueCarriesOne) {
  auto const nan = std::numeric_limits<double>::quiet_NaN();

  EXPECT_TRUE(HoldsANaN(PropertyValue(nan)));
  EXPECT_TRUE(HoldsANaN(PropertyValue(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(nan)})));
  EXPECT_TRUE(HoldsANaN(PropertyValue{DoubleListTag{}, std::vector<PropertyValue>{PropertyValue(nan)}}));
  EXPECT_TRUE(HoldsANaN(
      PropertyValue{NumericListTag{}, std::vector<PropertyValue>{PropertyValue(int64_t{1}), PropertyValue(nan)}}));
  EXPECT_TRUE(HoldsANaN(PropertyValue(Point2d{CoordinateReferenceSystem::WGS84_2d, 1.0, nan})));
  EXPECT_TRUE(HoldsANaN(PropertyValue(Point3d{CoordinateReferenceSystem::WGS84_3d, 1.0, 2.0, nan})));
  // Nesting is walked rather than only the outermost value.
  EXPECT_TRUE(HoldsANaN(
      PropertyValue(std::vector<PropertyValue>{PropertyValue(std::vector<PropertyValue>{PropertyValue(nan)})})));

  EXPECT_FALSE(HoldsANaN(PropertyValue()));
  EXPECT_FALSE(HoldsANaN(PropertyValue(1.0)));
  EXPECT_FALSE(HoldsANaN(PropertyValue(std::numeric_limits<double>::infinity())));
  EXPECT_FALSE(HoldsANaN(PropertyValue(int64_t{7})));
  EXPECT_FALSE(HoldsANaN(PropertyValue("a")));
  EXPECT_FALSE(HoldsANaN(PropertyValue(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(2.0)})));
  EXPECT_FALSE(HoldsANaN(PropertyValue(Point2d{CoordinateReferenceSystem::WGS84_2d, 1.0, 2.0})));
  EXPECT_FALSE(HoldsANaN(kSmallestEnum));
}

TEST(HoldsANull, FindsANullWhereverAValueCarriesOne) {
  EXPECT_TRUE(HoldsANull(PropertyValue()));
  EXPECT_TRUE(HoldsANull(PropertyValue(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue()})));
  EXPECT_TRUE(HoldsANull(PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue()}})));
  // Nesting is walked rather than only the outermost value.
  EXPECT_TRUE(HoldsANull(
      PropertyValue(std::vector<PropertyValue>{PropertyValue(std::vector<PropertyValue>{PropertyValue()})})));

  EXPECT_FALSE(HoldsANull(PropertyValue(1.0)));
  EXPECT_FALSE(HoldsANull(PropertyValue(int64_t{7})));
  EXPECT_FALSE(HoldsANull(PropertyValue("a")));
  EXPECT_FALSE(HoldsANull(PropertyValue(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(2.0)})));
  EXPECT_FALSE(HoldsANull(kSmallestEnum));
}

TEST(EqualsItself, HoldsOfEveryValueButTheTwoEqualityCannotDecide) {
  auto const nan = std::numeric_limits<double>::quiet_NaN();

  EXPECT_FALSE(EqualsItself(PropertyValue()));
  EXPECT_FALSE(EqualsItself(PropertyValue(nan)));
  EXPECT_FALSE(EqualsItself(PropertyValue(std::vector<PropertyValue>{PropertyValue(1), PropertyValue()})));
  EXPECT_FALSE(EqualsItself(PropertyValue(std::vector<PropertyValue>{PropertyValue(1.0), PropertyValue(nan)})));

  EXPECT_TRUE(EqualsItself(PropertyValue(1.0)));
  EXPECT_TRUE(EqualsItself(PropertyValue(std::numeric_limits<double>::infinity())));
  EXPECT_TRUE(EqualsItself(PropertyValue("a")));
  EXPECT_TRUE(EqualsItself(PropertyValue(std::vector<PropertyValue>{PropertyValue(1), PropertyValue(2)})));

  // The vector helper reads the whole array, at any position.
  EXPECT_FALSE(EveryValueEqualsItself({PropertyValue(1.0), PropertyValue(nan)}));
  EXPECT_FALSE(EveryValueEqualsItself({PropertyValue(nan), PropertyValue(1.0)}));
  EXPECT_FALSE(EveryValueEqualsItself({PropertyValue(1.0), PropertyValue()}));
  EXPECT_TRUE(EveryValueEqualsItself({PropertyValue(1.0), PropertyValue(2.0)}));
  EXPECT_TRUE(EveryValueEqualsItself({}));
}

TEST(ComparableBounds, AreComparableTellsTheTemporalKindsApart) {
  for (auto const a : kEveryTemporalKind) {
    for (auto const b : kEveryTemporalKind) {
      EXPECT_EQ(AreComparable(Temporal(a, 0), Temporal(b, 1)), a == b);
    }
  }

  EXPECT_TRUE(AreComparable(PropertyValue(int64_t{1}), PropertyValue(1.5)));
  EXPECT_TRUE(AreComparable(PropertyValue("a"), PropertyValue("b")));
  EXPECT_FALSE(AreComparable(PropertyValue(int64_t{1}), PropertyValue("a")));
  EXPECT_FALSE(AreComparable(Temporal(TemporalType::Date, 0), kSmallestZonedTemporalData));
}
