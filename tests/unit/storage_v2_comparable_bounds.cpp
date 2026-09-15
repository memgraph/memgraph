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

#include <limits>

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
