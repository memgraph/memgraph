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
#pragma once

#include "storage/v2/property_value.hpp"

namespace memgraph::storage {
// These constants represent the smallest possible value of each type that is
// contained in a `PropertyValue`. Note that numbers (integers and doubles) are
// treated as the same "type" in `PropertyValue`.
static const auto kSmallestNull = PropertyValue();
static const auto kSmallestBool = PropertyValue(false);
// NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
static_assert(-std::numeric_limits<double>::infinity() < std::numeric_limits<int64_t>::min());
static const auto kSmallestNumber = PropertyValue(-std::numeric_limits<double>::infinity());
// A NaN sorts above every other number and alongside every other NaN, so one of
// them names the point a range over the numbers has to stop at: every
// comparison against a NaN is false, and a range built from a comparison must
// not reach one.
static const auto kSmallestNaN = PropertyValue(std::numeric_limits<double>::quiet_NaN());
static const auto kSmallestString = PropertyValue("");
static const auto kSmallestList = PropertyValue(std::vector<PropertyValue>());
static const auto kSmallestMap = PropertyValue(PropertyValue::map_t{});
static const auto kSmallestTemporalData =
    PropertyValue(TemporalData{static_cast<TemporalType>(0), std::numeric_limits<int64_t>::min()});
static const auto kSmallestZonedTemporalData =
    PropertyValue(ZonedTemporalData{static_cast<ZonedTemporalType>(0),
                                    utils::AsSysTime(std::numeric_limits<int64_t>::min()),
                                    utils::Timezone(std::chrono::minutes{-utils::MAX_OFFSET_MINUTES})});
static const auto kSmallestEnum = PropertyValue(Enum{EnumTypeId{0}, EnumValueId{0}});
static const auto kSmallestPoint2d = PropertyValue(Point2d{CoordinateReferenceSystem::WGS84_2d, -180, -90});
static const auto kSmallestPoint3d =
    PropertyValue(Point3d{CoordinateReferenceSystem::WGS84_3d, -180, -90, -std::numeric_limits<double>::infinity()});
static const auto kSmallestVectorIndexId = PropertyValue(
    PropertyValue::VectorIndexIdData{.ids = utils::small_vector<uint64_t>{}, .vector = utils::small_vector<float>{}});
/// A value nothing sorts below, used to start a scan that has no lower bound of
/// its own.
///
/// It takes the type the order begins with, and the least value of it: a map is
/// ordered by what it holds, and the empty one holds nothing.
static const auto kSmallestProperty = kSmallestMap;
/// A value no stored value sorts above, used to fence a scan that has no upper
/// bound of its own.
///
/// It takes the type the order ends with. Every null shares one place, so
/// nothing sorts above this and a scan fenced by it inclusively reaches
/// everything.
static const auto kLargestProperty = kSmallestNull;

// Each type below is placed before the next, so the stretch a type sits in is
// checked here rather than trusted. The three that pack a list's elements are
// absent on purpose: each holds what a boxed list holds and shares its stretch,
// which a test asserts instead.
static_assert(StretchOf(PropertyValue::Type::Map) < StretchOf(PropertyValue::Type::List));
static_assert(StretchOf(PropertyValue::Type::List) < StretchOf(PropertyValue::Type::TemporalData));
static_assert(StretchOf(PropertyValue::Type::TemporalData) < StretchOf(PropertyValue::Type::ZonedTemporalData));
static_assert(StretchOf(PropertyValue::Type::ZonedTemporalData) < StretchOf(PropertyValue::Type::Enum));
static_assert(StretchOf(PropertyValue::Type::Enum) < StretchOf(PropertyValue::Type::Point2d));
static_assert(StretchOf(PropertyValue::Type::Point2d) < StretchOf(PropertyValue::Type::Point3d));
static_assert(StretchOf(PropertyValue::Type::Point3d) < StretchOf(PropertyValue::Type::String));
static_assert(StretchOf(PropertyValue::Type::String) < StretchOf(PropertyValue::Type::Bool));
static_assert(StretchOf(PropertyValue::Type::Bool) < StretchOf(PropertyValue::Type::Int));
static_assert(StretchOf(PropertyValue::Type::Int) == StretchOf(PropertyValue::Type::Double));
static_assert(StretchOf(PropertyValue::Type::Double) < StretchOf(PropertyValue::Type::VectorIndexId));
static_assert(StretchOf(PropertyValue::Type::VectorIndexId) < StretchOf(PropertyValue::Type::Null));
}  // namespace memgraph::storage
