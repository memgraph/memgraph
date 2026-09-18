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
static const auto kSmallestProperty = PropertyValue();
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
/// A value no stored value sorts above, used to fence a scan that has no upper
/// bound of its own.
///
/// The coordinates are NaNs rather than infinities because a NaN is placed after
/// every number, so a point holding one sorts above a point holding an infinity
/// and would fall outside a fence built from the latter.
static const auto kLargestProperty = PropertyValue(Point3d{CoordinateReferenceSystem::Cartesian_3d,
                                                           std::numeric_limits<double>::quiet_NaN(),
                                                           std::numeric_limits<double>::quiet_NaN(),
                                                           std::numeric_limits<double>::quiet_NaN()});

// We statically verify that the ordering of the property values holds.
static_assert(PropertyValue::Type::Null < PropertyValue::Type::Bool);
static_assert(PropertyValue::Type::Bool < PropertyValue::Type::Int);
static_assert(PropertyValue::Type::Int < PropertyValue::Type::Double);
static_assert(PropertyValue::Type::Double < PropertyValue::Type::String);
static_assert(PropertyValue::Type::String < PropertyValue::Type::List);
static_assert(PropertyValue::Type::List < PropertyValue::Type::Map);
static_assert(PropertyValue::Type::Map < PropertyValue::Type::TemporalData);
static_assert(PropertyValue::Type::TemporalData < PropertyValue::Type::ZonedTemporalData);
static_assert(PropertyValue::Type::ZonedTemporalData < PropertyValue::Type::Enum);
static_assert(PropertyValue::Type::Enum < PropertyValue::Type::Point2d);
static_assert(PropertyValue::Type::Point2d < PropertyValue::Type::Point3d);
static_assert(PropertyValue::Type::Point3d < PropertyValue::Type::VectorIndexId);
}  // namespace memgraph::storage
