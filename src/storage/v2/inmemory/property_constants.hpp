// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/property_value.hpp"

namespace memgraph::storage {
// These constants represent the smallest possible value of each type that is
// contained in a `PropertyValue`. Note that numbers (integers and doubles) are
// treated as the same "type" in `PropertyValue`.
const PropertyValue kSmallestBool = PropertyValue(false);
// NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
static_assert(-std::numeric_limits<double>::infinity() < std::numeric_limits<int64_t>::min());
const PropertyValue kSmallestNumber = PropertyValue(-std::numeric_limits<double>::infinity());
const PropertyValue kSmallestString = PropertyValue("");
const PropertyValue kSmallestList = PropertyValue(std::vector<PropertyValue>());
const PropertyValue kSmallestMap = PropertyValue(PropertyValue::map_t{});
const PropertyValue kSmallestTemporalData =
    PropertyValue(TemporalData{static_cast<TemporalType>(0), std::numeric_limits<int64_t>::min()});
const PropertyValue kSmallestZonedTemporalData = PropertyValue(
    ZonedTemporalData{static_cast<ZonedTemporalType>(0), utils::AsSysTime(std::numeric_limits<int64_t>::min()),
                      utils::Timezone(std::chrono::minutes{-utils::MAX_OFFSET_MINUTES})});
const PropertyValue kSmallestEnum = PropertyValue(Enum{EnumTypeId{0}, EnumValueId{0}});
const PropertyValue kSmallestPoint2d = PropertyValue(Point2d{CoordinateReferenceSystem::WGS84_2d, -180, -90});
const PropertyValue kSmallestPoint3d =
    PropertyValue(Point3d{CoordinateReferenceSystem::WGS84_3d, -180, -90, -std::numeric_limits<double>::infinity()});

// We statically verify that the ordering of the property values holds.
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
}  // namespace memgraph::storage
