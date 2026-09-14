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

#include "property_value_utils.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <variant>

namespace memgraph::storage {

auto UpperBoundForType(PropertyValueType type) -> std::optional<utils::Bound<PropertyValue>> {
  switch (type) {
    case PropertyValue::Type::Null:
      return utils::MakeBoundExclusive(kSmallestBool);
    case PropertyValue::Type::Bool:
      return utils::MakeBoundExclusive(kSmallestNumber);
    case PropertyValue::Type::Int:
    case PropertyValue::Type::Double:
      // Both integers and doubles are treated as the same type in
      // `PropertyValue` and they are interleaved when sorted. The stretch stops
      // below the NaNs, which sort above every number and answer false to every
      // comparison a range could be built from.
      return utils::MakeBoundExclusive(kSmallestNaN);
    case PropertyValue::Type::String:
      return utils::MakeBoundExclusive(kSmallestList);
    case PropertyValue::Type::List:
    case PropertyValue::Type::NumericList:
    case PropertyValue::Type::IntList:
    case PropertyValue::Type::DoubleList:
      return utils::MakeBoundExclusive(kSmallestMap);
    case PropertyValue::Type::Map:
      return utils::MakeBoundExclusive(kSmallestTemporalData);
    case PropertyValue::Type::TemporalData:
      return utils::MakeBoundExclusive(kSmallestZonedTemporalData);
    case PropertyValue::Type::ZonedTemporalData:
      return utils::MakeBoundExclusive(kSmallestEnum);
    case PropertyValue::Type::Enum:
      return utils::MakeBoundExclusive(kSmallestPoint2d);
    case PropertyValue::Type::Point2d:
      return utils::MakeBoundExclusive(kSmallestPoint3d);
    case PropertyValue::Type::Point3d:
      return utils::MakeBoundExclusive(kSmallestVectorIndexId);
    case PropertyValue::Type::VectorIndexId:
      // This is the last type in the order so we leave the upper bound empty.
      return std::nullopt;
  }
}

auto LowerBoundForType(PropertyValueType type) -> std::optional<utils::Bound<PropertyValue>> {
  switch (type) {
    case PropertyValue::Type::Null:
      return std::nullopt;
    case PropertyValue::Type::Bool:
      return utils::MakeBoundInclusive(kSmallestBool);
    case PropertyValue::Type::Int:
    case PropertyValue::Type::Double:
      // Both integers and doubles are treated as the same type in
      // `PropertyValue` and they are interleaved when sorted.
      return utils::MakeBoundInclusive(kSmallestNumber);
    case PropertyValue::Type::String:
      return utils::MakeBoundInclusive(kSmallestString);
    case PropertyValue::Type::List:
    case PropertyValue::Type::NumericList:
    case PropertyValue::Type::IntList:
    case PropertyValue::Type::DoubleList:
      return utils::MakeBoundInclusive(kSmallestList);
    case PropertyValue::Type::Map:
      return utils::MakeBoundInclusive(kSmallestMap);
    case PropertyValue::Type::TemporalData:
      return utils::MakeBoundInclusive(kSmallestTemporalData);
    case PropertyValue::Type::ZonedTemporalData:
      return utils::MakeBoundInclusive(kSmallestZonedTemporalData);
    case PropertyValue::Type::Enum:
      return utils::MakeBoundInclusive(kSmallestEnum);
    case PropertyValue::Type::Point2d:
      return utils::MakeBoundExclusive(kSmallestPoint2d);
    case PropertyValue::Type::Point3d:
      return utils::MakeBoundExclusive(kSmallestPoint3d);
    case PropertyValue::Type::VectorIndexId:
      return utils::MakeBoundInclusive(kSmallestVectorIndexId);
  }
}

namespace {

auto SmallestOfKind(TemporalType kind) -> PropertyValue {
  return PropertyValue(TemporalData{kind, std::numeric_limits<int64_t>::min()});
}

}  // namespace

auto LowerBoundComparableWith(PropertyValue const &value) -> std::optional<utils::Bound<PropertyValue>> {
  if (value.type() != PropertyValueType::TemporalData) return LowerBoundForType(value.type());
  return utils::MakeBoundInclusive(SmallestOfKind(value.ValueTemporalData().type));
}

auto UpperBoundComparableWith(PropertyValue const &value) -> std::optional<utils::Bound<PropertyValue>> {
  if (value.type() != PropertyValueType::TemporalData) return UpperBoundForType(value.type());
  switch (value.ValueTemporalData().type) {
    case TemporalType::Date:
      return utils::MakeBoundExclusive(SmallestOfKind(TemporalType::LocalTime));
    case TemporalType::LocalTime:
      return utils::MakeBoundExclusive(SmallestOfKind(TemporalType::LocalDateTime));
    case TemporalType::LocalDateTime:
      return utils::MakeBoundExclusive(SmallestOfKind(TemporalType::Duration));
    case TemporalType::Duration:
      // The last of the four, so the stretch ends where the stored type does.
      return UpperBoundForType(PropertyValueType::TemporalData);
  }
}

bool HoldsANaN(PropertyValue const &value) {
  switch (value.type()) {
    using enum PropertyValueType;
    case Double:
      return std::isnan(value.ValueDouble());
    case DoubleList:
      return std::ranges::any_of(value.ValueDoubleList(), [](double d) { return std::isnan(d); });
    case NumericList:
      return std::ranges::any_of(value.ValueNumericList(), [](auto const &held) {
        return std::holds_alternative<double>(held) && std::isnan(std::get<double>(held));
      });
    case List:
      return std::ranges::any_of(value.ValueList(), [](auto const &element) { return HoldsANaN(element); });
    case Map:
      return std::ranges::any_of(value.ValueMap(), [](auto const &entry) { return HoldsANaN(entry.second); });
    case Point2d: {
      auto const &point = value.ValuePoint2d();
      return std::isnan(point.x()) || std::isnan(point.y());
    }
    case Point3d: {
      auto const &point = value.ValuePoint3d();
      return std::isnan(point.x()) || std::isnan(point.y()) || std::isnan(point.z());
    }
    // A vector holds its coordinates as floats, which carry a NaN of their own.
    case VectorIndexId:
      return std::ranges::any_of(value.ValueVectorIndexList(), [](float f) { return std::isnan(f); });
    // No other type has a number in it to be one.
    case Null:
    case Bool:
    case Int:
    case IntList:
    case String:
    case TemporalData:
    case ZonedTemporalData:
    case Enum:
      return false;
  }
  return false;
}

bool HoldsANull(PropertyValue const &value) {
  switch (value.type()) {
    using enum PropertyValueType;
    case Null:
      return true;
    case List:
      return std::ranges::any_of(value.ValueList(), [](auto const &element) { return HoldsANull(element); });
    case Map:
      return std::ranges::any_of(value.ValueMap(), [](auto const &entry) { return HoldsANull(entry.second); });
    // Every remaining type is a value of its own, and the packed lists hold
    // numbers by construction.
    case Bool:
    case Int:
    case Double:
    case IntList:
    case DoubleList:
    case NumericList:
    case String:
    case TemporalData:
    case ZonedTemporalData:
    case Enum:
    case Point2d:
    case Point3d:
    case VectorIndexId:
      return false;
  }
  return false;
}

auto PrefixSuccessor(std::string_view prefix) -> std::optional<std::string> {
  auto result = std::string{prefix};
  while (!result.empty() && static_cast<unsigned char>(result.back()) == 0xFF) {
    result.pop_back();
  }
  if (result.empty()) return std::nullopt;
  result.back() = static_cast<char>(static_cast<unsigned char>(result.back()) + 1);
  return result;
}

}  // namespace memgraph::storage
