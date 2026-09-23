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
#include <array>
#include <cmath>
#include <limits>
#include <variant>

namespace memgraph::storage {

namespace {

/// The value each stretch begins at, written once in the order the stretches
/// run.
///
/// The constants are defined by the header this file includes, so they are
/// built before this array of their addresses is.
const std::array<PropertyValue const *, static_cast<std::size_t>(Stretch::Count)> kStretchStarts = {
    &kSmallestMap,
    &kSmallestList,
    &kSmallestTemporalData,
    &kSmallestZonedTemporalData,
    &kSmallestEnum,
    &kSmallestPoint2d,
    &kSmallestPoint3d,
    &kSmallestString,
    &kSmallestBool,
    &kSmallestNumber,
    &kSmallestNaN,
    &kSmallestVectorIndexId,
    &kSmallestNull,
};

}  // namespace

auto UpperBoundForType(PropertyValueType type) -> std::optional<utils::Bound<PropertyValue>> {
  auto const next = static_cast<std::size_t>(StretchOf(type)) + 1;
  if (next == static_cast<std::size_t>(Stretch::Count)) return std::nullopt;
  return utils::MakeBoundExclusive(*kStretchStarts[next]);
}

auto LowerBoundForType(PropertyValueType type) -> std::optional<utils::Bound<PropertyValue>> {
  return utils::MakeBoundInclusive(*kStretchStarts[static_cast<std::size_t>(StretchOf(type))]);
}

auto UpperBoundForNonNulls() -> utils::Bound<PropertyValue> { return utils::MakeBoundExclusive(kSmallestNull); }

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
