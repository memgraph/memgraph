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

/// The stretches of the stored order, in the order they run.
///
/// A range fenced to one type runs from that type's own stretch up to the next,
/// so the sequence below is the only statement of where each type sits: both
/// bounds are read off it, and a type cannot move at one end of a range without
/// moving at the other.
enum class Stretch : std::uint8_t {
  /// Nothing begins below this one, so a null has no lower bound.
  Null,
  Bool,
  /// One stretch for both numeric types, which are ordered against each other
  /// as the numbers they are rather than by their types.
  Number,
  /// A stretch nothing is stored in, naming where the numbers stop. Every
  /// comparison against a NaN is false, so a range built from one must not
  /// reach the NaNs, which sort above every other number.
  AboveEveryNumber,
  String,
  List,
  Map,
  Temporal,
  ZonedTemporal,
  Enum,
  Point2d,
  Point3d,
  VectorIndexId,
  /// Nothing begins above the last stretch, so it has no upper bound.
  Count,
};

/// The value each stretch begins at, written once in the order above.
///
/// The constants are defined by the header this file includes, so they are
/// built before this array of their addresses is.
const std::array<PropertyValue const *, static_cast<std::size_t>(Stretch::Count)> kStretchStarts = {
    &kSmallestProperty,
    &kSmallestBool,
    &kSmallestNumber,
    &kSmallestNaN,
    &kSmallestString,
    &kSmallestList,
    &kSmallestMap,
    &kSmallestTemporalData,
    &kSmallestZonedTemporalData,
    &kSmallestEnum,
    &kSmallestPoint2d,
    &kSmallestPoint3d,
    &kSmallestVectorIndexId,
};

/// The stretch a type's values are kept in.
Stretch StretchOf(PropertyValueType type) {
  switch (type) {
    using enum PropertyValueType;
    case Null:
      return Stretch::Null;
    case Bool:
      return Stretch::Bool;
    case Int:
    case Double:
      return Stretch::Number;
    case String:
      return Stretch::String;
    // The representations that pack a list's elements hold the same value a
    // boxed list holds, so they are kept where a list is kept.
    case List:
    case NumericList:
    case IntList:
    case DoubleList:
      return Stretch::List;
    case Map:
      return Stretch::Map;
    case TemporalData:
      return Stretch::Temporal;
    case ZonedTemporalData:
      return Stretch::ZonedTemporal;
    case Enum:
      return Stretch::Enum;
    case Point2d:
      return Stretch::Point2d;
    case Point3d:
      return Stretch::Point3d;
    case VectorIndexId:
      return Stretch::VectorIndexId;
  }
  return Stretch::Null;
}

}  // namespace

auto UpperBoundForType(PropertyValueType type) -> std::optional<utils::Bound<PropertyValue>> {
  auto const next = static_cast<std::size_t>(StretchOf(type)) + 1;
  if (next == static_cast<std::size_t>(Stretch::Count)) return std::nullopt;
  return utils::MakeBoundExclusive(*kStretchStarts[next]);
}

auto LowerBoundForType(PropertyValueType type) -> std::optional<utils::Bound<PropertyValue>> {
  auto const stretch = StretchOf(type);
  if (stretch == Stretch::Null) return std::nullopt;
  return utils::MakeBoundInclusive(*kStretchStarts[static_cast<std::size_t>(stretch)]);
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
