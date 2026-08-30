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

#include <array>
#include <limits>

namespace memgraph::storage {

namespace {

/// The places a stored value can be kept in, in the order the places run. A
/// range fenced to one type runs from that type's own place up to the next.
enum class Segment : std::size_t {
  Map,
  List,
  ZonedTemporal,
  Temporal,
  Enum,
  Point2d,
  Point3d,
  String,
  Bool,
  Number,
  /// The numbers end at a place nothing begins at. A range built from a
  /// comparison has to stop below the NaNs, which sort above every other number
  /// and which no comparison places at all.
  AboveEveryNumber,
};

/// The smallest value each place holds, written once in the order above. Both
/// bounds are read from here, so a place cannot move at one end of a range
/// without moving at the other.
auto const &SegmentStarts() {
  static auto const starts = std::array<PropertyValue, static_cast<std::size_t>(Segment::AboveEveryNumber) + 1>{
      kSmallestMap,
      kSmallestList,
      kSmallestZonedTemporalData,
      kSmallestTemporalData,
      kSmallestEnum,
      kSmallestPoint2d,
      kSmallestPoint3d,
      kSmallestString,
      kSmallestBool,
      kSmallestNumber,
      kSmallestNaN,
  };
  return starts;
}

/// The place a type's values begin at, or nothing for the last place of all.
std::optional<Segment> SegmentOf(PropertyValueType type) {
  switch (type) {
    using enum PropertyValueType;
    case Map:
      return Segment::Map;
    // A vector is handed to a query as the list of its coordinates, so it is
    // kept where a list is kept.
    case List:
    case NumericList:
    case IntList:
    case DoubleList:
    case VectorIndexId:
      return Segment::List;
    case ZonedTemporalData:
      return Segment::ZonedTemporal;
    case TemporalData:
      return Segment::Temporal;
    case Enum:
      return Segment::Enum;
    case Point2d:
      return Segment::Point2d;
    case Point3d:
      return Segment::Point3d;
    case String:
      return Segment::String;
    case Bool:
      return Segment::Bool;
    // Both are kept in one place and interleaved when sorted, since they are
    // ordered against each other as the numbers they are.
    case Int:
    case Double:
      return Segment::Number;
    // A null sorts above every value, so its place is the last one and nothing
    // begins after it.
    case Null:
      return std::nullopt;
  }
  return std::nullopt;
}

}  // namespace

auto UpperBoundForType(PropertyValueType type) -> std::optional<utils::Bound<PropertyValue>> {
  auto const segment = SegmentOf(type);
  if (!segment) return std::nullopt;
  return utils::MakeBoundExclusive(SegmentStarts()[static_cast<std::size_t>(*segment) + 1]);
}

auto LowerBoundForType(PropertyValueType type) -> std::optional<utils::Bound<PropertyValue>> {
  auto const segment = SegmentOf(type);
  if (!segment) return std::nullopt;
  return utils::MakeBoundInclusive(SegmentStarts()[static_cast<std::size_t>(*segment)]);
}

auto LowerBoundForTemporalType(TemporalType type) -> utils::Bound<PropertyValue> {
  return utils::MakeBoundInclusive(PropertyValue(TemporalData{type, std::numeric_limits<int64_t>::min()}));
}

auto UpperBoundForTemporalType(TemporalType type) -> utils::Bound<PropertyValue> {
  return utils::MakeBoundInclusive(PropertyValue(TemporalData{type, std::numeric_limits<int64_t>::max()}));
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
