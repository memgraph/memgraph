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

/// @file
/// What a property value's type is sent as, over the replication stream.
///
/// The stream carries a tag of its own rather than the number the type happens
/// to hold in memory. Those are two different things that were one: a tag is
/// part of a format two versions of the database have to agree on, and the
/// number a type holds in memory is nobody's business but the process holding
/// it. While they were one, renumbering a type compiled, passed every suite,
/// and parted a main from a replica built from another version.
///
/// Durability keeps the same separation, with markers of its own.
#pragma once

#include <cstdint>

#include "storage/v2/property_value.hpp"

namespace memgraph::wire_format {

/// The tag a type is sent as.
///
/// THESE NUMBERS MAY NOT CHANGE. They are the format itself: a database of one
/// version reads them from a database of another, so a number that moves parts
/// a main from its replica, and no test inside one version can see it happen.
/// Adding one is a change an older reader cannot be given after the fact, and
/// so needs a version to go with it.
///
/// The number a type holds in memory is a separate thing and may change
/// freely; that is what these exist to allow.
enum class PropertyTag : uint8_t {
  Null = 0,
  Bool = 1,
  Int = 2,
  Double = 3,
  String = 4,
  List = 5,
  Map = 6,
  TemporalData = 7,
  ZonedTemporalData = 8,
  Enum = 9,
  Point2d = 10,
  Point3d = 11,
  IntList = 12,
  DoubleList = 13,
  NumericList = 14,
  VectorIndexId = 15,
};

/// The tag a value of this type is sent as.
///
/// The switch has no default, so a type added to the value has to be given a
/// tag before this compiles.
constexpr PropertyTag TagOf(storage::PropertyValueType type) {
  switch (type) {
    using enum storage::PropertyValueType;
    case Null:
      return PropertyTag::Null;
    case Bool:
      return PropertyTag::Bool;
    case Int:
      return PropertyTag::Int;
    case Double:
      return PropertyTag::Double;
    case String:
      return PropertyTag::String;
    case List:
      return PropertyTag::List;
    case Map:
      return PropertyTag::Map;
    case TemporalData:
      return PropertyTag::TemporalData;
    case ZonedTemporalData:
      return PropertyTag::ZonedTemporalData;
    case Enum:
      return PropertyTag::Enum;
    case Point2d:
      return PropertyTag::Point2d;
    case Point3d:
      return PropertyTag::Point3d;
    case IntList:
      return PropertyTag::IntList;
    case DoubleList:
      return PropertyTag::DoubleList;
    case NumericList:
      return PropertyTag::NumericList;
    case VectorIndexId:
      return PropertyTag::VectorIndexId;
  }
  return PropertyTag::Null;
}

/// The type a tag names.
///
/// The switch has no default, so a tag added to the format has to be read as
/// some type before this compiles.
constexpr storage::PropertyValueType TypeOf(PropertyTag tag) {
  switch (tag) {
    using enum storage::PropertyValueType;
    case PropertyTag::Null:
      return Null;
    case PropertyTag::Bool:
      return Bool;
    case PropertyTag::Int:
      return Int;
    case PropertyTag::Double:
      return Double;
    case PropertyTag::String:
      return String;
    case PropertyTag::List:
      return List;
    case PropertyTag::Map:
      return Map;
    case PropertyTag::TemporalData:
      return TemporalData;
    case PropertyTag::ZonedTemporalData:
      return ZonedTemporalData;
    case PropertyTag::Enum:
      return Enum;
    case PropertyTag::Point2d:
      return Point2d;
    case PropertyTag::Point3d:
      return Point3d;
    case PropertyTag::IntList:
      return IntList;
    case PropertyTag::DoubleList:
      return DoubleList;
    case PropertyTag::NumericList:
      return NumericList;
    case PropertyTag::VectorIndexId:
      return VectorIndexId;
  }
  return storage::PropertyValueType::Null;
}

/// Whether the number read off the stream names a tag this version knows.
///
/// Read before the number is taken as a tag at all, since a stream written by
/// another version may carry one this one has never heard of.
constexpr bool IsATag(uint8_t number) { return number <= static_cast<uint8_t>(PropertyTag::VectorIndexId); }

}  // namespace memgraph::wire_format
