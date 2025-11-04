// Copyright 2025 Memgraph Ltd.
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
      // `PropertyValue` and they are interleaved when sorted.
      return utils::MakeBoundExclusive(kSmallestString);
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

}  // namespace memgraph::storage
