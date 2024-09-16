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

#pragma once

#include <cstdint>
#include <string>

#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::storage {

enum class TypeConstraintsType : uint8_t {
  STRING = 0,
  BOOLEAN = 1,
  INTEGER = 2,
  FLOAT = 3,
  LIST = 4,
  MAP = 5,
  DURATION = 6,
  DATE = 7,
  LOCALTIME = 8,
  LOCALDATETIME = 9,
  ZONEDDATETIME = 10,
  ENUM = 11,
  POINT = 12,
};

inline std::string TypeConstraintsTypeToString(TypeConstraintsType type) {
  switch (type) {
    case TypeConstraintsType::STRING:
      return "STRING";
    case TypeConstraintsType::BOOLEAN:
      return "BOOL";
    case TypeConstraintsType::INTEGER:
      return "INTEGER";
    case TypeConstraintsType::FLOAT:
      return "FLOAT";
    case TypeConstraintsType::LIST:
      return "LIST";
    case TypeConstraintsType::MAP:
      return "MAP";
    case TypeConstraintsType::DURATION:
      return "DURATION";
    case TypeConstraintsType::DATE:
      return "DATE";
    case TypeConstraintsType::LOCALTIME:
      return "LOCAL TIME";
    case TypeConstraintsType::LOCALDATETIME:
      return "LOCAL DATE TIME";
    case TypeConstraintsType::ZONEDDATETIME:
      return "ZONED DATE TIME";
    case TypeConstraintsType::ENUM:
      return "ENUM";
    case TypeConstraintsType::POINT:
      return "POINT";
  }
}

inline TypeConstraintsType PropertyValueToTypeConstraintType(const PropertyValue &property) {
  switch (property.type()) {
    case PropertyValueType::String:
      return TypeConstraintsType::STRING;
    case PropertyValueType::Bool:
      return TypeConstraintsType::BOOLEAN;
    case PropertyValueType::Int:
      return TypeConstraintsType::INTEGER;
    case PropertyValueType::Double:
      return TypeConstraintsType::FLOAT;
    case PropertyValueType::List:
      return TypeConstraintsType::LIST;
    case PropertyValueType::Map:
      return TypeConstraintsType::MAP;
    case PropertyValueType::TemporalData: {
      auto const temporal = property.ValueTemporalData();
      switch (temporal.type) {
        case TemporalType::Date:
          return TypeConstraintsType::DATE;
        case TemporalType::LocalTime:
          return TypeConstraintsType::LOCALTIME;
        case TemporalType::LocalDateTime:
          return TypeConstraintsType::LOCALDATETIME;
        case TemporalType::Duration:
          return TypeConstraintsType::DURATION;
      }
    }
    case PropertyValueType::ZonedTemporalData:
      return TypeConstraintsType::ZONEDDATETIME;
    case PropertyValueType::Enum:
      return TypeConstraintsType::ENUM;
    case PropertyValueType::Point2d:
    case PropertyValueType::Point3d:
      return TypeConstraintsType::POINT;
    case PropertyValueType::Null:
      MG_ASSERT(false, "Unexpected conversion from PropertyValueType::Null to TypeConstraint::Type");
  }
}

inline PropertyStoreType TypeConstraintsTypeToPropertyStoreType(TypeConstraintsType type) {
  switch (type) {
    case TypeConstraintsType::STRING:
      return PropertyStoreType::STRING;
    case TypeConstraintsType::BOOLEAN:
      return PropertyStoreType::BOOL;
    case TypeConstraintsType::INTEGER:
      return PropertyStoreType::INT;
    case TypeConstraintsType::FLOAT:
      return PropertyStoreType::DOUBLE;
    case TypeConstraintsType::LIST:
      return PropertyStoreType::LIST;
    case TypeConstraintsType::MAP:
      return PropertyStoreType::MAP;
    case TypeConstraintsType::DURATION:
    case TypeConstraintsType::DATE:
    case TypeConstraintsType::LOCALTIME:
    case TypeConstraintsType::LOCALDATETIME:
      return PropertyStoreType::TEMPORAL_DATA;
    case TypeConstraintsType::ZONEDDATETIME:
      return PropertyStoreType::ZONED_TEMPORAL_DATA;
    case TypeConstraintsType::ENUM:
      return PropertyStoreType::ENUM;
    case TypeConstraintsType::POINT:
      return PropertyStoreType::POINT;
  }
}

}  // namespace memgraph::storage
