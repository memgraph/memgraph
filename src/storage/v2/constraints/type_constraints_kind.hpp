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

#include "storage/v2/id_types.hpp"

#include "storage/v2/property_store_types.hpp"
#include "storage/v2/temporal.hpp"

#include <cstdint>
#include <string>

#include "absl/container/flat_hash_map.h"

namespace memgraph::storage {

enum class TypeConstraintKind : uint8_t {
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

inline std::string_view TypeConstraintKindToString(TypeConstraintKind type) {
  using namespace std::string_view_literals;
  switch (type) {
    case TypeConstraintKind::STRING:
      return "STRING"sv;
    case TypeConstraintKind::BOOLEAN:
      return "BOOL"sv;
    case TypeConstraintKind::INTEGER:
      return "INTEGER"sv;
    case TypeConstraintKind::FLOAT:
      return "FLOAT"sv;
    case TypeConstraintKind::LIST:
      return "LIST"sv;
    case TypeConstraintKind::MAP:
      return "MAP"sv;
    case TypeConstraintKind::DURATION:
      return "DURATION"sv;
    case TypeConstraintKind::DATE:
      return "DATE"sv;
    case TypeConstraintKind::LOCALTIME:
      return "LOCAL TIME"sv;
    case TypeConstraintKind::LOCALDATETIME:
      return "LOCAL DATE TIME"sv;
    case TypeConstraintKind::ZONEDDATETIME:
      return "ZONED DATE TIME"sv;
    case TypeConstraintKind::ENUM:
      return "ENUM"sv;
    case TypeConstraintKind::POINT:
      return "POINT"sv;
  }
}

inline PropertyStoreType TypeConstraintsKindToPropertyStoreType(TypeConstraintKind type) {
  switch (type) {
    case TypeConstraintKind::STRING:
      return PropertyStoreType::STRING;
    case TypeConstraintKind::BOOLEAN:
      return PropertyStoreType::BOOL;
    case TypeConstraintKind::INTEGER:
      return PropertyStoreType::INT;
    case TypeConstraintKind::FLOAT:
      return PropertyStoreType::DOUBLE;
    case TypeConstraintKind::LIST:
      return PropertyStoreType::LIST;
    case TypeConstraintKind::MAP:
      return PropertyStoreType::MAP;
    case TypeConstraintKind::DURATION:
    case TypeConstraintKind::DATE:
    case TypeConstraintKind::LOCALTIME:
    case TypeConstraintKind::LOCALDATETIME:
      return PropertyStoreType::TEMPORAL_DATA;
    case TypeConstraintKind::ZONEDDATETIME:
      return PropertyStoreType::ZONED_TEMPORAL_DATA;
    case TypeConstraintKind::ENUM:
      return PropertyStoreType::ENUM;
    case TypeConstraintKind::POINT:
      return PropertyStoreType::POINT;
  }
}

inline bool TemporalMatch(TemporalType type, TypeConstraintKind expected_type) {
  switch (type) {
    case TemporalType::Date:
      return expected_type == TypeConstraintKind::DATE;
    case TemporalType::LocalTime:
      return expected_type == TypeConstraintKind::LOCALTIME;
    case TemporalType::LocalDateTime:
      return expected_type == TypeConstraintKind::LOCALDATETIME;
    case TemporalType::Duration:
      return expected_type == TypeConstraintKind::DURATION;
  }
}

}  // namespace memgraph::storage
