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

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store_types.hpp"
#include "storage/v2/temporal.hpp"

#include <cstdint>
#include <span>
#include <string>
#include <utility>

#include "absl/container/flat_hash_map.h"

import memgraph.storage.property_value;

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
  std::unreachable();
}

/// The stored types a value of this constraint kind may be held as.
///
/// One kind can have more than one encoding: a zoned datetime is stored differently depending
/// on whether its timezone is a name or a numeric offset, and a list is stored out of line when
/// a vector index holds it. Each is the same type to the user, so each satisfies the constraint.
inline std::span<PropertyStoreType const> TypeConstraintsKindToPropertyStoreType(TypeConstraintKind type) {
  static constexpr PropertyStoreType kString[] = {PropertyStoreType::STRING};
  static constexpr PropertyStoreType kBool[] = {PropertyStoreType::BOOL};
  static constexpr PropertyStoreType kInt[] = {PropertyStoreType::INT};
  static constexpr PropertyStoreType kDouble[] = {PropertyStoreType::DOUBLE};
  static constexpr PropertyStoreType kList[] = {PropertyStoreType::LIST, PropertyStoreType::VECTOR};
  static constexpr PropertyStoreType kMap[] = {PropertyStoreType::MAP};
  static constexpr PropertyStoreType kTemporal[] = {PropertyStoreType::TEMPORAL_DATA};
  static constexpr PropertyStoreType kZonedTemporal[] = {PropertyStoreType::ZONED_TEMPORAL_DATA,
                                                         PropertyStoreType::OFFSET_ZONED_TEMPORAL_DATA};
  static constexpr PropertyStoreType kEnum[] = {PropertyStoreType::ENUM};
  static constexpr PropertyStoreType kPoint[] = {PropertyStoreType::POINT};

  switch (type) {
    case TypeConstraintKind::STRING:
      return kString;
    case TypeConstraintKind::BOOLEAN:
      return kBool;
    case TypeConstraintKind::INTEGER:
      return kInt;
    case TypeConstraintKind::FLOAT:
      return kDouble;
    case TypeConstraintKind::LIST:
      return kList;
    case TypeConstraintKind::MAP:
      return kMap;
    case TypeConstraintKind::DURATION:
    case TypeConstraintKind::DATE:
    case TypeConstraintKind::LOCALTIME:
    case TypeConstraintKind::LOCALDATETIME:
      return kTemporal;
    case TypeConstraintKind::ZONEDDATETIME:
      return kZonedTemporal;
    case TypeConstraintKind::ENUM:
      return kEnum;
    case TypeConstraintKind::POINT:
      return kPoint;
  }
  std::unreachable();
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
  std::unreachable();
}

/// Convert a PropertyValue to its corresponding TypeConstraintKind.
/// Asserts if called with a Null property value.
TypeConstraintKind PropertyValueToTypeConstraintKind(const PropertyValue &property);

/// Check if a PropertyValue matches a TypeConstraintKind.
/// For temporal data, performs fine-grained subtype matching.
/// For other types, performs coarse-grained type class matching.
/// Returns true for Null values (type constraints don't enforce existence).
bool PropertyValueMatchesTypeConstraint(const PropertyValue &property, TypeConstraintKind constraint_type);

}  // namespace memgraph::storage
