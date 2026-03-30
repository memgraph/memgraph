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

/// Lightweight forward-declaration header for PropertyValue types.
/// Include this instead of property_value.hpp when you only need the type
/// aliases, the PropertyValueType enum, or pointer/reference declarations.

#include <compare>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <variant>

#include "storage/v2/id_types.hpp"
#include "utils/exceptions.hpp"

namespace memgraph::storage {

// These are durable, do not change their values
enum class PropertyValueType : uint8_t {
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

// Tag types for dispatching between different list construction
struct IntListTag {};

struct DoubleListTag {};

struct NumericListTag {};

inline bool AreComparableTypes(PropertyValueType a, PropertyValueType b) {
  return (a == b) || (a == PropertyValueType::Int && b == PropertyValueType::Double) ||
         (a == PropertyValueType::Double && b == PropertyValueType::Int);
}

/// Helper function to compare two numeric values (int or double)
inline std::partial_ordering CompareNumericValues(const std::variant<int, double> &a,
                                                  const std::variant<int, double> &b) {
  return std::visit(
      [](const auto &val_a, const auto &val_b) -> std::partial_ordering { return val_a <=> val_b; }, a, b);
}

/// An exception raised by the PropertyValue. Typically when trying to perform
/// operations (such as addition) on PropertyValues of incompatible Types.
class PropertyValueException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(PropertyValueException)
};

// Forward declaration of the main template
template <typename Alloc, typename KeyType, typename VectorIndexIdType>
class PropertyValueImpl;

// Standard type aliases
using PropertyValue = PropertyValueImpl<std::allocator<std::byte>, PropertyId, uint64_t>;
using ExternalPropertyValue = PropertyValueImpl<std::allocator<std::byte>, std::string, std::string>;

namespace pmr {
using PropertyValue = PropertyValueImpl<std::pmr::polymorphic_allocator<std::byte>, PropertyId, uint64_t>;
}  // namespace pmr

}  // namespace memgraph::storage
