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
/// Comparability: one of the four relations openCypher defines over values, the
/// one `<`, `<=`, `>` and `>=` read.
///
/// It is partial. Two values it cannot place answer Null, and a type it does
/// not admit at all raises. This is where it parts from orderability, which
/// must place every pair because a sort is undefined without that.
#pragma once

#include "query/typed_value.hpp"

namespace memgraph::query::relations::comparability {

/// Whether this relation admits a type at all.
///
/// The cases are listed rather than defaulted so that a type added to the value
/// has to be placed here deliberately.
constexpr bool Admits(TypedValue::Type type) {
  switch (type) {
    case TypedValue::Type::Null:
    case TypedValue::Type::Int:
    case TypedValue::Type::Double:
    case TypedValue::Type::String:
    case TypedValue::Type::Date:
    case TypedValue::Type::LocalTime:
    case TypedValue::Type::LocalDateTime:
    case TypedValue::Type::ZonedDateTime:
    case TypedValue::Type::Duration:
      return true;

    case TypedValue::Type::Bool:
    case TypedValue::Type::List:
    case TypedValue::Type::Map:
    case TypedValue::Type::Vertex:
    case TypedValue::Type::Edge:
    case TypedValue::Type::VirtualEdge:
    case TypedValue::Type::VirtualNode:
    case TypedValue::Type::Path:
    case TypedValue::Type::Graph:
    case TypedValue::Type::VirtualGraph:
    case TypedValue::Type::Function:
    case TypedValue::Type::Enum:
    case TypedValue::Type::Point2d:
    case TypedValue::Type::Point3d:
      return false;
  }
}

constexpr bool IsTemporal(TypedValue::Type type) {
  switch (type) {
    case TypedValue::Type::Date:
    case TypedValue::Type::LocalTime:
    case TypedValue::Type::LocalDateTime:
    case TypedValue::Type::ZonedDateTime:
    case TypedValue::Type::Duration:
      return true;
    default:
      return false;
  }
}

// TODO: make it faster
/// Whether `a` orders before `b`, or Null where that cannot be decided.
///
/// The answer carries the memory resource `a` was allocated from, since the two
/// values need not share one.
///
/// @throw TypedValueException for a pair of types this relation does not admit.
inline TypedValue Less(const TypedValue &a, const TypedValue &b) {
  if (!Admits(a.type()) || !Admits(b.type())) {
    if ((is_canonical(a.type()) || is_canonical(b.type())) && (a.type() != b.type())) return {};
    throw TypedValueException("Invalid 'less' operand types({} + {})", a.type(), b.type());
  }

  if (a.IsNull() || b.IsNull()) {
    return TypedValue(a.get_allocator());
  }

  if (a.IsString() || b.IsString()) {
    if (a.type() != b.type()) {
      return {};
    } else {
      return TypedValue(a.ValueString() < b.ValueString(), a.get_allocator());
    }
  }

  // A filter compares numbers and strings far more often than temporals, and
  // saying so keeps the temporal cases off the path the common ones take.
  if (IsTemporal(a.type()) || IsTemporal(b.type())) [[unlikely]] {
    if (a.type() != b.type()) {
      return {};
    }

    switch (a.type()) {
      case TypedValue::Type::Date:
        // NOLINTNEXTLINE(modernize-use-nullptr)
        return TypedValue(a.ValueDate() < b.ValueDate(), a.get_allocator());
      case TypedValue::Type::LocalTime:
        // NOLINTNEXTLINE(modernize-use-nullptr)
        return TypedValue(a.ValueLocalTime() < b.ValueLocalTime(), a.get_allocator());
      case TypedValue::Type::LocalDateTime:
        // NOLINTNEXTLINE(modernize-use-nullptr)
        return TypedValue(a.ValueLocalDateTime() < b.ValueLocalDateTime(), a.get_allocator());
      case TypedValue::Type::ZonedDateTime:
        // NOLINTNEXTLINE(modernize-use-nullptr)
        return TypedValue(a.ValueZonedDateTime() < b.ValueZonedDateTime(), a.get_allocator());
      case TypedValue::Type::Duration:
        // NOLINTNEXTLINE(modernize-use-nullptr)
        return TypedValue(a.ValueDuration() < b.ValueDuration(), a.get_allocator());
      default:
        LOG_FATAL("Invalid temporal type");
    }
  }

  // at this point we only have int and double
  if (a.IsDouble() || b.IsDouble()) {
    return TypedValue(ToDouble(a) < ToDouble(b), a.get_allocator());
  } else {
    return TypedValue(a.ValueInt() < b.ValueInt(), a.get_allocator());
  }
}

}  // namespace memgraph::query::relations::comparability
