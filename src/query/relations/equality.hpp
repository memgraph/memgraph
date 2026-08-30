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
/// Equality: one of the four relations openCypher defines over values, the one
/// `= <> IN` and a CASE each read.
///
/// It is three-valued. A comparison turning on a Null answers Null and decides
/// nothing, and a container holding one propagates that Null outwards rather
/// than calling two such containers equal. That is the whole of its difference
/// from equivalence, which holds a Null equivalent to a Null and a NaN
/// equivalent to a NaN where this reads one as different from itself.
#pragma once

#include "query/typed_value.hpp"
// typed_value.hpp only forward-declares these, and equality reads the identity
// out of each.
#include "query/path.hpp"
#include "query/virtual_edge.hpp"
#include "query/virtual_node.hpp"

namespace memgraph::query::relations::equality {

/// Whether two values are equal, or Null where that cannot be decided.
///
/// The answer carries the memory resource `a` was allocated from, since the two
/// values need not share one.
///
/// @throw TypedValueException for a pair no equality is defined over.
inline TypedValue Equal(const TypedValue &a, const TypedValue &b) {
  if (a.IsNull() || b.IsNull()) return TypedValue(a.get_allocator());

  // Only a pair of one type can be equal, save for an integer and a double,
  // which are equal as the numbers they are.
  if ((a.type() != b.type() && !(a.IsNumeric() && b.IsNumeric()))) return TypedValue(false, a.get_allocator());

  switch (a.type()) {
    using enum TypedValue::Type;
    case Bool:
      return TypedValue(a.UnsafeValueBool() == b.UnsafeValueBool(), a.get_allocator());
    case Int:
      if (b.type() == TypedValue::Type::Double) return TypedValue(ToDouble(a) == ToDouble(b), a.get_allocator());
      return TypedValue(a.UnsafeValueInt() == b.UnsafeValueInt(), a.get_allocator());
    case Double:
      return TypedValue(ToDouble(a) == ToDouble(b), a.get_allocator());
    case String:
      return TypedValue(a.UnsafeValueString() == b.UnsafeValueString(), a.get_allocator());
    case Vertex:
      return TypedValue(a.UnsafeValueVertex() == b.UnsafeValueVertex(), a.get_allocator());
    case Edge:
      return TypedValue(a.UnsafeValueEdge() == b.UnsafeValueEdge(), a.get_allocator());
    case VirtualEdge:
      return TypedValue(a.UnsafeValueVirtualEdge() == b.UnsafeValueVirtualEdge(), a.get_allocator());
    case VirtualNode:
      return TypedValue(a.UnsafeValueVirtualNode() == b.UnsafeValueVirtualNode(), a.get_allocator());
    case List: {
      const auto &list_a = a.UnsafeValueList();
      const auto &list_b = b.UnsafeValueList();
      if (list_a.size() != list_b.size()) return TypedValue(false, a.get_allocator());
      auto saw_null = false;
      for (size_t i = 0; i != list_a.size(); ++i) {
        auto const element_result = Equal(list_a[i], list_b[i]);
        if (element_result.IsNull()) {
          saw_null = true;
        } else if (!element_result.UnsafeValueBool()) {
          return TypedValue(false, a.get_allocator());
        }
      }
      // One Null element pair only leaves the lists indistinguishable; another
      // pair may still have proved them different, which is why false wins.
      if (saw_null) return TypedValue(a.get_allocator());
      return TypedValue(true, a.get_allocator());
    }
    case Map: {
      const auto &map_a = a.UnsafeValueMap();
      const auto &map_b = b.UnsafeValueMap();
      if (map_a.size() != map_b.size()) return TypedValue(false, a.get_allocator());
      auto saw_null = false;
      for (const auto &kv_a : map_a) {
        auto found_b_it = map_b.find(kv_a.first);
        if (found_b_it == map_b.end()) return TypedValue(false, a.get_allocator());
        auto const value_result = Equal(kv_a.second, found_b_it->second);
        if (value_result.IsNull()) {
          saw_null = true;
        } else if (!value_result.UnsafeValueBool()) {
          return TypedValue(false, a.get_allocator());
        }
      }
      if (saw_null) return TypedValue(a.get_allocator());
      return TypedValue(true, a.get_allocator());
    }
    case Path:
      return TypedValue(a.UnsafeValuePath() == b.UnsafeValuePath(), a.get_allocator());
    case Date:
      return TypedValue(a.UnsafeValueDate() == b.UnsafeValueDate(), a.get_allocator());
    case LocalTime:
      return TypedValue(a.UnsafeValueLocalTime() == b.UnsafeValueLocalTime(), a.get_allocator());
    case LocalDateTime:
      return TypedValue(a.UnsafeValueLocalDateTime() == b.UnsafeValueLocalDateTime(), a.get_allocator());
    case ZonedDateTime:
      return TypedValue(a.UnsafeValueZonedDateTime() == b.UnsafeValueZonedDateTime(), a.get_allocator());
    case Duration:
      return TypedValue(a.UnsafeValueDuration() == b.UnsafeValueDuration(), a.get_allocator());
    case Enum:
      return TypedValue(a.UnsafeValueEnum() == b.UnsafeValueEnum(), a.get_allocator());
    case Point2d:
      return TypedValue(a.UnsafeValuePoint2d() == b.UnsafeValuePoint2d(), a.get_allocator());
    case Point3d:
      return TypedValue(a.UnsafeValuePoint3d() == b.UnsafeValuePoint3d(), a.get_allocator());
    case Graph:
    case VirtualGraph:
      throw TypedValueException("Unsupported comparison operator");
    case Function:
    case Null:
      LOG_FATAL("Unhandled comparison for types");
  }
}

}  // namespace memgraph::query::relations::equality
