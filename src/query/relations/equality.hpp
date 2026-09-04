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
/// `=`, `<>`, `IN` and a CASE each read.
///
/// It is three-valued: a comparison turning on a Null answers Null and decides
/// nothing. That is its difference from equivalence, which holds a Null
/// equivalent to a Null, and which a hash container reads.
#pragma once

#include "query/typed_value.hpp"
// typed_value.hpp only forward-declares these, and equality reads the identity
// out of each.
#include "query/path.hpp"
#include "query/virtual_edge.hpp"
#include "query/virtual_node.hpp"

namespace memgraph::query::relations::equality {

/// The two cases that walk what they hold, and so reach this relation again.
///
/// Out of line so that Equal does not call itself. A compiler will not inline a
/// function that recurses, whichever case reaches the recursion.
TypedValue EqualOfContainers(const TypedValue &a, const TypedValue &b);

/// Whether two values are equal, or Null where that cannot be decided.
///
/// The answer carries the memory resource `a` was allocated from, since the two
/// values need not share one.
///
/// @throw TypedValueException for a pair no equality is defined over.
inline TypedValue Equal(const TypedValue &a, const TypedValue &b) {
  if (a.IsNull() || b.IsNull()) return TypedValue(a.get_allocator());

  // check we have values that can be compared
  // this means that either they're the same type, or (int, double) combo
  if ((a.type() != b.type() && !(a.IsNumeric() && b.IsNumeric()))) return TypedValue(false, a.get_allocator());

  switch (a.type()) {
    case TypedValue::Type::Bool:
      return TypedValue(a.ValueBool() == b.ValueBool(), a.get_allocator());
    case TypedValue::Type::Int:
      if (b.IsDouble())
        return TypedValue(ToDouble(a) == ToDouble(b), a.get_allocator());
      else
        return TypedValue(a.ValueInt() == b.ValueInt(), a.get_allocator());
    case TypedValue::Type::Double:
      return TypedValue(ToDouble(a) == ToDouble(b), a.get_allocator());
    case TypedValue::Type::String:
      return TypedValue(a.ValueString() == b.ValueString(), a.get_allocator());
    case TypedValue::Type::Vertex:
      return TypedValue(a.ValueVertex() == b.ValueVertex(), a.get_allocator());
    case TypedValue::Type::Edge:
      return TypedValue(a.ValueEdge() == b.ValueEdge(), a.get_allocator());
    case TypedValue::Type::VirtualEdge:
      return TypedValue(a.ValueVirtualEdge() == b.ValueVirtualEdge(), a.get_allocator());
    case TypedValue::Type::VirtualNode:
      return TypedValue(a.ValueVirtualNode() == b.ValueVirtualNode(), a.get_allocator());
    case TypedValue::Type::List:
    case TypedValue::Type::Map:
      return EqualOfContainers(a, b);
    case TypedValue::Type::Path:
      return TypedValue(a.ValuePath() == b.ValuePath(), a.get_allocator());
    case TypedValue::Type::Date:
      return TypedValue(a.ValueDate() == b.ValueDate(), a.get_allocator());
    case TypedValue::Type::LocalTime:
      return TypedValue(a.ValueLocalTime() == b.ValueLocalTime(), a.get_allocator());
    case TypedValue::Type::LocalDateTime:
      return TypedValue(a.ValueLocalDateTime() == b.ValueLocalDateTime(), a.get_allocator());
    case TypedValue::Type::ZonedDateTime:
      return TypedValue(a.ValueZonedDateTime() == b.ValueZonedDateTime(), a.get_allocator());
    case TypedValue::Type::Duration:
      return TypedValue(a.ValueDuration() == b.ValueDuration(), a.get_allocator());
    case TypedValue::Type::Enum:
      return TypedValue(a.ValueEnum() == b.ValueEnum(), a.get_allocator());
    case TypedValue::Type::Point2d:
      return TypedValue(a.ValuePoint2d() == b.ValuePoint2d(), a.get_allocator());
    case TypedValue::Type::Point3d:
      return TypedValue(a.ValuePoint3d() == b.ValuePoint3d(), a.get_allocator());
    case TypedValue::Type::Graph:
    case TypedValue::Type::VirtualGraph:
      throw TypedValueException("Unsupported comparison operator");
    case TypedValue::Type::Function:
    case TypedValue::Type::Null:
      LOG_FATAL("Unhandled comparison for types");
  }
}

}  // namespace memgraph::query::relations::equality
