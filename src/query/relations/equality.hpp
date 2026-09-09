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

#include "query/path.hpp"
#include "query/typed_value.hpp"
// typed_value.hpp only forward-declares these two, and equality reads the
// identity out of each.
#include "query/virtual_edge.hpp"
#include "query/virtual_node.hpp"

namespace memgraph::query::relations::equality {

/// The two cases that walk what they hold, and so reach this relation again.
///
/// Out of line so that Equal does not call itself. A compiler will not inline a
/// function that recurses, whichever case reaches the recursion.
///
/// Each takes what it walks rather than the values holding it, so that neither
/// can be handed a pair of unlike things. The answer carries the allocator the
/// left value was made with, which is not always the one its elements use.
TypedValue EqualOfLists(TypedValue::TVector const &a, TypedValue::TVector const &b, TypedValue::allocator_type alloc);
TypedValue EqualOfMaps(TypedValue::TMap const &a, TypedValue::TMap const &b, TypedValue::allocator_type alloc);

/// Whether a Null sits anywhere within the value, however deeply nested.
///
/// Ask this before answering equality from anything other than this relation. A
/// value holding a Null can compare Null, and a container keyed by equivalence
/// has no way to say so: it holds a Null equivalent to a Null and answers that
/// the two are the same. So does a sorted index, and so does a hash join.
///
/// Both representations of a value are asked the same question, because a scan
/// reads what is stored and a filter reads what the query built.
bool HoldsANull(const TypedValue &value);
bool HoldsANull(const storage::PropertyValue &value);

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
  // The tag tests read type() rather than IsNumeric()/IsDouble(), which are
  // defined in the value's own translation unit and are calls anywhere else.
  // Every element of a container reaches this.
  auto const numeric = [](TypedValue::Type type) {
    return type == TypedValue::Type::Int || type == TypedValue::Type::Double;
  };
  if (a.type() != b.type() && !(numeric(a.type()) && numeric(b.type()))) {
    return TypedValue(false, a.get_allocator());
  }

  switch (a.type()) {
    case TypedValue::Type::Bool:
      return TypedValue(a.UnsafeValueBool() == b.UnsafeValueBool(), a.get_allocator());
    case TypedValue::Type::Int:
      if (b.type() == TypedValue::Type::Double)
        return TypedValue(a.UnsafeValueInt() == b.UnsafeValueDouble(), a.get_allocator());
      else
        return TypedValue(a.UnsafeValueInt() == b.UnsafeValueInt(), a.get_allocator());
    case TypedValue::Type::Double:
      if (b.type() == TypedValue::Type::Int)
        return TypedValue(a.UnsafeValueDouble() == b.UnsafeValueInt(), a.get_allocator());
      else
        return TypedValue(a.UnsafeValueDouble() == b.UnsafeValueDouble(), a.get_allocator());
    case TypedValue::Type::String:
      return TypedValue(a.UnsafeValueString() == b.UnsafeValueString(), a.get_allocator());
    case TypedValue::Type::Vertex:
      return TypedValue(a.UnsafeValueVertex() == b.UnsafeValueVertex(), a.get_allocator());
    case TypedValue::Type::Edge:
      return TypedValue(a.UnsafeValueEdge() == b.UnsafeValueEdge(), a.get_allocator());
    case TypedValue::Type::VirtualEdge:
      return TypedValue(a.UnsafeValueVirtualEdge() == b.UnsafeValueVirtualEdge(), a.get_allocator());
    case TypedValue::Type::VirtualNode:
      return TypedValue(a.UnsafeValueVirtualNode() == b.UnsafeValueVirtualNode(), a.get_allocator());
    // Reading `b` at `a`'s type is sound here: the guard above returns for a
    // pair of unlike types unless both are numbers, and neither of these is.
    case TypedValue::Type::List:
      return EqualOfLists(a.UnsafeValueList(), b.UnsafeValueList(), a.get_allocator());
    case TypedValue::Type::Map:
      return EqualOfMaps(a.UnsafeValueMap(), b.UnsafeValueMap(), a.get_allocator());
    case TypedValue::Type::Path:
      return TypedValue(a.UnsafeValuePath() == b.UnsafeValuePath(), a.get_allocator());
    case TypedValue::Type::Date:
      return TypedValue(a.UnsafeValueDate() == b.UnsafeValueDate(), a.get_allocator());
    case TypedValue::Type::LocalTime:
      return TypedValue(a.UnsafeValueLocalTime() == b.UnsafeValueLocalTime(), a.get_allocator());
    case TypedValue::Type::LocalDateTime:
      return TypedValue(a.UnsafeValueLocalDateTime() == b.UnsafeValueLocalDateTime(), a.get_allocator());
    case TypedValue::Type::ZonedDateTime:
      return TypedValue(a.UnsafeValueZonedDateTime() == b.UnsafeValueZonedDateTime(), a.get_allocator());
    case TypedValue::Type::Duration:
      return TypedValue(a.UnsafeValueDuration() == b.UnsafeValueDuration(), a.get_allocator());
    case TypedValue::Type::Enum:
      return TypedValue(a.UnsafeValueEnum() == b.UnsafeValueEnum(), a.get_allocator());
    case TypedValue::Type::Point2d:
      return TypedValue(a.UnsafeValuePoint2d() == b.UnsafeValuePoint2d(), a.get_allocator());
    case TypedValue::Type::Point3d:
      return TypedValue(a.UnsafeValuePoint3d() == b.UnsafeValuePoint3d(), a.get_allocator());
    case TypedValue::Type::Graph:
    case TypedValue::Type::VirtualGraph:
      throw TypedValueException("Unsupported comparison operator");
    case TypedValue::Type::Function:
    case TypedValue::Type::Null:
      LOG_FATAL("Unhandled comparison for types");
  }
}

}  // namespace memgraph::query::relations::equality
