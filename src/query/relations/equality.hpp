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
#include "query/relations/payload_order.hpp"
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
/// A stored value is asked the same question by a walk of storage's own, since
/// a scan reads what is stored where a filter reads what the query built.
bool HoldsANull(const TypedValue &value);

/// Whether the value is equal to itself.
///
/// Equality is reflexive for almost every value, and the exceptions are the two
/// it cannot decide: a Null, which leaves the pair undecided, and a NaN, which
/// it holds equal to nothing. A container keyed by equivalence finds either
/// again, because finding an entry again is what equivalence is for, so a
/// caller reading such a container for an equality has to ask this first. An
/// equality against a value that is not equal to itself keeps no row, whichever
/// way the query is answered.
bool EqualsItself(const TypedValue &value);

/// Whether two values are equal, or Null where that cannot be decided.
///
/// The answer carries the memory resource `a` was allocated from, since the two
/// values need not share one.
///
/// @throw TypedValueException for a pair no equality is defined over.
inline TypedValue Equal(const TypedValue &a, const TypedValue &b) {
  auto const alloc = a.get_allocator();
  if (a.IsNull() || b.IsNull()) return TypedValue(alloc);

  // A pair of unlike types is equal only where both are numbers. That pair is
  // read at the width each holds, by the placement the order reads: taking
  // either through the other's type would hold two integers equal to one double
  // while telling the two apart, and this relation has to agree with the order
  // over the same pair.
  //
  // The tag tests read type() rather than IsNumeric()/IsDouble(), which are
  // defined in the value's own translation unit and are calls anywhere else.
  // Every element of a container reaches this.
  if (a.type() != b.type()) {
    if (!AreMixedNumbers(a.type(), b.type())) return TypedValue(false, alloc);
    return TypedValue(std::is_eq(ComparePayloadOfMixedNumbers(a, b)), alloc);
  }

  // Both are of one type from here, so reading `b` at `a`'s is sound.
  switch (a.type()) {
    case TypedValue::Type::Bool:
      return TypedValue(a.UnsafeValueBool() == b.UnsafeValueBool(), alloc);
    case TypedValue::Type::Int:
      return TypedValue(a.UnsafeValueInt() == b.UnsafeValueInt(), alloc);
    case TypedValue::Type::Double:
      return TypedValue(a.UnsafeValueDouble() == b.UnsafeValueDouble(), alloc);
    case TypedValue::Type::String:
      return TypedValue(a.UnsafeValueString() == b.UnsafeValueString(), alloc);
    case TypedValue::Type::Vertex:
      return TypedValue(a.UnsafeValueVertex() == b.UnsafeValueVertex(), alloc);
    case TypedValue::Type::Edge:
      return TypedValue(a.UnsafeValueEdge() == b.UnsafeValueEdge(), alloc);
    case TypedValue::Type::VirtualEdge:
      return TypedValue(a.UnsafeValueVirtualEdge() == b.UnsafeValueVirtualEdge(), alloc);
    case TypedValue::Type::VirtualNode:
      return TypedValue(a.UnsafeValueVirtualNode() == b.UnsafeValueVirtualNode(), alloc);
    case TypedValue::Type::List:
      return EqualOfLists(a.UnsafeValueList(), b.UnsafeValueList(), alloc);
    case TypedValue::Type::Map:
      return EqualOfMaps(a.UnsafeValueMap(), b.UnsafeValueMap(), alloc);
    case TypedValue::Type::Path:
      return TypedValue(a.UnsafeValuePath() == b.UnsafeValuePath(), alloc);
    case TypedValue::Type::Date:
      return TypedValue(a.UnsafeValueDate() == b.UnsafeValueDate(), alloc);
    case TypedValue::Type::LocalTime:
      return TypedValue(a.UnsafeValueLocalTime() == b.UnsafeValueLocalTime(), alloc);
    case TypedValue::Type::LocalDateTime:
      return TypedValue(a.UnsafeValueLocalDateTime() == b.UnsafeValueLocalDateTime(), alloc);
    case TypedValue::Type::ZonedDateTime:
      return TypedValue(a.UnsafeValueZonedDateTime() == b.UnsafeValueZonedDateTime(), alloc);
    case TypedValue::Type::Duration:
      return TypedValue(a.UnsafeValueDuration() == b.UnsafeValueDuration(), alloc);
    case TypedValue::Type::Enum:
      return TypedValue(a.UnsafeValueEnum() == b.UnsafeValueEnum(), alloc);
    case TypedValue::Type::Point2d:
      return TypedValue(a.UnsafeValuePoint2d() == b.UnsafeValuePoint2d(), alloc);
    case TypedValue::Type::Point3d:
      return TypedValue(a.UnsafeValuePoint3d() == b.UnsafeValuePoint3d(), alloc);
    case TypedValue::Type::Graph:
    case TypedValue::Type::VirtualGraph:
      throw TypedValueException("Unsupported comparison operator");
    case TypedValue::Type::Function:
    case TypedValue::Type::Null:
      LOG_FATAL("Unhandled comparison for types");
  }
}

}  // namespace memgraph::query::relations::equality
