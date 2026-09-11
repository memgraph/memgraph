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
/// Orderability: one of the four relations openCypher defines over values, the
/// one a sort reads.
///
/// It answers for pairs comparability refuses, so that `ORDER BY` has somewhere
/// to put every row: a null sorts after everything, and two values of unlike
/// type are placed rather than reported as unknown.
#pragma once

#include <compare>

#include "query/exceptions.hpp"
#include "query/fmt.hpp"
#include "query/relations/payload_order.hpp"
#include "query/typed_value.hpp"

namespace memgraph::query::relations::orderability {

/// Orders two lists element by element, the shorter one first where they agree.
///
/// Out of line so that Compare does not call itself. A compiler will not inline
/// a function that recurses, and every sort reaches Compare through the caller.
///
/// Takes what it walks rather than the values holding it, so that it cannot be
/// handed a pair of unlike things.
std::partial_ordering CompareOfLists(TypedValue::TVector const &a, TypedValue::TVector const &b);

/// Where `a` falls relative to `b`.
///
/// @throw QueryRuntimeException for a pair this relation does not place.
inline std::partial_ordering Compare(TypedValue const &a, TypedValue const &b) {
  // First assume typical same type comparisons
  if (a.type() == b.type()) {
    switch (a.type()) {
      using enum TypedValue::Type;
      case Bool:
        return ComparePayloadOf<Bool>(a, b);
      case Int:
        return ComparePayloadOf<Int>(a, b);
      case Double:
        return ComparePayloadOf<Double>(a, b);
      case String:
        return ComparePayloadOf<String>(a, b);
      case Date:
        return ComparePayloadOf<Date>(a, b);
      case LocalTime:
        return ComparePayloadOf<LocalTime>(a, b);
      case LocalDateTime:
        return ComparePayloadOf<LocalDateTime>(a, b);
      case ZonedDateTime:
        return ComparePayloadOf<ZonedDateTime>(a, b);
      case Duration:
        return ComparePayloadOf<Duration>(a, b);
      case Enum:
        return ComparePayloadOf<Enum>(a, b);
      case Point2d:
        return ComparePayloadOf<Point2d>(a, b);
      case Point3d:
        return ComparePayloadOf<Point3d>(a, b);

      // The two this relation places that carry no order of their own: a null
      // is the same position as any other null, and a list is ordered by what
      // it holds rather than by a payload.
      case Null:
        return std::partial_ordering::equivalent;
      case List:
        return CompareOfLists(a.UnsafeValueList(), b.UnsafeValueList());

      case Map:
      case Vertex:
      case Edge:
      case VirtualEdge:
      case VirtualNode:
      case Path:
      case Graph:
      case VirtualGraph:
      case Function:
        throw QueryRuntimeException("Comparison is not defined for values of type {}.", a.type());
    }
  } else {
    // A null sorts after everything, and two nulls are the same position, which
    // the same-type branch above has already answered.
    if (a.IsNull()) return std::partial_ordering::greater;
    if (b.IsNull()) return std::partial_ordering::less;

    // One Int against one Double is the only unlike pair left that is ordered.
    if (!AreMixedNumbers(a.type(), b.type())) [[unlikely]]
      throw QueryRuntimeException("Can't compare value of type {} to value of type {}.", a.type(), b.type());
    return ComparePayloadOfMixedNumbers(a, b);
  }
}

}  // namespace memgraph::query::relations::orderability
