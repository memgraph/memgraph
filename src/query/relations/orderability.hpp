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

#include <cmath>
#include <compare>

#include "query/exceptions.hpp"
#include "query/fmt.hpp"
#include "query/relations/payload_order.hpp"
#include "query/typed_value.hpp"

namespace memgraph::query::relations::orderability {

/// Places two doubles, giving a NaN the position IEEE gives it nowhere: after
/// every number, and alongside another NaN.
///
/// Comparability reads the same pair and answers that it has no order for it,
/// which is why the payload order the two relations share leaves a NaN
/// unplaced and this relation places it here instead. A sort handed a pair with
/// no position treats the two as interchangeable, which would make a NaN
/// interchangeable with every number while no two numbers are with each other.
inline std::partial_ordering PlaceDoubles(double a, double b) {
  auto const order = a <=> b;
  if (order != std::partial_ordering::unordered) [[likely]]
    return order;

  if (std::isnan(a) && std::isnan(b)) return std::partial_ordering::equivalent;
  return std::isnan(a) ? std::partial_ordering::greater : std::partial_ordering::less;
}

/// Places two points, coordinate by coordinate, in the order the point's own
/// comparison reads them.
///
/// A point carries its coordinates as doubles, so one holding a NaN has no
/// position for the reason a NaN has none, and it is given one here for the same
/// reason: two values are equivalent exactly where they share a position under
/// this relation, and equivalence holds two such points alike.
template <typename Point>
std::partial_ordering PlacePoints(Point const &a, Point const &b) {
  if (auto const system = a.crs() <=> b.crs(); system != 0) return system;
  if (auto const x = PlaceDoubles(a.x(), b.x()); !std::is_eq(x)) return x;
  if (auto const y = PlaceDoubles(a.y(), b.y()); !std::is_eq(y)) return y;

  if constexpr (requires { a.z(); }) {
    return PlaceDoubles(a.z(), b.z());
  } else {
    return std::partial_ordering::equivalent;
  }
}

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
        return PlaceDoubles(a.UnsafeValueDouble(), b.UnsafeValueDouble());
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
        return PlacePoints(a.UnsafeValuePoint2d(), b.UnsafeValuePoint2d());
      case Point3d:
        return PlacePoints(a.UnsafeValuePoint3d(), b.UnsafeValuePoint3d());

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

    auto const order = ComparePayloadOfMixedNumbers(a, b);
    if (order != std::partial_ordering::unordered) [[likely]]
      return order;

    // An integer is never a NaN, so the pair is unplaced only where the double
    // is one, and a NaN goes after every number.
    return a.type() == TypedValue::Type::Double ? std::partial_ordering::greater : std::partial_ordering::less;
  }
}

}  // namespace memgraph::query::relations::orderability
