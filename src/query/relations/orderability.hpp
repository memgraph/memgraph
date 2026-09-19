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

#include <array>
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

namespace detail {

/// Where one type sits in the order a sort reads, lowest first.
///
/// The specification fixes the run a user sees: a map, a node, a relationship, a
/// list, a path, a string, a boolean, a number, and a null last. The types it
/// does not name are placed around that run rather than inside it, which leaves
/// every pair it does name where it asks for.
///
/// It also says where they may not go: a type it does not name must not sit
/// above a NaN. A NaN is the largest number, so that rules out the whole gap
/// between the numbers and the null, and every unnamed type is seated below the
/// strings instead. A date read against a string, a number and a NaN comes back
/// first, which is the order the reference implementation gives.
///
/// The two numeric types share a position, and that is load-bearing rather than
/// a convenience: an integer and a double holding the same number are equal, so
/// seating them apart would put them on either side of every string.
///
/// Named one type at a time rather than read off the enumerator, so that the
/// order a user sees and the number an enumerator happens to carry stay free of
/// each other. The switch has no default, so a type added to the value has to be
/// placed here before this compiles.
constexpr unsigned PositionOf(TypedValue::Type type) {
  using enum TypedValue::Type;
  switch (type) {
    case Map:
      return 0;
    case Vertex:
      return 1;
    case VirtualNode:
      return 2;
    case Edge:
      return 3;
    case VirtualEdge:
      return 4;
    case List:
      return 5;
    case Path:
      return 6;
    case Graph:
      return 7;
    case VirtualGraph:
      return 8;
    case Function:
      return 9;
    case Date:
      return 10;
    case LocalTime:
      return 11;
    case LocalDateTime:
      return 12;
    case ZonedDateTime:
      return 13;
    case Duration:
      return 14;
    case Enum:
      return 15;
    case Point2d:
      return 16;
    case Point3d:
      return 17;
    case String:
      return 18;
    case Bool:
      return 19;
    case Int:
    case Double:
      return 20;
    case Null:
      return 21;
  }
}

/// The positions as a table, so that placing a pair of unlike types costs two
/// loads rather than a switch a sort walks on every comparison it makes.
inline constexpr auto kPositions = [] {
  constexpr auto kTypeCount = static_cast<unsigned>(TypedValue::Type::VirtualNode) + 1U;
  std::array<unsigned, kTypeCount> positions{};
  for (auto type = 0U; type != kTypeCount; ++type) positions[type] = PositionOf(static_cast<TypedValue::Type>(type));
  return positions;
}();

}  // namespace detail

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
    // One Int against one Double is the only unlike pair with a payload to read.
    // The two share a position, so where each type sits cannot tell them apart.
    if (AreMixedNumbers(a.type(), b.type())) {
      auto const order = ComparePayloadOfMixedNumbers(a, b);
      if (order != std::partial_ordering::unordered) [[likely]]
        return order;

      // An integer is never a NaN, so the pair is unplaced only where the double
      // is one, and a NaN goes after every number.
      return a.type() == TypedValue::Type::Double ? std::partial_ordering::greater : std::partial_ordering::less;
    }

    // Every other unlike pair is placed by where its two types sit. A null is
    // last of them, so it sorts after everything without being asked about here.
    return detail::kPositions[static_cast<unsigned>(a.type())] <=> detail::kPositions[static_cast<unsigned>(b.type())];
  }
}

}  // namespace memgraph::query::relations::orderability
