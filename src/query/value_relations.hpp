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
/// The relations openCypher defines over values, under the names it gives them.
///
/// There are four, and they differ only over the values that sit outside them:
/// a Null, a NaN, and a pair of unlike types. Everywhere else they agree, which
/// is why they are only comprehensible as a set and why they are written
/// together here. A relation answering a question that belongs to one of the
/// others is the defect this arrangement exists to make visible.
///
/// * comparability, which `< <= > >=` each read.
/// * equality, which `= <> IN` read.
/// * equivalence, which DISTINCT and grouping read.
/// * orderability, which ORDER BY, min and max read.
#pragma once

#include <algorithm>
#include <compare>

#include "query/exceptions.hpp"
#include "query/fmt.hpp"
#include "query/typed_value.hpp"
// typed_value.hpp only forward-declares these two, and the ordering reads the
// identity out of each.
#include "query/virtual_edge.hpp"
#include "query/virtual_node.hpp"

namespace memgraph::query::relations {

/// The total order behind ORDER BY, min and max.
///
/// Alone among the four it places every pair of values, which is what makes a
/// sort possible: `std::sort` has a strict weak ordering as its precondition
/// and is undefined behaviour without one. A Null sorts after everything and
/// alongside another Null, a NaN sorts after the numbers, and values of unlike
/// types are separated by where their types sit.
namespace orderability {

namespace detail {

/// Reads an ordering from a type that leaves no pair unordered as the total one
/// it already is. Only sound for such a type; the two that are not, a double
/// and a point, are placed by TypedValue::CompareDoublesNaNLast instead.
inline std::weak_ordering AlreadyTotal(std::partial_ordering order) {
  if (std::is_lt(order)) return std::weak_ordering::less;
  if (std::is_gt(order)) return std::weak_ordering::greater;
  DMG_ASSERT(order == std::partial_ordering::equivalent, "A type that orders every pair reported one unordered");
  return std::weak_ordering::equivalent;
}

/// Orders two paths as the alternating list of nodes and relationships each
/// runs through, from its start.
inline std::weak_ordering ComparePathsAsAlternating(Path const &lhs, Path const &rhs) {
  auto const length = [](Path const &p) { return p.vertices().size() + p.edges().size(); };
  auto const common = std::min(length(lhs), length(rhs));
  for (size_t i = 0; i != common; ++i) {
    // Even positions hold a node, odd positions the relationship after it.
    auto const order = (i % 2 == 0) ? (lhs.vertices()[i / 2].Gid().AsUint() <=> rhs.vertices()[i / 2].Gid().AsUint())
                                    : (lhs.edges()[i / 2].Gid().AsUint() <=> rhs.edges()[i / 2].Gid().AsUint());
    if (order != 0) return order;
  }
  return length(lhs) <=> length(rhs);
}

}  // namespace detail

/// Where a type sits in the global sort order, which is what lets values of
/// unlike types be ordered against one another at all.
///
/// Cypher fixes this order down to NUMBER and then VOID, and requires only that
/// the types it does not name are not placed after a NaN. The ones it does not
/// name are put where Neo4j puts them, so that a query ordering a mixed column
/// answers alike on both.
///
/// @throw QueryRuntimeException for a type no order is defined over.
inline int SortRank(TypedValue::Type type) {
  switch (type) {
    using enum TypedValue::Type;
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
    case Point2d:
      return 7;
    case Point3d:
      return 8;
    case ZonedDateTime:
      return 9;
    case LocalDateTime:
      return 10;
    case Date:
      return 11;
    case LocalTime:
      return 12;
    case Duration:
      return 13;
    case Enum:
      return 14;
    case String:
      return 15;
    case Bool:
      return 16;
    // The one rank two types share, since an integer and a double are ordered
    // against each other as numbers rather than by their types.
    case Int:
    case Double:
      return 17;
    case Null:
      return 18;

    case Graph:
    case VirtualGraph:
    case Function:
      throw QueryRuntimeException("Comparison is not defined for values of type {}.", type);
  }
}

/// Orders two values.
///
/// Unlike comparability, which the `<` family reads, this one places every pair
/// somewhere: a Null sorts after everything and alongside another Null, and a
/// list is ordered against another list element by element. What a type's own
/// values order like is not decided here; that is shared with comparability.
///
/// Defined in the header because a sort calls it once per comparison.
///
/// @throw QueryRuntimeException for a type no order is defined over.
inline std::weak_ordering Compare(TypedValue const &a, TypedValue const &b) {
  // First assume typical same type comparisons
  if (a.type() == b.type()) {
    switch (a.type()) {
      using enum TypedValue::Type;
      // Null sorts alongside Null here, where comparability would refuse to say.
      case Null:
        return std::weak_ordering::equivalent;
      case List:
        return std::lexicographical_compare_three_way(a.UnsafeValueList().begin(),
                                                      a.UnsafeValueList().end(),
                                                      b.UnsafeValueList().begin(),
                                                      b.UnsafeValueList().end(),
                                                      Compare);
      case Map: {
        // By key and then by value, in the order the keys are held, with a map
        // that runs out first coming before one that continues.
        auto const &lhs = a.UnsafeValueMap();
        auto const &rhs = b.UnsafeValueMap();
        auto lhs_it = lhs.begin();
        auto rhs_it = rhs.begin();
        for (; lhs_it != lhs.end() && rhs_it != rhs.end(); ++lhs_it, ++rhs_it) {
          if (auto const order = lhs_it->first <=> rhs_it->first; order != 0) return order;
          if (auto const order = Compare(lhs_it->second, rhs_it->second); order != 0) return order;
        }
        return lhs.size() <=> rhs.size();
      }
      case Vertex:
        return a.UnsafeValueVertex().Gid().AsUint() <=> b.UnsafeValueVertex().Gid().AsUint();
      case Edge:
        return a.UnsafeValueEdge().Gid().AsUint() <=> b.UnsafeValueEdge().Gid().AsUint();
      case VirtualNode:
        return a.UnsafeValueVirtualNode().Gid().AsUint() <=> b.UnsafeValueVirtualNode().Gid().AsUint();
      case VirtualEdge:
        return a.UnsafeValueVirtualEdge().Gid().AsUint() <=> b.UnsafeValueVirtualEdge().Gid().AsUint();
      case Path:
        // As the list of nodes and relationships the path alternates between,
        // read from its start.
        return detail::ComparePathsAsAlternating(a.UnsafeValuePath(), b.UnsafeValuePath());

      case Graph:
      case VirtualGraph:
      case Function:
        throw QueryRuntimeException("Comparison is not defined for values of type {}.", a.type());

      // The two types that can hold a NaN, which has to be given a place here
      // rather than left beside nothing as comparability leaves it.
      case Double:
        return TypedValue::CompareDoublesNaNLast(a.UnsafeValueDouble(), b.UnsafeValueDouble());
      case Point2d: {
        auto const &lhs = a.UnsafeValuePoint2d();
        auto const &rhs = b.UnsafeValuePoint2d();
        if (auto const order = lhs.crs() <=> rhs.crs(); order != 0) return order;
        if (auto const order = TypedValue::CompareDoublesNaNLast(lhs.x(), rhs.x()); order != 0) return order;
        return TypedValue::CompareDoublesNaNLast(lhs.y(), rhs.y());
      }
      case Point3d: {
        auto const &lhs = a.UnsafeValuePoint3d();
        auto const &rhs = b.UnsafeValuePoint3d();
        if (auto const order = lhs.crs() <=> rhs.crs(); order != 0) return order;
        if (auto const order = TypedValue::CompareDoublesNaNLast(lhs.x(), rhs.x()); order != 0) return order;
        if (auto const order = TypedValue::CompareDoublesNaNLast(lhs.y(), rhs.y()); order != 0) return order;
        return TypedValue::CompareDoublesNaNLast(lhs.z(), rhs.z());
      }

      case Bool:
      case Int:
      case String:
      case Date:
      case LocalTime:
      case LocalDateTime:
      case ZonedDateTime:
      case Duration:
      case Enum:
        // Ordered by what they hold, which comparability orders them by too.
        // Between them the two cover every type this case admits, and none of
        // them holds a value that sits outside its own order.
        if (auto const order = TypedValue::ComparePayload(a, b)) return detail::AlreadyTotal(*order);
        return detail::AlreadyTotal(*TypedValue::ComparePayloadOrderOnly(a, b));
    }
  }

  // Unlike types are separated by where their types sit, which puts a null
  // after everything and a map before it.
  if (auto const order = SortRank(a.type()) <=> SortRank(b.type()); order != 0) return order;

  // The one rank two types share: an integer against a double, where the double
  // may again be a NaN.
  if (a.IsInt()) {
    return TypedValue::CompareDoublesNaNLast(static_cast<double>(a.UnsafeValueInt()), b.UnsafeValueDouble());
  }
  return TypedValue::CompareDoublesNaNLast(a.UnsafeValueDouble(), static_cast<double>(b.UnsafeValueInt()));
}

}  // namespace orderability

}  // namespace memgraph::query::relations
