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
/// Orderability: the relation ORDER BY, min and max read.
///
/// Alone among the four relations it places every pair of values it has a rank
/// for, which is what makes a sort possible at all. The three types no rank is
/// given to raise instead.
#pragma once

#include <algorithm>
#include <compare>

#include "query/exceptions.hpp"
#include "query/fmt.hpp"
#include "query/relations/comparability.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/property_value.hpp"
// typed_value.hpp only forward-declares these two, and the ordering reads the
// identity out of each.
#include "query/virtual_edge.hpp"
#include "query/virtual_node.hpp"

namespace memgraph::query::relations {

/// Where a NaN sits against a number is one decision, and an index holds its
/// entries by it as much as a sort reads it, so both layers order two doubles
/// by the same function rather than by two that agree.
using storage::CompareDoublesNaNLast;

/// The total order behind ORDER BY, min and max.
///
/// `std::sort` has a strict weak ordering as its precondition and is undefined
/// behaviour without one, which is why every pair a column can hold has to be
/// placed. A Null sorts after everything and alongside another Null, a NaN
/// sorts after the numbers, and values of unlike types are separated by where
/// their types sit.
namespace orderability {

namespace detail {

/// Reads an ordering from a type that leaves no pair unordered as the total one
/// it already is. Only sound for such a type; the two that are not, a double
/// and a point, are placed by CompareDoublesNaNLast instead.
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
inline storage::ValueRank SortRank(TypedValue::Type type) {
  switch (type) {
    using enum TypedValue::Type;
    using Rank = storage::ValueRank;
    case Map:
      return Rank::Map;
    case Vertex:
      return Rank::Vertex;
    case VirtualNode:
      return Rank::VirtualNode;
    case Edge:
      return Rank::Edge;
    case VirtualEdge:
      return Rank::VirtualEdge;
    case List:
      return Rank::List;
    case Path:
      return Rank::Path;
    case Point2d:
      return Rank::Point2d;
    case Point3d:
      return Rank::Point3d;
    case ZonedDateTime:
      return Rank::ZonedDateTime;
    case LocalDateTime:
      return Rank::LocalDateTime;
    case Date:
      return Rank::Date;
    case LocalTime:
      return Rank::LocalTime;
    case Duration:
      return Rank::Duration;
    case Enum:
      return Rank::Enum;
    case String:
      return Rank::String;
    case Bool:
      return Rank::Bool;
    case Int:
    case Double:
      return Rank::Number;
    case Null:
      return Rank::Null;

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
        // By size, then by every key, and only then by the values under them.
        // The map holding fewer entries comes first whatever its keys are, and
        // two maps of one size are separated by the first key they do not
        // share before either map's values are looked at.
        auto const &lhs = a.UnsafeValueMap();
        auto const &rhs = b.UnsafeValueMap();
        if (auto const order = lhs.size() <=> rhs.size(); order != 0) return order;

        // The keys are held in the order they sort in, so one walk reads them
        // in that order.
        for (auto lhs_it = lhs.begin(), rhs_it = rhs.begin(); lhs_it != lhs.end(); ++lhs_it, ++rhs_it) {
          if (auto const order = lhs_it->first <=> rhs_it->first; order != 0) return order;
        }
        for (auto lhs_it = lhs.begin(), rhs_it = rhs.begin(); lhs_it != lhs.end(); ++lhs_it, ++rhs_it) {
          if (auto const order = Compare(lhs_it->second, rhs_it->second); order != 0) return order;
        }
        return std::weak_ordering::equivalent;
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

      // The three types that can hold a NaN, which has to be given a place here
      // rather than left beside nothing as comparability leaves it.
      case Double:
        return CompareDoublesNaNLast(a.UnsafeValueDouble(), b.UnsafeValueDouble());
      case Point2d: {
        auto const &lhs = a.UnsafeValuePoint2d();
        auto const &rhs = b.UnsafeValuePoint2d();
        if (auto const order = lhs.crs() <=> rhs.crs(); order != 0) return order;
        if (auto const order = CompareDoublesNaNLast(lhs.x(), rhs.x()); order != 0) return order;
        return CompareDoublesNaNLast(lhs.y(), rhs.y());
      }
      case Point3d: {
        auto const &lhs = a.UnsafeValuePoint3d();
        auto const &rhs = b.UnsafeValuePoint3d();
        if (auto const order = lhs.crs() <=> rhs.crs(); order != 0) return order;
        if (auto const order = CompareDoublesNaNLast(lhs.x(), rhs.x()); order != 0) return order;
        if (auto const order = CompareDoublesNaNLast(lhs.y(), rhs.y()); order != 0) return order;
        return CompareDoublesNaNLast(lhs.z(), rhs.z());
      }

      // Ordered by what they hold, which comparability orders them by too. Each
      // names its type so the comparison is reached from this switch rather
      // than from a second one, and none of them holds a value that sits
      // outside its own order.
      case Bool:
        return detail::AlreadyTotal(comparability::ComparePayloadOf<Bool>(a, b));
      case Int:
        return detail::AlreadyTotal(comparability::ComparePayloadOf<Int>(a, b));
      case String:
        return detail::AlreadyTotal(comparability::ComparePayloadOf<String>(a, b));
      case Date:
        return detail::AlreadyTotal(comparability::ComparePayloadOf<Date>(a, b));
      case LocalTime:
        return detail::AlreadyTotal(comparability::ComparePayloadOf<LocalTime>(a, b));
      case LocalDateTime:
        return detail::AlreadyTotal(comparability::ComparePayloadOf<LocalDateTime>(a, b));
      case ZonedDateTime:
        return detail::AlreadyTotal(comparability::ComparePayloadOf<ZonedDateTime>(a, b));
      case Duration:
        return detail::AlreadyTotal(comparability::ComparePayloadOf<Duration>(a, b));
      case Enum:
        return detail::AlreadyTotal(comparability::ComparePayloadOf<Enum>(a, b));
    }
  }

  // Unlike types are separated by where their types sit, which puts a null
  // after everything and a map before it.
  if (auto const order = SortRank(a.type()) <=> SortRank(b.type()); order != 0) return order;

  // The one rank two types share: an integer against a double, where the double
  // may again be a NaN.
  if (a.IsInt()) {
    return CompareDoublesNaNLast(static_cast<double>(a.UnsafeValueInt()), b.UnsafeValueDouble());
  }
  return CompareDoublesNaNLast(a.UnsafeValueDouble(), static_cast<double>(b.UnsafeValueInt()));
}

}  // namespace orderability

}  // namespace memgraph::query::relations
