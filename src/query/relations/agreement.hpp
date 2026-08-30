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
/// Where equality and equivalence give the same answer.
///
/// The relations openCypher defines over values agree everywhere except over
/// two of them. A Null is one: equality answers Null about it and decides
/// nothing, where equivalence holds two Nulls alike. A NaN is the other:
/// equality reads it as different from itself, where the order places every NaN
/// together and equivalence follows the order. A structure built on one relation
/// and asked a question belonging to another is sound exactly where they agree,
/// which is what this answers.
#pragma once

#include <algorithm>
#include <cmath>
#include <variant>

#include "query/typed_value.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::query::relations {

namespace detail {

/// Whether a value carries a NaN anywhere, itself included.
///
/// A point is walked too: it holds its coordinates as doubles, any of which can
/// be one.
inline bool ContainsNaN(TypedValue const &value) {
  switch (value.type()) {
    using enum TypedValue::Type;
    case Double:
      return std::isnan(value.ValueDouble());
    case List:
      return std::ranges::any_of(value.ValueList(), [](auto const &element) { return ContainsNaN(element); });
    case Map:
      return std::ranges::any_of(value.ValueMap(), [](auto const &kv) { return ContainsNaN(kv.second); });
    case Point2d: {
      auto const point = value.ValuePoint2d();
      return std::isnan(point.x()) || std::isnan(point.y());
    }
    case Point3d: {
      auto const point = value.ValuePoint3d();
      return std::isnan(point.x()) || std::isnan(point.y()) || std::isnan(point.z());
    }
    case Null:
    case Bool:
    case Int:
    case String:
    case Vertex:
    case Edge:
    case Path:
    case Date:
    case LocalTime:
    case LocalDateTime:
    case ZonedDateTime:
    case Duration:
    case Graph:
    case VirtualGraph:
    case Function:
    case Enum:
    case VirtualEdge:
    case VirtualNode:
      return false;
  }
  return false;
}

/// Whether a value carries a Null anywhere, itself included.
///
/// Exhaustive rather than defaulted, so that a type added later has to be
/// considered here rather than quietly answering that it holds none.
inline bool ContainsNull(TypedValue const &value) {
  switch (value.type()) {
    using enum TypedValue::Type;
    case Null:
      return true;
    case List:
      return std::ranges::any_of(value.ValueList(), [](auto const &element) { return ContainsNull(element); });
    case Map:
      return std::ranges::any_of(value.ValueMap(), [](auto const &kv) { return ContainsNull(kv.second); });
    case Bool:
    case Int:
    case Double:
    case String:
    case Vertex:
    case Edge:
    case Path:
    case Date:
    case LocalTime:
    case LocalDateTime:
    case ZonedDateTime:
    case Duration:
    case Graph:
    case VirtualGraph:
    case Function:
    case Enum:
    case Point2d:
    case Point3d:
    case VirtualEdge:
    case VirtualNode:
      return false;
  }
  return false;
}

/// The same question over a stored value.
inline bool ContainsNaN(storage::PropertyValue const &value) {
  switch (value.type()) {
    using enum storage::PropertyValueType;
    case Double:
      return std::isnan(value.ValueDouble());
    case List:
      return std::ranges::any_of(value.ValueList(), [](auto const &element) { return ContainsNaN(element); });
    case Map:
      return std::ranges::any_of(value.ValueMap(), [](auto const &kv) { return ContainsNaN(kv.second); });
    case DoubleList:
      return std::ranges::any_of(value.ValueDoubleList(), [](double element) { return std::isnan(element); });
    case NumericList:
      return std::ranges::any_of(value.ValueNumericList(), [](auto const &element) {
        return std::holds_alternative<double>(element) && std::isnan(std::get<double>(element));
      });
    case Point2d: {
      auto const point = value.ValuePoint2d();
      return std::isnan(point.x()) || std::isnan(point.y());
    }
    case Point3d: {
      auto const point = value.ValuePoint3d();
      return std::isnan(point.x()) || std::isnan(point.y()) || std::isnan(point.z());
    }
    // An integer list holds no double, so none of its elements can be one.
    case IntList:
    case Null:
    case Bool:
    case Int:
    case String:
    case TemporalData:
    case ZonedTemporalData:
    case Enum:
    case VectorIndexId:
      return false;
  }
  return false;
}

/// Whether a stored value carries a Null anywhere, itself included.
inline bool ContainsNull(storage::PropertyValue const &value) {
  switch (value.type()) {
    using enum storage::PropertyValueType;
    case Null:
      return true;
    case List:
      return std::ranges::any_of(value.ValueList(), [](auto const &element) { return ContainsNull(element); });
    case Map:
      return std::ranges::any_of(value.ValueMap(), [](auto const &kv) { return ContainsNull(kv.second); });
    // The numeric lists hold their elements unboxed, so none of them can be a Null.
    case IntList:
    case DoubleList:
    case NumericList:
    case Bool:
    case Int:
    case Double:
    case String:
    case TemporalData:
    case ZonedTemporalData:
    case Enum:
    case Point2d:
    case Point3d:
    case VectorIndexId:
      return false;
  }
}

}  // namespace detail

/// Whether the two layers keep values of a type in one order.
///
/// A stored value carries the order an index holds its entries in, and a query
/// value carries the order a sort reads. They are one order for most types, and
/// not for a map, a temporal or a list.
///
/// A map is kept by the identifiers its keys were given, which is the order
/// they were first seen in, where a sort reads their names; nothing either side
/// does reconciles that while one of them cannot read the other's keys. One
/// stored type carries the four temporal types and tells them apart by the
/// enumeration they are declared in, where a sort gives each its own place. A
/// list can hold either, and what it holds is not a thing a plan can see, so it
/// is not relied upon.
///
/// A plan that drops a sort because an index already walked the column is sound
/// only for the types this admits.
inline bool LayersKeepThisTypeInOneOrder(storage::PropertyValueType type) {
  switch (type) {
    using enum storage::PropertyValueType;
    case Map:
    case TemporalData:
    case List:
    case IntList:
    case DoubleList:
    case NumericList:
    // Read back as a list, so it is refused for the same reason one is.
    case VectorIndexId:
      return false;

    case Null:
    case Bool:
    case Int:
    case Double:
    case String:
    case ZonedTemporalData:
    case Enum:
    case Point2d:
    case Point3d:
      return true;
  }
  return false;
}

/// Whether a value carries a Null anywhere, itself included.
///
/// Equivalence holds two Nulls alike where equality answers Null and decides
/// nothing, so a structure keyed by the one cannot answer for the other about
/// such a value. A comparison turning on a Null decides nothing either, but only
/// against what sits alongside it, which is why a caller may want this apart
/// from the NaN below rather than the two together.
inline bool HoldsANull(TypedValue const &value) { return detail::ContainsNull(value); }

inline bool HoldsANull(storage::PropertyValue const &value) { return detail::ContainsNull(value); }

/// Whether a value carries a NaN anywhere, itself included.
///
/// Equality reads a NaN as different from itself where the order places every
/// NaN together and equivalence follows the order. Every comparison against one
/// is false, so a range built from a comparison holds nothing at all, which is a
/// different consequence from the one a Null has and the reason these are named
/// apart.
inline bool HoldsANaN(TypedValue const &value) { return detail::ContainsNaN(value); }

inline bool HoldsANaN(storage::PropertyValue const &value) { return detail::ContainsNaN(value); }

/// Whether a value is itself a NaN, rather than a container carrying one.
///
/// Every comparison against a NaN is false, so a range with one for a bound
/// admits nothing at all. A container carrying one says nothing of the sort: a
/// comparison against it is settled by the first element the two do not share,
/// or by their lengths, and only reaches the NaN if nothing before it decided.
/// Such a range admits what its own comparison admits, which is why the two
/// questions are named apart.
inline bool IsANaN(TypedValue const &value) { return value.IsDouble() && std::isnan(value.ValueDouble()); }

inline bool IsANaN(storage::PropertyValue const &value) { return value.IsDouble() && std::isnan(value.ValueDouble()); }

/// Whether comparability orders two stored values of this type against one
/// another.
///
/// An index keeps its entries in a total order, so it can place a pair the
/// `< <= > >=` family refuses outright. A scan standing in for one of those
/// comparisons may only run where the comparison itself would have answered;
/// where it would have raised, an index that quietly returns rows has answered
/// a question Cypher does not define.
enum class RangeReading : std::uint8_t {
  /// The comparison operators carry no order over this type, so a scan reading
  /// one raises rather than answering from the order an index keeps.
  Refused,
  /// The bounds are the whole answer: the comparison settles every pair the
  /// band between them admits.
  BoundsAlone,
  /// One stored type carries several of the types a query tells apart, and the
  /// comparison places no pair drawn from two of them. The band belonging to
  /// the bound's own kind is the whole of what the comparison admits, so the
  /// range is narrowed to it.
  BoundsNarrowedToKind,
  /// The band admits pairs the comparison declines to place, so every candidate
  /// it gives has to be put to the comparison as well. Two lists are ordered
  /// element by element while the comparison declines wherever an element is a
  /// Null or the two elements are of unlike types.
  BoundsThenComparison,
};

inline RangeReading HowARangeReadsStoredType(storage::PropertyValueType type) {
  switch (type) {
    using enum storage::PropertyValueType;
    case Null:
    case Bool:
    case Int:
    case Double:
    case String:
    case ZonedTemporalData:
      return RangeReading::BoundsAlone;

    case TemporalData:
      return RangeReading::BoundsNarrowedToKind;

    case List:
    case IntList:
    case DoubleList:
    case NumericList:
      return RangeReading::BoundsThenComparison;

    // Each of these carries an order that only a sort reads. The comparison
    // operators refuse them, so a scan reading this must refuse them too.
    case Map:
    case Enum:
    case Point2d:
    case Point3d:
    case VectorIndexId:
      return RangeReading::Refused;
  }
  return RangeReading::Refused;
}

inline bool ComparabilityOrdersStoredType(storage::PropertyValueType type) {
  return HowARangeReadsStoredType(type) != RangeReading::Refused;
}

/// The same reading, asked of a value a query holds rather than a stored one.
///
/// A bound is written as a query value and only becomes a stored one where an
/// index is read, so the two are asked the same question at different points.
/// The four temporal types a query tells apart are the ones a single stored type
/// carries, which is why each of them narrows a range to its own kind.
inline RangeReading HowARangeReadsQueryType(TypedValue::Type type) {
  switch (type) {
    using enum TypedValue::Type;
    case Null:
    case Bool:
    case Int:
    case Double:
    case String:
    case ZonedDateTime:
      return RangeReading::BoundsAlone;

    case Date:
    case LocalTime:
    case LocalDateTime:
    case Duration:
      return RangeReading::BoundsNarrowedToKind;

    case List:
      return RangeReading::BoundsThenComparison;

    case Map:
    case Enum:
    case Point2d:
    case Point3d:
    case Vertex:
    case Edge:
    case VirtualEdge:
    case VirtualNode:
    case Path:
    case Graph:
    case VirtualGraph:
    case Function:
      return RangeReading::Refused;
  }
  return RangeReading::Refused;
}

/// Whether a structure matching by equivalence may answer an equality question
/// about this value.
///
/// The two relations agree everywhere except over the two values above, so a
/// value carrying neither is one such a structure may answer for: every entry it
/// returns is equal and every entry it passes over is not. Carrying either, the
/// caller has to ask equality directly instead.
///
/// The lookup structures in the engine are keyed by equivalence because that is
/// what a hash table can do. This is the condition under which they are entitled
/// to answer.
inline bool EqualityAgreesWithEquivalence(TypedValue const &value) { return !HoldsANull(value) && !HoldsANaN(value); }

/// The same condition over a stored value, which is what an index holds.
inline bool EqualityAgreesWithEquivalence(storage::PropertyValue const &value) {
  return !HoldsANull(value) && !HoldsANaN(value);
}

}  // namespace memgraph::query::relations
