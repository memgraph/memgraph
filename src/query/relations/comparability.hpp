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
/// Comparability: one of the four relations openCypher defines over values, the
/// one `< <= > >=` each read.
///
/// It is partial. `unordered` means two values have no order between them, which
/// is what a NaN is, and all four comparisons answer false for such a pair.
/// Nothing at all is returned when the two are incomparable, and all four then
/// answer Null. No pair raises: incomparability is an answer the relation gives
/// rather than a question it refuses.
///
/// A list is placed by its elements, in the dictionary order the specification
/// gives: pairwise from the start, and a shorter list first where the two agree
/// up to its end. An element pair this relation leaves undecided leaves the two
/// lists undecided, which is what makes `[1, 2] >= [1, null]` Null while
/// `[1] < [1, null]` is true: the second compares no element to the null.
#pragma once

#include <cmath>
#include <compare>
#include <optional>

#include "query/relations/payload_order.hpp"
#include "query/typed_value.hpp"

namespace memgraph::query::relations::comparability {

/**
 * Whether comparability places values of a type at all.
 *
 * This is the same set ComparePayload answers for. Neither switch names a
 * default, so a type added to the enumeration fails to compile in both rather
 * than silently gaining an answer in one.
 */
constexpr bool ValidFor(TypedValue::Type type) {
  switch (type) {
    using enum TypedValue::Type;
    case Bool:
    case Int:
    case Double:
    case String:
    case Date:
    case LocalTime:
    case LocalDateTime:
    case ZonedDateTime:
    case Duration:
      return true;

    case List:
      return true;

    case Null:
    case Enum:
    case Point2d:
    case Point3d:
    case Map:
    case Vertex:
    case Edge:
    case VirtualEdge:
    case VirtualNode:
    case Path:
    case Graph:
    case VirtualGraph:
    case Function:
      return false;
  }
}

/// Places two lists, in the dictionary order the specification gives.
///
/// Out of line so that Compare does not call itself. A compiler will not inline
/// a function that recurses, and every filter reaches Compare through the
/// caller.
///
/// Takes what it walks rather than the values holding it, so that it cannot be
/// handed a pair of unlike things.
std::optional<std::partial_ordering> CompareOfLists(TypedValue::TVector const &a, TypedValue::TVector const &b);

/**
 * Whether comparability places a value against the values of its own type.
 *
 * A type being valid is not enough to say this, because one admitted type holds a
 * value with no order: a NaN is unordered against every number and against
 * itself, so all four comparisons answer false for a pair holding one and a
 * filter keeps no row.
 */
inline bool ValidFor(const TypedValue &value) {
  if (!ValidFor(value.type())) return false;
  return value.type() != TypedValue::Type::Double || !std::isnan(value.UnsafeValueDouble());
}

/**
 * Whether a band drawn around this bound hands back the rows a filter reading it
 * would keep, and only those.
 *
 * Being placed is not enough. The stored order decides every pair, including the
 * ones this relation leaves undecided, and a filter drops a row it cannot decide.
 * Where the two part, a band holds rows no filter keeps.
 *
 * A list is where they part. A null element is ordered after every number in the
 * store, so `[1, null]` sits above `[1, 2]` there, while a filter reading
 * `> [1, 2]` cannot decide it and drops it. Both the rows a band keeps and the
 * rows it drops lie on one side of the bound, so no fence separates them, and a
 * list bound is left to the filter until a scan can read the pairs a band cannot.
 *
 * A value the relation cannot place at all is refused for the older reason: a
 * band drawn around it holds whatever the stored order happens to put there.
 */
inline bool AnIndexCanFence(const TypedValue &bound) {
  return ValidFor(bound) && bound.type() != TypedValue::Type::List;
}

/// The same question where the type is already known at compile time.
template <TypedValue::Type T>
constexpr bool ValidFor() {
  return ValidFor(T);
}

/**
 * Orders two values of one type by what they hold, for the types
 * comparability admits.
 *
 * Nothing is returned for a type it is not valid for, which is every type
 * carrying no order of its own plus enums and the two point types, which
 * orderability places and this relation does not.
 *
 * The two values must be of the same type.
 *
 * Inlined on demand rather than at the compiler's discretion. Its caller has
 * already switched on the type, so folding this in leaves one dispatch where
 * there would otherwise be two and a call between them. Left to its own
 * judgement the compiler declines, reading the arm count as bulk, and a filter
 * asks this once per row.
 */
[[gnu::always_inline]] inline std::optional<std::partial_ordering> ComparePayload(const TypedValue &a,
                                                                                  const TypedValue &b) {
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

    case Null:
    case Enum:
    case Point2d:
    case Point3d:
    case List:
    case Map:
    case Vertex:
    case Edge:
    case VirtualEdge:
    case VirtualNode:
    case Path:
    case Graph:
    case VirtualGraph:
    case Function:
      return std::nullopt;
  }
}

/**
 * Where one value falls relative to another under comparability, the relation
 * the four ordered comparisons below are each one reading of.
 *
 * The ordering is partial. `unordered` means the two have no order between
 * them, which is what a NaN is, and all four comparisons are false for such a
 * pair. Nothing at all is returned when the two are incomparable, and all four
 * are then Null: a Null operand, a pair of unlike types, and a pair of one type
 * that carries no order of its own are each incomparable.
 *
 * Every pair is answered for. Raising for some pairs and answering Null for
 * others would make what a filter does depend on which types a column happened
 * to hold, and would leave a scan fenced to one type passing over a row the
 * filter it stands in for could not reach at all.
 */
inline std::optional<std::partial_ordering> Compare(const TypedValue &a, const TypedValue &b) {
  // Two values of one admitted type are the common case and the whole answer.
  if (a.type() == b.type()) {
    // The one type it places that carries no payload: a list is placed by what
    // it holds rather than by anything read off the value itself.
    if (a.type() == TypedValue::Type::List) return CompareOfLists(a.UnsafeValueList(), b.UnsafeValueList());

    if (auto const order = ComparePayload(a, b)) return order;

    // A Null orders against nothing, itself included, and a type carrying no
    // order of its own places no pair of its values either. Both are
    // incomparable, and this relation says so by having no answer to give.
    //
    // Two equal values of such a type are the one place this parts from the
    // rule that equality and comparability agree: `=` holds them equal while
    // all four ordered comparisons answer Null. The reference implementation
    // answers the same way, and closing the gap would mean an index scan
    // reading `<=` over a point column had to ask the comparison of every
    // candidate it fenced.
    return std::nullopt;
  }

  // Numbers are the only unlike pair the relation places. Every other pair of
  // unlike types is incomparable, as is any pair involving a Null.
  if (!AreMixedNumbers(a.type(), b.type())) return std::nullopt;
  return ComparePayloadOfMixedNumbers(a, b);
}

/** Reads a comparison the way one operator asks it, carrying `a`'s memory resource. */
template <typename Reading>
inline TypedValue FromComparison(const TypedValue &a, std::optional<std::partial_ordering> order, Reading reading) {
  if (!order) return TypedValue(a.get_allocator());
  return TypedValue(reading(*order), a.get_allocator());
}

}  // namespace memgraph::query::relations::comparability

namespace memgraph::query {

// The presentation surface over comparability. Defined here rather than in the
// class so that the relation these four read is inlined with them: a filter asks
// one of these once per row.
//
// Each answers true, false or Null, carrying the memory resource its left
// operand was allocated from. Null is the answer wherever the relation has none
// to give, which is a Null operand, a pair of unlike types, and a pair of one
// type that carries no order of its own. A NaN has no order against anything,
// itself included, so all four answer false for a pair holding one. None of
// them raises.

inline TypedValue operator<(const TypedValue &a, const TypedValue &b) {
  return relations::comparability::FromComparison(
      a, relations::comparability::Compare(a, b), [](auto order) { return std::is_lt(order); });
}

inline TypedValue operator<=(const TypedValue &a, const TypedValue &b) {
  return relations::comparability::FromComparison(
      a, relations::comparability::Compare(a, b), [](auto order) { return std::is_lteq(order); });
}

inline TypedValue operator>(const TypedValue &a, const TypedValue &b) {
  return relations::comparability::FromComparison(
      a, relations::comparability::Compare(a, b), [](auto order) { return std::is_gt(order); });
}

inline TypedValue operator>=(const TypedValue &a, const TypedValue &b) {
  return relations::comparability::FromComparison(
      a, relations::comparability::Compare(a, b), [](auto order) { return std::is_gteq(order); });
}

}  // namespace memgraph::query
