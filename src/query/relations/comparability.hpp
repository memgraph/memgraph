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
/// Lists compare lexicographically: the first unequal element decides, and a
/// shorter prefix sorts first. An undecided element pair makes the lists
/// undecided, so `[1, 2] >= [1, null]` is Null, while `[1] < [1, null]` is true
/// because the null is never compared. A NaN element also makes them undecided:
/// `[1] < [NaN]` is Null, although `1 < NaN` is false.
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
 * This is the same set Compare answers for: ComparePayload's, plus List. Neither
 * switch names a default, so a type added to the enumeration fails to compile in
 * both rather than silently gaining an answer in one.
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
    case List:  // compared by CompareOfLists, not ComparePayload
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

/// Lexicographic order: the first unequal element decides; a shorter prefix sorts first.
/// Never answers `unordered`: a NaN element leaves the lists undecided.
/// Out of line: Compare recurses through it for nested lists, and a self-recursive
/// Compare would not be inlined into the comparison operators.
/// Takes the vectors so both arguments are lists by type.
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
 * Whether an index range bounded by this value returns exactly the rows the filter keeps.
 *
 * False for a list. The index sorts `[1, null]` above `[1, 2]`, but the filter
 * `> [1, 2]` answers Null for it and drops it. Rows kept and dropped interleave
 * on the same side of the bound, so no range separates them; the scan must
 * evaluate the comparison per row instead.
 *
 * False for a value ValidFor rejects, such as NaN: every comparison against it
 * fails, while the index still places it somewhere.
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
    // A list compares by its elements, which ComparePayload cannot do.
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
// type that carries no order of its own. A scalar NaN has no order against
// anything, itself included, so all four answer false for a pair holding one;
// inside a list it makes the pair Null. None of them raises.

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
