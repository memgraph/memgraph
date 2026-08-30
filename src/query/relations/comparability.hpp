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
#pragma once

#include <compare>
#include <optional>

#include "query/fmt.hpp"
#include "query/typed_value.hpp"

namespace memgraph::query::relations::comparability {

template <TypedValue::Type>
inline constexpr bool kNoPayloadOrder = false;

/**
 * Orders two values of one named type by what they hold.
 *
 * The type is a template argument, so a caller whose own switch has already
 * selected it reaches the comparison without a second dispatch. Every type that
 * carries an order of its own has that order written here once, and
 * ComparePayload below is the runtime dispatch onto them for a caller whose
 * type is not settled until it runs.
 *
 * The two values must be of type T.
 */
template <TypedValue::Type T>
inline std::partial_ordering ComparePayloadOf(const TypedValue &a, const TypedValue &b) {
  using enum TypedValue::Type;
  if constexpr (T == Bool) {
    return a.UnsafeValueBool() <=> b.UnsafeValueBool();
  } else if constexpr (T == Int) {
    return a.UnsafeValueInt() <=> b.UnsafeValueInt();
  } else if constexpr (T == Double) {
    return a.UnsafeValueDouble() <=> b.UnsafeValueDouble();
  } else if constexpr (T == String) {
    return a.UnsafeValueString() <=> b.UnsafeValueString();
  } else if constexpr (T == Date) {
    return a.UnsafeValueDate() <=> b.UnsafeValueDate();
  } else if constexpr (T == LocalTime) {
    return a.UnsafeValueLocalTime() <=> b.UnsafeValueLocalTime();
  } else if constexpr (T == LocalDateTime) {
    return a.UnsafeValueLocalDateTime() <=> b.UnsafeValueLocalDateTime();
  } else if constexpr (T == ZonedDateTime) {
    return a.UnsafeValueZonedDateTime() <=> b.UnsafeValueZonedDateTime();
  } else if constexpr (T == Duration) {
    return a.UnsafeValueDuration() <=> b.UnsafeValueDuration();
  } else if constexpr (T == Enum) {
    return a.UnsafeValueEnum() <=> b.UnsafeValueEnum();
  } else if constexpr (T == Point2d) {
    return a.UnsafeValuePoint2d() <=> b.UnsafeValuePoint2d();
  } else if constexpr (T == Point3d) {
    return a.UnsafeValuePoint3d() <=> b.UnsafeValuePoint3d();
  } else {
    static_assert(kNoPayloadOrder<T>, "This type carries no order of its own");
  }
}

/**
 * Whether comparability places values of a type at all.
 *
 * This is the same set ComparePayload answers for. Neither switch names a
 * default, so a type added to the enumeration fails to compile in both rather
 * than silently gaining an answer in one.
 */
inline bool Admits(TypedValue::Type type) {
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
      return false;
  }
}

/**
 * Orders two values of one type by what they hold, for the types
 * comparability admits.
 *
 * Nothing is returned for a type it does not admit, which is every type that
 * carries no order of its own plus the three orderability alone places. Those
 * three reach their order through ComparePayloadOf, which holds each type's
 * own ordering once for both relations to read.
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
    if (auto const order = ComparePayload(a, b)) return order;

    if (a.IsList()) {
      // Element by element from the start, in dictionary order.
      auto const &lhs = a.UnsafeValueList();
      auto const &rhs = b.UnsafeValueList();
      auto const common = std::min(lhs.size(), rhs.size());
      for (size_t i = 0; i != common; ++i) {
        auto const order = Compare(lhs[i], rhs[i]);
        // A pair the ordering cannot decide, or has no place for, leaves the
        // lists undecided as well.
        if (!order || *order == std::partial_ordering::unordered) return std::nullopt;
        if (*order != std::partial_ordering::equivalent) return order;
      }
      // One list is the start of the other, and the shorter comes first
      // whatever the element that would have followed it is. That element is
      // never compared against, so a Null there decides nothing.
      return lhs.size() <=> rhs.size();
    }

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
  if (!(a.IsNumeric() && b.IsNumeric())) return std::nullopt;
  return ToDouble(a) <=> ToDouble(b);
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
