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
/// The order a type's own values carry, for the types that carry one.
///
/// This is not one of the relations. It is the fact they share: how two
/// dates sit relative to each other is one answer, and comparability reading it
/// one way while orderability reads it another would sort a column differently
/// from how a filter selects it. The relations differ in which types they
/// place and in what they answer for a pair they cannot, and each states that
/// for itself.
///
/// One number against another of the other numeric type is here for the same
/// reason, and equality reads it too, since a pair it holds equal has to be a
/// pair the other two put in one place.
#pragma once

#include <cmath>
#include <compare>
#include <cstdint>

#include "query/typed_value.hpp"

namespace memgraph::query::relations {

template <TypedValue::Type>
inline constexpr bool kNoPayloadOrder = false;

/**
 * Orders two values of one named type by what they hold.
 *
 * The type is a template argument, so a caller whose own switch has already
 * selected it reaches the comparison without a second dispatch. A type that
 * carries no order of its own does not compile here, which is what stops a
 * relation dispatching to an arm that does not exist.
 *
 * The two values must both be of type T.
 *
 * Inlined on demand rather than at the compiler's discretion. The caller has
 * already settled the type, so folding this in leaves the comparison where the
 * switch lands rather than a call after it. Left to its own judgement the
 * compiler declines for the bulkier payloads, a string among them, and a sort
 * asks this once per comparison.
 */
template <TypedValue::Type T>
[[gnu::always_inline]] inline std::partial_ordering ComparePayloadOf(const TypedValue &a, const TypedValue &b) {
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

/// The same order read from the other side.
inline std::partial_ordering ReversedOrder(std::partial_ordering order) {
  if (std::is_lt(order)) return std::partial_ordering::greater;
  if (std::is_gt(order)) return std::partial_ordering::less;
  return order;
}

/**
 * Places an integer against a double by what each holds, rather than by reading
 * one of them at the other's type.
 *
 * The usual arithmetic conversions widen the integer, which is exact only while
 * the doubles are still spaced one apart. Past that point distinct integers
 * arrive at one double, and a relation reading the pair that way holds them
 * equal: equality then holds two values equal that are not equal to each other,
 * and a sort is handed a pair it treats as interchangeable while telling the
 * two apart, which is not a strict weak ordering.
 *
 * @return unordered only where the double is a NaN.
 */
inline std::partial_ordering PlaceIntegerAgainstDouble(int64_t whole, double other) {
  if (std::isnan(other)) [[unlikely]]
    return std::partial_ordering::unordered;

  // One past the widest integer, exactly a double. A double outside the range it
  // fences cannot be made into an integer at all, so the range is settled before
  // the conversion below rather than trusted to it.
  constexpr auto kJustPastTheWidest = 9223372036854775808.0;
  if (other >= kJustPastTheWidest) [[unlikely]]
    return std::partial_ordering::less;
  if (other < -kJustPastTheWidest) [[unlikely]]
    return std::partial_ordering::greater;

  auto const truncated = static_cast<int64_t>(other);
  if (auto const by_whole_part = whole <=> truncated; by_whole_part != 0) return by_whole_part;

  // The two share a whole part, so whatever the double carries past it decides.
  // Truncation is toward zero, so the remainder takes the double's own sign.
  //
  // Reading the whole part back as a double is exact either way: where the
  // doubles are still spaced one apart it is small enough to carry, and past
  // that point the double was already whole and the remainder is zero.
  auto const remainder = other - static_cast<double>(truncated);
  if (remainder > 0) return std::partial_ordering::less;
  if (remainder < 0) return std::partial_ordering::greater;
  return std::partial_ordering::equivalent;
}

/**
 * Orders one number against another of the other numeric type.
 *
 * Written here rather than at each relation because the widening is part of how
 * numbers are ordered. The payloads are read directly, and the caller settles
 * which type each holds, because the accessors that would check are defined in
 * the value's own translation unit and are calls from anywhere else.
 *
 * @pre One of the two is an Int and the other a Double.
 */
inline std::partial_ordering ComparePayloadOfMixedNumbers(const TypedValue &a, const TypedValue &b) {
  return a.type() == TypedValue::Type::Int
             ? PlaceIntegerAgainstDouble(a.UnsafeValueInt(), b.UnsafeValueDouble())
             : ReversedOrder(PlaceIntegerAgainstDouble(b.UnsafeValueInt(), a.UnsafeValueDouble()));
}

/// Whether the two are one Int and one Double, which is the only unlike pair of
/// types either relation orders.
inline bool AreMixedNumbers(TypedValue::Type a, TypedValue::Type b) {
  using enum TypedValue::Type;
  return (a == Int && b == Double) || (a == Double && b == Int);
}

}  // namespace memgraph::query::relations
