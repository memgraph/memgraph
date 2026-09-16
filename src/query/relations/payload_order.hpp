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
/// This is not one of the relations. It is the fact two of them share: how two
/// dates sit relative to each other is one answer, and comparability reading it
/// one way while orderability reads it another would sort a column differently
/// from how a filter selects it. The relations differ in which types they
/// place and in what they answer for a pair they cannot, and each states that
/// for itself.
#pragma once

#include <compare>

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
  return a.type() == TypedValue::Type::Int ? a.UnsafeValueInt() <=> b.UnsafeValueDouble()
                                           : a.UnsafeValueDouble() <=> b.UnsafeValueInt();
}

/// Whether the two are one Int and one Double, which is the only unlike pair of
/// types either relation orders.
inline bool AreMixedNumbers(TypedValue::Type a, TypedValue::Type b) {
  using enum TypedValue::Type;
  return (a == Int && b == Double) || (a == Double && b == Int);
}

}  // namespace memgraph::query::relations
