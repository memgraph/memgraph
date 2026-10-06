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
/// How two numbers are placed relative to each other.
///
/// A value is held one way by the store and another by a query, and the two are
/// compared by separate code reading separate types. What the two must never
/// differ on is the arithmetic: a pair of numbers one places differently from
/// the other is a row an index returns and a filter does not. That arithmetic
/// is here, in terms of the numbers alone, so that neither layer carries a copy
/// of it to drift from.
#pragma once

#include <cmath>
#include <compare>
#include <cstdint>
#include <limits>

namespace memgraph::value_order {

/// One past the widest integer, exactly a double.
///
/// Taken from the smallest integer rather than the largest, because that one is
/// a power of two and survives the conversion exactly; the largest is one short
/// of it and would round.
inline constexpr auto kJustPastTheWidestInteger = -static_cast<double>(std::numeric_limits<std::int64_t>::min());

/// Whether the integer range holds this double, so that a whole one can be read
/// as the integer it equals.
///
/// Reading a double outside the range as an integer is undefined, and an
/// infinity is outside it however whole it looks.
inline constexpr bool AnIntegerCanHold(double value) noexcept {
  return value >= -kJustPastTheWidestInteger && value < kJustPastTheWidestInteger;
}

/// The same order read from the other side.
inline constexpr std::partial_ordering ReversedOrder(std::partial_ordering order) noexcept {
  if (std::is_lt(order)) return std::partial_ordering::greater;
  if (std::is_gt(order)) return std::partial_ordering::less;
  return order;
}

inline constexpr std::weak_ordering ReversedOrder(std::weak_ordering order) noexcept {
  if (std::is_lt(order)) return std::weak_ordering::greater;
  if (std::is_gt(order)) return std::weak_ordering::less;
  return order;
}

/// Places an integer against a double by what each holds, rather than by
/// reading one of them at the other's type.
///
/// The usual arithmetic conversions widen the integer, which is exact only
/// while the doubles are still spaced one apart. Past that point distinct
/// integers arrive at one double, and a relation reading the pair that way
/// holds them equal: equality then holds two values equal that are not equal to
/// each other, and a sort is handed a pair it treats as interchangeable while
/// telling the two apart, which is not a strict weak ordering.
///
/// @return unordered only where the double is a NaN.
inline std::partial_ordering PlaceIntegerAgainstDouble(std::int64_t whole, double other) noexcept {
  if (std::isnan(other)) [[unlikely]]
    return std::partial_ordering::unordered;

  // A double outside the integer range cannot be made into one at all, so the
  // range is settled before the conversion below rather than trusted to it.
  if (other >= kJustPastTheWidestInteger) [[unlikely]]
    return std::partial_ordering::less;
  if (other < -kJustPastTheWidestInteger) [[unlikely]]
    return std::partial_ordering::greater;

  auto const truncated = static_cast<std::int64_t>(other);
  if (auto const by_whole_part = whole <=> truncated; std::is_neq(by_whole_part)) return by_whole_part;

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

/// Orders two doubles, giving a NaN the place IEEE gives it nowhere: after
/// every number, and alongside every other NaN.
///
/// An ordered structure needs an answer for every pair it is handed, and a pair
/// it has none for is a pair it treats as interchangeable. A NaN left unordered
/// would be interchangeable with every number while no two numbers are with
/// each other.
inline std::weak_ordering CompareDoublesNaNLast(double lhs, double rhs) noexcept {
  if (auto const order = lhs <=> rhs; order != std::partial_ordering::unordered) [[likely]] {
    if (std::is_lt(order)) return std::weak_ordering::less;
    if (std::is_gt(order)) return std::weak_ordering::greater;
    return std::weak_ordering::equivalent;
  }

  auto const lhs_is_nan = std::isnan(lhs);
  if (lhs_is_nan && std::isnan(rhs)) return std::weak_ordering::equivalent;
  return lhs_is_nan ? std::weak_ordering::greater : std::weak_ordering::less;
}

/// The same placement, for a caller that has an order for every pair.
inline std::weak_ordering PlaceIntegerAgainstDoubleNaNLast(std::int64_t whole, double other) noexcept {
  auto const placed = PlaceIntegerAgainstDouble(whole, other);
  if (std::is_lt(placed)) return std::weak_ordering::less;
  if (std::is_gt(placed)) return std::weak_ordering::greater;
  if (std::is_eq(placed)) return std::weak_ordering::equivalent;
  // Unordered only for a NaN, and every number comes before one.
  return std::weak_ordering::less;
}

}  // namespace memgraph::value_order
