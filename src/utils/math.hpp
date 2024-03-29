// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <boost/math/special_functions/math_fwd.hpp>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <type_traits>

#include <boost/math/special_functions/relative_difference.hpp>

namespace memgraph::utils {

static_assert(std::is_same_v<uint64_t, unsigned long>,
              "utils::Log requires uint64_t to be implemented as unsigned long.");

/// This function computes the log2 function on integer types. It is faster than
/// the cmath `log2` function because it doesn't use floating point values for
/// calculation.
constexpr uint64_t Log2(uint64_t val) {
  // The `clz` function is undefined when the passed value is 0 and the value of
  // `log` is `-inf` so we special case it here.
  if (val == 0) return 0;
  // clzl = count leading zeros from long
  //        ^     ^       ^          ^
  int ret = __builtin_clzl(val);
  return 64UL - static_cast<uint64_t>(ret) - 1UL;
}

/// Return `true` if `val` is a power of 2.
constexpr bool IsPow2(uint64_t val) noexcept { return val != 0ULL && (val & (val - 1ULL)) == 0ULL; }

/// Return `val` if it is power of 2, otherwise get the next power of 2 value.
/// If `val` is sufficiently large, the next power of 2 value may not fit into
/// the result type and you will get a wrapped value to 1ULL.
constexpr uint64_t Ceil2(uint64_t val) noexcept {
  if (val == 0ULL || val == 1ULL) return 1ULL;
  return 1ULL << (Log2(val - 1ULL) + 1ULL);
}

/// Round `val` to the next `multiple` value, if needed.
/// `std::nullopt` is returned in case of an overflow or if `multiple` is 0.
///
/// Examples:
///
///     RoundUint64ToMultiple(5, 8) == 8
///     RoundUint64ToMultiple(8, 8) == 8
///     RoundUint64ToMultiple(9, 8) == 16
constexpr std::optional<uint64_t> RoundUint64ToMultiple(uint64_t val, uint64_t multiple) noexcept {
  if (multiple == 0) return std::nullopt;
  uint64_t numerator = val + multiple - 1;
  // Check for overflow.
  if (numerator < val) return std::nullopt;
  // Rely on integer division to get the rounded multiple.
  // No overflow is possible as the final, rounded value can only be less than
  // or equal to `numerator`.
  return (numerator / multiple) * multiple;
}

template <typename T>
concept FloatingPoint = std::is_floating_point_v<T>;

template <FloatingPoint T>
bool ApproxEqualDecimal(T a, T b) {
  return boost::math::relative_difference(a, b) < std::numeric_limits<T>::epsilon();
}

template <FloatingPoint T>
bool LessThanDecimal(T a, T b) {
  return (b - a) > std::numeric_limits<T>::epsilon();
}

/// @return 0 if a == b
/// @return 1 if a > b
/// @return -1 if a < b
template <FloatingPoint T>
int CompareDecimal(T a, T b) {
  if (ApproxEqualDecimal(a, b)) return 0;
  if (LessThanDecimal(a, b)) return -1;
  return 1;
}

constexpr double ChiSquaredValue(double observed, double expected) {
  if (utils::ApproxEqualDecimal(expected, 0.0)) {
    return std::numeric_limits<double>::max();
  }
  return (observed - expected) * (observed - expected) / expected;
}

}  // namespace memgraph::utils
