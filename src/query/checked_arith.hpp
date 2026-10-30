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

#pragma once

#include <cstdint>

namespace memgraph::query {

enum class ArithOp : uint8_t { Add, Sub, Mul, Div, Mod, Neg };

/// Throw ArithmeticException. Out of line so the checked fast paths stay small.
[[noreturn, gnu::cold, gnu::noinline]] void ThrowIntegerOverflow(ArithOp op);
[[noreturn, gnu::cold, gnu::noinline]] void ThrowDivisionByZero(ArithOp op);

[[nodiscard]] inline bool TryAdd(int64_t a, int64_t b, int64_t &out) { return !__builtin_add_overflow(a, b, &out); }

[[nodiscard]] inline bool TrySub(int64_t a, int64_t b, int64_t &out) { return !__builtin_sub_overflow(a, b, &out); }

[[gnu::always_inline]] inline int64_t CheckedAdd(int64_t a, int64_t b) {
  int64_t r;
  if (__builtin_add_overflow(a, b, &r)) [[unlikely]] {
    ThrowIntegerOverflow(ArithOp::Add);
  }
  return r;
}

[[gnu::always_inline]] inline int64_t CheckedSub(int64_t a, int64_t b) {
  int64_t r;
  if (__builtin_sub_overflow(a, b, &r)) [[unlikely]] {
    ThrowIntegerOverflow(ArithOp::Sub);
  }
  return r;
}

[[gnu::always_inline]] inline int64_t CheckedMul(int64_t a, int64_t b) {
  int64_t r;
  if (__builtin_mul_overflow(a, b, &r)) [[unlikely]] {
    ThrowIntegerOverflow(ArithOp::Mul);
  }
  return r;
}

[[gnu::always_inline]] inline int64_t CheckedNeg(int64_t a) {
  int64_t r;
  if (__builtin_sub_overflow(int64_t{0}, a, &r)) [[unlikely]] {
    ThrowIntegerOverflow(ArithOp::Neg);
  }
  return r;
}

[[gnu::always_inline]] inline int64_t CheckedDiv(int64_t a, int64_t b) {
  if (b == 0) [[unlikely]] {
    ThrowDivisionByZero(ArithOp::Div);
  }
  if (b == -1) [[unlikely]] {
    int64_t r;
    if (__builtin_sub_overflow(int64_t{0}, a, &r)) [[unlikely]] {
      ThrowIntegerOverflow(ArithOp::Div);
    }
    return r;
  }
  return a / b;
}

/// x % -1 is always 0; the native instruction traps for INT64_MIN % -1, so it is never issued.
[[gnu::always_inline]] inline int64_t CheckedMod(int64_t a, int64_t b) {
  if (b == 0) [[unlikely]] {
    ThrowDivisionByZero(ArithOp::Mod);
  }
  if (b == -1) [[unlikely]] {
    return 0;
  }
  return a % b;
}

}  // namespace memgraph::query
