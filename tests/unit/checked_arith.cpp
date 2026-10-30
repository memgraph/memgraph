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

#include <array>
#include <cstdint>
#include <limits>
#include <string>

#include <gtest/gtest.h>

#include "query/checked_arith.hpp"
#include "query/exceptions.hpp"

using namespace memgraph::query;

namespace {
constexpr int64_t kMin = std::numeric_limits<int64_t>::min();
constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
constexpr std::array<int64_t, 7> kValues{kMin, kMin + 1, -1, 0, 1, kMax - 1, kMax};

bool Fits(__int128 v) { return v >= kMin && v <= kMax; }

template <typename Op, typename Ref>
void ExpectMatchesReference(Op op, Ref ref) {
  for (auto a : kValues) {
    for (auto b : kValues) {
      auto const expected = ref(static_cast<__int128>(a), static_cast<__int128>(b));
      if (Fits(expected)) {
        EXPECT_EQ(op(a, b), static_cast<int64_t>(expected)) << a << " , " << b;
      } else {
        EXPECT_THROW(op(a, b), ArithmeticException) << a << " , " << b;
      }
    }
  }
}
}  // namespace

TEST(CheckedArith, AddSubMulMatchReference) {
  ExpectMatchesReference(CheckedAdd, [](__int128 a, __int128 b) { return a + b; });
  ExpectMatchesReference(CheckedSub, [](__int128 a, __int128 b) { return a - b; });
  ExpectMatchesReference(CheckedMul, [](__int128 a, __int128 b) { return a * b; });
}

TEST(CheckedArith, DivModMatchReference) {
  for (auto a : kValues) {
    for (auto b : kValues) {
      if (b == 0) continue;
      auto const q = static_cast<__int128>(a) / b;  // truncating, remainder takes the dividend's sign
      auto const r = static_cast<__int128>(a) % b;
      if (Fits(q)) {
        EXPECT_EQ(CheckedDiv(a, b), static_cast<int64_t>(q)) << a << " / " << b;
      } else {
        EXPECT_THROW(CheckedDiv(a, b), ArithmeticException) << a << " / " << b;
      }
      EXPECT_EQ(CheckedMod(a, b), static_cast<int64_t>(r)) << a << " % " << b;
    }
  }
}

TEST(CheckedArith, NegAbsMatchReference) {
  for (auto a : kValues) {
    __int128 const n = -static_cast<__int128>(a);
    if (Fits(n)) {
      EXPECT_EQ(CheckedNeg(a), static_cast<int64_t>(n)) << a;
    } else {
      EXPECT_THROW(CheckedNeg(a), ArithmeticException) << a;
    }
    __int128 const ab = a < 0 ? n : a;
    if (Fits(ab)) {
      EXPECT_EQ(CheckedAbs(a), static_cast<int64_t>(ab)) << a;
    } else {
      EXPECT_THROW(CheckedAbs(a), ArithmeticException) << a;
    }
  }
}

TEST(CheckedArith, SpecialCases) {
  EXPECT_EQ(CheckedMod(kMin, -1), 0);
  EXPECT_THROW(CheckedDiv(kMin, -1), ArithmeticException);
  EXPECT_EQ(CheckedDiv(kMax, -1), -kMax);
}

namespace {
template <typename F>
std::string WhatOf(F f) {
  try {
    f();
  } catch (const ArithmeticException &e) {
    return e.what();
  }
  return {};
}
}  // namespace

TEST(CheckedArith, Messages) {
  EXPECT_EQ(WhatOf([] { CheckedDiv(1, 0); }), "Division by zero in '/'.");
  EXPECT_EQ(WhatOf([] { CheckedMod(1, 0); }), "Division by zero in '%'.");
  EXPECT_EQ(WhatOf([] { CheckedAbs(kMin); }), "Integer overflow in 'abs'.");
  EXPECT_EQ(WhatOf([] { CheckedNeg(kMin); }), "Integer overflow in '-'.");
  EXPECT_EQ(WhatOf([] { CheckedAdd(kMax, 1); }), "Integer overflow in '+'.");
  EXPECT_EQ(WhatOf([] { CheckedSub(kMin, 1); }), "Integer overflow in '-'.");
  EXPECT_EQ(WhatOf([] { CheckedMul(kMax, 2); }), "Integer overflow in '*'.");
  EXPECT_EQ(WhatOf([] { CheckedDiv(kMin, -1); }), "Integer overflow in '/'.");
}

TEST(CheckedArith, TryVariants) {
  int64_t out = 0;
  EXPECT_FALSE(TryAdd(kMax, 1, out));
  EXPECT_FALSE(TrySub(kMin, 1, out));
  EXPECT_TRUE(TryAdd(1, 2, out));
  EXPECT_EQ(out, 3);
  EXPECT_TRUE(TrySub(1, 2, out));
  EXPECT_EQ(out, -1);
}
