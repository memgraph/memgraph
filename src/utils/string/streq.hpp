#pragma once

#include <x86intrin.h>

namespace sse42 {
constexpr int strcmp_mode = _SIDD_CMP_EQUAL_EACH | _SIDD_NEGATIVE_POLARITY;

constexpr unsigned CF = 0x1;
constexpr unsigned ZF = 0x40;

bool streq(const char* lhs, const char* rhs) {
  int idx, eflags;

  while (true) {
    auto lhs_mm = _mm_loadu_si128((__m128i*)lhs);
    auto rhs_mm = _mm_loadu_si128((__m128i*)rhs);

    idx = _mm_cmpistri(lhs_mm, rhs_mm, strcmp_mode);
    eflags = __readeflags();

    if (idx != 0x10) return false;

    if ((eflags & (ZF | CF)) != 0) return true;

    lhs += 16;
    rhs += 16;
  }
}
}
