#pragma once

#include <cstdint>
#include <type_traits>

#include <glog/logging.h>

namespace utils {

static_assert(
    std::is_same_v<uint64_t, unsigned long>,
    "utils::Log requires uint64_t to be implemented as unsigned long.");

/// This function computes the log2 function on integer types. It is faster than
/// the cmath `log2` function because it doesn't use floating point values for
/// calculation.
inline uint64_t Log2(uint64_t val) {
  // The `clz` function is undefined when the passed value is 0 and the value of
  // `log` is `-inf` so we special case it here.
  if (val == 0) return 0;
  // clzl = count leading zeros from long
  //        ^     ^       ^          ^
  int ret = __builtin_clzl(val);
  return 64UL - static_cast<uint64_t>(ret) - 1UL;
}
}  // namespace utils
