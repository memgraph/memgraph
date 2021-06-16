#pragma once
#include <cstdint>
#include <functional>

namespace utils {
template <typename T>
void BoostHashCombine(size_t &seed, const T &value) {
  std::hash<T> h;
  seed ^= h(value) + 0x9e3779b9 + (seed << 6U) + (seed >> 2U);
}
}  // namespace utils
