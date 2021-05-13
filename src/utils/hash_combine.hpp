#pragma once

#include <cstddef>
#include <functional>

namespace utils {
// http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2014/n3876.pdf
template <typename T>
void hash_combine(std::size_t &seed, const T &val) {
  seed ^= std::hash<T>()(val) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

template <typename T, typename... Types>
void hash_combine(std::size_t &seed, const T &val, const Types &...args) {
  hash_combine(seed, val);
  hash_combine(seed, args...);
}

inline void hash_combine(std::size_t &seed) {}

//  generic function to create a hash value out of a heterogeneous list of arguments
template <typename... Types>
std::size_t hash_val(const Types &...args) {
  std::size_t seed = 0;
  hash_combine(seed, args...);
  return seed;
}
}  // namespace utils