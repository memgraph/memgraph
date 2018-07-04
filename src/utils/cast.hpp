#pragma once

#include <cstdint>
#include <cstring>
#include <type_traits>

namespace utils {

template <typename T>
constexpr typename std::underlying_type<T>::type UnderlyingCast(T e) {
  return static_cast<typename std::underlying_type<T>::type>(e);
}

/**
 * uint to int conversion in C++ is a bit tricky. Take a look here
 * https://stackoverflow.com/questions/14623266/why-cant-i-reinterpret-cast-uint-to-int
 * for more details.
 *
 * @tparam TDest Returned datatype.
 * @tparam TSrc Input datatype.
 *
 * @return "copy casted" value.
 */
template <typename TDest, typename TSrc>
TDest MemcpyCast(TSrc src) {
  TDest dest;
  static_assert(sizeof(dest) == sizeof(src),
                "MemcpyCast expects source and destination to be of same size");
  static_assert(std::is_arithmetic<TSrc>::value,
                "MemcpyCast expects source is an arithmetic type");
  static_assert(std::is_arithmetic<TDest>::value,
                "MemcypCast expects destination is an arithmetic type");
  std::memcpy(&dest, &src, sizeof(src));
  return dest;
}

}  // namespace utils
