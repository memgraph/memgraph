#pragma once

#include <type_traits>

namespace utils {

template <typename T>
constexpr typename std::underlying_type<T>::type UnderlyingCast(T e) {
  return static_cast<typename std::underlying_type<T>::type>(e);
}

}  // namespace utils
