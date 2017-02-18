#pragma once

#include <type_traits>

template <typename T>
constexpr typename std::underlying_type<T>::type underlying_cast(T e) {
  return static_cast<typename std::underlying_type<T>::type>(e);
}
