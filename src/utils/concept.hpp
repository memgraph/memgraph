#pragma once
#include <type_traits>

namespace utils {

namespace detail {
template <typename T, typename U>
concept SameAsImpl = std::is_same_v<T, U>;
}  // namespace detail

// https://stackoverflow.com/a/58511321
template <typename T, typename U>
concept SameAs = detail::SameAsImpl<T, U> &&detail::SameAsImpl<U, T>;
}  // namespace utils
