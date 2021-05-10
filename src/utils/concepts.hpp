#pragma once
#include <concepts>

namespace utils {
template <typename T, typename... Args>
concept SameAsAnyOf = (std::same_as<T, Args> || ...);
}  // namespace utils
