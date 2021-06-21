#pragma once
#include <concepts>

namespace utils {
template <typename T, typename... Args>
concept SameAsAnyOf = (std::same_as<T, Args> || ...);

template <typename T>
concept Enum = std::is_enum_v<T>;
}  // namespace utils
