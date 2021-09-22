#pragma once

#include <fmt/format.h>

namespace utils {

template <typename... Args>
std::string MessageWithLink(const std::string_view link, Args &&...args) {
  return fmt::format("{} For more details, visit {}.", fmt::format(std::forward<Args>(args)...), link);
}

}  // namespace utils
