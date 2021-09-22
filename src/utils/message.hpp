#pragma once

#include <fmt/format.h>

namespace utils {

template <typename... Args>
std::string MessageWithLink(const std::string_view format, Args &&...args) {
  return fmt::format(fmt::format("{} For more details, visit {{}}.", format), std::forward<Args>(args)...);
}

}  // namespace utils
