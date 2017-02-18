#pragma once

#include <iterator>
#include <sstream>
#include <string>
#include <vector>

namespace utils {

std::string join(const std::vector<std::string>& strings,
                 const char* separator);

template <typename... Args>
std::string prints(const Args&... args) {
  std::vector<std::string> strings = {args...};
  return join(strings, " ");
}
}
