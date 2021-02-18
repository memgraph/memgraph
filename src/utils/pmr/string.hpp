#pragma once

#include <string>

#include "utils/memory.hpp"

namespace utils::pmr {

using string = std::basic_string<char, std::char_traits<char>, utils::Allocator<char>>;

}  // namespace utils::pmr
