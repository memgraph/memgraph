#pragma once

#include <vector>

#include "utils/memory.hpp"

namespace utils::pmr {

template <class T>
using vector = std::vector<T, utils::Allocator<T>>;

}  // namespace utils::pmr
