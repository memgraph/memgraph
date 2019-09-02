#pragma once

#include <list>

#include "utils/memory.hpp"

namespace utils::pmr {

template <class T>
using list = std::list<T, utils::Allocator<T>>;

}  // namespace utils::pmr
