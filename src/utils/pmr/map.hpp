#pragma once

#include <map>

#include "utils/memory.hpp"

namespace utils::pmr {

// Use transparent std::less<void> which forwards to `operator<`, so that, for
// example, it's possible to use `find` with C-style (null terminated) strings
// without actually constructing (and allocating) a key.
template <class Key, class T, class Compare = std::less<void>>
using map = std::map<Key, T, Compare, utils::Allocator<std::pair<const Key, T>>>;

}  // namespace utils::pmr
