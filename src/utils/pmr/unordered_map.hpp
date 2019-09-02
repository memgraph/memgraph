#pragma once

#include <unordered_map>

#include "utils/memory.hpp"

namespace utils::pmr {

// Use transparent std::equal_to<void> which forwards to `operator==`, so that,
// for example, it's possible to use `find` with C-style (null terminated)
// strings without actually constructing (and allocating) a key.
template <class Key, class T, class Hash = std::hash<Key>,
          class Pred = std::equal_to<void>>
using unordered_map =
    std::unordered_map<Key, T, Hash, Pred,
                       utils::Allocator<std::pair<const Key, T>>>;

}  // namespace utils::pmr
