#pragma once

#include <unordered_set>

#include "utils/memory.hpp"

namespace utils::pmr {

// Use transparent std::equal_to<void> which forwards to `operator==`, so that,
// for example, it's possible to use `find` with C-style (null terminated)
// strings without actually constructing (and allocating) a key.
template <class Key, class Hash = std::hash<Key>,
          class Pred = std::equal_to<void>>
using unordered_set =
    std::unordered_set<Key, Hash, Pred, utils::Allocator<Key>>;

}  // namespace utils::pmr
