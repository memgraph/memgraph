#pragma once

#include <cstdlib>
#include <string>

#include "utils/platform.hpp"

#include "fnv32.hpp"
#include "fnv64.hpp"

// fnv1a is recommended so use it as a default implementation. also use the
// platform specific version of the function

namespace {

#ifdef MEMGRAPH64

__attribute__((unused)) uint64_t fnv(const std::string &s) {
  return fnv1a64(s);
}

using HashType = uint64_t;

#elif

__attribute__((unused)) uint32_t fnv(const std::string &s) {
  return fnv1a32(s);
}

using HashType = uint32_t;

#endif
}

/**
 * Does FNV-like hashing on a collection. Not truly FNV
 * because it operates on 8-bit elements, while this
 * implementation uses size_t elements (collection item
 * hash).
 *
 * https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
 *
 *
 * @tparam TIterable A collection type that has begin() and end().
 * @tparam TElement Type of element in the collection.
 * @tparam THash Hash type (has operator() that accepts a 'const TEelement &'
 *  and returns size_t. Defaults to std::hash<TElement>.
 * @param iterable A collection of elements.
 * @param element_hash Function for hashing a single element.
 * @return The hash of the whole collection.
 */
template <typename TIterable, typename TElement,
          typename THash = std::hash<TElement>>
struct FnvCollection {
  size_t operator()(const TIterable &iterable) const {
    uint64_t hash = 14695981039346656037u;
    THash element_hash;
    for (const TElement &element : iterable) {
      hash *= fnv_prime;
      hash ^= element_hash(element);
    }
    return hash;
  }

 private:
  static const uint64_t fnv_prime = 1099511628211u;
};

template<typename TA, typename TB>
struct HashCombine {
  size_t operator()(const TA& a, const TB& b) const {
    constexpr size_t fnv_prime = 1099511628211UL;
    constexpr size_t fnv_offset = 14695981039346656037UL;
    size_t ret = fnv_offset;
    ret ^= std::hash<TA>()(a);
    ret *= fnv_prime;
    ret ^= std::hash<TB>()(b);
    return ret;
  }
};
