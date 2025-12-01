// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

module;

#include <cstdint>
#include <cstdlib>
#include <functional>
#include <string_view>

export module memgraph.utils.fnv;

namespace memgraph::utils::detail {
// FNV-1a constants
constexpr uint64_t FNV_OFFSET_BASIS = 14695981039346656037UL;
constexpr uint64_t FNV_PRIME = 1099511628211UL;
}  // namespace memgraph::utils::detail

export namespace memgraph::utils {

inline uint64_t Fnv(const std::string_view s) {
  // fnv1a is recommended so use it as the default implementation.
  uint64_t hash = detail::FNV_OFFSET_BASIS;
  for (const auto &ch : s) {
    hash = (hash ^ static_cast<uint64_t>(ch)) * detail::FNV_PRIME;
  }
  return hash;
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
template <typename TIterable, typename TElement, typename THash = std::hash<TElement>>
struct FnvCollection {
  size_t operator()(const TIterable &iterable) const {
    uint64_t hash = detail::FNV_OFFSET_BASIS;
    THash element_hash;
    for (const TElement &element : iterable) {
      hash *= detail::FNV_PRIME;
      hash ^= element_hash(element);
    }
    return hash;
  }
};

/**
 * Like FNV hashing for a collection, just specialized for two elements to avoid
 * iteration overhead.
 */
template <typename TA, typename TB, typename TAHash = std::hash<TA>, typename TBHash = std::hash<TB>>
struct HashCombine {
  size_t operator()(const TA &a, const TB &b) const {
    size_t ret = detail::FNV_OFFSET_BASIS;
    ret ^= TAHash()(a);
    ret *= detail::FNV_PRIME;
    ret ^= TBHash()(b);
    return ret;
  }
};

}  // namespace memgraph::utils
