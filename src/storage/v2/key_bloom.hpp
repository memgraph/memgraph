// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#ifdef MG_ENTERPRISE

#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>

#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex_row_filter.hpp"

namespace memgraph::storage {

/// A fixed-size, insert-only bloom of label+property index keys (see KeyHash), lock-free; its words are allocated on
/// the first Insert (LazyGidFilter), so a diff index that received no entry holds none. A branch's diff index inserts
/// the key of every entry it receives, so a miss proves the diff index holds no entry for a looked-up key; a
/// rolled-back or garbage-collected entry leaves its bit, which only costs a false positive.
class KeyBloom {
 public:
  explicit KeyBloom(size_t num_words) : filter_(num_words) {}

  KeyBloom(KeyBloom const &) = delete;
  KeyBloom &operator=(KeyBloom const &) = delete;

  /// Throws std::bad_alloc from the first Insert, with nothing recorded.
  void Insert(uint64_t key_hash) { filter_.Insert(Gid::FromUint(key_hash)); }

  [[nodiscard]] bool MayContain(uint64_t key_hash) const noexcept {
    return filter_.MayContain(Gid::FromUint(key_hash));
  }

 private:
  LazyGidFilter filter_;
};

namespace key_bloom_detail {

inline uint64_t Mix(uint64_t x) noexcept {
  x += 0x9E3779B97F4A7C15ULL;
  x = (x ^ (x >> 30U)) * 0xBF58476D1CE4E5B9ULL;
  x = (x ^ (x >> 27U)) * 0x94D049BB133111EBULL;
  return x ^ (x >> 31U);
}

}  // namespace key_bloom_detail

/// The hash of one label+property index key: the label, the index's property paths, and the key's values in entry
/// order. Values go through `std::hash<PropertyValue>`, which agrees with the order the index compares by (an Int and
/// the equal Double share a hash, the four list forms are one value), so keys the index treats as equal hash equal.
template <typename Values>
uint64_t KeyHash(LabelId label, std::span<PropertyPath const> paths, Values const &values) noexcept {
  using key_bloom_detail::Mix;
  uint64_t h = Mix(label.AsUint());
  for (auto const &path : paths) {
    h = Mix(h ^ 0xA5A5A5A5ULL);
    for (auto const id : path) h = Mix(h ^ id.AsUint());
  }
  std::hash<PropertyValue> const hash_of_value;
  for (auto const &value : values) h = Mix(h ^ hash_of_value(value));
  return h;
}

}  // namespace memgraph::storage

#endif
