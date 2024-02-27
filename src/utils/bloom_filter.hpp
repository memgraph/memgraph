// Copyright 2024 Memgraph Ltd.
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

#include "absl/container/flat_hash_set.h"

namespace memgraph::utils {

template <typename T>
struct BloomFilter {
  void insert(T const &value) {
    constexpr auto hasher = absl::Hash<T>{};
    auto hash_1 = hasher(value);
    auto hash_2 = hasher(value + 1987);
    store.insert(hash_1);
    store.insert(hash_2);
  }

  bool maybe_contains(T const &value) const {
    constexpr auto hasher = absl::Hash<T>{};
    auto hash_1 = hasher(value);
    if (!store.contains(hash_1)) return false;
    auto hash_2 = hasher(value + 1987);
    return store.contains(hash_2);
  }

  void merge(BloomFilter &&other) { store.merge(std::move(other.store)); }

  bool empty() const { return store.empty(); }

 private:
  // Deliberate truncate to uint32_t
  absl::flat_hash_set<uint32_t> store;  // TODO: replace with roaring bitmap?
};

}  // namespace memgraph::utils
