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

#include <algorithm>
#include <ranges>
#include <vector>

namespace memgraph::storage {

/// The objects a collection pass has already taken responsibility for retiring, keyed by whatever
/// identifies the object in its store. A second source of collectable objects consults this so that
/// it cannot name one of them again: retiring an object twice removes it from storage twice.
///
/// Membership is answered from a sorted copy taken at construction, so the source list may change
/// afterwards without invalidating an instance.
template <typename TKey>
class ClaimedObjects {
 public:
  ClaimedObjects() = default;

  template <std::ranges::input_range TRange>
    requires std::same_as<std::ranges::range_value_t<TRange>, TKey>
  explicit ClaimedObjects(TRange const &claimed) : keys_(std::ranges::begin(claimed), std::ranges::end(claimed)) {
    std::ranges::sort(keys_);
  }

  [[nodiscard]] bool contains(TKey const &key) const {
    // Nothing was claimed on the paths that do not hand objects over, which is the common case, so
    // keep that a single predictable branch rather than a search over an empty range.
    if (keys_.empty()) return false;
    return std::ranges::binary_search(keys_, key);
  }

  [[nodiscard]] bool empty() const { return keys_.empty(); }

  [[nodiscard]] std::size_t size() const { return keys_.size(); }

 private:
  std::vector<TKey> keys_;
};

template <std::ranges::input_range TRange>
ClaimedObjects(TRange const &) -> ClaimedObjects<std::ranges::range_value_t<TRange>>;

}  // namespace memgraph::storage
