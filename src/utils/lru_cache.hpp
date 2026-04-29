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

#include <list>
#include <optional>
#include <unordered_map>
#include <utility>

namespace memgraph::utils {

/// A simple LRU cache implementation.
/// It is not thread-safe.

template <class TKey, class TVal, class TAlloc = std::allocator<std::pair<const TKey, TVal>>>
class LRUCache {
 public:
  explicit LRUCache(std::size_t cache_size_) : cache_size(cache_size_) {};

  void put(const TKey &key, const TVal &val) {
    auto it = item_map.find(key);
    if (it != item_map.end()) {
      item_list.erase(it->second);
      item_map.erase(it);
    }
    item_list.emplace_front(key, val);
    item_map.insert(std::make_pair(key, item_list.begin()));
    try_clean();
  };

  std::optional<TVal> get(const TKey &key) {
    auto const it = item_map.find(key);
    if (it == item_map.end()) {
      return std::nullopt;
    }
    item_list.splice(item_list.begin(), item_list, it->second);
    return it->second->second;
  }

  void invalidate(const TKey &key) {
    auto const it = item_map.find(key);
    if (it != item_map.end()) {
      item_list.erase(it->second);
      item_map.erase(it);
    }
  }

  void reset() {
    item_list.clear();
    item_map.clear();
  };

  std::size_t size() const { return item_map.size(); }

 private:
  void try_clean() {
    while (item_map.size() > cache_size) {
      auto last = std::prev(item_list.end());
      item_map.erase(last->first);
      item_list.pop_back();
    }
  };

  using ListType = std::list<std::pair<TKey, TVal>,
                             typename std::allocator_traits<TAlloc>::template rebind_alloc<std::pair<TKey, TVal>>>;
  ListType item_list;

  using MapType = std::unordered_map<TKey, typename ListType::iterator, std::hash<TKey>, std::equal_to<TKey>,
                                     typename std::allocator_traits<TAlloc>::template rebind_alloc<
                                         std::pair<const TKey, typename ListType::iterator>>>;
  MapType item_map;

  std::size_t cache_size;
};
}  // namespace memgraph::utils
