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
/// put()/get()/invalidate()/reset() mutate item_list (recency order) and are
/// NOT thread-safe against each other or against themselves concurrently.
/// peek() is the sole exception: it performs only a map lookup and a value
/// copy, touching neither item_list nor item_map's structure, so it is safe
/// to call concurrently with other peek() calls (e.g. many readers holding
/// only a shared/read lock) as long as no put()/get()/invalidate() runs at
/// the same time (those still require exclusive access).

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

  /// Non-mutating lookup: returns the cached value (a copy) without moving
  /// the entry to the front of item_list, i.e. WITHOUT bumping LRU recency.
  /// Use this from concurrent readers that only need the value (e.g. a
  /// plan-cache hit path) and cannot tolerate taking an exclusive lock just
  /// to call get(). Tradeoff: entries looked up only via peek() age towards
  /// eviction as if they were never accessed, so hot entries that are always
  /// read via peek() (never put()/invalidated()) can be evicted "early" by
  /// try_clean() even though they are still in active use. For a bounded
  /// plan cache this is acceptable — eviction becomes approximate-LRU rather
  /// than exact-LRU, but correctness is unaffected: a cache miss just costs a
  /// re-plan, and the returned value is an independent copy (a shared_ptr
  /// copy for the plan cache), so it stays valid regardless of what later
  /// happens to the cache entry.
  std::optional<TVal> peek(const TKey &key) const {
    auto const it = item_map.find(key);
    if (it == item_map.end()) {
      return std::nullopt;
    }
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
