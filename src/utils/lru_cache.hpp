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

#include <cstddef>
#include <list>
#include <optional>
#include <unordered_set>
#include <utility>

namespace memgraph::utils {

/// A simple LRU cache implementation.
///
/// The list owns each entry (key and value, both const) and carries the LRU
/// order. The index is a set of list iterators, hashed and compared by the key
/// they point at, so a lookup locates the node in O(1) with no second copy of
/// the key and no linear scan. Lookups pass the bare key through transparent
/// hashing and allocate nothing.
///
/// An entry is immutable once inserted: a put for a key already present keeps
/// the stored value and only refreshes its recency.
///
/// Thread-safety: put()/get()/invalidate()/reset() mutate item_list (recency
/// order) and/or index and are NOT thread-safe against each other or against
/// themselves concurrently. peek() is the sole exception: it performs only an
/// index lookup and a value copy, mutating neither item_list nor index, so it
/// is safe to call concurrently with other peek() calls (e.g. many readers
/// holding only a shared/read lock) as long as no put()/get()/invalidate()/
/// reset() runs at the same time (those still require exclusive access).
template <class TKey, class TVal, class TAlloc = std::allocator<std::pair<const TKey, TVal>>>
class LRUCache {
  using Entry = std::pair<const TKey, const TVal>;
  using ListType = std::list<Entry, typename std::allocator_traits<TAlloc>::template rebind_alloc<Entry>>;
  using ListIt = typename ListType::iterator;

 public:
  explicit LRUCache(std::size_t cache_size_) : cache_size(cache_size_) {};

  void put(const TKey &key, const TVal &val) {
    if (auto it = index.find(key); it != index.end()) {
      item_list.splice(item_list.begin(), item_list, *it);
      return;
    }
    item_list.emplace_front(key, val);
    index.insert(item_list.begin());
    try_clean();
  };

  std::optional<TVal> get(const TKey &key) {
    auto const it = index.find(key);
    if (it == index.end()) {
      return std::nullopt;
    }
    item_list.splice(item_list.begin(), item_list, *it);
    return (*it)->second;
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
    auto const it = index.find(key);
    if (it == index.end()) {
      return std::nullopt;
    }
    return (*it)->second;
  }

  void invalidate(const TKey &key) {
    auto const it = index.find(key);
    if (it != index.end()) {
      ListIt const node = *it;
      index.erase(it);
      item_list.erase(node);
    }
  }

  void reset() {
    index.clear();
    item_list.clear();
  };

  std::size_t size() const { return index.size(); }

 private:
  struct IterHash {
    using is_transparent = void;

    std::size_t operator()(ListIt it) const noexcept { return std::hash<TKey>{}(it->first); }

    std::size_t operator()(const TKey &key) const noexcept { return std::hash<TKey>{}(key); }
  };

  struct IterEqual {
    using is_transparent = void;

    bool operator()(ListIt lhs, ListIt rhs) const { return lhs->first == rhs->first; }

    bool operator()(ListIt lhs, const TKey &rhs) const { return lhs->first == rhs; }

    bool operator()(const TKey &lhs, ListIt rhs) const { return lhs == rhs->first; }
  };

  void try_clean() {
    while (index.size() > cache_size) {
      auto last = std::prev(item_list.end());
      // Erase the index entry before freeing the node: erasing hashes the key,
      // which the node still owns.
      index.erase(last);
      item_list.pop_back();
    }
  };

  ListType item_list;

  using IndexType = std::unordered_set<ListIt, IterHash, IterEqual,
                                       typename std::allocator_traits<TAlloc>::template rebind_alloc<ListIt>>;
  IndexType index;

  std::size_t cache_size;
};
}  // namespace memgraph::utils
