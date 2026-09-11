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
/// Thread-safety: put()/get()/invalidate()/reset() mutate state and need
/// exclusive access. peek() mutates nothing, so concurrent peek()s are safe
/// (e.g. readers under a shared lock) provided no mutator runs concurrently.
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

  /// Non-mutating lookup: returns a copy of the value without bumping LRU
  /// recency, so it is safe for concurrent readers (unlike get()). Tradeoff:
  /// entries only ever read via peek() age as if unaccessed, so eviction is
  /// approximate-LRU. Correctness is unaffected: a miss just costs a re-plan,
  /// and the returned copy stays valid regardless of later cache changes.
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
