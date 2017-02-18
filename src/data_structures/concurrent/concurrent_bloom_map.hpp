#pragma once

#include "data_structures/concurrent/common.hpp"
#include "data_structures/concurrent/concurrent_map.hpp"
#include "data_structures/concurrent/skiplist.hpp"

using std::pair;

template <class Key, class Value, class BloomFilter>
class ConcurrentBloomMap {
  using item_t = Item<Key, Value>;
  using list_it = typename SkipList<item_t>::Iterator;

 private:
  ConcurrentMap<Key, Value> map_;
  BloomFilter filter_;

 public:
  ConcurrentBloomMap(BloomFilter filter) : filter_(filter) {}

  std::pair<list_it, bool> insert(const Key &key, const Value &data) {
    filter_.insert(key);

    auto accessor = std::move(map_.access());

    return accessor.insert(key, data);
  }

  bool contains(const Key &key) {
    if (!filter_.contains(key)) return false;

    auto accessor = map_.access();
    return accessor.contains(key);
  }
};
