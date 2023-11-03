// Copyright 2023 Memgraph Ltd.
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
#include <unordered_map>
#include <utility>

namespace memgraph::utils {

/// A simple LRU cache implementation.
/// It is not thread-safe.

template <class TKey, class TVal>
class LRUCache {
 public:
  LRUCache(int cache_size_) : cache_size(cache_size_){};

  void put(const TKey &key, const TVal &val) {
    auto it = item_map.find(key);
    if (it != item_map.end()) {
      item_list.erase(it->second);
      item_map.erase(it);
    }
    item_list.push_front(std::make_pair(key, val));
    item_map.insert(std::make_pair(key, item_list.begin()));
    try_clean();
  };
  bool get(const TKey &key, TVal &result) {
    if (!exists(key)) {
      return false;
    }
    auto it = item_map.find(key);
    item_list.splice(item_list.begin(), item_list, it->second);
    result = it->second->second;
    return true;
  }
  void reset() {
    item_list.clear();
    item_map.clear();
  };
  std::size_t size() { return item_map.size(); };

 private:
  void try_clean() {
    while (item_map.size() > cache_size) {
      auto last_it_elem_it = item_list.end();
      last_it_elem_it--;
      item_map.erase(last_it_elem_it->first);
      item_list.pop_back();
    }
  };
  bool exists(const TKey &key) { return (item_map.count(key) > 0); };

  std::list<std::pair<TKey, TVal>> item_list;
  std::unordered_map<TKey, decltype(item_list.begin())> item_map;
  std::size_t cache_size;
};
}  // namespace memgraph::utils
