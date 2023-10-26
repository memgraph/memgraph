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

template <class KEY, class VAL>
class LRUCache {
 public:
  LRUCache(int cache_size_) : cache_size(cache_size_){};

  void put(const KEY &key, const VAL &val) {
    auto it = item_map.find(key);
    if (it != item_map.end()) {
      item_list.erase(it->second);
      item_map.erase(it);
    }
    item_list.push_front(std::make_pair(key, val));
    item_map.insert(std::make_pair(key, item_list.begin()));
    clean();
  };
  bool exist(const KEY &key) { return (item_map.count(key) > 0); };
  VAL get(const KEY &key) {
    if (!exist(key)) {
      return nullptr;
    }
    auto it = item_map.find(key);
    item_list.splice(item_list.begin(), item_list, it->second);
    return it->second->second;
  };
  void clean(void) {
    while (item_map.size() > cache_size) {
      auto last_it = item_list.end();
      last_it--;
      item_map.erase(last_it->first);
      item_list.pop_back();
    }
  };

 private:
  std::list<std::pair<KEY, VAL>> item_list;
  std::unordered_map<KEY, decltype(item_list.begin())> item_map;
  size_t cache_size;
};
}  // namespace memgraph::utils
