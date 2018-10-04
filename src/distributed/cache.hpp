/// @file

#pragma once

#include <mutex>
#include <unordered_map>

#include "distributed/data_rpc_clients.hpp"
#include "storage/distributed/gid.hpp"

namespace database {
class Storage;
}

namespace distributed {

// TODO Improvements:
// 1) Use combination of std::unoredered_map<TKey, list<...>::iterator
// and std::list<std::pair<TKey, TValue>>. Use map for quick access and
// checking if TKey exists in map, list for keeping track of LRU order.
//
// 2) Implement adaptive replacement cache policy instead of LRU.
// http://theory.stanford.edu/~megiddo/pdf/IEEE_COMPUTER_0404.pdf/

/// Used for caching objects. Uses least recently used page replacement
/// algorithm for evicting elements when maximum size is reached. This class
/// is NOT thread safe.
///
/// @see ThreadSafeCache
/// @tparam TKey - any object that has hash() defined
/// @tparam TValue - any object
template <typename TKey, typename TValue>
class Cache {
 public:
  using Iterator = typename std::unordered_map<TKey, TValue>::iterator;

  Cache() = default;

  Iterator find(const TKey &key) { return cache_.find(key); }

  std::pair<Iterator, bool> emplace(TKey &&key, TValue &&value) {
    return cache_.emplace(std::forward<TKey>(key), std::forward<TValue>(value));
  }

  void erase(const TKey &key) { cache_.erase(key); }

  Iterator end() { return cache_.end(); }

  bool contains(const TKey &key) { return find(key) != end(); }

  void clear() { cache_.clear(); }

 private:
  std::unordered_map<TKey, TValue> cache_;
};

}  // namespace distributed
