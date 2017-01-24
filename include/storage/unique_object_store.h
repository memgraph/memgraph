//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 23.01.17.
//

#pragma once

#include <memory>
#include <map>
#include <atomic>
#include <stdint.h>

#include "data_structures/concurrent/concurrent_map.hpp"

/**
 * Storage for ensuring object uniqueness. Returns the same
 * Key for all equal objects, of which only one is stored
 * in this data structure. This data structure supports
 * concurrent access with the above stated guarantees.
 *
 * Useful for both reducing the number of equal objects for which
 * references are kept, and reducing the footprint of such objects
 * (Keys are typically 4 byte unsigned ints).
 *
 * IMPORTANT: The store currently does not handle object lifetimes:
 * they live forever. In many intended use cases this is fine. When
 * it becomes necessary a garbage collection over this structure
 * can be implemented.
 *
 * @tparam TObject Type of object managed by this store.
 * @tparam TKey Type of key.
 */
template <typename TObject, typename TKey = uint32_t>
class UniqueObjectStore {

public:
  // Key is the type returned by this class, it's a template param
  using Key = TKey;

  UniqueObjectStore() : next_key_counter_(0) {}

  // atomic member var will ensure that we can't move nor copy this

  ~UniqueObjectStore() = default;

  /**
   * Returns a Key for the given name. If a Key already exists for
   * that name in this UniqueObjectStore, it is returned. Otherwise a new Key
   * is generated and returned.
   *
   * @param obj The object for which we seek a Key
   * @return Key See above.
   */
  Key GetKey(const TObject &obj) {

    auto accessor = storage_.access();

    // try to find an existing key
    auto found = accessor.find(obj);
    if (found != accessor.end())
      return found->second;

    // failed to find an existing key, create and insert a new one
    // return insertion result (either the new key, or an existing one
    // for the same value)
    return accessor.insert(obj, next_key_counter_++).first->second;
  }

private:
  // maps objects to keys with support for concurrent writes
  ConcurrentMap<TObject, Key> storage_;

  // counter of keys that have been generated
  // note that this is not necessary the count of all keys being used since
  // it's possible that Keys were generated and then discarded due to concurrency
  std::atomic<Key> next_key_counter_;
};
