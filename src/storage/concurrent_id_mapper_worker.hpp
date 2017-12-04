#pragma once

#include "glog/logging.h"

#include "data_structures/concurrent/concurrent_map.hpp"
#include "storage/concurrent_id_mapper.hpp"

/** Worker implementation of ConcurrentIdMapper. */
template <typename TId, typename TValue>
class WorkerConcurrentIdMapper : public ConcurrentIdMapper<TId, TValue> {
 public:
  TId value_to_id(const TValue &value) override {
    auto accessor = value_to_id_cache_.accessor();
    auto found = accessor.find(value);
    if (found != accessor.end()) return found.second;
    // TODO make an RPC call to get the ID for value
    TId id;
    accessor.insert(value, id);
    return id;
  }

  const TValue &id_to_value(const TId &id) override {
    auto accessor = id_to_value_cache_.accessor();
    auto found = accessor.find(id);
    if (found != accessor.end()) return found.second;
    // TODO make an RPC call to get the value for ID
    TValue value;
    return accessor.insert(id, value).second.second;
  }

 private:
  // Sources of truth for the mappings are on the master, not on this worker. We
  // keep the caches.
  ConcurrentMap<TValue, TId> value_to_id_cache_;
  ConcurrentMap<TId, TValue> id_to_value_cache_;
};
