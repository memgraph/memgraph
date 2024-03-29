// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/event_map.hpp"

namespace {
template <typename T, typename K>
int NameToId(const T &names, const K &name) {
  const auto &id = std::find(names.begin(), names.end(), name);
  if (id == names.end()) return -1;
  return std::distance(names.begin(), id);
}

template <typename T, typename K>
int NameToId(T &names, const K &name, uint64_t &free) {
  const auto id = NameToId(names, name);
  if (id != -1) return id;
  // Add new name
  if (free < 1) return -1;  // No more space
  int idx = names.size() - free;
  names[idx] = name;
  --free;
  return idx;
}
}  // namespace

namespace memgraph::metrics {

// Initialize global counters memory
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
Counter global_counters_map_array[EventMap::kMaxCounters]{};

// Initialize global counters map
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
EventMap global_counters_map(global_counters_map_array);

bool EventMap::Increment(const std::string_view event, Count amount) {
  const auto id = NameToId(name_to_id_, event, num_free_counters_);
  if (id != -1) {
    counters_[id].fetch_add(amount, std::memory_order_relaxed);
    return true;
  }
  return false;
}

bool EventMap::Decrement(const std::string_view event, Count amount) {
  const auto id = NameToId(name_to_id_, event, num_free_counters_);
  if (id != -1) {
    counters_[id].fetch_sub(amount, std::memory_order_relaxed);
    return true;
  }
  return false;
}

bool IncrementCounter(const std::string_view event, Count amount) {
  return global_counters_map.Increment(event, amount);
}
bool DecrementCounter(const std::string_view event, Count amount) {
  return global_counters_map.Decrement(event, amount);
}

}  // namespace memgraph::metrics
