/// @file
#pragma once

#include <atomic>
#include <cstdint>
#include <string>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/counters.hpp"

namespace database {

/// Implementation for the single-node memgraph
class SingleNodeCounters : public Counters {
 public:
  int64_t Get(const std::string &name) override {
    return counters_.access()
        .emplace(name, std::make_tuple(name), std::make_tuple(0))
        .first->second.fetch_add(1);
  }

  void Set(const std::string &name, int64_t value) override {
    auto name_counter_pair = counters_.access().emplace(
        name, std::make_tuple(name), std::make_tuple(value));
    if (!name_counter_pair.second) name_counter_pair.first->second.store(value);
  }

 private:
  ConcurrentMap<std::string, std::atomic<int64_t>> counters_;
};

}  // namespace database
