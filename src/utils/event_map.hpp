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

#pragma once

#include <atomic>
#include <cstdlib>
#include <memory>

#include "json/json.hpp"

namespace memgraph::metrics {
using Count = uint64_t;
using Counter = std::atomic<Count>;

class EventMap {
 public:
  static constexpr int kMaxCounters = 48;

  explicit EventMap(Counter *allocated_counters) noexcept : counters_(allocated_counters) {}

  auto &operator[](std::string_view event);

  const auto &operator[](std::string_view event) const;

  bool Increment(std::string_view event, Count amount = 1);

  bool Decrement(std::string_view event, Count amount = 1);

  nlohmann::json ToJson() {
    auto res = nlohmann::json::array();
    for (size_t i = 0; i < kMaxCounters - num_free_counters_; ++i) {
      const auto &event_name = name_to_id_[i];
      res.push_back({{"name", event_name}, {"count", counters_[i].load()}});
    }
    return res;
  }

  uint64_t num_free_counters_{kMaxCounters};
  std::array<std::string, kMaxCounters> name_to_id_;

 private:
  Counter *counters_;
  const auto &operator[](int event) const;
};

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extern EventMap global_counters_map;

bool IncrementCounter(std::string_view event, Count amount = 1);
bool DecrementCounter(std::string_view event, Count amount = 1);

}  // namespace memgraph::metrics
