// Copyright 2025 Memgraph Ltd.
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

#include <array>
#include <atomic>
#include <memory>
#include <string>

#include <nlohmann/json_fwd.hpp>

namespace memgraph::metrics {
using Count = uint64_t;
using Counter = std::atomic<Count>;

class EventMap {
 public:
  static constexpr int kMaxCounters = 48;

  explicit EventMap(Counter *allocated_counters) noexcept : counters_(allocated_counters) {}

  bool Increment(std::string_view event, Count amount = 1);

  bool Decrement(std::string_view event, Count amount = 1);

  nlohmann::json ToJson() const;

  uint64_t num_free_counters_{kMaxCounters};
  std::array<std::string, kMaxCounters> name_to_id_;

 private:
  Counter *counters_;
};

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extern EventMap global_counters_map;

bool IncrementCounter(std::string_view event, Count amount = 1);
bool DecrementCounter(std::string_view event, Count amount = 1);

}  // namespace memgraph::metrics
