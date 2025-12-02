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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>

#include "flags/query.hpp"

namespace memgraph::query {

struct HopsLimit {
  std::optional<int64_t> limit{std::nullopt};                                                     // Local limit value
  std::shared_ptr<std::atomic<int64_t>> hops_counter{std::make_shared<std::atomic<int64_t>>(0)};  // Global hops counter
  bool is_limit_reached{false};  // Local limit reached flag
  int64_t hops{0};               // Local copy (to avoid reading from atomic variable in parallel execution)
  int64_t recheck_interval{
      FLAGS_hops_limit_recheck_interval};  // Interval at which to recheck the limit (useful for parallel execution)

  bool IsUsed() const { return limit.has_value(); }

  int64_t GetLimit() const { return limit.value(); }
  int64_t GetHopsCounter() const { return hops_counter->load(std::memory_order_acquire); }

  bool IsLimitReached() const { return is_limit_reached; }

  // Return the number of available hops
  int64_t IncrementHopsCount(int64_t increment = 1) {
    if (IsUsed()) {
      // Once reached the limit, return 0
      if (is_limit_reached) return 0;
      const auto interval = FLAGS_hops_limit_recheck_interval;
      // Fast path for parallel execution
      int64_t prev_count = hops;
      recheck_interval -= increment;
      if (recheck_interval <= 0) {
        // Slow path for eventual consistency
        const auto increment_to_add = interval - recheck_interval;  // Missed increments (with the current increment)
        prev_count = hops_counter->fetch_add(increment_to_add, std::memory_order_acq_rel);
        prev_count += increment_to_add - increment;  // Add the missed increments (without the current increment)
        recheck_interval = interval;
      }
      hops = prev_count + increment;
      const auto available_hops = limit.value() - prev_count;
      is_limit_reached = (increment > available_hops);
      return std::min(increment, available_hops);
    }
    return increment;
  }
};

}  // namespace memgraph::query
