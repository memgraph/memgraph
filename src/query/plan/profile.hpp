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

#include <cstdint>
#include <vector>

#include <json/json.hpp>

#include "query/typed_value.hpp"

namespace memgraph::query::plan {

/**
 * Stores profiling statistics for a single logical operator.
 */
struct ProfilingStats {
  int64_t actual_hits{0};
  unsigned long long num_cycles{0};
  uint64_t key{0};
  std::string name;
  // TODO: This should use the allocator for query execution
  std::vector<ProfilingStats> children;
};

struct ProfilingStatsWithTotalTime {
  ProfilingStats cumulative_stats{};
  std::chrono::duration<double> total_time{};
};

std::vector<std::vector<TypedValue>> ProfilingStatsToTable(const ProfilingStatsWithTotalTime &stats);

nlohmann::json ProfilingStatsToJson(const ProfilingStatsWithTotalTime &stats);

}  // namespace memgraph::query::plan
