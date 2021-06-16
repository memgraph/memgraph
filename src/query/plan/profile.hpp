#pragma once

#include <cstdint>
#include <vector>

#include <json/json.hpp>

#include "query/typed_value.hpp"

namespace query {

namespace plan {

/**
 * Stores profiling statistics for a single logical operator.
 */
struct ProfilingStats {
  int64_t actual_hits{0};
  unsigned long long num_cycles{0};
  uint64_t key{0};
  const char *name{nullptr};
  // TODO: This should use the allocator for query execution
  std::vector<ProfilingStats> children;
};

struct ProfilingStatsWithTotalTime {
  ProfilingStats cumulative_stats{};
  std::chrono::duration<double> total_time{};
};

std::vector<std::vector<TypedValue>> ProfilingStatsToTable(const ProfilingStatsWithTotalTime &stats);

nlohmann::json ProfilingStatsToJson(const ProfilingStatsWithTotalTime &stats);

}  // namespace plan
}  // namespace query
