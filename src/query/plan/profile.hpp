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
  std::vector<ProfilingStats> children;
};

std::vector<std::vector<TypedValue>> ProfilingStatsToTable(
    const ProfilingStats &cumulative_stats, std::chrono::duration<double>);

nlohmann::json ProfilingStatsToJson(const ProfilingStats &cumulative_stats,
                                    std::chrono::duration<double>);

}  // namespace plan
}  // namespace query
