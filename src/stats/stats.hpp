/// @file

#pragma once

#include <thread>
#include <vector>

#include "gflags/gflags.h"

#include "stats/metrics.hpp"

namespace stats {

static const std::string kStatsServiceName = "statsd-service";

/**
 * Start sending metrics to StatsD server.
 *
 * @param prefix prefix to prepend to exported keys
 */
void InitStatsLogging(std::string prefix = "");

/**
 * Stop sending metrics to StatsD server. This should be called before exiting
 * program.
 */
void StopStatsLogging();

/**
 * Send a value to StatsD with current timestamp.
 */
void LogStat(const std::string &metric_path, double value,
             const std::vector<std::pair<std::string, std::string>> &tags = {});

}  // namespace stats
