#pragma once

#include <json/json.hpp>

namespace telemetry {

/**
 * This function returs a dictionary containing resource usage information
 * (total cpu usage and current memory usage).
 */
const nlohmann::json GetResourceUsage();

}  // namespace telemetry
