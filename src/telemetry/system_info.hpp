#pragma once

#include <json/json.hpp>

namespace telemetry {

// TODO (mferencevic): merge with `utils/sysinfo`

/**
 * This function returs a dictionary containing some basic system information
 * (eg. operating system name, cpu information, memory information, etc.).
 */
const nlohmann::json GetSystemInfo();

}  // namespace telemetry
