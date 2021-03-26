#pragma once

#include <fstream>
#include <iostream>
#include <limits>
#include <optional>

#include "utils/logging.hpp"

namespace utils::sysinfo {

/**
 * Gets the amount of available RAM in KiB. If the information is
 * unavalable an empty value is returned.
 */
std::optional<uint64_t> AvailableMemory();

/**
 * Gets the amount of total RAM in KiB. If the information is
 * unavalable an empty value is returned.
 */
std::optional<uint64_t> TotalMemory();

/**
 * Gets the amount of total swap space in KiB. If the information is
 * unavalable an empty value is returned.
 */
std::optional<uint64_t> SwapTotalMemory();

}  // namespace utils::sysinfo
