#include <fstream>
#include <iostream>
#include <limits>
#include <optional>

#include "glog/logging.h"

namespace utils::sysinfo {

/**
 * Gets the amount of available RAM in kilobytes. If the information is
 * unavalable an empty value is returned.
 */
inline std::optional<uint64_t> AvailableMemoryKilobytes() {
  std::string token;
  std::ifstream meminfo("/proc/meminfo");
  while (meminfo >> token) {
    if (token == "MemAvailable:") {
      uint64_t mem = 0;
      if (meminfo >> mem) {
        return mem;
      } else {
        return std::nullopt;
      }
    }
    meminfo.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
  }
  DLOG(WARNING)
      << "Failed to read amount of available memory from /proc/meminfo";
  return std::nullopt;
}

}  // namespace utils::sysinfo
