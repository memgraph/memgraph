#include <fstream>
#include <iostream>
#include <limits>

#include "glog/logging.h"

namespace utils {

/**
 * Gets the amount of available RAM in kilobytes. If the information is
 * unavalable zero is returned.
 */
inline auto AvailableMem() {
  std::string token;
  std::ifstream meminfo("/proc/meminfo");
  while (meminfo >> token) {
    if (token == "MemAvailable:") {
      unsigned long mem;
      if (meminfo >> mem) {
        return mem;
      } else {
        return 0UL;
      }
    }
    meminfo.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
  }
  LOG(ERROR) << "Failed to read amount of available memory from /proc/meminfo";
  return 0UL;
}
}  // namespace utils
