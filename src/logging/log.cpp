#include <iostream>

#include "logging/log.hpp"
#include "logging/logger.hpp"
#include "utils/assert.hpp"

Logger Log::logger(const std::string &name) {
  // TODO: once when properties are refactored enable this
  // debug_assert(this != nullptr,
  //                "This shouldn't be null. This method is "
  //                "called before the log object is created. "
  //                "E.g. static variables before main method.");
  return Logger(this, name);
}
