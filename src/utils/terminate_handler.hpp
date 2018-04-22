#pragma once

#include <execinfo.h>
#include <iostream>

#include "utils/stacktrace.hpp"

namespace utils {

/**
 * Dump stacktrace to the stream and abort the probram. For more details
 * about the abort please take a look at
 * http://en.cppreference.com/w/cpp/utility/program/abort.
 */
void TerminateHandler(std::ostream &stream) noexcept {
  if (auto exc = std::current_exception()) {
    try {
      std::rethrow_exception(exc);
    } catch (std::exception &ex) {
      stream << ex.what() << std::endl << std::endl;
      utils::Stacktrace stacktrace;
      stacktrace.dump(stream);
    }
  }
  std::abort();
}

void TerminateHandler() noexcept { TerminateHandler(std::cout); }

}  // namespace utils
