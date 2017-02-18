#pragma once

#include "utils/auto_scope.hpp"
#include "utils/stacktrace/stacktrace.hpp"

#include <execinfo.h>
#include <iostream>

// TODO: log to local file or remote database
void stacktrace(std::ostream &stream) noexcept {
  Stacktrace stacktrace;
  stacktrace.dump(stream);
}

// TODO: log to local file or remote database
void terminate_handler(std::ostream &stream) noexcept {
  if (auto exc = std::current_exception()) {
    try {
      std::rethrow_exception(exc);
    } catch (std::exception &ex) {
      stream << ex.what() << std::endl << std::endl;
      stacktrace(stream);
    }
  }
  std::abort();
}

void terminate_handler() noexcept { terminate_handler(std::cout); }
