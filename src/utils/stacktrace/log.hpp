#pragma once

#include "logging/default.hpp"
#include "utils/stacktrace/stacktrace.hpp"

void log_stacktrace(const std::string& title) {
  Stacktrace stacktrace;
  logging::info(title);
  logging::info(stacktrace.dump());
}
