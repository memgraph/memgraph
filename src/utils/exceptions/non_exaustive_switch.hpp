#pragma once

#include "utils/exceptions/stacktrace_exception.hpp"

class NonExhaustiveSwitch : public StacktraceException {
 public:
  using StacktraceException::StacktraceException;
};
