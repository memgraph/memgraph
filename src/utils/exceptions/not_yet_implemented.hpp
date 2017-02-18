#pragma once

#include "utils/exceptions/stacktrace_exception.hpp"

class NotYetImplemented : public StacktraceException {
 public:
  using StacktraceException::StacktraceException;

  NotYetImplemented() : StacktraceException("") {}
};
