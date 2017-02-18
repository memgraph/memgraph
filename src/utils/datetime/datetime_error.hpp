#pragma once

#include "utils/exceptions/stacktrace_exception.hpp"

class DatetimeError : public StacktraceException {
 public:
  using StacktraceException::StacktraceException;
};
