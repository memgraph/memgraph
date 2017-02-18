#pragma once

#include "utils/exceptions/stacktrace_exception.hpp"

class PlanExecutionException : public StacktraceException {
 public:
  using StacktraceException::StacktraceException;
};
