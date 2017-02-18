#pragma once

#include "utils/exceptions/stacktrace_exception.hpp"

class PlanCompilationException : public StacktraceException {
 public:
  using StacktraceException::StacktraceException;
};
