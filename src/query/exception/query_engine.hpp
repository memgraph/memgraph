#pragma once

#include "utils/exceptions/stacktrace_exception.hpp"

class QueryEngineException : public StacktraceException {
 public:
  using StacktraceException::StacktraceException;
};
