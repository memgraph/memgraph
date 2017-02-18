#pragma once

#include "utils/exceptions/stacktrace_exception.hpp"

class CppCodeGeneratorException : public StacktraceException {
 public:
  using StacktraceException::StacktraceException;
};
