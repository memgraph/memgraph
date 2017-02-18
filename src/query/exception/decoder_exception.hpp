#pragma once

#include "utils/exceptions/stacktrace_exception.hpp"

class DecoderException : public StacktraceException {
 public:
  using StacktraceException::StacktraceException;
};
