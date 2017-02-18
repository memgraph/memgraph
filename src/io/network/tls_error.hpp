#pragma once

#include "utils/exceptions/stacktrace_exception.hpp"

namespace io {

class TlsError : public StacktraceException {
 public:
  using StacktraceException::StacktraceException;
};
}
