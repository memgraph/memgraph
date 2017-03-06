#pragma once

#include <stdexcept>

#include "utils/exceptions/stacktrace_exception.hpp"

namespace io::network {

class NetworkError : public StacktraceException {
 public:
  using StacktraceException::StacktraceException;
};
}
