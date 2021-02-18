#pragma once

#include "utils/exceptions.hpp"

namespace io::network {

class NetworkError : public utils::StacktraceException {
 public:
  using utils::StacktraceException::StacktraceException;
};
}  // namespace io::network
