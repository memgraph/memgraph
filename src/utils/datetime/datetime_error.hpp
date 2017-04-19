#pragma once

#include "utils/exceptions.hpp"

class DatetimeError : public utils::StacktraceException {
 public:
  using utils::StacktraceException::StacktraceException;
};
