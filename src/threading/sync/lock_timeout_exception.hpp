#pragma once

#include "utils/exceptions/basic_exception.hpp"

class LockTimeoutException : public BasicException {
 public:
  using BasicException::BasicException;
};
