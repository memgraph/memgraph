#pragma once

#include "utils/exceptions.hpp"

class LockTimeoutException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
};
