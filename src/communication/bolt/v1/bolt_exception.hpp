#pragma once

#include "utils/exceptions/basic_exception.hpp"

namespace communication::bolt {

class BoltException : public BasicException {
 public:
  using BasicException::BasicException;
};
}
