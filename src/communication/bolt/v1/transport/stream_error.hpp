#pragma once

#include "utils/exceptions/basic_exception.hpp"

namespace bolt {

class StreamError : BasicException {
 public:
  using BasicException::BasicException;
};
}
