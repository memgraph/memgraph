#pragma once

#include "utils/exceptions/basic_exception.hpp"

namespace io {

class TlsError : public BasicException {
 public:
  using BasicException::BasicException;
};
}
