#pragma once

#include "utils/exceptions/basic_exception.hpp"

class DecoderException : public BasicException {
 public:
  using BasicException::BasicException;
};
