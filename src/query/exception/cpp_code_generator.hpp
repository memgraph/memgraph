#pragma once

#include "utils/exceptions/basic_exception.hpp"

class CppCodeGeneratorException : public BasicException {
 public:
  using BasicException::BasicException;
};
