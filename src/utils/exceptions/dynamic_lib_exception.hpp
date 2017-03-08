#pragma once

#include <dlfcn.h>
#include "utils/exceptions/basic_exception.hpp"

class DynamicLibException : public BasicException {
 public:
  using BasicException::BasicException;
};
