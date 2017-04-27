//
// Created by buda on 18/02/17.
//
#pragma once

#include "utils/exceptions/basic_exception.hpp"

class LockTimeoutError : public BasicException {
 public:
  using BasicException::BasicException;
};
