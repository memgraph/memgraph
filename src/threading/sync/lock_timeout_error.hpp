//
// Created by buda on 18/02/17.
//
#pragma once

#include <stdexcept>

class LockTimeoutError : public std::runtime_error {
 public:
  using runtime_error::runtime_error;
};
