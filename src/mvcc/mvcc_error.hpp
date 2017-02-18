#pragma once

#include <stdexcept>

namespace mvcc {

class MvccError : public std::runtime_error {
 public:
  using runtime_error::runtime_error;
};
}
