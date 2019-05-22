#pragma once

#include "utils/exceptions.hpp"

namespace mvcc {
class SerializationError : public utils::BasicException {
  static constexpr const char *default_message =
      "Can't serialize due to concurrent operations.";

 public:
  using utils::BasicException::BasicException;
  SerializationError() : BasicException(default_message) {}
};

}  // namespace mvcc
