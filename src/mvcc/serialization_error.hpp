#pragma once

#include "utils/exceptions/basic_exception.hpp"

class SerializationError : public BasicException {
  static constexpr const char* default_message =
      "Can't serialize due to\
        concurrent operation(s)";

 public:
  SerializationError() : BasicException(default_message) {}

  SerializationError(const std::string& message) : BasicException(message) {}
};
