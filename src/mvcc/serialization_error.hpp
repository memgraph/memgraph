#pragma once

#include <stdexcept>

class SerializationError : public std::runtime_error {
  static constexpr const char* default_message =
      "Can't serialize due to\
        concurrent operation(s)";

 public:
  SerializationError() : runtime_error(default_message) {}

  SerializationError(const std::string& message) : runtime_error(message) {}
};
