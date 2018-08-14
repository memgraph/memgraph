#pragma once

#include "utils/exceptions.hpp"

namespace auth {

/**
 * This exception class is thrown for all exceptions that can occur when dealing
 * with the Auth library.
 */
class AuthException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
};
}  // namespace auth
