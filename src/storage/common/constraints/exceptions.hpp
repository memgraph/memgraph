/// @file

#pragma once

#include "utils/exceptions.hpp"

namespace storage::constraints {

/// Thrown when a violation of a constraint occurs.
class ViolationException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

/// Thrown when multiple transactions alter the same constraint.
class SerializationException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

} // namespace database
