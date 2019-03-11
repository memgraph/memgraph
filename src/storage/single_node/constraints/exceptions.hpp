/// @file

#pragma once

#include "utils/exceptions.hpp"

namespace database {
/** Thrown when inserting in an unique constraint. */
class IndexConstraintViolationException : public utils::BasicException {
  using utils::BasicException::BasicException;
};
} // namespace database
