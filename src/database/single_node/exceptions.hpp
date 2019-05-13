/// @file

#pragma once

#include "utils/exceptions.hpp"

namespace database {
/// Thrown when creating an index which already exists.
class IndexExistsException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

/// Thrown when the transaction engine fails to start a new transaction.
class TransactionException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

/// Thrown when the data violates given constraints.
class ConstraintViolationException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

}  // namespace database
