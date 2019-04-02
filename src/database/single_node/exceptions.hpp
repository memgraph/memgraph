/// @file

#pragma once

#include "utils/exceptions.hpp"

namespace database {
/// Thrown when creating an index which already exists.
class IndexExistsException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

/// Thrown on concurrent index creation when the transaction engine fails to
/// start a new transaction.
class IndexTransactionException : public utils::BasicException {
  using utils::BasicException::BasicException;
};
}  // namespace database
