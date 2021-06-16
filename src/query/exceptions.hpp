#pragma once

#include "utils/exceptions.hpp"

#include <fmt/format.h>

namespace query {

/**
 * @brief Base class of all query language related exceptions. All exceptions
 * derived from this one will be interpreted as ClientError-s, i. e. if client
 * executes same query again without making modifications to the database data,
 * query will fail again.
 */
class QueryException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

class LexingException : public QueryException {
 public:
  using QueryException::QueryException;
  LexingException() : QueryException("") {}
};

class SyntaxException : public QueryException {
 public:
  using QueryException::QueryException;
  SyntaxException() : QueryException("") {}
};

// TODO: Figure out what information to put in exception.
// Error reporting is tricky since we get stripped query and position of error
// in original query is not same as position of error in stripped query. Most
// correct approach would be to do semantic analysis with original query even
// for already hashed queries, but that has obvious performance issues. Other
// approach would be to report some of the semantic errors in runtime of the
// query and only report line numbers of semantic errors (not position in the
// line) if multiple line strings are not allowed by grammar. We could also
// print whole line that contains error instead of specifying line number.
class SemanticException : public QueryException {
 public:
  using QueryException::QueryException;
  SemanticException() : QueryException("") {}
};

class UnboundVariableError : public SemanticException {
 public:
  explicit UnboundVariableError(const std::string &name) : SemanticException("Unbound variable: " + name + ".") {}
};

class RedeclareVariableError : public SemanticException {
 public:
  explicit RedeclareVariableError(const std::string &name) : SemanticException("Redeclaring variable: " + name + ".") {}
};

class TypeMismatchError : public SemanticException {
 public:
  TypeMismatchError(const std::string &name, const std::string &datum, const std::string &expected)
      : SemanticException(fmt::format("Type mismatch: {} already defined as {}, expected {}.", name, datum, expected)) {
  }
};

class UnprovidedParameterError : public QueryException {
 public:
  using QueryException::QueryException;
};

class ProfileInMulticommandTxException : public QueryException {
 public:
  using QueryException::QueryException;
  ProfileInMulticommandTxException() : QueryException("PROFILE not allowed in multicommand transactions.") {}
};

class IndexInMulticommandTxException : public QueryException {
 public:
  using QueryException::QueryException;
  IndexInMulticommandTxException() : QueryException("Index manipulation not allowed in multicommand transactions.") {}
};

class ConstraintInMulticommandTxException : public QueryException {
 public:
  using QueryException::QueryException;
  ConstraintInMulticommandTxException()
      : QueryException(
            "Constraint manipulation not allowed in multicommand "
            "transactions.") {}
};

class InfoInMulticommandTxException : public QueryException {
 public:
  using QueryException::QueryException;
  InfoInMulticommandTxException() : QueryException("Info reporting not allowed in multicommand transactions.") {}
};

/**
 * An exception for an illegal operation that can not be detected
 * before the query starts executing over data.
 */
class QueryRuntimeException : public QueryException {
 public:
  using QueryException::QueryException;
};

// This one is inherited from BasicException and will be treated as
// TransientError, i. e. client will be encouraged to retry execution because it
// could succeed if executed again.
class HintedAbortError : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  HintedAbortError()
      : utils::BasicException(
            "Transaction was asked to abort, most likely because it was "
            "executing longer than time specified by "
            "--query-execution-timeout-sec flag.") {}
};

class ExplicitTransactionUsageException : public QueryRuntimeException {
 public:
  using QueryRuntimeException::QueryRuntimeException;
};

class ReconstructionException : public QueryException {
 public:
  ReconstructionException()
      : QueryException(
            "Record invalid after WITH clause. Most likely deleted by a "
            "preceeding DELETE.") {}
};

class RemoveAttachedVertexException : public QueryRuntimeException {
 public:
  RemoveAttachedVertexException()
      : QueryRuntimeException(
            "Failed to remove node because of it's existing "
            "connections. Consider using DETACH DELETE.") {}
};

class UserModificationInMulticommandTxException : public QueryException {
 public:
  UserModificationInMulticommandTxException()
      : QueryException("Authentication clause not allowed in multicommand transactions.") {}
};

class StreamClauseInMulticommandTxException : public QueryException {
 public:
  StreamClauseInMulticommandTxException() : QueryException("Stream clause not allowed in multicommand transactions.") {}
};

class InvalidArgumentsException : public QueryException {
 public:
  InvalidArgumentsException(const std::string &argument_name, const std::string &message)
      : QueryException(fmt::format("Invalid arguments sent: {} - {}", argument_name, message)) {}
};

class ReplicationModificationInMulticommandTxException : public QueryException {
 public:
  ReplicationModificationInMulticommandTxException()
      : QueryException("Replication clause not allowed in multicommand transactions.") {}
};

class LockPathModificationInMulticommandTxException : public QueryException {
 public:
  LockPathModificationInMulticommandTxException()
      : QueryException("Lock path query not allowed in multicommand transactions.") {}
};

class FreeMemoryModificationInMulticommandTxException : public QueryException {
 public:
  FreeMemoryModificationInMulticommandTxException()
      : QueryException("Free memory query not allowed in multicommand transactions.") {}
};

class TriggerModificationInMulticommandTxException : public QueryException {
 public:
  TriggerModificationInMulticommandTxException()
      : QueryException("Trigger queries not allowed in multicommand transactions.") {}
};

class IsolationLevelModificationInMulticommandTxException : public QueryException {
 public:
  IsolationLevelModificationInMulticommandTxException()
      : QueryException("Isolation level cannot be modified in multicommand transactions.") {}
};
}  // namespace query
