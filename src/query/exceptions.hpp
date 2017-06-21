#pragma once

#include "utils/exceptions.hpp"

#include <fmt/format.h>

namespace query {

/** @brief Base class of all query language related exceptions.
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
  UnboundVariableError(const std::string &name)
      : SemanticException("Unbound variable: " + name) {}
};

class RedeclareVariableError : public SemanticException {
 public:
  RedeclareVariableError(const std::string &name)
      : SemanticException("Redeclaring variable: " + name) {}
};

class TypeMismatchError : public SemanticException {
 public:
  TypeMismatchError(const std::string &name, const std::string &datum,
                    const std::string &expected)
      : SemanticException(fmt::format(
            "Type mismatch: '{}' already defined as '{}', but expected '{}'.",
            name, datum, expected)) {}
};

/**
 * An exception for an illegal operation that can not be detected
 * before the query starts executing over data.
 */
class QueryRuntimeException : public QueryException {
 public:
  using QueryException::QueryException;
};

// TODO: Move this elsewhere, it has no place in query.
class DecoderException : public utils::StacktraceException {
 public:
  using utils::StacktraceException::StacktraceException;
};

class PlanCompilationException : public utils::StacktraceException {
 public:
  using utils::StacktraceException::StacktraceException;
};

class PlanExecutionException : public utils::StacktraceException {
 public:
  using utils::StacktraceException::StacktraceException;
};

}  // namespace query
