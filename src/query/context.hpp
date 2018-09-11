#pragma once

#include "antlr4-runtime.h"
#include "database/graph_db_accessor.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/parameters.hpp"

namespace auth {
class Auth;
}  // namespace auth

namespace integrations::kafka {
class Streams;
}  // namespace integrations::kafka

namespace query {

struct EvaluationContext {
  int64_t timestamp{-1};
  query::Parameters parameters;
};

class Context {
 public:
  // Since we also return some information from context (is_index_created_) we
  // need to be sure that we have only one Context instance per query.
  Context(const Context &) = delete;
  Context &operator=(const Context &) = delete;
  Context(Context &&) = default;
  Context &operator=(Context &&) = default;

  explicit Context(database::GraphDbAccessor &db_accessor)
      : db_accessor_(db_accessor) {}
  database::GraphDbAccessor &db_accessor_;
  bool in_explicit_transaction_ = false;
  bool is_index_created_ = false;
  SymbolTable symbol_table_;
  EvaluationContext evaluation_context_;

  auth::Auth *auth_ = nullptr;
  integrations::kafka::Streams *kafka_streams_ = nullptr;
};

struct ParsingContext {
  bool is_query_cached = false;
};

}  // namespace query
