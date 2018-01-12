#pragma once

#include "antlr4-runtime.h"
#include "database/graph_db_accessor.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/parameters.hpp"

namespace query {

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
  SymbolTable symbol_table_;
  Parameters parameters_;
  bool is_query_cached_ = false;
  bool in_explicit_transaction_ = false;
  bool is_index_created_ = false;
};

}  // namespace query
