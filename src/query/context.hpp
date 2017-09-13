#pragma once

#include "antlr4-runtime.h"
#include "database/graph_db_accessor.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/parameters.hpp"

namespace query {

class Context {
 public:
  Context(GraphDbAccessor &db_accessor) : db_accessor_(db_accessor) {}
  GraphDbAccessor &db_accessor_;
  SymbolTable symbol_table_;
  Parameters parameters_;
};

}  // namespace query
