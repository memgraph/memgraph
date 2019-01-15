#pragma once

#include "antlr4-runtime.h"
#include "database/graph_db_accessor.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/parameters.hpp"
#include "query/plan/profile.hpp"

namespace query {

struct EvaluationContext {
  int64_t timestamp{-1};
  Parameters parameters;
};

class Context {
 public:
  Context(const Context &) = delete;
  Context &operator=(const Context &) = delete;
  Context(Context &&) = default;
  Context &operator=(Context &&) = default;

  explicit Context(database::GraphDbAccessor &db_accessor)
      : db_accessor_(db_accessor) {}

  database::GraphDbAccessor &db_accessor_;
  SymbolTable symbol_table_;
  EvaluationContext evaluation_context_;
  bool is_profile_query_{false};
  plan::ProfilingStats stats_;
  plan::ProfilingStats *stats_root_{nullptr};
};

struct ParsingContext {
  bool is_query_cached = false;
};

}  // namespace query
