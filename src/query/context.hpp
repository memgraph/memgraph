#pragma once

#include "antlr4-runtime.h"
#include "database/graph_db_accessor.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"

class TypedcheckedTree {};

class LogicalPlan {};

class Context;

class LogicalPlanGenerator {
 public:
  std::vector<LogicalPlan> Generate(TypedcheckedTree&, Context&) {
    return {LogicalPlan()};
  }
};

struct Config {
  LogicalPlanGenerator logical_plan_generator;
};

class Context {
 public:
  int uid_counter;
  Context(Config config, GraphDbAccessor& db_accessor)
      : config(config), db_accessor(db_accessor) {}

  Config config;
  GraphDbAccessor& db_accessor;
};

class LogicalPlanner {
 public:
  LogicalPlanner(Context ctx) : ctx_(ctx) {}

  LogicalPlan Apply(TypedcheckedTree typedchecked_tree) {
    return ctx_.config.logical_plan_generator.Generate(typedchecked_tree,
                                                       ctx_)[0];
  }

 private:
  Context ctx_;
};

class HighLevelAstConversion {
 public:
  void Apply(const Context& ctx, antlr4::tree::ParseTree* tree) {
    query::frontend::CypherMainVisitor visitor(ctx);
    visitor.visit(tree);
  }
};
