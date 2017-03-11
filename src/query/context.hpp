#pragma once

#include "antlr4-runtime.h"
#include "database/graph_db_accessor.hpp"

namespace query {

class TypedcheckedTree {};

class LogicalPlan {};

class Context;
class Query;

class LogicalPlanGenerator {
public:
  std::vector<LogicalPlan> Generate(TypedcheckedTree &, Context &) {
    return {LogicalPlan()};
  }
};

struct Config {
  LogicalPlanGenerator logical_plan_generator;
};

class Context {
public:
  Context(Config config, GraphDbAccessor &db_accessor)
      : config_(config), db_accessor_(db_accessor) {}
  int next_uid() { return uid_counter_++; }

  Config config_;
  GraphDbAccessor &db_accessor_;
  int uid_counter_ = 0;
};

class LogicalPlanner {
public:
  LogicalPlanner(Context ctx) : ctx_(ctx) {}

  LogicalPlan Apply(TypedcheckedTree typedchecked_tree) {
    return ctx_.config_.logical_plan_generator.Generate(typedchecked_tree,
                                                        ctx_)[0];
  }

private:
  Context ctx_;
};

class HighLevelAstConversion {
public:
  std::shared_ptr<Query> Apply(Context &ctx, antlr4::tree::ParseTree *tree);
};
}
