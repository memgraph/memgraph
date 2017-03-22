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
  Config config_;
  GraphDbAccessor &db_accessor_;
};
}
