#pragma once

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"

namespace database {
class GraphDbAccessor;
}

struct InteractivePlan {
  // Original plan after going only through the RuleBasedPlanner.
  std::unique_ptr<query::plan::LogicalOperator> unoptimized_plan;
  // Storage for the AST used in unoptimized_plan
  query::AstStorage ast_storage;
  // Final plan after being rewritten and optimized.
  std::unique_ptr<query::plan::LogicalOperator> final_plan;
  // Cost of the final plan.
  double cost;
};

typedef std::vector<InteractivePlan> PlansWithCost;

// Encapsulates a consoles command function.
struct Command {
  typedef std::vector<std::string> Args;
  // Function of this command
  std::function<void(database::GraphDbAccessor &, const query::SymbolTable &,
                     PlansWithCost &, const Args &, const query::AstStorage &)>
      function;
  // Number of arguments the function works with.
  int arg_count;
  // Explanation of the command.
  std::string documentation;
};

#define DEFCOMMAND(Name)                                              \
  void Name##Command(database::GraphDbAccessor &dba,                  \
                     const query::SymbolTable &symbol_table,          \
                     PlansWithCost &plans, const Command::Args &args, \
                     const query::AstStorage &ast_storage)

void AddCommand(const std::string &name, const Command &command);

void RunInteractivePlanning(database::GraphDbAccessor *dba);
