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

// Shorthand for a vector of pairs (logical_plan, cost).
typedef std::vector<
    std::pair<std::unique_ptr<query::plan::LogicalOperator>, double>>
    PlansWithCost;

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
