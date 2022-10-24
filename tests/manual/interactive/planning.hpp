// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "query/db_accessor.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"

#include "plan.hpp"

namespace database {
class GraphDbAccessor;
}

typedef std::vector<InteractivePlan> PlansWithCost;

// Encapsulates a consoles command function.
struct Command {
  typedef std::vector<std::string> Args;
  // Function of this command
  std::function<void(memgraph::query::DbAccessor &, const memgraph::query::SymbolTable &, PlansWithCost &, const Args &,
                     const memgraph::query::AstStorage &)>
      function;
  // Number of arguments the function works with.
  int arg_count;
  // Explanation of the command.
  std::string documentation;
};

#define DEFCOMMAND(Name)                                                                                 \
  void Name##Command(memgraph::query::DbAccessor &dba, const memgraph::query::SymbolTable &symbol_table, \
                     PlansWithCost &plans, const Command::Args &args, const memgraph::query::AstStorage &ast_storage)

void AddCommand(const std::string &name, const Command &command);

void RunInteractivePlanning(memgraph::query::DbAccessor *dba);
