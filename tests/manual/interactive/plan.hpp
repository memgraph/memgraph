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

#include <string>

#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/plan/planner.hpp"

#include "db_accessor.hpp"

struct InteractivePlan {
  // Original plan after going only through the RuleBasedPlanner.
  std::unique_ptr<memgraph::query::plan::LogicalOperator> unoptimized_plan;
  // Storage for the AST used in unoptimized_plan
  memgraph::query::AstStorage ast_storage;
  // Final plan after being rewritten and optimized.
  std::unique_ptr<memgraph::query::plan::LogicalOperator> final_plan;
  // Cost of the final plan.
  double cost;
};

inline memgraph::query::Query *MakeAst(const std::string &query, memgraph::query::AstStorage *storage) {
  memgraph::query::frontend::ParsingContext parsing_context;
  parsing_context.is_query_cached = false;
  // query -> AST
  auto parser = std::make_unique<memgraph::query::frontend::opencypher::Parser>(query);
  // AST -> high level tree
  memgraph::query::frontend::CypherMainVisitor visitor(parsing_context, storage);
  visitor.visit(parser->tree());
  return visitor.query();
}

// Returns a list of InteractivePlan instances, sorted in the ascending order by
// cost.
inline auto MakeLogicalPlans(memgraph::query::CypherQuery *query, memgraph::query::AstStorage &ast,
                             memgraph::query::SymbolTable &symbol_table, InteractiveDbAccessor *dba) {
  auto query_parts = memgraph::query::plan::CollectQueryParts(symbol_table, ast, query);
  std::vector<InteractivePlan> interactive_plans;
  auto ctx = memgraph::query::plan::MakePlanningContext(&ast, &symbol_table, query, dba);
  if (query_parts.query_parts.size() <= 0) {
    std::cerr << "Failed to extract query parts" << std::endl;
    std::exit(EXIT_FAILURE);
  }
  memgraph::query::Parameters parameters;
  memgraph::query::plan::PostProcessor post_process(parameters);
  auto plans = memgraph::query::plan::MakeLogicalPlanForSingleQuery<memgraph::query::plan::VariableStartPlanner>(
      query_parts.query_parts.at(0).single_query_parts, &ctx);
  for (auto plan : plans) {
    memgraph::query::AstStorage ast_copy;
    auto unoptimized_plan = plan->Clone(&ast_copy);
    auto rewritten_plan = post_process.Rewrite(std::move(plan), &ctx);
    double cost = post_process.EstimatePlanCost(rewritten_plan, dba);
    interactive_plans.push_back(
        InteractivePlan{std::move(unoptimized_plan), std::move(ast_copy), std::move(rewritten_plan), cost});
  }
  std::stable_sort(interactive_plans.begin(), interactive_plans.end(),
                   [](const auto &a, const auto &b) { return a.cost < b.cost; });
  return interactive_plans;
}
