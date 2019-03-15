/// @file
#pragma once

#include <memory>

#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"

namespace query::plan {

/// Complete plan split into master/worker parts.
struct DistributedPlan {
  int64_t master_plan_id;
  /// Plan to be executed on the master server.
  std::unique_ptr<LogicalOperator> master_plan;
  /// Pairs of {plan_id, plan} for execution on each worker.
  std::vector<std::pair<int64_t, std::shared_ptr<LogicalOperator>>>
      worker_plans;
  /// Ast storage with newly added expressions.
  AstStorage ast_storage;
  /// Symbol table with newly added symbols.
  SymbolTable symbol_table;
};

/// Creates a `DistributedPlan` from a regular plan.
DistributedPlan MakeDistributedPlan(
    const AstStorage &ast_storage, const LogicalOperator &plan,
    const SymbolTable &symbol_table, std::atomic<int64_t> &next_plan_id,
    const std::vector<storage::Property> &properties_by_ix);

}  // namespace query::plan
