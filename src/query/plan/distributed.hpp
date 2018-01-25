/// @file
#pragma once

#include <memory>

#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"

namespace query::plan {

/// Complete plan split into master/worker parts.
struct DistributedPlan {
  int64_t plan_id;
  /// Plan to be executed on the master server.
  std::unique_ptr<LogicalOperator> master_plan;
  /// Plan to be executed on each worker.
  ///
  /// Worker plan is also shared in the master plan.
  std::shared_ptr<LogicalOperator> worker_plan;
  /// Ast storage with newly added expressions.
  AstTreeStorage ast_storage;
  /// Symbol table with newly added symbols.
  SymbolTable symbol_table;
};

/// Creates a `DistributedPlan` from a regular plan.
DistributedPlan MakeDistributedPlan(const LogicalOperator &plan,
                                    const SymbolTable &symbol_table,
                                    std::atomic<int64_t> &next_plan_id);

}  // namespace query::plan
