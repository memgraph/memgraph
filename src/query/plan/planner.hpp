/// @file
/// This file is an entry point for invoking various planners via
/// `MakeLogicalPlan` API.

#pragma once

#include "query/plan/rule_based_planner.hpp"
#include "query/plan/variable_start_planner.hpp"

namespace query {

class AstTreeStorage;
class SymbolTable;

namespace plan {

/// @brief Generates the LogicalOperator tree and returns the resulting plan.
///
/// @tparam TPlanner Type of the planner used for generation.
/// @tparam TDbAccessor Type of the database accessor used for generation.
/// @param storage AstTreeStorage used to construct the operator tree by
///     traversing the @c Query node. The storage may also be used to create new
///     AST nodes for use in operators.
/// @param symbol_table SymbolTable used to determine inputs and outputs of
///     certain operators. Newly created AST nodes may be added to this symbol
///     table.
/// @param db @c TDbAccessor, which is used to query database information in
///     order to improve generated plans.
/// @return @c PlanResult which depends on the @c TPlanner used.
///
/// @sa RuleBasedPlanner
/// @sa VariableStartPlanner
template <template <class> class TPlanner, class TDbAccessor>
auto MakeLogicalPlan(AstTreeStorage &storage, SymbolTable &symbol_table,
                     const TDbAccessor &db) {
  auto query_parts = CollectQueryParts(symbol_table, storage);
  PlanningContext<TDbAccessor> context{symbol_table, storage, db};
  return TPlanner<decltype(context)>(context).Plan(query_parts);
}

}  // namespace plan

}  // namespace query
