/// @file
/// This file is an entry point for invoking various planners via
/// `MakeLogicalPlan` API.

#pragma once

#include "query/plan/preprocess.hpp"
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
/// @param context PlanningContext used for generating plans.
/// @return @c PlanResult which depends on the @c TPlanner used.
///
/// @sa PlanningContext
/// @sa RuleBasedPlanner
/// @sa VariableStartPlanner
template <template <class> class TPlanner, class TDbAccessor>
auto MakeLogicalPlan(PlanningContext<TDbAccessor> &context) {
  context.bound_symbols.clear();
  auto query_parts =
      CollectQueryParts(context.symbol_table, context.ast_storage);
  return TPlanner<decltype(context)>(context).Plan(query_parts);
}

}  // namespace plan

}  // namespace query
