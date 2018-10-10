/// @file
/// This file is an entry point for invoking various planners via the following
/// API:
///   * `MakeLogicalPlanForSingleQuery`
///   * `MakeLogicalPlan`

#pragma once

#include "query/plan/cost_estimator.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/preprocess.hpp"
#include "query/plan/pretty_print.hpp"
#include "query/plan/rule_based_planner.hpp"
#include "query/plan/variable_start_planner.hpp"
#include "query/plan/vertex_count_cache.hpp"

namespace query {

class AstStorage;
class SymbolTable;

namespace plan {

/// @brief Generates the LogicalOperator tree for a single query and returns the
/// resulting plan.
///
/// @tparam TPlanner Type of the planner used for generation.
/// @tparam TDbAccessor Type of the database accessor used for generation.
/// @param vector of @c SingleQueryPart from the single query
/// @param context PlanningContext used for generating plans.
/// @return @c PlanResult which depends on the @c TPlanner used.
///
/// @sa PlanningContext
/// @sa RuleBasedPlanner
/// @sa VariableStartPlanner
template <template <class> class TPlanner, class TDbAccessor>
auto MakeLogicalPlanForSingleQuery(
    std::vector<SingleQueryPart> single_query_parts,
    PlanningContext<TDbAccessor> &context) {
  context.bound_symbols.clear();
  return TPlanner<decltype(context)>(context).Plan(single_query_parts);
}

/// Generates the LogicalOperator tree and returns the resulting plan.
///
/// @tparam TPlanningContext Type of the context used.
/// @param context PlanningContext used for generating plans.
/// @param parameters Parameters used in query .
/// @param boolean flag use_variable_planner to choose which planner to use
/// @return pair consisting of the plan's first logical operator @c
/// LogicalOperator and the estimated cost of that plan
template <class TPlanningContext>
auto MakeLogicalPlan(TPlanningContext &context, const Parameters &parameters,
                     bool use_variable_planner) {
  auto query_parts = CollectQueryParts(context.symbol_table,
                                       context.ast_storage, context.query);
  auto &vertex_counts = context.db;
  double total_cost = 0;
  std::unique_ptr<LogicalOperator> last_op;

  for (const auto &query_part : query_parts.query_parts) {
    std::unique_ptr<LogicalOperator> op;
    double min_cost = std::numeric_limits<double>::max();

    if (use_variable_planner) {
      auto plans = MakeLogicalPlanForSingleQuery<VariableStartPlanner>(
          query_part.single_query_parts, context);
      for (auto plan : plans) {
        auto cost = EstimatePlanCost(vertex_counts, parameters, *plan);
        if (!op || cost < min_cost) {
          // Plans are generated lazily and the current plan will disappear, so
          // it's ok to move it.
          op = std::move(plan);
          min_cost = cost;
        }
      }
    } else {
      op = MakeLogicalPlanForSingleQuery<RuleBasedPlanner>(
          query_part.single_query_parts, context);
      min_cost = EstimatePlanCost(vertex_counts, parameters, *op);
    }

    total_cost += min_cost;
    if (auto *union_ =
            dynamic_cast<CypherUnion *>(query_part.query_combinator)) {
      std::shared_ptr<LogicalOperator> curr_op(std::move(op));
      std::shared_ptr<LogicalOperator> prev_op(std::move(last_op));
      last_op = std::unique_ptr<LogicalOperator>(
          impl::GenUnion(*union_, prev_op, curr_op, context.symbol_table));
    } else if (query_part.query_combinator) {
      throw utils::NotYetImplemented("query combinator");
    } else {
      last_op = std::move(op);
    }
  }

  if (query_parts.distinct) {
    std::shared_ptr<LogicalOperator> prev_op(std::move(last_op));
    last_op = std::make_unique<Distinct>(
        prev_op, prev_op->OutputSymbols(context.symbol_table));
  }

  if (context.query->explain_) {
    last_op = std::make_unique<Explain>(
        std::move(last_op),
        context.symbol_table.CreateSymbol("QUERY PLAN", false),
        [](const auto &dba, auto *root, auto *stream) {
          return PrettyPrint(dba, root, stream);
        });
  }

  return std::make_pair(std::move(last_op), total_cost);
}

}  // namespace plan

}  // namespace query
