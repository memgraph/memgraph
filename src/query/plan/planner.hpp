// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// @file
/// This file is an entry point for invoking various planners via the following
/// API:
///   * `MakeLogicalPlanForSingleQuery`
///   * `MakeLogicalPlan`

#pragma once

#include <utility>

#include "query/plan/cost_estimator.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/preprocess.hpp"
#include "query/plan/pretty_print.hpp"
#include "query/plan/rewrite/index_lookup.hpp"
#include "query/plan/rewrite/join.hpp"
#include "query/plan/rule_based_planner.hpp"
#include "query/plan/variable_start_planner.hpp"
#include "query/plan/vertex_count_cache.hpp"

namespace memgraph::query {

class AstStorage;
class SymbolTable;

namespace plan {

class PostProcessor final {
  Parameters parameters_;

 public:
  IndexHints index_hints_{};

  using ProcessedPlan = std::unique_ptr<LogicalOperator>;

  explicit PostProcessor(Parameters parameters) : parameters_(std::move(parameters)) {}

  template <class TDbAccessor>
  PostProcessor(Parameters parameters, std::vector<IndexHint> index_hints, TDbAccessor *db)
      : parameters_(std::move(parameters)), index_hints_(IndexHints(index_hints, db)) {}

  template <class TPlanningContext>
  std::unique_ptr<LogicalOperator> Rewrite(std::unique_ptr<LogicalOperator> plan, TPlanningContext *context) {
    auto index_lookup_plan =
        RewriteWithIndexLookup(std::move(plan), context->symbol_table, context->ast_storage, context->db, index_hints_);
    return RewriteWithJoinRewriter(std::move(index_lookup_plan), context->symbol_table, context->ast_storage,
                                   context->db);
  }

  template <class TVertexCounts>
  PlanCost EstimatePlanCost(const std::unique_ptr<LogicalOperator> &plan, TVertexCounts *vertex_counts,
                            const SymbolTable &table) {
    return query::plan::EstimatePlanCost(vertex_counts, table, parameters_, *plan, index_hints_);
  }
};

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
auto MakeLogicalPlanForSingleQuery(QueryParts query_parts, PlanningContext<TDbAccessor> *context) {
  context->bound_symbols.clear();
  return TPlanner<PlanningContext<TDbAccessor>>(context).Plan(query_parts);
}

/// Generates the LogicalOperator tree and returns the resulting plan.
///
/// @tparam TPlanningContext Type of the context used.
/// @tparam TPlanPostProcess Type of the plan post processor used.
///
/// @param context PlanningContext used for generating plans.
/// @param post_process performs plan rewrites and cost estimation.
/// @param use_variable_planner boolean flag to choose which planner to use.
///
/// @return pair consisting of the final `TPlanPostProcess::ProcessedPlan` and
/// the estimated cost of that plan as a `double`.
template <class TPlanningContext, class TPlanPostProcess>
auto MakeLogicalPlan(TPlanningContext *context, TPlanPostProcess *post_process, bool use_variable_planner) {
  auto query_parts = CollectQueryParts(*context->symbol_table, *context->ast_storage, context->query);
  auto &vertex_counts = *context->db;
  double total_cost = std::numeric_limits<double>::max();
  bool curr_uses_index_hint = false;

  using ProcessedPlan = typename TPlanPostProcess::ProcessedPlan;
  ProcessedPlan plan_with_least_cost;

  std::optional<ProcessedPlan> curr_plan;
  if (use_variable_planner) {
    auto plans = MakeLogicalPlanForSingleQuery<VariableStartPlanner>(query_parts, context);
    for (auto plan : plans) {
      // Plans are generated lazily and the current plan will disappear, so
      // it's ok to move it.
      auto rewritten_plan = post_process->Rewrite(std::move(plan), context);
      auto plan_cost = post_process->EstimatePlanCost(rewritten_plan, &vertex_counts, *context->symbol_table);
      // if we have a plan that uses index hints, we reject all the plans that don't use index hinting because we want
      // to force the plan using the index hints to be executed
      if (curr_uses_index_hint && !plan_cost.use_index_hints) continue;
      // if a plan uses index hints, and there is currently not yet a plan that utilizes it, we will take it regardless
      if (plan_cost.use_index_hints && !curr_uses_index_hint) {
        curr_uses_index_hint = plan_cost.use_index_hints;
        curr_plan.emplace(std::move(rewritten_plan));
        total_cost = plan_cost.cost;
        continue;
      }
      // if both plans either use or don't use index hints, we want to use the one with the least cost
      if (!curr_plan || plan_cost.cost < total_cost) {
        curr_uses_index_hint = plan_cost.use_index_hints;
        curr_plan.emplace(std::move(rewritten_plan));
        total_cost = plan_cost.cost;
      }
    }
  } else {
    auto plan = MakeLogicalPlanForSingleQuery<RuleBasedPlanner>(query_parts, context);
    auto rewritten_plan = post_process->Rewrite(std::move(plan), context);
    total_cost = post_process->EstimatePlanCost(rewritten_plan, &vertex_counts, *context->symbol_table).cost;
    curr_plan.emplace(std::move(rewritten_plan));
  }

  plan_with_least_cost = std::move(*curr_plan);

  return std::make_pair(std::move(plan_with_least_cost), total_cost);
}

template <class TPlanningContext>
auto MakeLogicalPlan(TPlanningContext *context, const Parameters &parameters, bool use_variable_planner) {
  PostProcessor post_processor(parameters, context->query->index_hints_, context->db);
  return MakeLogicalPlan(context, &post_processor, use_variable_planner);
}

}  // namespace plan

}  // namespace memgraph::query
