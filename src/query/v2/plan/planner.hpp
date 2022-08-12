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

/// @file
/// This file is an entry point for invoking various planners via the following
/// API:
///   * `MakeLogicalPlanForSingleQuery`
///   * `MakeLogicalPlan`

#pragma once

#include "query/v2/plan/cost_estimator.hpp"
#include "query/v2/plan/operator.hpp"
#include "query/v2/plan/preprocess.hpp"
#include "query/v2/plan/pretty_print.hpp"
#include "query/v2/plan/rewrite/index_lookup.hpp"
#include "query/v2/plan/rule_based_planner.hpp"
#include "query/v2/plan/variable_start_planner.hpp"
#include "query/v2/plan/vertex_count_cache.hpp"

namespace memgraph::query::v2 {

class AstStorage;
class SymbolTable;

namespace plan {

class PostProcessor final {
  Parameters parameters_;

 public:
  using ProcessedPlan = std::unique_ptr<LogicalOperator>;

  explicit PostProcessor(const Parameters &parameters) : parameters_(parameters) {}

  template <class TPlanningContext>
  std::unique_ptr<LogicalOperator> Rewrite(std::unique_ptr<LogicalOperator> plan, TPlanningContext *context) {
    return RewriteWithIndexLookup(std::move(plan), context->symbol_table, context->ast_storage, context->db);
  }

  template <class TVertexCounts>
  double EstimatePlanCost(const std::unique_ptr<LogicalOperator> &plan, TVertexCounts *vertex_counts) {
    return query::v2::plan::EstimatePlanCost(vertex_counts, parameters_, *plan);
  }

  template <class TPlanningContext>
  std::unique_ptr<LogicalOperator> MergeWithCombinator(std::unique_ptr<LogicalOperator> curr_op,
                                                       std::unique_ptr<LogicalOperator> last_op, const Tree &combinator,
                                                       TPlanningContext *context) {
    if (const auto *union_ = utils::Downcast<const CypherUnion>(&combinator)) {
      return std::unique_ptr<LogicalOperator>(
          impl::GenUnion(*union_, std::move(last_op), std::move(curr_op), *context->symbol_table));
    }
    throw utils::NotYetImplemented("query combinator");
  }

  template <class TPlanningContext>
  std::unique_ptr<LogicalOperator> MakeDistinct(std::unique_ptr<LogicalOperator> last_op, TPlanningContext *context) {
    auto output_symbols = last_op->OutputSymbols(*context->symbol_table);
    return std::make_unique<Distinct>(std::move(last_op), output_symbols);
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
auto MakeLogicalPlanForSingleQuery(std::vector<SingleQueryPart> single_query_parts,
                                   PlanningContext<TDbAccessor> *context) {
  context->bound_symbols.clear();
  return TPlanner<PlanningContext<TDbAccessor>>(context).Plan(single_query_parts);
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
  double total_cost = 0;

  using ProcessedPlan = typename TPlanPostProcess::ProcessedPlan;
  ProcessedPlan last_plan;

  for (const auto &query_part : query_parts.query_parts) {
    std::optional<ProcessedPlan> curr_plan;
    double min_cost = std::numeric_limits<double>::max();

    if (use_variable_planner) {
      auto plans = MakeLogicalPlanForSingleQuery<VariableStartPlanner>(query_part.single_query_parts, context);
      for (auto plan : plans) {
        // Plans are generated lazily and the current plan will disappear, so
        // it's ok to move it.
        auto rewritten_plan = post_process->Rewrite(std::move(plan), context);
        double cost = post_process->EstimatePlanCost(rewritten_plan, &vertex_counts);
        if (!curr_plan || cost < min_cost) {
          curr_plan.emplace(std::move(rewritten_plan));
          min_cost = cost;
        }
      }
    } else {
      auto plan = MakeLogicalPlanForSingleQuery<RuleBasedPlanner>(query_part.single_query_parts, context);
      auto rewritten_plan = post_process->Rewrite(std::move(plan), context);
      min_cost = post_process->EstimatePlanCost(rewritten_plan, &vertex_counts);
      curr_plan.emplace(std::move(rewritten_plan));
    }

    total_cost += min_cost;
    if (query_part.query_combinator) {
      last_plan = post_process->MergeWithCombinator(std::move(*curr_plan), std::move(last_plan),
                                                    *query_part.query_combinator, context);
    } else {
      last_plan = std::move(*curr_plan);
    }
  }

  if (query_parts.distinct) {
    last_plan = post_process->MakeDistinct(std::move(last_plan), context);
  }

  return std::make_pair(std::move(last_plan), total_cost);
}

template <class TPlanningContext>
auto MakeLogicalPlan(TPlanningContext *context, const Parameters &parameters, bool use_variable_planner) {
  PostProcessor post_processor(parameters);
  return MakeLogicalPlan(context, &post_processor, use_variable_planner);
}

}  // namespace plan

}  // namespace memgraph::query::v2
