#pragma once

#include <ctime>
#include <limits>

#include "database/graph_db_accessor.hpp"
#include "query/context.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/cost_estimator.hpp"
#include "query/plan/planner.hpp"

namespace query {

class Interpreter {
 public:
  template <typename Stream>
  void Interpret(const std::string &query, GraphDbAccessor &db_accessor,
                 Stream &stream) {
    clock_t start_time = clock();

    Config config;
    Context ctx(config, db_accessor);
    std::map<std::string, TypedValue> summary;

    // query -> AST
    frontend::opencypher::Parser parser(query);

    auto low_level_tree = parser.tree();

    clock_t antlr_end_time = clock();

    // AST -> high level tree
    frontend::CypherMainVisitor visitor(ctx);
    visitor.visit(low_level_tree);
    auto high_level_tree = visitor.query();

    // symbol table fill
    SymbolTable symbol_table;
    SymbolGenerator symbol_generator(symbol_table);
    high_level_tree->Accept(symbol_generator);

    // high level tree -> logical plan
    std::unique_ptr<plan::LogicalOperator> logical_plan;
    double query_plan_cost_estimation = 0.0;
    // TODO: Use gflags
    bool FLAGS_query_cost_planner = true;
    if (FLAGS_query_cost_planner) {
      auto plans = plan::MakeLogicalPlan<plan::VariableStartPlanner>(
          visitor.storage(), symbol_table, &db_accessor);
      double min_cost = std::numeric_limits<double>::max();
      for (auto &plan : plans) {
        plan::CostEstimator estimator(db_accessor);
        plan->Accept(estimator);
        auto cost = estimator.cost();
        if (!logical_plan || cost < min_cost) {
          // We won't be iterating over plans anymore, so it's ok to invalidate
          // unique_ptrs inside.
          logical_plan = std::move(plan);
          min_cost = cost;
        }
      }
      query_plan_cost_estimation = min_cost;
    } else {
      logical_plan = plan::MakeLogicalPlan<plan::RuleBasedPlanner>(
          visitor.storage(), symbol_table, &db_accessor);
      plan::CostEstimator cost_estimator(db_accessor);
      logical_plan->Accept(cost_estimator);
      query_plan_cost_estimation = cost_estimator.cost();
    }

    // generate frame based on symbol table max_position
    Frame frame(symbol_table.max_position());

    clock_t planning_end_time = clock();

    std::vector<std::string> header;
    std::vector<Symbol> output_symbols(
        logical_plan->OutputSymbols(symbol_table));
    if (!output_symbols.empty()) {
      // Since we have output symbols, this means that the query contains RETURN
      // clause, so stream out the results.

      // generate header
      for (const auto &symbol : output_symbols) header.push_back(symbol.name());
      stream.Header(header);

      // stream out results
      auto cursor = logical_plan->MakeCursor(db_accessor);
      while (cursor->Pull(frame, symbol_table)) {
        std::vector<TypedValue> values;
        for (const auto &symbol : output_symbols)
          values.emplace_back(frame[symbol]);
        stream.Result(values);
      }
    } else if (dynamic_cast<plan::CreateNode *>(logical_plan.get()) ||
               dynamic_cast<plan::CreateExpand *>(logical_plan.get()) ||
               dynamic_cast<plan::SetProperty *>(logical_plan.get()) ||
               dynamic_cast<plan::SetProperties *>(logical_plan.get()) ||
               dynamic_cast<plan::SetLabels *>(logical_plan.get()) ||
               dynamic_cast<plan::RemoveProperty *>(logical_plan.get()) ||
               dynamic_cast<plan::RemoveLabels *>(logical_plan.get()) ||
               dynamic_cast<plan::Delete *>(logical_plan.get()) ||
               dynamic_cast<plan::Merge *>(logical_plan.get())) {
      stream.Header(header);
      auto cursor = logical_plan->MakeCursor(db_accessor);
      while (cursor->Pull(frame, symbol_table)) continue;
    } else {
      throw QueryRuntimeException("Unknown top level LogicalOperator");
    }

    clock_t execution_end_time = clock();

    // helper function for calculating time in seconds
    auto time_second = [](clock_t start, clock_t end) {
      return TypedValue(double(end - start) / CLOCKS_PER_SEC);
    };

    summary["query_parsing_time"] = time_second(start_time, antlr_end_time);
    summary["query_planning_time"] =
        time_second(antlr_end_time, planning_end_time);
    summary["query_plan_execution_time"] =
        time_second(planning_end_time, execution_end_time);
    summary["query_cost_estimate"] = query_plan_cost_estimation;

    // TODO: set summary['type'] based on transaction metadata
    // the type can't be determined based only on top level LogicalOp
    // (for example MATCH DELETE RETURN will have Produce as it's top)
    // for now always use "rw" because something must be set, but it doesn't
    // have to be correct (for Bolt clients)
    summary["type"] = "rw";
    stream.Summary(summary);
  }

 private:
  //  ConcurrentMap<HashType, std::unique_ptr<QueryPlanLib>> query_plans;
};

}  // namespace query
