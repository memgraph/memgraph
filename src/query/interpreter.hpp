#pragma once

#include <ctime>
#include <limits>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "database/graph_db_accessor.hpp"
#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/frontend/stripped.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/cost_estimator.hpp"
#include "query/plan/planner.hpp"
#include "utils/timer.hpp"

// TODO: Remove ast_cache flag and add flag that limits cache size.
DECLARE_bool(ast_cache);
DECLARE_bool(query_cost_planner);

namespace query {

class Interpreter {
 public:
  Interpreter() {}
  template <typename Stream>
  void Interpret(const std::string &query, GraphDbAccessor &db_accessor,
                 Stream &stream,
                 const std::map<std::string, TypedValue> &params) {
    utils::Timer frontend_timer;
    Config config;
    Context ctx(config, db_accessor);
    std::map<std::string, TypedValue> summary;

    // stripped query -> high level tree
    AstTreeStorage ast_storage = [&]() {
      if (!FLAGS_ast_cache) {
        // This is totally fine, since we don't really expect anyone to turn off
        // the cache.
        if (!params.empty()) {
          throw utils::NotYetImplemented(
              "Params not implemented if ast cache is turned off");
        }

        // stripped query -> AST
        auto parser = [&] {
          // Be careful about unlocking since parser can throw.
          std::unique_lock<SpinLock>(antlr_lock_);
          return std::make_unique<frontend::opencypher::Parser>(query);
        }();
        auto low_level_tree = parser->tree();

        // AST -> high level tree
        frontend::CypherMainVisitor visitor(ctx);
        visitor.visit(low_level_tree);
        return std::move(visitor.storage());
      }

      // query -> stripped query
      StrippedQuery stripped(query);

      auto ast_cache_accessor = ast_cache_.access();
      auto it = ast_cache_accessor.find(stripped.hash());
      if (it == ast_cache_accessor.end()) {
        // stripped query -> AST
        auto parser = [&] {
          // Be careful about unlocking since parser can throw.
          std::unique_lock<SpinLock>(antlr_lock_);
          return std::make_unique<frontend::opencypher::Parser>(
              stripped.query());
        }();
        auto low_level_tree = parser->tree();

        // AST -> high level tree
        frontend::CypherMainVisitor visitor(ctx);
        visitor.visit(low_level_tree);

        // Cache it.
        it = ast_cache_accessor
                 .insert(stripped.hash(),
                         CachedAst(std::move(visitor.storage())))
                 .first;
      }

      // Update literals map with provided parameters.
      auto literals = stripped.literals();
      for (const auto &param_pair : stripped.parameters()) {
        auto param_it = params.find(param_pair.second);
        if (param_it == params.end()) {
          throw query::UnprovidedParameterError(
              fmt::format("Parameter$ {} not provided", param_pair.second));
        }
        literals.Add(param_pair.first, param_it->second);
      }

      // Plug literals, parameters and named expressions.
      return it->second.Plug(literals, stripped.named_expressions());
    }();
    auto frontend_time = frontend_timer.Elapsed();

    utils::Timer planning_timer;
    // symbol table fill
    SymbolTable symbol_table;
    SymbolGenerator symbol_generator(symbol_table);
    ast_storage.query()->Accept(symbol_generator);

    // high level tree -> logical plan
    std::unique_ptr<plan::LogicalOperator> logical_plan;
    double query_plan_cost_estimation = 0.0;
    if (FLAGS_query_cost_planner) {
      auto plans = plan::MakeLogicalPlan<plan::VariableStartPlanner>(
          ast_storage, symbol_table, db_accessor);
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
          ast_storage, symbol_table, db_accessor);
      plan::CostEstimator cost_estimator(db_accessor);
      logical_plan->Accept(cost_estimator);
      query_plan_cost_estimation = cost_estimator.cost();
    }

    // generate frame based on symbol table max_position
    Frame frame(symbol_table.max_position());
    auto planning_time = planning_timer.Elapsed();

    utils::Timer execution_timer;
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
               dynamic_cast<plan::Merge *>(logical_plan.get()) ||
               dynamic_cast<plan::CreateIndex *>(logical_plan.get())) {
      stream.Header(header);
      auto cursor = logical_plan->MakeCursor(db_accessor);
      while (cursor->Pull(frame, symbol_table)) continue;
    } else {
      throw QueryRuntimeException("Unknown top level LogicalOperator");
    }
    auto execution_time = execution_timer.Elapsed();

    summary["query_parsing_time"] = frontend_time.count();
    summary["query_planning_time"] = planning_time.count();
    summary["query_plan_execution_time"] = execution_time.count();
    summary["query_cost_estimate"] = query_plan_cost_estimation;

    // TODO: set summary['type'] based on transaction metadata
    // the type can't be determined based only on top level LogicalOp
    // (for example MATCH DELETE RETURN will have Produce as it's top)
    // for now always use "rw" because something must be set, but it doesn't
    // have to be correct (for Bolt clients)
    summary["type"] = "rw";
    stream.Summary(summary);
    LOG(INFO) << "Execute " << query << ", " << summary;
  }

 private:
  ConcurrentMap<HashType, CachedAst> ast_cache_;
  // Antlr has singleton instance that is shared between threads. It is
  // protected by locks inside of antlr. Unfortunately, they are not protected
  // in a very good way. Once we have antlr version without race conditions we
  // can remove this lock. This will probably never happen since antlr
  // developers introduce more bugs in each version. Fortunately, we have cache
  // so this lock probably won't impact performance much...
  SpinLock antlr_lock_;
};

}  // namespace query
