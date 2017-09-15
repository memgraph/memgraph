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
#include "query/plan/vertex_count_cache.hpp"
#include "threading/sync/spinlock.hpp"
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
    Context ctx(db_accessor);
    ctx.is_query_cached_ = FLAGS_ast_cache;
    std::map<std::string, TypedValue> summary;

    // query -> stripped query
    StrippedQuery stripped(query);
    // stripped query -> high level tree
    AstTreeStorage ast_storage = [&]() {
      if (!ctx.is_query_cached_) {
        // This is totally fine, since we don't really expect anyone to turn off
        // the cache.
        if (!params.empty()) {
          throw utils::NotYetImplemented(
              "Params not implemented if ast cache is turned off");
        }

        // stripped query -> AST
        auto parser = [&] {
          // Be careful about unlocking since parser can throw.
          std::unique_lock<SpinLock> guard(antlr_lock_);
          return std::make_unique<frontend::opencypher::Parser>(query);
        }();
        auto low_level_tree = parser->tree();

        // AST -> high level tree
        frontend::CypherMainVisitor visitor(ctx);
        visitor.visit(low_level_tree);
        return std::move(visitor.storage());
      }

      auto ast_cache_accessor = ast_cache_.access();
      auto ast_it = ast_cache_accessor.find(stripped.hash());
      if (ast_it == ast_cache_accessor.end()) {
        // stripped query -> AST
        auto parser = [&] {
          // Be careful about unlocking since parser can throw.
          std::unique_lock<SpinLock> guard(antlr_lock_);
          return std::make_unique<frontend::opencypher::Parser>(
              stripped.query());
        }();
        auto low_level_tree = parser->tree();

        // AST -> high level tree
        frontend::CypherMainVisitor visitor(ctx);
        visitor.visit(low_level_tree);

        // Cache it.
        ast_it = ast_cache_accessor
                     .insert(stripped.hash(), std::move(visitor.storage()))
                     .first;
      }

      // Update context with provided parameters.
      ctx.parameters_ = stripped.literals();
      for (const auto &param_pair : stripped.parameters()) {
        auto param_it = params.find(param_pair.second);
        if (param_it == params.end()) {
          throw query::UnprovidedParameterError(
              fmt::format("Parameter$ {} not provided", param_pair.second));
        }
        ctx.parameters_.Add(param_pair.first, param_it->second);
      }

      AstTreeStorage new_ast;
      ast_it->second.query()->Clone(new_ast);
      return new_ast;
    }();
    auto frontend_time = frontend_timer.Elapsed();

    utils::Timer planning_timer;
    // symbol table fill
    SymbolGenerator symbol_generator(ctx.symbol_table_);
    ast_storage.query()->Accept(symbol_generator);

    // high level tree -> logical plan
    std::unique_ptr<plan::LogicalOperator> logical_plan;
    auto vertex_counts = plan::MakeVertexCountCache(db_accessor);
    double query_plan_cost_estimation = 0.0;
    if (FLAGS_query_cost_planner) {
      auto plans = plan::MakeLogicalPlan<plan::VariableStartPlanner>(
          ast_storage, ctx.symbol_table_, vertex_counts);
      double min_cost = std::numeric_limits<double>::max();
      for (auto &plan : plans) {
        auto cost = EstimatePlanCost(vertex_counts, ctx.parameters_, *plan);
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
          ast_storage, ctx.symbol_table_, vertex_counts);
      query_plan_cost_estimation =
          EstimatePlanCost(vertex_counts, ctx.parameters_, *logical_plan);
    }

    // generate frame based on symbol table max_position
    Frame frame(ctx.symbol_table_.max_position());
    auto planning_time = planning_timer.Elapsed();

    utils::Timer execution_timer;
    std::vector<std::string> header;
    std::vector<Symbol> output_symbols(
        logical_plan->OutputSymbols(ctx.symbol_table_));
    if (!output_symbols.empty()) {
      // Since we have output symbols, this means that the query contains RETURN
      // clause, so stream out the results.

      // generate header
      for (const auto &symbol : output_symbols) {
        // When the symbol is aliased or expanded from '*' (inside RETURN or
        // WITH), then there is no token position, so use symbol name.
        // Otherwise, find the name from stripped query.
        if (symbol.token_position() == -1)
          header.push_back(symbol.name());
        else
          header.push_back(
              stripped.named_expressions().at(symbol.token_position()));
      }
      stream.Header(header);

      // stream out results
      auto cursor = logical_plan->MakeCursor(db_accessor);
      while (cursor->Pull(frame, ctx)) {
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
      while (cursor->Pull(frame, ctx)) continue;
    } else {
      throw QueryRuntimeException("Unknown top level LogicalOperator");
    }
    auto execution_time = execution_timer.Elapsed();

    summary["parsing_time"] = frontend_time.count();
    summary["planning_time"] = planning_time.count();
    summary["plan_execution_time"] = execution_time.count();
    summary["cost_estimate"] = query_plan_cost_estimation;

    // TODO: set summary['type'] based on transaction metadata
    // the type can't be determined based only on top level LogicalOp
    // (for example MATCH DELETE RETURN will have Produce as it's top)
    // for now always use "rw" because something must be set, but it doesn't
    // have to be correct (for Bolt clients)
    summary["type"] = "rw";
    stream.Summary(summary);
    DLOG(INFO) << "Executed '" << query << "', params: " << params
               << ", summary: " << summary;
  }

 private:
  ConcurrentMap<HashType, AstTreeStorage> ast_cache_;
  // Antlr has singleton instance that is shared between threads. It is
  // protected by locks inside of antlr. Unfortunately, they are not protected
  // in a very good way. Once we have antlr version without race conditions we
  // can remove this lock. This will probably never happen since antlr
  // developers introduce more bugs in each version. Fortunately, we have cache
  // so this lock probably won't impact performance much...
  SpinLock antlr_lock_;
};

}  // namespace query
