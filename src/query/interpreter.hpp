#pragma once

#include <ctime>
#include <limits>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/graph_db_accessor.hpp"
#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/frontend/stripped.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/operator.hpp"
#include "threading/sync/spinlock.hpp"
#include "utils/timer.hpp"

DECLARE_bool(query_cost_planner);
DECLARE_bool(query_plan_cache);
DECLARE_int32(query_plan_cache_ttl);

namespace query {

class Interpreter {
 private:
  class CachedPlan {
   public:
    CachedPlan(std::unique_ptr<plan::LogicalOperator> plan, double cost,
               SymbolTable symbol_table, AstTreeStorage storage)
        : plan_(std::move(plan)),
          cost_(cost),
          symbol_table_(symbol_table),
          ast_storage_(std::move(storage)) {}

    const auto &plan() const { return *plan_; }
    double cost() const { return cost_; }
    const auto &symbol_table() const { return symbol_table_; }

    bool IsExpired() const {
      auto elapsed = cache_timer_.Elapsed();
      return std::chrono::duration_cast<std::chrono::seconds>(elapsed) >
             std::chrono::seconds(FLAGS_query_plan_cache_ttl);
    };

   private:
    std::unique_ptr<plan::LogicalOperator> plan_;
    double cost_;
    SymbolTable symbol_table_;
    AstTreeStorage ast_storage_;
    utils::Timer cache_timer_;
  };

 public:
  Interpreter() {}

  template <typename Stream>
  void Interpret(const std::string &query, GraphDbAccessor &db_accessor,
                 Stream &stream,
                 const std::map<std::string, TypedValue> &params,
                 bool in_explicit_transaction) {
    utils::Timer frontend_timer;
    Context ctx(db_accessor);
    ctx.is_query_cached_ = true;
    ctx.in_explicit_transaction_ = in_explicit_transaction;
    std::map<std::string, TypedValue> summary;

    // query -> stripped query
    StrippedQuery stripped(query);

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

    std::shared_ptr<CachedPlan> cached_plan;
    std::experimental::optional<AstTreeStorage> ast_storage;
    // Check if we have a cached logical plan ready, so that we can skip the
    // whole query -> AST -> logical_plan process.
    auto plan_cache_accessor = plan_cache_.access();
    auto plan_cache_it = plan_cache_accessor.find(stripped.hash());
    if (plan_cache_it != plan_cache_accessor.end() &&
        plan_cache_it->second->IsExpired()) {
      // Remove the expired plan.
      plan_cache_accessor.remove(stripped.hash());
      plan_cache_it = plan_cache_accessor.end();
    }
    if (plan_cache_it == plan_cache_accessor.end()) {
      // We didn't find a cached plan or it was expired.
      // stripped query -> high level tree
      ast_storage = QueryToAst(stripped, ctx);
    } else {
      cached_plan = plan_cache_it->second;
    }

    auto frontend_time = frontend_timer.Elapsed();

    utils::Timer planning_timer;

    auto fill_symbol_table = [](auto &ast_storage, auto &symbol_table) {
      SymbolGenerator symbol_generator(symbol_table);
      ast_storage.query()->Accept(symbol_generator);
    };

    // If the plan is not stored in the cache, `tmp_logical_plan` owns the newly
    // generated plan. Otherwise, it is empty and `cached_plan` owns the plan.
    // In all cases, `logical_plan` references the plan to be used.
    std::unique_ptr<plan::LogicalOperator> tmp_logical_plan;
    const plan::LogicalOperator *logical_plan = nullptr;
    double query_plan_cost_estimation = 0.0;
    if (FLAGS_query_plan_cache) {
      if (!cached_plan) {
        debug_assert(ast_storage, "AST is required to generate a plan");
        fill_symbol_table(*ast_storage, ctx.symbol_table_);
        std::tie(tmp_logical_plan, query_plan_cost_estimation) =
            MakeLogicalPlan(*ast_storage, db_accessor, ctx);
        // Cache the generated plan.
        auto plan_cache_accessor = plan_cache_.access();
        auto plan_cache_it =
            plan_cache_accessor
                .insert(
                    stripped.hash(),
                    std::make_shared<CachedPlan>(
                        std::move(tmp_logical_plan), query_plan_cost_estimation,
                        ctx.symbol_table_, std::move(*ast_storage)))
                .first;
        cached_plan = plan_cache_it->second;
      }
      query_plan_cost_estimation = cached_plan->cost();
      ctx.symbol_table_ = cached_plan->symbol_table();
      logical_plan = &cached_plan->plan();
    } else {
      debug_assert(ast_storage, "Without plan caching, AST must be generated.");
      fill_symbol_table(*ast_storage, ctx.symbol_table_);
      std::tie(tmp_logical_plan, query_plan_cost_estimation) =
          MakeLogicalPlan(*ast_storage, db_accessor, ctx);
      logical_plan = tmp_logical_plan.get();
    }
    // Below this point, ast_storage should not be used. Other than not allowing
    // modifications, the ast_storage may have moved to a cache.

    // Generate frame based on symbol table max_position.
    Frame frame(ctx.symbol_table_.max_position());
    auto planning_time = planning_timer.Elapsed();

    utils::Timer execution_timer;
    std::vector<std::string> header;
    std::vector<Symbol> output_symbols(
        logical_plan->OutputSymbols(ctx.symbol_table_));
    if (!output_symbols.empty()) {
      // Since we have output symbols, this means that the query contains RETURN
      // clause, so stream out the results.

      // Generate header.
      for (const auto &symbol : output_symbols) {
        // When the symbol is aliased or expanded from '*' (inside RETURN or
        // WITH), then there is no token position, so use symbol name.
        // Otherwise, find the name from stripped query.
        header.push_back(utils::FindOr(stripped.named_expressions(),
                                       symbol.token_position(), symbol.name())
                             .first);
      }
      stream.Header(header);

      // Stream out results.
      auto cursor = logical_plan->MakeCursor(db_accessor);
      while (cursor->Pull(frame, ctx)) {
        std::vector<TypedValue> values;
        for (const auto &symbol : output_symbols)
          values.emplace_back(frame[symbol]);
        stream.Result(values);
      }
    } else if (dynamic_cast<const plan::CreateNode *>(logical_plan) ||
               dynamic_cast<const plan::CreateExpand *>(logical_plan) ||
               dynamic_cast<const plan::SetProperty *>(logical_plan) ||
               dynamic_cast<const plan::SetProperties *>(logical_plan) ||
               dynamic_cast<const plan::SetLabels *>(logical_plan) ||
               dynamic_cast<const plan::RemoveProperty *>(logical_plan) ||
               dynamic_cast<const plan::RemoveLabels *>(logical_plan) ||
               dynamic_cast<const plan::Delete *>(logical_plan) ||
               dynamic_cast<const plan::Merge *>(logical_plan) ||
               dynamic_cast<const plan::CreateIndex *>(logical_plan)) {
      stream.Header(header);
      auto cursor = logical_plan->MakeCursor(db_accessor);
      while (cursor->Pull(frame, ctx)) continue;
    } else {
      throw QueryRuntimeException("Unknown top level LogicalOperator");
    }
    auto execution_time = execution_timer.Elapsed();

    if (ctx.is_index_created_) {
      // If index is created we invalidate cache so that we can try to generate
      // better plan with that cache.
      auto accessor = plan_cache_.access();
      for (const auto &cached_plan : accessor) {
        accessor.remove(cached_plan.first);
      }
    }

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
  // stripped query -> high level tree
  AstTreeStorage QueryToAst(const StrippedQuery &stripped, Context &ctx);

  // high level tree -> (logical plan, plan cost)
  // AstTreeStorage and SymbolTable may be modified during planning.
  std::pair<std::unique_ptr<plan::LogicalOperator>, double> MakeLogicalPlan(
      AstTreeStorage &, const GraphDbAccessor &, Context &);

  ConcurrentMap<HashType, AstTreeStorage> ast_cache_;
  ConcurrentMap<HashType, std::shared_ptr<CachedPlan>> plan_cache_;
  // Antlr has singleton instance that is shared between threads. It is
  // protected by locks inside of antlr. Unfortunately, they are not protected
  // in a very good way. Once we have antlr version without race conditions we
  // can remove this lock. This will probably never happen since antlr
  // developers introduce more bugs in each version. Fortunately, we have cache
  // so this lock probably won't impact performance much...
  SpinLock antlr_lock_;
};

}  // namespace query
