#include "query/interpreter.hpp"

#include <glog/logging.h>

#include "query/exceptions.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/plan/planner.hpp"
#include "query/plan/vertex_count_cache.hpp"
#include "utils/flag_validation.hpp"

DEFINE_HIDDEN_bool(query_cost_planner, true,
                   "Use the cost-estimating query planner.");
DEFINE_bool(query_plan_cache, true, "Cache generated query plans");
DEFINE_VALIDATED_int32(query_plan_cache_ttl, 60,
                       "Time to live for cached query plans, in seconds.",
                       FLAG_IN_RANGE(0, std::numeric_limits<int32_t>::max()));

namespace query {

Interpreter::Results Interpreter::operator()(
    const std::string &query, GraphDbAccessor &db_accessor,
    const std::map<std::string, TypedValue> &params,
    bool in_explicit_transaction) {
  utils::Timer frontend_timer;
  Context ctx(db_accessor);
  ctx.in_explicit_transaction_ = in_explicit_transaction;
  ctx.is_query_cached_ = true;

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

  // Check if we have a cached logical plan ready, so that we can skip the
  // whole query -> AST -> logical_plan process. Note that this local shared_ptr
  // might be the only owner of the CachedPlan (if caching is turned off).
  // Ensure it lives during the whole interpretation.
  std::shared_ptr<CachedPlan> plan(nullptr);
  {
    auto plan_cache_accessor = plan_cache_.access();
    auto plan_cache_it = plan_cache_accessor.find(stripped.hash());
    if (plan_cache_it != plan_cache_accessor.end()) {
      if (plan_cache_it->second->IsExpired()) {
        plan_cache_accessor.remove(stripped.hash());
      } else {
        plan = plan_cache_it->second;
      }
    }
  }

  auto frontend_time = frontend_timer.Elapsed();

  utils::Timer planning_timer;

  if (!plan) {
    AstTreeStorage ast_storage = QueryToAst(stripped, ctx);
    SymbolGenerator symbol_generator(ctx.symbol_table_);
    ast_storage.query()->Accept(symbol_generator);

    std::unique_ptr<plan::LogicalOperator> tmp_logical_plan;
    double query_plan_cost_estimation = 0.0;
    std::tie(tmp_logical_plan, query_plan_cost_estimation) =
        MakeLogicalPlan(ast_storage, db_accessor, ctx);

    plan = std::make_shared<CachedPlan>(
        std::move(tmp_logical_plan), query_plan_cost_estimation,
        ctx.symbol_table_, std::move(ast_storage));

    if (FLAGS_query_plan_cache) {
      plan_cache_.access().insert(stripped.hash(), plan);
    }
  }

  auto *logical_plan = &plan->plan();
  // TODO review: is the check below necessary?
  DCHECK(dynamic_cast<const plan::Produce *>(logical_plan) ||
         dynamic_cast<const plan::Skip *>(logical_plan) ||
         dynamic_cast<const plan::Limit *>(logical_plan) ||
         dynamic_cast<const plan::OrderBy *>(logical_plan) ||
         dynamic_cast<const plan::Distinct *>(logical_plan) ||
         dynamic_cast<const plan::Union *>(logical_plan) ||
         dynamic_cast<const plan::CreateNode *>(logical_plan) ||
         dynamic_cast<const plan::CreateExpand *>(logical_plan) ||
         dynamic_cast<const plan::SetProperty *>(logical_plan) ||
         dynamic_cast<const plan::SetProperties *>(logical_plan) ||
         dynamic_cast<const plan::SetLabels *>(logical_plan) ||
         dynamic_cast<const plan::RemoveProperty *>(logical_plan) ||
         dynamic_cast<const plan::RemoveLabels *>(logical_plan) ||
         dynamic_cast<const plan::Delete *>(logical_plan) ||
         dynamic_cast<const plan::Merge *>(logical_plan) ||
         dynamic_cast<const plan::CreateIndex *>(logical_plan))
      << "Unknown top level LogicalOperator";

  ctx.symbol_table_ = plan->symbol_table();

  auto planning_time = planning_timer.Elapsed();

  std::map<std::string, TypedValue> summary;
  summary["parsing_time"] = frontend_time.count();
  summary["planning_time"] = planning_time.count();
  summary["cost_estimate"] = plan->cost();
  // TODO: set summary['type'] based on transaction metadata
  // the type can't be determined based only on top level LogicalOp
  // (for example MATCH DELETE RETURN will have Produce as it's top)
  // for now always use "rw" because something must be set, but it doesn't
  // have to be correct (for Bolt clients)
  summary["type"] = "rw";

  auto cursor = logical_plan->MakeCursor(ctx.db_accessor_);
  std::vector<std::string> header;
  std::vector<Symbol> output_symbols(
      logical_plan->OutputSymbols(ctx.symbol_table_));
  for (const auto &symbol : output_symbols) {
    // When the symbol is aliased or expanded from '*' (inside RETURN or
    // WITH), then there is no token position, so use symbol name.
    // Otherwise, find the name from stripped query.
    header.push_back(utils::FindOr(stripped.named_expressions(),
                                   symbol.token_position(), symbol.name())
                         .first);
  }

  return Results(std::move(ctx), plan, std::move(cursor), output_symbols,
                 header, summary, plan_cache_);
}

AstTreeStorage Interpreter::QueryToAst(const StrippedQuery &stripped,
                                       Context &ctx) {
  if (!ctx.is_query_cached_) {
    // stripped query -> AST
    auto parser = [&] {
      // Be careful about unlocking since parser can throw.
      std::unique_lock<SpinLock> guard(antlr_lock_);
      return std::make_unique<frontend::opencypher::Parser>(
          stripped.original_query());
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
      try {
        return std::make_unique<frontend::opencypher::Parser>(stripped.query());
      } catch (const SyntaxException &e) {
        // There is syntax exception in stripped query. Rerun parser with
        // original query to get appropriate error messsage.
        auto parser = std::make_unique<frontend::opencypher::Parser>(
            stripped.original_query());
        // If exception was not thrown here, it means StrippedQuery messed up
        // something.
        LOG(FATAL) << "Stripped query can't be parsed, original can";
        return parser;
      }
    }();
    auto low_level_tree = parser->tree();
    // AST -> high level tree
    frontend::CypherMainVisitor visitor(ctx);
    visitor.visit(low_level_tree);
    // Cache it.
    ast_it =
        ast_cache_accessor.insert(stripped.hash(), std::move(visitor.storage()))
            .first;
  }
  AstTreeStorage new_ast;
  ast_it->second.query()->Clone(new_ast);
  return new_ast;
}

std::pair<std::unique_ptr<plan::LogicalOperator>, double>
Interpreter::MakeLogicalPlan(AstTreeStorage &ast_storage,
                             const GraphDbAccessor &db_accessor,
                             Context &context) {
  std::unique_ptr<plan::LogicalOperator> logical_plan;
  auto vertex_counts = plan::MakeVertexCountCache(db_accessor);
  auto planning_context = plan::MakePlanningContext(
      ast_storage, context.symbol_table_, vertex_counts);
  return plan::MakeLogicalPlan(planning_context, context.parameters_,
                               FLAGS_query_cost_planner);
};

}  // namespace query
