#include "query/interpreter.hpp"

#include <glog/logging.h>
#include <limits>

#include "distributed/plan_dispatcher.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/plan/planner.hpp"
#include "query/plan/vertex_count_cache.hpp"
#include "utils/flag_validation.hpp"

DEFINE_HIDDEN_bool(query_cost_planner, true,
                   "Use the cost-estimating query planner.");
DEFINE_VALIDATED_int32(query_plan_cache_ttl, 60,
                       "Time to live for cached query plans, in seconds.",
                       FLAG_IN_RANGE(0, std::numeric_limits<int32_t>::max()));

namespace query {

std::shared_ptr<Interpreter::CachedPlan> Interpreter::PlanCache::Find(
    HashType hash) {
  auto accessor = cache_.access();
  auto it = accessor.find(hash);
  if (it == accessor.end()) return nullptr;
  if (!it->second->IsExpired()) return it->second;
  Remove(hash);
  return nullptr;
}

std::shared_ptr<Interpreter::CachedPlan> Interpreter::PlanCache::Insert(
    HashType hash, std::shared_ptr<CachedPlan> plan) {
  // Dispatch plans to workers (if we have any for them).
  if (plan_dispatcher_) {
    for (const auto &plan_pair : plan->distributed_plan().worker_plans) {
      const auto &plan_id = plan_pair.first;
      const auto &worker_plan = plan_pair.second;
      plan_dispatcher_->DispatchPlan(plan_id, worker_plan,
                                     plan->symbol_table());
    }
  }

  auto access = cache_.access();
  auto insertion = access.insert(hash, plan);

  // If the same plan was already cached, invalidate the above dispatched
  // plans on the workers.
  if (!insertion.second && plan_dispatcher_) {
    RemoveFromWorkers(*plan);
  }

  return insertion.first->second;
}

void Interpreter::PlanCache::Remove(HashType hash) {
  auto access = cache_.access();
  auto found = access.find(hash);
  if (found == access.end()) return;

  RemoveFromWorkers(*found->second);
  cache_.access().remove(hash);
}

void Interpreter::PlanCache::Clear() {
  auto access = cache_.access();
  for (auto &kv : access) {
    RemoveFromWorkers(*kv.second);
    access.remove(kv.first);
  }
}

void Interpreter::PlanCache::RemoveFromWorkers(const CachedPlan &plan) {
  for (const auto &plan_pair : plan.distributed_plan().worker_plans) {
    const auto &plan_id = plan_pair.first;
    plan_dispatcher_->RemovePlan(plan_id);
  }
}

Interpreter::Interpreter(database::GraphDb &db)
    : plan_cache_(db.type() == database::GraphDb::Type::DISTRIBUTED_MASTER
                      ? &db.plan_dispatcher()
                      : nullptr) {}

Interpreter::Results Interpreter::operator()(
    const std::string &query, database::GraphDbAccessor &db_accessor,
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
  auto frontend_time = frontend_timer.Elapsed();

  // Try to get a cached plan. Note that this local shared_ptr might be the only
  // owner of the CachedPlan, so ensure it lives during the whole
  // interpretation.
  std::shared_ptr<CachedPlan> plan = plan_cache_.Find(stripped.hash());
  utils::Timer planning_timer;
  if (!plan) {
    plan = plan_cache_.Insert(stripped.hash(), QueryToPlan(stripped, ctx));
  }
  auto planning_time = planning_timer.Elapsed();

  ctx.symbol_table_ = plan->symbol_table();

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

  auto cursor = plan->plan().MakeCursor(ctx.db_accessor_);
  std::vector<std::string> header;
  std::vector<Symbol> output_symbols(
      plan->plan().OutputSymbols(ctx.symbol_table_));
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

std::shared_ptr<Interpreter::CachedPlan> Interpreter::QueryToPlan(
    const StrippedQuery &stripped, Context &ctx) {
  AstTreeStorage ast_storage = QueryToAst(stripped, ctx);
  SymbolGenerator symbol_generator(ctx.symbol_table_);
  ast_storage.query()->Accept(symbol_generator);

  std::unique_ptr<plan::LogicalOperator> tmp_logical_plan;
  double query_plan_cost_estimation = 0.0;
  std::tie(tmp_logical_plan, query_plan_cost_estimation) =
      MakeLogicalPlan(ast_storage, ctx);

  DCHECK(ctx.db_accessor_.db().type() !=
         database::GraphDb::Type::DISTRIBUTED_WORKER);
  if (ctx.db_accessor_.db().type() ==
      database::GraphDb::Type::DISTRIBUTED_MASTER) {
    auto distributed_plan = MakeDistributedPlan(
        *tmp_logical_plan, ctx.symbol_table_, next_plan_id_);
    return std::make_shared<CachedPlan>(std::move(distributed_plan),
                                        query_plan_cost_estimation);
  } else {
    return std::make_shared<CachedPlan>(
        std::move(tmp_logical_plan), query_plan_cost_estimation,
        ctx.symbol_table_, std::move(ast_storage));
  }
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
Interpreter::MakeLogicalPlan(AstTreeStorage &ast_storage, Context &context) {
  std::unique_ptr<plan::LogicalOperator> logical_plan;
  auto vertex_counts = plan::MakeVertexCountCache(context.db_accessor_);
  auto planning_context = plan::MakePlanningContext(
      ast_storage, context.symbol_table_, vertex_counts);
  return plan::MakeLogicalPlan(planning_context, context.parameters_,
                               FLAGS_query_cost_planner);
};
}  // namespace query
