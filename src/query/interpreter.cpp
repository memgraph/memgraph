#include "query/interpreter.hpp"

#include <glog/logging.h>
#include <limits>

#include "database/distributed_graph_db.hpp"
#include "distributed/plan_dispatcher.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/required_privileges.hpp"
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

Interpreter::CachedPlan::CachedPlan(
    plan::DistributedPlan distributed_plan, double cost,
    distributed::PlanDispatcher *plan_dispatcher)
    : distributed_plan_(std::move(distributed_plan)),
      cost_(cost),
      plan_dispatcher_(plan_dispatcher) {
  if (plan_dispatcher_) {
    for (const auto &plan_pair : distributed_plan_.worker_plans) {
      const auto &plan_id = plan_pair.first;
      const auto &worker_plan = plan_pair.second;
      plan_dispatcher_->DispatchPlan(plan_id, worker_plan,
                                     distributed_plan_.symbol_table);
    }
  }
}

Interpreter::CachedPlan::~CachedPlan() {
  if (plan_dispatcher_) {
    for (const auto &plan_pair : distributed_plan_.worker_plans) {
      const auto &plan_id = plan_pair.first;
      plan_dispatcher_->RemovePlan(plan_id);
    }
  }
}

Interpreter::Interpreter(database::GraphDb &db)
    : plan_dispatcher_(
          db.type() == database::GraphDb::Type::DISTRIBUTED_MASTER
              // TODO: Replace this with virtual call or some other mechanism.
              ? &dynamic_cast<database::Master *>(&db)->plan_dispatcher()
              : nullptr) {}

Interpreter::Results Interpreter::operator()(
    const std::string &query, database::GraphDbAccessor &db_accessor,
    const std::map<std::string, TypedValue> &params,
    bool in_explicit_transaction) {
  utils::Timer frontend_timer;
  Context ctx(db_accessor);
  ctx.in_explicit_transaction_ = in_explicit_transaction;
  ctx.is_query_cached_ = true;
  ctx.timestamp_ = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  ctx.auth_ = auth_;
  ctx.kafka_streams_ = kafka_streams_;

  // query -> stripped query
  StrippedQuery stripped(query);

  // Update context with provided parameters.
  ctx.parameters_ = stripped.literals();
  for (const auto &param_pair : stripped.parameters()) {
    auto param_it = params.find(param_pair.second);
    if (param_it == params.end()) {
      throw query::UnprovidedParameterError(
          fmt::format("Parameter ${} not provided", param_pair.second));
    }
    ctx.parameters_.Add(param_pair.first, param_it->second);
  }
  AstStorage ast_storage = QueryToAst(stripped, ctx);
  // TODO: Maybe cache required privileges to improve performance on very simple
  // queries.
  auto required_privileges = query::GetRequiredPrivileges(ast_storage);
  auto frontend_time = frontend_timer.Elapsed();

  // Try to get a cached plan. Note that this local shared_ptr might be the only
  // owner of the CachedPlan, so ensure it lives during the whole
  // interpretation.
  std::shared_ptr<CachedPlan> plan{nullptr};
  auto plan_cache_access = plan_cache_.access();
  auto it = plan_cache_access.find(stripped.hash());
  if (it != plan_cache_access.end()) {
    if (it->second->IsExpired())
      plan_cache_access.remove(stripped.hash());
    else
      plan = it->second;
  }
  utils::Timer planning_timer;
  if (!plan) {
    plan =
        plan_cache_access.insert(stripped.hash(), AstToPlan(ast_storage, ctx))
            .first->second;
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
                 header, summary, plan_cache_, required_privileges);
}

std::shared_ptr<Interpreter::CachedPlan> Interpreter::AstToPlan(
    AstStorage &ast_storage, Context &ctx) {
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
    VLOG(10) << "[Interpreter] Created plan for distributed execution "
             << next_plan_id_ - 1;
    return std::make_shared<CachedPlan>(std::move(distributed_plan),
                                        query_plan_cost_estimation,
                                        plan_dispatcher_);
  } else {
    return std::make_shared<CachedPlan>(
        plan::DistributedPlan{0,
                              std::move(tmp_logical_plan),
                              {},
                              std::move(ast_storage),
                              ctx.symbol_table_},
        query_plan_cost_estimation, plan_dispatcher_);
  }
}

AstStorage Interpreter::QueryToAst(const StrippedQuery &stripped,
                                   Context &ctx) {
  if (!ctx.is_query_cached_) {
    // stripped query -> AST
    auto parser = [&] {
      // Be careful about unlocking since parser can throw.
      std::unique_lock<utils::SpinLock> guard(antlr_lock_);
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
      std::unique_lock<utils::SpinLock> guard(antlr_lock_);
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
  AstStorage new_ast;
  ast_it->second.query()->Clone(new_ast);
  return new_ast;
}

std::pair<std::unique_ptr<plan::LogicalOperator>, double>
Interpreter::MakeLogicalPlan(AstStorage &ast_storage, Context &context) {
  std::unique_ptr<plan::LogicalOperator> logical_plan;
  auto vertex_counts = plan::MakeVertexCountCache(context.db_accessor_);
  auto planning_context = plan::MakePlanningContext(
      ast_storage, context.symbol_table_, vertex_counts);
  return plan::MakeLogicalPlan(planning_context, context.parameters_,
                               FLAGS_query_cost_planner);
};
}  // namespace query
