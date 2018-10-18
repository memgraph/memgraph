#include "query/interpreter.hpp"

#include <glog/logging.h>
#include <limits>

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

Interpreter::CachedPlan::CachedPlan(std::unique_ptr<LogicalPlan> plan)
    : plan_(std::move(plan)) {}

Interpreter::Results Interpreter::operator()(
    const std::string &query, database::GraphDbAccessor &db_accessor,
    const std::map<std::string, PropertyValue> &params,
    bool in_explicit_transaction) {
  utils::Timer frontend_timer;

  EvaluationContext evaluation_context;
  evaluation_context.timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();

  // query -> stripped query
  StrippedQuery stripped(query);

  // Update context with provided parameters.
  evaluation_context.parameters = stripped.literals();
  for (const auto &param_pair : stripped.parameters()) {
    auto param_it = params.find(param_pair.second);
    if (param_it == params.end()) {
      throw query::UnprovidedParameterError(
          fmt::format("Parameter ${} not provided.", param_pair.second));
    }
    evaluation_context.parameters.Add(param_pair.first, param_it->second);
  }

  Context ctx(db_accessor);
  ctx.in_explicit_transaction_ = in_explicit_transaction;
  ctx.evaluation_context_ = evaluation_context;

  ParsingContext parsing_context;
  parsing_context.is_query_cached = true;

  AstStorage ast_storage;
  Query *ast =
      QueryToAst(stripped, parsing_context, &ast_storage, &db_accessor);
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
    plan = plan_cache_access
               .insert(stripped.hash(),
                       AstToPlan(ast, std::move(ast_storage), &ctx))
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
                 header, summary, plan_cache_);
}

std::shared_ptr<Interpreter::CachedPlan> Interpreter::AstToPlan(
    Query *query, AstStorage ast_storage, Context *ctx) {
  SymbolGenerator symbol_generator(ctx->symbol_table_);
  query->Accept(symbol_generator);
  return std::make_shared<CachedPlan>(
      MakeLogicalPlan(query, std::move(ast_storage), ctx));
}

Query *Interpreter::QueryToAst(const StrippedQuery &stripped,
                               const ParsingContext &context,
                               AstStorage *ast_storage,
                               database::GraphDbAccessor *db_accessor) {
  if (!context.is_query_cached) {
    // stripped query -> AST
    auto parser = [&] {
      // Be careful about unlocking since parser can throw.
      std::unique_lock<utils::SpinLock> guard(antlr_lock_);
      return std::make_unique<frontend::opencypher::Parser>(
          stripped.original_query());
    }();
    auto low_level_tree = parser->tree();
    // AST -> high level tree
    frontend::CypherMainVisitor visitor(context, ast_storage, db_accessor);
    visitor.visit(low_level_tree);
    return visitor.query();
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
    CachedQuery cached_query;
    frontend::CypherMainVisitor visitor(context, &cached_query.ast_storage,
                                        db_accessor);
    visitor.visit(low_level_tree);
    cached_query.query = visitor.query();
    // Cache it.
    ast_it = ast_cache_accessor.insert(stripped.hash(), std::move(cached_query))
                 .first;
  }
  return ast_it->second.query->Clone(*ast_storage);
}

class SingleNodeLogicalPlan final : public LogicalPlan {
 public:
  SingleNodeLogicalPlan(std::unique_ptr<plan::LogicalOperator> root,
                        double cost, AstStorage storage,
                        const SymbolTable &symbol_table)
      : root_(std::move(root)),
        cost_(cost),
        storage_(std::move(storage)),
        symbol_table_(symbol_table) {}

  const plan::LogicalOperator &GetRoot() const override { return *root_; }
  double GetCost() const override { return cost_; }
  const SymbolTable &GetSymbolTable() const override { return symbol_table_; }

 private:
  std::unique_ptr<plan::LogicalOperator> root_;
  double cost_;
  AstStorage storage_;
  SymbolTable symbol_table_;
};

std::unique_ptr<LogicalPlan> Interpreter::MakeLogicalPlan(
    Query *query, AstStorage ast_storage, Context *context) {
  auto vertex_counts = plan::MakeVertexCountCache(context->db_accessor_);
  auto planning_context = plan::MakePlanningContext(
      ast_storage, context->symbol_table_, query, vertex_counts);
  std::unique_ptr<plan::LogicalOperator> root;
  double cost;
  std::tie(root, cost) = plan::MakeLogicalPlan(
      planning_context, context->evaluation_context_.parameters,
      FLAGS_query_cost_planner);
  return std::make_unique<SingleNodeLogicalPlan>(
      std::move(root), cost, std::move(ast_storage), context->symbol_table_);
}

}  // namespace query
