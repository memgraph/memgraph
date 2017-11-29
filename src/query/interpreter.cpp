#include "query/interpreter.hpp"

#include <glog/logging.h>

#include "query/exceptions.hpp"
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
