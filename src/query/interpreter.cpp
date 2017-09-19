#include "query/interpreter.hpp"

#include "query/plan/cost_estimator.hpp"
#include "query/plan/planner.hpp"
#include "query/plan/vertex_count_cache.hpp"
#include "utils/flag_validation.hpp"

// TODO: Remove this flag. Ast caching can be disabled by setting this flag to
// false, this is useful for recerating antlr crashes in highly concurrent test.
// Once antlr bugs are fixed, or real test is written this flag can be removed.
DEFINE_bool(ast_cache, true, "Use ast caching.");

DEFINE_bool(query_cost_planner, true,
            "Use the cost estimator to generate plans for queries.");

DEFINE_bool(query_plan_cache, true, "Cache generated query plans");

DEFINE_VALIDATED_int32(
    query_cache_expire_seconds, 60,
    "Expire cached queries after this amount of seconds since caching",
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
      return std::make_unique<frontend::opencypher::Parser>(stripped.query());
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
  double min_cost = std::numeric_limits<double>::max();
  auto vertex_counts = plan::MakeVertexCountCache(db_accessor);
  if (FLAGS_query_cost_planner) {
    auto plans = plan::MakeLogicalPlan<plan::VariableStartPlanner>(
        ast_storage, context.symbol_table_, vertex_counts);
    for (auto &plan : plans) {
      auto cost = EstimatePlanCost(vertex_counts, context.parameters_, *plan);
      if (!logical_plan || cost < min_cost) {
        // We won't be iterating over plans anymore, so it's ok to invalidate
        // unique_ptrs inside.
        logical_plan = std::move(plan);
        min_cost = cost;
      }
    }
  } else {
    logical_plan = plan::MakeLogicalPlan<plan::RuleBasedPlanner>(
        ast_storage, context.symbol_table_, vertex_counts);
    min_cost =
        EstimatePlanCost(vertex_counts, context.parameters_, *logical_plan);
  }
  return {std::move(logical_plan), min_cost};
};

}  // namespace query
