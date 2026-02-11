// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/plan_v2/rewrites.hpp"

#include "planner/core/ematch.hpp"
#include "planner/core/pattern.hpp"
#include "query/plan_v2/egraph_internal.hpp"

namespace memgraph::query::plan::v2 {

using planner::core::EMatchContext;
using planner::core::EMatcher;
using planner::core::Pattern;
using planner::core::PatternVar;
using planner::core::ProcessingContext;

namespace {

/**
 * @brief Build pattern for Bind(?input, ?sym, ?expr)
 *
 * Pattern structure:
 *   [0]: ?input (var 0)
 *   [1]: ?sym (var 1)
 *   [2]: ?expr (var 2)
 *   [3]: Bind(?input, ?sym, ?expr) - root
 */
auto BuildBindPattern() -> Pattern<symbol> {
  auto builder = Pattern<symbol>::Builder{};
  auto input = builder.var(0);  // ?input
  auto sym = builder.var(1);    // ?sym
  auto expr = builder.var(2);   // ?expr
  auto bind = builder.sym(symbol::Bind, {input, sym, expr});
  return std::move(builder).build(bind);
}

/**
 * @brief Build pattern for Identifier(?sym)
 *
 * Pattern structure:
 *   [0]: ?sym (var 1) - same var ID as in Bind pattern for correlation
 *   [1]: Identifier(?sym) - root
 */
auto BuildIdentifierPattern() -> Pattern<symbol> {
  auto builder = Pattern<symbol>::Builder{};
  auto sym = builder.var(1);  // ?sym - var ID 1 to correlate with Bind pattern
  auto ident = builder.sym(symbol::Identifier, {sym});
  return std::move(builder).build(ident);
}

}  // namespace

auto ApplyInlineRewrite(egraph &eg) -> std::size_t {
  auto &impl = internal::get_impl(eg);
  auto &core_egraph = impl.egraph_;

  // Build patterns
  auto bind_pattern = BuildBindPattern();
  auto identifier_pattern = BuildIdentifierPattern();

  // Create matcher and build index
  EMatcher<symbol, analysis> ematcher;
  ematcher.build_index(core_egraph);

  // Reusable context for matching
  EMatchContext ctx;

  // Find all Bind matches
  auto bind_matches = ematcher.match(core_egraph, bind_pattern, ctx);

  // Build map from sym e-class -> expr e-class for all bindings
  // Key: canonical sym e-class ID
  // Value: canonical expr e-class ID
  boost::unordered_flat_map<planner::core::EClassId, planner::core::EClassId> sym_to_expr;

  for (auto const &match : bind_matches) {
    auto sym_var = PatternVar{1};
    auto expr_var = PatternVar{2};

    auto sym_eclass = core_egraph.find(match.subst.at(sym_var));
    auto expr_eclass = core_egraph.find(match.subst.at(expr_var));

    // Store mapping (first binding wins if multiple bindings exist)
    sym_to_expr.try_emplace(sym_eclass, expr_eclass);
  }

  if (sym_to_expr.empty()) {
    return 0;  // No bindings found
  }

  // Find all Identifier matches
  ctx.clear();
  auto identifier_matches = ematcher.match(core_egraph, identifier_pattern, ctx);

  std::size_t merges = 0;

  // For each Identifier, check if its sym has a binding, and merge with expr
  for (auto const &match : identifier_matches) {
    auto sym_var = PatternVar{1};
    auto sym_eclass = core_egraph.find(match.subst.at(sym_var));

    auto it = sym_to_expr.find(sym_eclass);
    if (it == sym_to_expr.end()) {
      continue;  // No binding for this symbol
    }

    auto expr_eclass = it->second;
    auto identifier_eclass = match.matched_eclass;

    // Merge Identifier's e-class with expr's e-class
    // This makes Identifier(sym) equivalent to expr
    if (core_egraph.find(identifier_eclass) != core_egraph.find(expr_eclass)) {
      core_egraph.merge(identifier_eclass, expr_eclass);
      ++merges;
    }
  }

  // Rebuild the e-graph if any merges occurred
  if (merges > 0) {
    ProcessingContext<symbol> proc_ctx;
    core_egraph.rebuild(proc_ctx);
  }

  return merges;
}

auto ApplyAllRewrites(egraph &eg, RewriteConfig const &config) -> RewriteResult {
  RewriteResult result;
  auto const start_time = std::chrono::steady_clock::now();

  auto &impl = internal::get_impl(eg);
  auto &core_egraph = impl.egraph_;

  for (std::size_t iter = 0; iter < config.max_iterations; ++iter) {
    result.iterations = iter + 1;

    // Check timeout
    auto const elapsed = std::chrono::steady_clock::now() - start_time;
    if (elapsed >= config.timeout) {
      result.stop_reason = RewriteResult::StopReason::Timeout;
      return result;
    }

    // Check e-node limit
    if (core_egraph.num_nodes() > config.max_enodes) {
      result.stop_reason = RewriteResult::StopReason::ENodeLimit;
      return result;
    }

    std::size_t rewrites_this_iter = 0;

    // Apply inline rewrite
    rewrites_this_iter += ApplyInlineRewrite(eg);

    // Add more rewrites here as they are implemented
    // rewrites_this_iter += ApplyConstantFoldingRewrite(eg);
    // rewrites_this_iter += ApplyDeadCodeEliminationRewrite(eg);
    // etc.

    result.rewrites_applied += rewrites_this_iter;

    // Fixed point reached
    if (rewrites_this_iter == 0) {
      result.stop_reason = RewriteResult::StopReason::Saturated;
      return result;
    }
  }

  // Reached iteration limit
  result.stop_reason = RewriteResult::StopReason::IterationLimit;
  return result;
}

}  // namespace memgraph::query::plan::v2
