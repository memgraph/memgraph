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

#include "planner/core/rewrite.hpp"
#include "query/plan_v2/egraph_internal.hpp"

namespace memgraph::query::plan::v2 {

using planner::core::EMatchContext;
using planner::core::EMatcher;
using planner::core::Pattern;
using planner::core::PatternVar;
using planner::core::ProcessingContext;
using planner::core::Rewriter;
using planner::core::RewriteRule;
using planner::core::UnifiedMatch;

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

/**
 * @brief Create the inline rewrite rule
 *
 * This rule correlates Bind patterns with Identifier patterns:
 * - For each Bind(?input, ?sym, ?expr), find all Identifier(?sym)
 * - Merge each Identifier's e-class with expr's e-class
 *
 * The framework handles the join on shared variable ?sym automatically,
 * so each UnifiedMatch contains a correlated (Bind, Identifier) pair.
 */
auto MakeInlineRule() -> RewriteRule<symbol> {
  auto builder = RewriteRule<symbol>::Builder{};
  builder.pattern(BuildBindPattern(), "Bind")
      .pattern(BuildIdentifierPattern(), "Identifier")
      .apply<analysis>([](planner::core::EGraph<symbol, analysis> &core_egraph,
                          std::span<UnifiedMatch const>
                              matches,
                          ProcessingContext<symbol> & /*proc_ctx*/) -> std::size_t {
        std::size_t merges = 0;

        for (auto const &match : matches) {
          // pattern_roots[0] = Bind e-class, pattern_roots[1] = Identifier e-class
          auto identifier_eclass = match.pattern_roots[1];

          // ?expr is var 2 in the Bind pattern
          auto expr_eclass = core_egraph.find(match.subst.at(PatternVar{2}));

          // Merge Identifier's e-class with expr's e-class
          if (core_egraph.find(identifier_eclass) != core_egraph.find(expr_eclass)) {
            core_egraph.merge(identifier_eclass, expr_eclass);
            ++merges;
          }
        }

        return merges;
      });
  return std::move(builder).build("inline");
}

}  // namespace

// Public API: creates its own rewriter for standalone use
auto ApplyInlineRewrite(egraph &eg) -> std::size_t {
  auto &impl = internal::get_impl(eg);
  auto &core_egraph = impl.egraph_;

  Rewriter<symbol, analysis> rewriter(core_egraph);
  rewriter.add_rule(MakeInlineRule());

  // Single iteration - not full saturation
  return rewriter.apply_once();
}

auto ApplyAllRewrites(egraph &eg, RewriteConfig const &config) -> RewriteResult {
  auto &impl = internal::get_impl(eg);
  auto &core_egraph = impl.egraph_;

  Rewriter<symbol, analysis> rewriter(core_egraph);
  rewriter.add_rule(MakeInlineRule());

  // Add more rules here as they are implemented
  // rewriter.add_rule(MakeConstantFoldingRule());
  // rewriter.add_rule(MakeDeadCodeEliminationRule());
  // etc.

  return rewriter.saturate(config);
}

}  // namespace memgraph::query::plan::v2
