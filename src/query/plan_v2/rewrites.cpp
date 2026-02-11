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

#include "planner/rewrite/rewriter.hpp"
#include "query/plan_v2/egraph_internal.hpp"

namespace memgraph::query::plan::v2 {

using planner::core::Match;
using planner::core::Pattern;
using planner::core::PatternVar;
using planner::core::Rewriter;
using planner::core::RewriteRule;
using planner::core::RuleContext;
using planner::core::RuleSet;
using planner::core::Var;
using planner::core::Wildcard;

/// Inline rule: For Bind(_, ?sym, ?expr) and ?ident=Identifier(?sym), merge Identifier with ?expr
struct InlineRule {
  static constexpr PatternVar kSym{0};    // ?sym - shared between Bind and Identifier
  static constexpr PatternVar kExpr{1};   // ?expr in Bind(_, ?sym, ?expr)
  static constexpr PatternVar kIdent{2};  // Binding for Identifier root e-class

  static auto Make() -> RewriteRule<symbol, analysis> {
    auto bind_pattern = Pattern<symbol>::build(symbol::Bind, {Wildcard{}, Var{kSym}, Var{kExpr}});
    auto ident_pattern = Pattern<symbol>::build(symbol::Identifier, {Var{kSym}}, kIdent);

    return RewriteRule<symbol, analysis>::Builder{"inline"}
        .pattern(std::move(bind_pattern), "Bind")
        .pattern(std::move(ident_pattern), "Identifier")
        .apply([](RuleContext<symbol, analysis> &ctx, Match const &match) { ctx.merge(match[kIdent], match[kExpr]); });
  }
};

/// Singleton for default plan_v2 rewrite rules
auto DefaultRules() -> RuleSet<symbol, analysis> const & {
  static auto const rules = RuleSet<symbol, analysis>::Build(InlineRule::Make());
  return rules;
}

// Public API: creates its own rewriter for standalone use
auto ApplyInlineRewrite(egraph &eg) -> std::size_t {
  auto &impl = internal::get_impl(eg);
  auto &core_egraph = impl.egraph_;

  // Single iteration - not full saturation
  return Rewriter(core_egraph, DefaultRules()).iterate_once();
}

auto ApplyAllRewrites(egraph &eg, RewriteConfig const &config) -> RewriteResult {
  auto &impl = internal::get_impl(eg);
  auto &core_egraph = impl.egraph_;

  return Rewriter(core_egraph, DefaultRules()).saturate(config);
}

}  // namespace memgraph::query::plan::v2
