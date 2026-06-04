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

#include "query/plan_v2/rewrite/rewrites.hpp"

#include <array>
#include <span>
#include <string>

#include "planner/rewrite/rewriter.hpp"
#include "query/plan_v2/egraph/egraph_internal.hpp"
#include "query/plan_v2/rewrite/fold.hpp"

namespace memgraph::query::plan::v2 {

using planner::core::EClassId;

// Pattern types
using planner::core::pattern::Match;
using planner::core::pattern::Pattern;
using planner::core::pattern::PatternVar;
using planner::core::pattern::dsl::BoundSym;
using planner::core::pattern::dsl::Var;
using planner::core::pattern::dsl::Wildcard;

// Rewrite types
using planner::core::rewrite::Rewriter;
using planner::core::rewrite::RewriteRule;
using planner::core::rewrite::RuleContext;
using planner::core::rewrite::RuleSet;

using enum symbol;

/// Inline rule: For Bind(_, ?sym, ?expr), ?ident=Identifier(?sym=Symbol()), merge ?ident with ?expr
struct InlineRule {
  static constexpr PatternVar kSym{0};    // ?sym - shared between Bind and Identifier
  static constexpr PatternVar kExpr{1};   // ?expr in Bind(_, ?sym, ?expr)
  static constexpr PatternVar kIdent{2};  // Binding for Identifier root e-class

  static auto Make() -> RewriteRule<typed_egraph> {
    auto bind_pattern = Pattern<symbol>::build(Bind, {Wildcard{}, Var{kSym}, Var{kExpr}});
    auto ident_pattern = Pattern<symbol>::build(kIdent, Identifier, {BoundSym(kSym, Symbol)});

    return RewriteRule<typed_egraph>::Builder{"inline"}
        .pattern(std::move(bind_pattern), "Bind")
        .pattern(std::move(ident_pattern), "Identifier")
        .apply([](RuleContext<typed_egraph> &ctx, Match const &match) { ctx.merge(match[kIdent], match[kExpr]); });
  }
};

/// Constant-fold rule: for an operator e-node whose operand e-classes all carry
/// a `known_constant_value`, evaluate it and merge the e-class with a freshly
/// interned result Literal. Fact-gated per ADR-0018 - constant-ness is read
/// from analysis, never sniffed off a `Literal` e-node - so it composes under
/// saturation: a folded operand seeds its e-class's constant, which lets the
/// enclosing operator fold on the next pass.
namespace {

using FoldCtx = RuleContext<typed_egraph>;

constexpr PatternVar kFoldRoot{0};
constexpr PatternVar kFoldArg0{1};
constexpr PatternVar kFoldArg1{2};

/// If every operand e-class is a known constant, evaluate `op` over them and
/// merge `root` with the interned result. A non-constant operand or an
/// evaluation that declines (runtime error, non-scalar) leaves `root` alone.
void TryFold(FoldCtx &ctx, symbol op, EClassId root, std::span<EClassId const> operand_classes) {
  std::array<storage::ExternalPropertyValue, 2> operands;
  std::size_t n = 0;
  for (auto cls : operand_classes) {
    auto const *expr = ctx.analysis(cls).expression();
    if (expr == nullptr || !expr->known_constant_value) return;
    operands[n++] = *expr->known_constant_value;
  }
  auto const result = FoldConstant(op, std::span{operands.data(), n});
  if (!result) return;
  ctx.merge(root, ctx.Make<Literal>(*result));
}

template <symbol Op>
auto MakeBinaryFoldRule() -> RewriteRule<typed_egraph> {
  return RewriteRule<typed_egraph>::Builder{"fold"}
      .pattern(Pattern<symbol>::build(kFoldRoot, Op, {Var{kFoldArg0}, Var{kFoldArg1}}))
      .apply([](FoldCtx &ctx, Match const &match) {
        EClassId const operands[] = {match[kFoldArg0], match[kFoldArg1]};
        TryFold(ctx, Op, match[kFoldRoot], operands);
      });
}

template <symbol Op>
auto MakeUnaryFoldRule() -> RewriteRule<typed_egraph> {
  return RewriteRule<typed_egraph>::Builder{"fold"}
      .pattern(Pattern<symbol>::build(kFoldRoot, Op, {Var{kFoldArg0}}))
      .apply([](FoldCtx &ctx, Match const &match) {
        EClassId const operands[] = {match[kFoldArg0]};
        TryFold(ctx, Op, match[kFoldRoot], operands);
      });
}

/// Singleton for default plan_v2 rewrite rules
auto DefaultRules() -> RuleSet<typed_egraph> const & {
  static auto const rules = [] {
    RuleSet<typed_egraph>::Builder builder;
    builder.add_rule(InlineRule::Make());
#define MG_ADD_BINARY_FOLD(Name, ...) builder.add_rule(MakeBinaryFoldRule<symbol::Name>());
    EGRAPH_BINARY_OPS(MG_ADD_BINARY_FOLD)
#undef MG_ADD_BINARY_FOLD
#define MG_ADD_UNARY_FOLD(Name, ...) builder.add_rule(MakeUnaryFoldRule<symbol::Name>());
    EGRAPH_UNARY_OPS(MG_ADD_UNARY_FOLD)
#undef MG_ADD_UNARY_FOLD
    return builder.build();
  }();
  return rules;
}
}  // namespace

// Public API: creates its own rewriter for standalone use
auto ApplyInlineRewrite(egraph &eg) -> std::size_t {
  // Single iteration - not full saturation
  return Rewriter(impl_of(eg).graph, DefaultRules()).iterate_once();
}

auto ApplyAllRewrites(egraph &eg, RewriteConfig const &config) -> RewriteResult {
  return Rewriter(impl_of(eg).graph, DefaultRules()).saturate(config);
}

}  // namespace memgraph::query::plan::v2
