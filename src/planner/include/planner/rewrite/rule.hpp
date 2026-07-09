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

#pragma once

#include <cassert>
#include <cstddef>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "planner/pattern/match.hpp"
#include "planner/pattern/match_index.hpp"
#include "planner/pattern/pattern.hpp"
#include "planner/pattern/vm/compiler.hpp"
#include "planner/rewrite/rule_context.hpp"

namespace memgraph::planner::core::rewrite {

// Import specific types from pattern namespace
using pattern::EMatchContext;
using pattern::Match;
using pattern::MatchBindings;
using pattern::MatcherContext;
using pattern::MatcherIndex;
using pattern::Pattern;
using pattern::PatternMatch;
using pattern::SymbolWithChildren;

namespace detail {

/// Wrap user's per-match apply function into bulk apply function.
template <RewritableGraph Graph, typename F>
auto make_apply_fn(F &&fn) {
  using ApplyFn = std::function<void(
      RuleContext<Graph> & ctx, std::span<PatternMatch const> matches, MatchBindings const &bindings)>;
  return ApplyFn{
      [fn = std::forward<F>(fn)](
          RuleContext<Graph> &ctx, std::span<PatternMatch const> matches, MatchBindings const &bindings) mutable {
        for (auto const &pattern_match : matches) {
          fn(ctx, bindings.match(pattern_match));
        }
      }};
}

}  // namespace detail

/// A rewrite rule: patterns + apply function. Use Builder to construct.
///
/// `Graph` is the graph the rule's apply runs over: a bare `EGraph` for rules
/// that only merge/emplace, or a `TypedEGraph` for rules whose apply mints
/// interned nodes via `ctx.Make<S>`. `Symbol` and `Analysis` are derived from it.
template <RewritableGraph Graph>
class RewriteRule {
  using Symbol = typename Graph::symbol_type;
  using Analysis = typename Graph::analysis_type;

  /// Internal storage type - receives match buffer and bindings for iteration
  using ApplyFn = std::function<void(RuleContext<Graph> &ctx, std::span<PatternMatch const> matches,
                                     MatchBindings const &bindings)>;

 public:
  class Builder {
   public:
    explicit Builder(std::string name) : name_(std::move(name)) {}

    auto pattern(Pattern<Symbol> p, std::string pattern_name = "") && -> Builder {
      patterns_.push_back(std::move(p));
      pattern_names_.push_back(std::move(pattern_name));
      return std::move(*this);
    }

    /// Set apply function (RuleContext&, Match const&) and build the rule.
    /// @pre At least one pattern must have been added
    template <typename F>
    auto apply(F &&fn) && -> RewriteRule {
      pattern::vm::PatternsCompiler<Symbol> compiler;
      auto compiled = compiler.compile(patterns_);
      return RewriteRule{std::move(patterns_),
                         std::move(pattern_names_),
                         detail::make_apply_fn<Graph>(std::forward<F>(fn)),
                         std::move(name_),
                         std::move(compiled)};
    }

   private:
    std::string name_;
    std::vector<Pattern<Symbol>> patterns_;
    std::vector<std::string> pattern_names_;
  };

  [[nodiscard]] auto name() const -> std::string_view { return name_; }

  [[nodiscard]] auto patterns() const -> std::span<Pattern<Symbol> const> { return patterns_; }

  /// Access the compiled bytecode for VM execution
  [[nodiscard]] auto compiled() const -> pattern::vm::CompiledMatcher<Symbol> const & { return compiled_; }

  /// The root symbol of every pattern (nullopt where a pattern roots at a
  /// variable/wildcard and so could match any e-class). A new firing of this
  /// rule requires a change at some bound position, which surfaces at one of its
  /// pattern roots - so these symbols are the rule's arming triggers. A nullopt
  /// means the rule cannot be symbol-filtered and must always be armed.
  [[nodiscard]] auto pattern_root_symbols() const -> std::vector<std::optional<Symbol>> {
    std::vector<std::optional<Symbol>> roots;
    roots.reserve(patterns_.size());
    for (auto const &pattern : patterns_) {
      auto const &root_node = pattern[pattern.root()];
      if (auto const *sym = std::get_if<SymbolWithChildren<Symbol>>(&root_node)) {
        roots.emplace_back(sym->sym);
      } else {
        roots.emplace_back(std::nullopt);
      }
    }
    return roots;
  }

  /// Whether the per-candidate active-set restriction (VMExecutor::execute's
  /// `active`) is SOUND for this rule. The active set is closed under parents, so
  /// it holds a new match's *root* but not a deeper e-class the VM may enter at
  /// (the compiler enters at the deepest symbol and walks up). So restricting the
  /// entry candidates is sound only when the pattern's *only* symbol node is its
  /// root - then the single IterSymbolEClasses the VM emits *is* the root
  /// iteration. Multi-pattern rules, or any pattern with a symbol below the root,
  /// fall back to symbol-granularity arming (active == nullptr), which never prunes.
  [[nodiscard]] auto supports_active_root_restriction() const -> bool {
    if (patterns_.size() != 1) return false;
    auto const &pattern = patterns_[0];
    auto const root = pattern.root().value_of();
    auto const nodes = pattern.nodes();
    for (std::size_t i = 0; i < nodes.size(); ++i) {
      if (i == root) continue;
      if (std::holds_alternative<SymbolWithChildren<Symbol>>(nodes[i])) return false;
    }
    return true;
  }

  /// Populate match buffer using VM executor. `active`, when non-null, restricts
  /// the (root) symbol iteration's candidates to that set (see VMExecutor::execute);
  /// null matches every candidate. Only pass a non-null `active` when
  /// supports_active_root_restriction() holds.
  template <typename VMExecutor>
  void match(MatcherIndex<Symbol, Analysis> &index, VMExecutor &vm_executor, MatcherContext &ctx,
             boost::unordered_flat_set<EClassId> const *active = nullptr) const {
    assert((active == nullptr || supports_active_root_restriction()) &&
           "active-set restriction is sound only for root-entry single-pattern rules");
    ctx.clear();
    vm_executor.execute(compiled_, index, ctx.match_ctx.arena(), ctx.match_buffer, active);
  }

  /// Apply matches from buffer to egraph. Returns number of rewrites.
  auto apply(RuleContext<Graph> &rule_ctx, MatcherContext const &matcher_ctx) const -> std::size_t {
    if (matcher_ctx.match_buffer.empty()) [[unlikely]] {
      return 0;
    }
    MatchBindings bindings(compiled_.var_slots(), matcher_ctx.arena());
    apply_fn_(rule_ctx, matcher_ctx.match_buffer, bindings);
    return rule_ctx.rewrites();
  }

 private:
  RewriteRule(std::vector<Pattern<Symbol>> patterns, std::vector<std::string> pattern_names, ApplyFn apply_fn,
              std::string name, pattern::vm::CompiledMatcher<Symbol> compiled)
      : patterns_(std::move(patterns)),
        pattern_names_(std::move(pattern_names)),
        apply_fn_(std::move(apply_fn)),
        name_(std::move(name)),
        compiled_(std::move(compiled)) {}

  std::vector<Pattern<Symbol>> patterns_;
  std::vector<std::string> pattern_names_;
  ApplyFn apply_fn_;
  std::string name_;
  pattern::vm::CompiledMatcher<Symbol> compiled_;
};

}  // namespace memgraph::planner::core::rewrite
