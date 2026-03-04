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

#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "planner/pattern/match.hpp"
#include "planner/pattern/match_index.hpp"
#include "planner/pattern/pattern.hpp"
#include "planner/pattern/vm/compiler.hpp"
#include "planner/rewrite/rule_context.hpp"

namespace memgraph::planner::core::rewrite {

// Namespace alias for VM types
namespace vm = pattern::vm;

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
template <typename Symbol, typename Analysis, typename F>
auto make_apply_fn(F &&fn) {
  using ApplyFn = std::function<void(
      RuleContext<Symbol, Analysis> & ctx, std::span<PatternMatch const> matches, MatchBindings const &bindings)>;
  return ApplyFn{[fn = std::forward<F>(fn)](RuleContext<Symbol, Analysis> &ctx,
                                            std::span<PatternMatch const>
                                                matches,
                                            MatchBindings const &bindings) mutable {
    for (auto const &pattern_match : matches) {
      fn(ctx, bindings.match(pattern_match));
    }
  }};
}

}  // namespace detail

/// A rewrite rule: patterns + apply function. Use Builder to construct.
template <typename Symbol, typename Analysis>
  requires ENodeSymbol<Symbol>
class RewriteRule {
  /// Internal storage type - receives match buffer and bindings for iteration
  using ApplyFn = std::function<void(RuleContext<Symbol, Analysis> &ctx, std::span<PatternMatch const> matches,
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
      vm::PatternCompiler<Symbol> compiler;
      auto compiled = compiler.compile(patterns_);
      if (!compiled) throw std::runtime_error("Pattern compilation failed (register limit exceeded?)");
      return RewriteRule{std::move(patterns_),
                         std::move(pattern_names_),
                         detail::make_apply_fn<Symbol, Analysis>(std::forward<F>(fn)),
                         std::move(name_),
                         std::move(*compiled)};
    }

   private:
    std::string name_;
    std::vector<Pattern<Symbol>> patterns_;
    std::vector<std::string> pattern_names_;
  };

  [[nodiscard]] auto name() const -> std::string_view { return name_; }

  [[nodiscard]] auto patterns() const -> std::span<Pattern<Symbol> const> { return patterns_; }

  /// Access the compiled bytecode for VM execution
  [[nodiscard]] auto compiled() const -> vm::CompiledPattern<Symbol> const & { return compiled_; }

  /// Get the root symbol of the first pattern (for index-based candidate lookup)
  [[nodiscard]] auto first_pattern_root_symbol() const -> std::optional<Symbol> {
    auto const &root_node = patterns_[0][patterns_[0].root()];
    if (auto const *sym = std::get_if<SymbolWithChildren<Symbol>>(&root_node)) {
      return sym->sym;
    }
    return std::nullopt;
  }

  /// Populate match buffer using VM executor.
  template <typename VMExecutor>
  void match(MatcherIndex<Symbol, Analysis> &index, VMExecutor &vm_executor, MatcherContext &ctx) const {
    ctx.clear();
    vm_executor.execute(compiled_, index, ctx.match_ctx.arena(), ctx.match_buffer);
  }

  /// Apply matches from buffer to egraph. Returns number of rewrites.
  auto apply(RuleContext<Symbol, Analysis> &rule_ctx, MatcherContext const &matcher_ctx) const -> std::size_t {
    if (matcher_ctx.match_buffer.empty()) [[unlikely]] {
      return 0;
    }
    MatchBindings bindings(compiled_.var_slots(), matcher_ctx.arena());
    apply_fn_(rule_ctx, matcher_ctx.match_buffer, bindings);
    return rule_ctx.rewrites();
  }

 private:
  RewriteRule(std::vector<Pattern<Symbol>> patterns, std::vector<std::string> pattern_names, ApplyFn apply_fn,
              std::string name, vm::CompiledPattern<Symbol> compiled)
      : patterns_(std::move(patterns)),
        pattern_names_(std::move(pattern_names)),
        apply_fn_(std::move(apply_fn)),
        name_(std::move(name)),
        compiled_(std::move(compiled)) {}

  std::vector<Pattern<Symbol>> patterns_;
  std::vector<std::string> pattern_names_;
  ApplyFn apply_fn_;
  std::string name_;
  vm::CompiledPattern<Symbol> compiled_;
};

}  // namespace memgraph::planner::core::rewrite
