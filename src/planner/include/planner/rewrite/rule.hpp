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

#include <cstddef>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <boost/unordered/unordered_flat_map.hpp>

#include "planner/egraph/egraph.hpp"
#include "planner/pattern/match.hpp"
#include "planner/pattern/matcher.hpp"
#include "planner/pattern/pattern.hpp"
#include "planner/pattern/vm/compiler.hpp"
#include "utils/small_vector.hpp"

import memgraph.planner.core.concepts;
import memgraph.planner.core.eids;

namespace memgraph::planner::core {

/// Reusable buffer for VM pattern matching results
using MatchBuffer = std::vector<PatternMatch>;

/// All reusable buffers for the rewrite process
struct RewriteContext {
  EMatchContext match_ctx;
  MatchBuffer match_buffer;
  std::vector<EClassId> new_eclasses;

  void clear() {
    match_ctx.clear();
    match_buffer.clear();
    new_eclasses.clear();
  }

  void clear_new_eclasses() { new_eclasses.clear(); }
};

/// Safe context for rule apply functions. Auto-tracks new e-classes and counts rewrites.
template <typename Symbol, typename Analysis>
  requires ENodeSymbol<Symbol>
class RuleContext {
 public:
  RuleContext(EGraph<Symbol, Analysis> &egraph, std::vector<EClassId> &new_eclasses)
      : egraph_(egraph), new_eclasses_(new_eclasses) {}

  RuleContext(RuleContext const &) = delete;
  RuleContext(RuleContext &&) = delete;
  auto operator=(RuleContext const &) -> RuleContext & = delete;
  auto operator=(RuleContext &&) -> RuleContext & = delete;
  ~RuleContext() = default;

  void reset_rewrites() { rewrites_ = 0; }

  [[nodiscard]] auto rewrites() const -> std::size_t { return rewrites_; }

  /// Add e-node, auto-tracking new e-classes.
  auto emplace(Symbol symbol, utils::small_vector<EClassId> children) -> EmplaceResult {
    auto result = egraph_.emplace(symbol, std::move(children));
    if (result.did_insert) {
      new_eclasses_.push_back(result.eclass_id);
    }
    return result;
  }

  auto emplace(Symbol symbol, uint64_t disambiguator) -> EmplaceResult {
    auto result = egraph_.emplace(symbol, disambiguator);
    if (result.did_insert) {
      new_eclasses_.push_back(result.eclass_id);
    }
    return result;
  }

  /// Merge e-classes, auto-counting rewrites.
  auto merge(EClassId a, EClassId b) -> EClassId {
    auto [canonical, did_merge] = egraph_.merge(a, b);
    if (did_merge) {
      ++rewrites_;
    }
    return canonical;
  }

  [[nodiscard]] auto find(EClassId id) const -> EClassId { return egraph_.find(id); }

 private:
  EGraph<Symbol, Analysis> &egraph_;
  std::vector<EClassId> &new_eclasses_;
  std::size_t rewrites_ = 0;
};

/// A rewrite rule: patterns + apply function. Use Builder to construct.
template <typename Symbol, typename Analysis>
  requires ENodeSymbol<Symbol>
class RewriteRule {
  using VarLocMap = boost::unordered_flat_map<PatternVar, VarLocation>;

  /// Internal storage type - receives flat storage with stride for iteration
  using ApplyFn =
      std::function<void(RuleContext<Symbol, Analysis> &ctx, std::span<PatternMatch const> result_flat,
                         std::size_t result_stride, VarLocMap const &var_locations, MatchArena const &arena)>;

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
    template <typename F>
    auto apply(F &&fn) && -> RewriteRule {
      auto apply_fn = [fn = std::forward<F>(fn)](RuleContext<Symbol, Analysis> &ctx,
                                                 std::span<PatternMatch const>
                                                     result_flat,
                                                 std::size_t result_stride,
                                                 VarLocMap const &var_locations,
                                                 MatchArena const &arena) mutable {
        for (std::size_t i = 0; i < result_flat.size(); i += result_stride) {
          JoinMatchView view(result_flat.subspan(i, result_stride));
          Match match(view, var_locations, arena);
          fn(ctx, match);
        }
      };
      return RewriteRule{std::move(patterns_), std::move(pattern_names_), std::move(apply_fn), std::move(name_)};
    }

   private:
    std::string name_;
    std::vector<Pattern<Symbol>> patterns_;
    std::vector<std::string> pattern_names_;
  };

  [[nodiscard]] auto name() const -> std::string_view { return name_; }

  [[nodiscard]] auto patterns() const -> std::span<Pattern<Symbol> const> { return patterns_; }

  /// Access the compiled pattern for VM execution
  [[nodiscard]] auto compiled_pattern() const -> std::optional<vm::CompiledPattern<Symbol>> const & {
    return compiled_pattern_;
  }

  /// Get the root symbol of the first pattern (for index-based candidate lookup)
  [[nodiscard]] auto first_pattern_root_symbol() const -> std::optional<Symbol> {
    if (patterns_.empty()) return std::nullopt;
    auto const &root_node = patterns_[0][patterns_[0].root()];
    if (auto const *sym = std::get_if<SymbolWithChildren<Symbol>>(&root_node)) {
      return sym->sym;
    }
    return std::nullopt;
  }

  /// Match patterns using VM executor. Uses unified fused pattern for all rules.
  template <typename VMExecutor>
  auto apply_vm(EGraph<Symbol, Analysis> &egraph, EMatcher<Symbol, Analysis> &matcher, VMExecutor &vm_executor,
                RewriteContext &ctx) const -> std::size_t {
    if (patterns_.empty() || !apply_fn_ || !compiled_pattern_) [[unlikely]] {
      return 0;
    }

    ctx.match_ctx.clear();
    auto &match_buffer = ctx.match_buffer;
    auto &arena = ctx.match_ctx.arena();

    // Execute VM - candidate lookup is handled internally by the executor
    match_buffer.clear();
    vm_executor.execute(*compiled_pattern_, matcher, ctx.match_ctx, match_buffer);

    if (match_buffer.empty()) return 0;

    // All patterns (single or multi) produce single-stride matches via unified fused compilation
    // TODO: we want RuleContext to also handle any updates we will need in the VMExecutor
    //      for example: all_eclasses_buffer_ we shouldn't need to do a fresh rebuild every execute, it should be
    //      maintained via incremental updates
    RuleContext rule_ctx(egraph, ctx.new_eclasses);
    apply_fn_(rule_ctx, match_buffer, 1, compiled_var_locations_, arena);
    return rule_ctx.rewrites();
  }

 private:
  RewriteRule(std::vector<Pattern<Symbol>> patterns, std::vector<std::string> pattern_names, ApplyFn apply_fn,
              std::string name)
      : patterns_(std::move(patterns)),
        pattern_names_(std::move(pattern_names)),
        apply_fn_(std::move(apply_fn)),
        name_(std::move(name)) {
    compile_patterns();
  }

  /// Compile all patterns into a single unified VM pattern.
  /// Uses PatternCompiler which handles both single and multi-pattern cases.
  void compile_patterns() {
    compiled_pattern_.reset();
    compiled_var_locations_.clear();

    if (patterns_.empty()) {
      return;
    }

    // PatternCompiler handles both single and multi-pattern rules uniformly
    vm::PatternCompiler<Symbol> compiler;
    compiled_pattern_ = compiler.compile(patterns_);

    // Build var_locations map using the compiler's actual slot assignments.
    // All vars are in a single match (pattern_index=0), with slot indices
    // from the compiler's slot_map.
    for (auto const &[var, slot] : compiler.slot_map()) {
      compiled_var_locations_[var] = VarLocation{0, static_cast<uint8_t>(slot)};
    }
  }

  std::vector<Pattern<Symbol>> patterns_;
  std::vector<std::string> pattern_names_;
  ApplyFn apply_fn_;
  std::string name_;
  std::optional<vm::CompiledPattern<Symbol>> compiled_pattern_;  // Unified VM pattern (single or multi-pattern)
  VarLocMap compiled_var_locations_;                             // Var locations for compiled pattern
};

/// Immutable, shareable collection of rewrite rules. Cheap to copy (shared_ptr).
template <typename Symbol, typename Analysis>
  requires ENodeSymbol<Symbol>
class RuleSet {
 public:
  using RulePtr = std::shared_ptr<RewriteRule<Symbol, Analysis> const>;

  class Builder {
   public:
    auto add_rule(RewriteRule<Symbol, Analysis> rule) -> Builder & {
      rules_.push_back(std::make_shared<RewriteRule<Symbol, Analysis> const>(std::move(rule)));
      return *this;
    }

    auto build() -> RuleSet { return RuleSet{std::make_shared<std::vector<RulePtr> const>(std::move(rules_))}; }

   private:
    std::vector<RulePtr> rules_;
  };

  RuleSet() : rules_(std::make_shared<std::vector<RulePtr> const>()) {}

  template <typename... Rules>
  static auto Build(Rules &&...rules) -> RuleSet {
    std::vector<RulePtr> rule_vec;
    rule_vec.reserve(sizeof...(rules));
    (rule_vec.push_back(std::make_shared<RewriteRule<Symbol, Analysis> const>(std::forward<Rules>(rules))), ...);
    return RuleSet{std::make_shared<std::vector<RulePtr> const>(std::move(rule_vec))};
  }

  RuleSet(RuleSet const &) = default;
  RuleSet(RuleSet &&) noexcept = default;
  auto operator=(RuleSet const &) -> RuleSet & = default;
  auto operator=(RuleSet &&) noexcept -> RuleSet & = default;
  ~RuleSet() = default;

  [[nodiscard]] auto rules() const -> std::span<RulePtr const> { return *rules_; }

  [[nodiscard]] auto size() const -> std::size_t { return rules_->size(); }

  [[nodiscard]] auto empty() const -> bool { return rules_->empty(); }

 private:
  explicit RuleSet(std::shared_ptr<std::vector<RulePtr> const> rules) : rules_(std::move(rules)) {}

  std::shared_ptr<std::vector<RulePtr> const> rules_;
};

}  // namespace memgraph::planner::core
