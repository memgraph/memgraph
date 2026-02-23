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
#include <boost/unordered/unordered_flat_set.hpp>

#include "planner/egraph/egraph.hpp"
#include "planner/pattern/matcher.hpp"
#include "planner/pattern/pattern.hpp"
#include "planner/pattern/vm/compiler.hpp"
#include "planner/rewrite/join.hpp"
#include "utils/small_vector.hpp"

import memgraph.planner.core.concepts;
import memgraph.planner.core.eids;

namespace memgraph::planner::core {

/// All reusable buffers for the rewrite process
struct RewriteContext {
  EMatchContext match_ctx;
  JoinContext join_ctx;
  std::vector<EClassId> new_eclasses;

  void clear() {
    match_ctx.clear();
    join_ctx.clear();
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
        // Iterate flat storage: each joined match is result_stride contiguous PatternMatches
        for (std::size_t i = 0; i < result_flat.size(); i += result_stride) {
          JoinMatchView view(result_flat.subspan(i, result_stride));
          Match match(view, var_locations, arena);
          fn(ctx, match);
        }
      };
      auto plan = compute_join_plan(patterns_);
      return RewriteRule{std::move(patterns_),
                         std::move(pattern_names_),
                         std::move(plan.steps),
                         std::move(plan.var_locations),
                         std::move(apply_fn),
                         std::move(name_)};
    }

   private:
    struct JoinPlan {
      std::vector<JoinStep> steps;
      boost::unordered_flat_map<PatternVar, VarLocation> var_locations;
    };

    /// Graph-based join ordering to minimize intermediate result sizes.
    /// Models patterns as nodes with edges weighted by shared variable count.
    /// Starts with highest-degree pattern (most connections), then greedily picks
    /// patterns sharing most variables with seen set, tie-breaking by remaining degree.
    /// Example: X(?a),Y(?b),A(?a,?b),B(?b,?c),Z(?c),C(?c,?a) -> A,B,C,X,Y,Z
    /// (processes hub patterns A,B,C first to maximize early filtering)
    static auto compute_join_plan(std::vector<Pattern<Symbol>> const &patterns) -> JoinPlan {
      if (patterns.empty()) return {};
      auto const n = patterns.size();
      JoinPlan plan;
      plan.steps.reserve(n);

      // Collect variables per pattern
      std::vector<boost::unordered_flat_set<PatternVar>> pattern_vars(n);
      for (std::size_t i = 0; i < n; ++i) {
        for (auto const &[var, _] : patterns[i].var_slots()) {
          pattern_vars[i].insert(var);
        }
      }

      // Compute pairwise shared variable counts (edge weights)
      // shared_counts[i][j] = number of variables shared between pattern i and j
      std::vector<std::vector<std::size_t>> shared_counts(n, std::vector<std::size_t>(n, 0));
      for (std::size_t i = 0; i < n; ++i) {
        for (std::size_t j = i + 1; j < n; ++j) {
          std::size_t count = 0;
          for (auto const &var : pattern_vars[i]) {
            if (pattern_vars[j].contains(var)) ++count;
          }
          shared_counts[i][j] = shared_counts[j][i] = count;
        }
      }

      // Compute weighted degree for each pattern (sum of edge weights)
      std::vector<std::size_t> degree(n, 0);
      for (std::size_t i = 0; i < n; ++i) {
        for (std::size_t j = 0; j < n; ++j) {
          degree[i] += shared_counts[i][j];
        }
      }

      boost::unordered_flat_set<PatternVar> seen_vars;
      auto remaining = std::views::iota(0uz, n) | std::ranges::to<boost::unordered_flat_set<std::size_t>>();

      // Count variables shared with seen_vars
      auto count_shared_with_seen = [&](std::size_t idx) {
        std::size_t count = 0;
        for (auto const &var : pattern_vars[idx]) {
          if (seen_vars.contains(var)) ++count;
        }
        return count;
      };

      // Compute remaining degree (sum of edge weights to unprocessed patterns)
      auto remaining_degree = [&](std::size_t idx) {
        std::size_t deg = 0;
        for (auto other : remaining) {
          if (other != idx) deg += shared_counts[idx][other];
        }
        return deg;
      };

      // Build JoinStep for pattern at given position
      auto make_step = [&](std::size_t idx, std::size_t join_pos) {
        auto const &p = patterns[idx];
        JoinStep step{.pattern_index = idx};

        for (auto const &[var, slot] : p.var_slots()) {
          if (auto it = plan.var_locations.find(var); it != plan.var_locations.end()) {
            step.shared_vars.push_back(var);
            step.left_locs.push_back(it->second);
            step.right_locs.push_back(VarLocation{0, static_cast<uint8_t>(slot)});
          } else {
            plan.var_locations[var] = VarLocation{static_cast<uint8_t>(join_pos), static_cast<uint8_t>(slot)};
          }
          seen_vars.insert(var);
        }
        return step;
      };

      // Start with highest-degree pattern (most connections in variable-sharing graph)
      auto start = std::ranges::max_element(remaining, {}, [&](std::size_t i) { return degree[i]; });
      plan.steps.push_back(make_step(*start, 0));
      remaining.erase(start);

      // Greedily pick patterns: most shared vars with seen, tie-break by remaining degree
      while (!remaining.empty()) {
        auto best = std::ranges::max_element(
            remaining, {}, [&](std::size_t i) { return std::pair{count_shared_with_seen(i), remaining_degree(i)}; });
        plan.steps.push_back(make_step(*best, plan.steps.size()));
        remaining.erase(best);
      }

      return plan;
    }

   private:
    std::string name_;
    std::vector<Pattern<Symbol>> patterns_;
    std::vector<std::string> pattern_names_;
  };

  [[nodiscard]] auto name() const -> std::string_view { return name_; }

  [[nodiscard]] auto patterns() const -> std::span<Pattern<Symbol> const> { return patterns_; }

  /// Test accessor: returns the computed join steps for verifying join ordering.
  [[nodiscard]] auto join_steps() const -> std::span<JoinStep const> { return join_steps_; }

  /// Access compiled patterns for VM execution (top-down)
  [[nodiscard]] auto compiled_patterns() const -> std::span<std::optional<vm::CompiledPattern<Symbol>> const> {
    return compiled_patterns_;
  }

  /// Access bottom-up compiled patterns for VM execution
  [[nodiscard]] auto compiled_bottomup_patterns() const -> std::span<std::optional<vm::CompiledPattern<Symbol>> const> {
    return bottomup_compiled_patterns_;
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

  /// Match patterns and invoke apply function. Returns number of rewrites.
  auto apply(EGraph<Symbol, Analysis> &egraph, EMatcher<Symbol, Analysis> &matcher, RewriteContext &ctx) const
      -> std::size_t {
    if (patterns_.empty() || !apply_fn_) [[unlikely]] {
      return 0;
    }

    ctx.match_ctx.clear();
    auto &join_ctx = ctx.join_ctx;
    auto &arena = ctx.match_ctx.arena();

    // Match first pattern
    matcher.match_into(patterns_[join_steps_[0].pattern_index], ctx.match_ctx, join_ctx.right);
    if (join_ctx.right.empty()) return 0;

    join_ctx.left_flat.assign(join_ctx.right.begin(), join_ctx.right.end());
    join_ctx.left_stride = 1;

    // Join with remaining patterns
    for (std::size_t i = 1; i < join_steps_.size(); ++i) {
      auto const &step = join_steps_[i];
      matcher.match_into(patterns_[step.pattern_index], ctx.match_ctx, join_ctx.right);
      if (join_ctx.right.empty()) return 0;

      step.join(join_ctx, arena, egraph);
      if (join_ctx.result_flat.empty()) return 0;

      std::swap(join_ctx.left_flat, join_ctx.result_flat);
      join_ctx.left_stride = join_ctx.result_stride;
    }

    RuleContext rule_ctx(egraph, ctx.new_eclasses);
    apply_fn_(rule_ctx, join_ctx.left_flat, join_ctx.left_stride, var_locations_, arena);
    return rule_ctx.rewrites();
  }

  /// Match patterns using VM executor. Uses fused pattern for multi-pattern rules.
  /// Falls back to EMatcher only for register overflow (patterns exceeding 64 registers).
  template <typename VMExecutor>
  auto apply_vm(EGraph<Symbol, Analysis> &egraph, EMatcher<Symbol, Analysis> &matcher, VMExecutor &vm_executor,
                RewriteContext &ctx, std::vector<EClassId> &candidates_buffer) const -> std::size_t {
    if (patterns_.empty() || !apply_fn_) [[unlikely]] {
      return 0;
    }

    // For multi-pattern rules, use fused pattern if available
    if (patterns_.size() > 1) {
      if (!fused_compiled_pattern_) {
        // Fused compilation failed (register overflow), fall back to EMatcher
        return apply(egraph, matcher, ctx);
      }
      return apply_vm_fused(egraph, matcher, vm_executor, ctx, candidates_buffer);
    }

    // For single-pattern rules, use VM if compiled successfully
    auto const &compiled = compiled_patterns_[0];
    if (!compiled) {
      // Pattern too deep for VM (register overflow), fall back to EMatcher
      return apply(egraph, matcher, ctx);
    }

    ctx.match_ctx.clear();
    auto &join_ctx = ctx.join_ctx;
    auto &arena = ctx.match_ctx.arena();

    // Get candidates based on pattern's entry symbol
    if (auto entry_sym = compiled->entry_symbol()) {
      matcher.candidates_for_symbol(*entry_sym, candidates_buffer);
    } else {
      // Root is variable/wildcard - get all e-classes
      matcher.all_candidates(candidates_buffer);
    }

    if (candidates_buffer.empty()) return 0;

    // Execute VM to find matches
    join_ctx.right.clear();
    vm_executor.execute(*compiled, candidates_buffer, ctx.match_ctx, join_ctx.right);

    if (join_ctx.right.empty()) return 0;

    // For single-pattern rules, matches go directly to apply function
    // (no join needed, stride is 1)
    RuleContext rule_ctx(egraph, ctx.new_eclasses);
    apply_fn_(rule_ctx, join_ctx.right, 1, var_locations_, arena);
    return rule_ctx.rewrites();
  }

  /// Match multi-pattern rules using fused VM pattern.
  /// The fused pattern performs the join internally via parent traversal.
  template <typename VMExecutor>
  auto apply_vm_fused(EGraph<Symbol, Analysis> &egraph, EMatcher<Symbol, Analysis> &matcher, VMExecutor &vm_executor,
                      RewriteContext &ctx, std::vector<EClassId> &candidates_buffer) const -> std::size_t {
    ctx.match_ctx.clear();
    auto &join_ctx = ctx.join_ctx;
    auto &arena = ctx.match_ctx.arena();

    // Get candidates based on fused pattern's entry symbol (anchor's root symbol)
    if (auto entry_sym = fused_compiled_pattern_->entry_symbol()) {
      matcher.candidates_for_symbol(*entry_sym, candidates_buffer);
    } else {
      matcher.all_candidates(candidates_buffer);
    }

    if (candidates_buffer.empty()) return 0;

    // Execute fused VM - produces all joined bindings in single match
    join_ctx.right.clear();
    vm_executor.execute(*fused_compiled_pattern_, candidates_buffer, ctx.match_ctx, join_ctx.right);

    if (join_ctx.right.empty()) return 0;

    // Fused pattern produces single-stride matches with all vars bound
    RuleContext rule_ctx(egraph, ctx.new_eclasses);
    apply_fn_(rule_ctx, join_ctx.right, 1, fused_var_locations_, arena);
    return rule_ctx.rewrites();
  }

  /// Match patterns using VM executor with bottom-up traversal.
  /// This starts from all e-classes and traverses UP via parents.
  /// Better when leaf variable has fewer candidates than root symbol.
  template <typename VMExecutor>
  auto apply_vm_bottomup(EGraph<Symbol, Analysis> &egraph, EMatcher<Symbol, Analysis> &matcher, VMExecutor &vm_executor,
                         RewriteContext &ctx, std::vector<EClassId> &candidates_buffer) const -> std::size_t {
    if (patterns_.empty() || !apply_fn_) [[unlikely]] {
      return 0;
    }

    // For multi-pattern rules, use fused pattern (joins via parent traversal)
    if (patterns_.size() > 1) {
      if (!fused_compiled_pattern_) {
        return apply(egraph, matcher, ctx);
      }
      return apply_vm_fused(egraph, matcher, vm_executor, ctx, candidates_buffer);
    }

    // For single-pattern rules, use bottom-up VM if compiled successfully
    auto const &compiled = bottomup_compiled_patterns_[0];
    if (!compiled) {
      // Pattern not suitable for bottom-up (no leaf variable), fall back to top-down
      return apply_vm(egraph, matcher, vm_executor, ctx, candidates_buffer);
    }

    ctx.match_ctx.clear();
    auto &join_ctx = ctx.join_ctx;
    auto &arena = ctx.match_ctx.arena();

    // Bottom-up: iterate all indexed e-classes as candidates (leaf variable bindings)
    matcher.all_indexed_candidates(candidates_buffer);

    if (candidates_buffer.empty()) return 0;

    // Execute VM to find matches via parent traversal
    join_ctx.right.clear();
    vm_executor.execute(*compiled, candidates_buffer, ctx.match_ctx, join_ctx.right);

    if (join_ctx.right.empty()) return 0;

    // For single-pattern rules, matches go directly to apply function
    RuleContext rule_ctx(egraph, ctx.new_eclasses);
    apply_fn_(rule_ctx, join_ctx.right, 1, var_locations_, arena);
    return rule_ctx.rewrites();
  }

 private:
  RewriteRule(std::vector<Pattern<Symbol>> patterns, std::vector<std::string> pattern_names,
              std::vector<JoinStep> join_steps, VarLocMap var_locations, ApplyFn apply_fn, std::string name)
      : patterns_(std::move(patterns)),
        pattern_names_(std::move(pattern_names)),
        join_steps_(std::move(join_steps)),
        var_locations_(std::move(var_locations)),
        apply_fn_(std::move(apply_fn)),
        name_(std::move(name)) {
    // Pre-compile patterns for VM execution
    compile_patterns();
  }

  void compile_patterns() {
    compiled_patterns_.clear();
    compiled_patterns_.reserve(patterns_.size());
    bottomup_compiled_patterns_.clear();
    bottomup_compiled_patterns_.reserve(patterns_.size());

    vm::PatternCompiler<Symbol> compiler;
    vm::BottomUpPatternCompiler<Symbol> bottomup_compiler;

    for (auto const &pattern : patterns_) {
      compiled_patterns_.push_back(compiler.compile(pattern));
      bottomup_compiled_patterns_.push_back(bottomup_compiler.compile(pattern));
    }

    // Compile fused pattern for multi-pattern rules using join order
    compile_fused_pattern();
  }

  void compile_fused_pattern() {
    fused_compiled_pattern_.reset();
    fused_var_locations_.clear();

    if (patterns_.size() <= 1) {
      return;  // No fused pattern needed for single-pattern rules
    }

    // Use simplified API - compiler computes join order internally
    vm::FusedPatternCompiler<Symbol> fused_compiler;
    fused_compiled_pattern_ = fused_compiler.compile(patterns_);

    if (!fused_compiled_pattern_) {
      return;  // Compilation failed (too many registers)
    }

    // Build fused var_locations map using the compiler's actual slot assignments.
    // All vars are in a single fused match (pattern_index=0), with slot indices
    // from the compiler's slot_map.
    for (auto const &[var, slot] : fused_compiler.slot_map()) {
      fused_var_locations_[var] = VarLocation{0, static_cast<uint8_t>(slot)};
    }
  }

  std::vector<Pattern<Symbol>> patterns_;
  std::vector<std::string> pattern_names_;
  std::vector<JoinStep> join_steps_;
  VarLocMap var_locations_;
  ApplyFn apply_fn_;
  std::string name_;
  std::vector<std::optional<vm::CompiledPattern<Symbol>>> compiled_patterns_;           // Top-down
  std::vector<std::optional<vm::CompiledPattern<Symbol>>> bottomup_compiled_patterns_;  // Bottom-up
  std::optional<vm::CompiledPattern<Symbol>> fused_compiled_pattern_;                   // Multi-pattern fused
  VarLocMap fused_var_locations_;                                                       // Var locations for fused
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
