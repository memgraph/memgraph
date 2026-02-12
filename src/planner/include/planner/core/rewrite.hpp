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

#include <algorithm>
#include <any>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "planner/core/egraph.hpp"
#include "planner/core/ematch.hpp"
#include "planner/core/pattern.hpp"
#include "planner/core/processing_context.hpp"

import memgraph.planner.core.concepts;

namespace memgraph::planner::core {

/**
 * @brief Configuration for the rewrite engine
 *
 * Controls limits on rewriting to prevent runaway saturation.
 * All limits are "soft" in that they're checked between iterations,
 * not during individual rewrites.
 */
struct RewriteConfig {
  /// Maximum number of rewrite iterations before stopping
  std::size_t max_iterations = 10;

  /// Maximum number of e-nodes allowed in the e-graph
  /// Rewriting stops if this limit is exceeded
  std::size_t max_enodes = 10000;

  /// Maximum time budget for rewriting
  /// Rewriting stops if this timeout is exceeded
  std::chrono::milliseconds timeout = std::chrono::milliseconds{1000};

  /// Default configuration with reasonable limits
  static auto Default() -> RewriteConfig { return RewriteConfig{}; }

  /// Unlimited configuration (for testing)
  /// Uses very large but safe values that won't cause overflow in duration comparisons
  static auto Unlimited() -> RewriteConfig {
    return RewriteConfig{
        .max_iterations = std::numeric_limits<std::size_t>::max(),
        .max_enodes = std::numeric_limits<std::size_t>::max(),
        .timeout = std::chrono::hours{24 * 365},  // 1 year - effectively unlimited
    };
  }
};

/**
 * @brief Result of applying rewrites
 *
 * Contains statistics about the rewriting process and the reason
 * for stopping (saturation, limit reached, or timeout).
 */
struct RewriteResult {
  /// Total number of rewrites (merges) applied
  std::size_t rewrites_applied = 0;

  /// Number of iterations completed
  std::size_t iterations = 0;

  /// Why rewriting stopped
  enum class StopReason : std::uint8_t {
    Saturated,       ///< Fixed point reached (no more rewrites possible)
    IterationLimit,  ///< Reached max_iterations
    ENodeLimit,      ///< Exceeded max_enodes
    Timeout,         ///< Exceeded timeout
  } stop_reason = StopReason::Saturated;

  [[nodiscard]] auto saturated() const -> bool { return stop_reason == StopReason::Saturated; }
};

/**
 * @brief A rewrite rule combining patterns with an application function
 *
 * RewriteRule bundles one or more patterns with an apply function that
 * uses the pattern matches to perform rewrites on the e-graph. The
 * patterns are matched first, then the apply function receives all
 * matches and can perform merges or add new e-nodes.
 *
 * The apply function is type-erased to support any Analysis type,
 * allowing rules to be defined once and used with different e-graphs.
 *
 * Example usage:
 * @code
 *   // Create a rule for double negation: Neg(Neg(?x)) -> ?x
 *   auto rule = RewriteRule<Op>::Builder{}
 *       .pattern(neg_neg_pattern)
 *       .apply<NoAnalysis>([](auto& eg, auto matches, auto& ctx) {
 *         std::size_t count = 0;
 *         for (auto& match : matches[0]) {
 *           auto x = match.subst.at(PatternVar{0});
 *           eg.merge(match.matched_eclass, x);
 *           count++;
 *         }
 *         return count;
 *       })
 *       .build("double_negation");
 * @endcode
 *
 * @tparam Symbol Must satisfy ENodeSymbol concept
 */
template <typename Symbol>
  requires ENodeSymbol<Symbol>
class RewriteRule {
 public:
  /**
   * @brief Type signature for rule apply functions
   *
   * @param egraph The e-graph to apply rewrites to
   * @param matches_per_pattern Matches for each pattern (indexed by pattern order)
   * @param proc_ctx Processing context for rebuild operations
   * @return Number of rewrites applied (typically number of merges)
   */
  template <typename Analysis>
  using ApplyFn = std::function<std::size_t(EGraph<Symbol, Analysis> &egraph,
                                            std::span<std::vector<Match> const> matches_per_pattern,
                                            ProcessingContext<Symbol> &proc_ctx)>;

  /**
   * @brief Builder for constructing RewriteRule instances
   *
   * Provides a fluent interface for adding patterns and setting
   * the apply function before building the final rule.
   */
  class Builder {
   public:
    /**
     * @brief Add a pattern to the rule
     * @param p The pattern to add
     * @param name Optional name for debugging/logging
     * @return Reference to this builder for chaining
     */
    auto pattern(Pattern<Symbol> p, std::string name = "") -> Builder & {
      patterns_.push_back(std::move(p));
      pattern_names_.push_back(std::move(name));
      return *this;
    }

    /**
     * @brief Set the apply function for the rule
     *
     * The function receives all pattern matches and should perform
     * the actual rewrites (merges, new nodes) on the e-graph.
     *
     * @tparam Analysis The e-graph analysis type
     * @param fn Apply function
     * @return Reference to this builder for chaining
     */
    template <typename Analysis>
    auto apply(ApplyFn<Analysis> fn) -> Builder & {
      apply_fn_ = std::move(fn);
      return *this;
    }

    /**
     * @brief Build the final RewriteRule
     * @param name Name for the rule (for debugging/logging)
     * @return The constructed rule
     */
    auto build(std::string name) && -> RewriteRule {
      return RewriteRule{std::move(patterns_), std::move(pattern_names_), std::move(apply_fn_), std::move(name)};
    }

   private:
    std::vector<Pattern<Symbol>> patterns_;
    std::vector<std::string> pattern_names_;
    std::any apply_fn_;
  };

  /**
   * @brief Get the rule name
   */
  [[nodiscard]] auto name() const -> std::string_view { return name_; }

  /**
   * @brief Get all patterns for this rule
   */
  [[nodiscard]] auto patterns() const -> std::span<Pattern<Symbol> const> { return patterns_; }

  /**
   * @brief Apply this rule to an e-graph
   *
   * Matches all patterns against the e-graph using the provided matcher,
   * then invokes the apply function with the collected matches.
   *
   * For multi-pattern rules with shared variables, uses constrained matching
   * to efficiently find correlated matches. This avoids O(n²) joins by using
   * parent tracking to find candidates.
   *
   * @tparam Analysis The e-graph analysis type
   * @param egraph The e-graph to rewrite
   * @param matcher Pre-built matcher for the e-graph
   * @param match_ctx Reusable match context
   * @param proc_ctx Processing context for rebuild operations
   * @return Number of rewrites applied
   */
  template <typename Analysis>
  auto apply(EGraph<Symbol, Analysis> &egraph, EMatcher<Symbol, Analysis> &matcher, EMatchContext &match_ctx,
             ProcessingContext<Symbol> &proc_ctx) const -> std::size_t {
    if (patterns_.empty()) {
      return 0;
    }

    std::vector<std::vector<Match>> all_matches;
    all_matches.reserve(patterns_.size());

    // Match first pattern normally
    match_ctx.clear();
    all_matches.push_back(matcher.match(patterns_[0], match_ctx));

    // For subsequent patterns, check for shared variables and use constrained matching
    for (std::size_t p = 1; p < patterns_.size(); ++p) {
      auto shared_vars = find_shared_variables(p);

      if (shared_vars.empty() || all_matches[0].empty()) {
        // No shared variables or no matches to correlate with - match normally
        match_ctx.clear();
        all_matches.push_back(matcher.match(patterns_[p], match_ctx));
      } else {
        // Use constrained matching with unique constraint sets from previous matches
        auto constraint_sets = collect_unique_constraints(all_matches, shared_vars);
        std::vector<Match> constrained_results;

        for (auto const &constraints : constraint_sets) {
          match_ctx.clear();
          auto matches = matcher.match_constrained(patterns_[p], constraints, match_ctx);
          constrained_results.insert(constrained_results.end(),
                                     std::make_move_iterator(matches.begin()),
                                     std::make_move_iterator(matches.end()));
        }

        all_matches.push_back(std::move(constrained_results));
      }
    }

    // Apply the rule
    auto *fn = std::any_cast<ApplyFn<Analysis>>(&apply_fn_);
    if (fn == nullptr) {
      return 0;  // Type mismatch or no apply function set
    }

    return (*fn)(egraph, all_matches, proc_ctx);
  }

 private:
  /**
   * @brief Find variables in pattern p that also appear in earlier patterns
   */
  [[nodiscard]] auto find_shared_variables(std::size_t pattern_index) const -> std::vector<PatternVar> {
    std::vector<PatternVar> shared;
    boost::unordered_flat_set<PatternVar> earlier_vars;

    // Collect variables from all earlier patterns
    for (std::size_t i = 0; i < pattern_index; ++i) {
      for (auto const &node : patterns_[i].nodes()) {
        if (node.is_variable()) {
          earlier_vars.insert(node.variable());
        }
      }
    }

    // Find which variables in pattern p are also in earlier patterns
    for (auto const &node : patterns_[pattern_index].nodes()) {
      if (node.is_variable() && earlier_vars.contains(node.variable())) {
        // Avoid duplicates
        if (std::find(shared.begin(), shared.end(), node.variable()) == shared.end()) {
          shared.push_back(node.variable());
        }
      }
    }

    return shared;
  }

  /**
   * @brief Collect unique constraint sets from previous matches
   *
   * Given matches from previous patterns and a set of shared variables,
   * extracts unique combinations of variable bindings to use as constraints.
   */
  [[nodiscard]] static auto collect_unique_constraints(std::vector<std::vector<Match>> const &all_matches,
                                                       std::vector<PatternVar> const &shared_vars)
      -> std::vector<Substitution> {
    boost::unordered_flat_set<Substitution> unique_constraints;

    // For each match in earlier patterns, extract the shared variable bindings
    for (auto const &pattern_matches : all_matches) {
      for (auto const &match : pattern_matches) {
        Substitution constraints;
        for (auto var : shared_vars) {
          auto it = match.subst.find(var);
          if (it != match.subst.end()) {
            constraints[var] = it->second;
          }
        }
        if (!constraints.empty()) {
          unique_constraints.insert(std::move(constraints));
        }
      }
    }

    return {unique_constraints.begin(), unique_constraints.end()};
  }

  RewriteRule(std::vector<Pattern<Symbol>> patterns, std::vector<std::string> pattern_names, std::any apply_fn,
              std::string name)
      : patterns_(std::move(patterns)),
        pattern_names_(std::move(pattern_names)),
        apply_fn_(std::move(apply_fn)),
        name_(std::move(name)) {}

  std::vector<Pattern<Symbol>> patterns_;
  std::vector<std::string> pattern_names_;
  std::any apply_fn_;
  std::string name_;
};

/**
 * @brief Rewrite engine for equality saturation
 *
 * Orchestrates the application of rewrite rules to an e-graph until
 * saturation (fixed point) or a limit is reached. Maintains a shared
 * matcher and contexts for efficient repeated rule application.
 *
 * Example usage:
 * @code
 *   EGraph<Op, NoAnalysis> egraph;
 *   // ... populate egraph ...
 *
 *   Rewriter<Op, NoAnalysis> rewriter(egraph);
 *   rewriter.add_rule(double_negation_rule);
 *   rewriter.add_rule(commutativity_rule);
 *
 *   auto result = rewriter.saturate(RewriteConfig::Default());
 *   if (result.saturated()) {
 *     // Fixed point reached
 *   }
 * @endcode
 *
 * @tparam Symbol Must satisfy ENodeSymbol concept
 * @tparam Analysis E-graph analysis type (can be NoAnalysis)
 */
template <typename Symbol, typename Analysis>
  requires ENodeSymbol<Symbol>
class Rewriter {
 public:
  /**
   * @brief Construct a rewriter for the given e-graph
   * @param egraph Reference to the e-graph to rewrite (must remain valid)
   */
  explicit Rewriter(EGraph<Symbol, Analysis> &egraph) : egraph_(&egraph), matcher_(egraph) {}

  /**
   * @brief Add a rewrite rule
   * @param rule The rule to add
   */
  void add_rule(RewriteRule<Symbol> rule) { rules_.push_back(std::move(rule)); }

  /**
   * @brief Run equality saturation with the configured rules
   *
   * Applies all rules repeatedly until one of:
   * - Fixed point (no rule produces any rewrites)
   * - Iteration limit reached
   * - E-node limit exceeded
   * - Timeout exceeded
   *
   * After rewrites, the e-graph is rebuilt to restore invariants
   * and the matcher index is refreshed.
   *
   * @param config Limits and timeout configuration
   * @return Result containing statistics and stop reason
   */
  auto saturate(RewriteConfig const &config = RewriteConfig::Default()) -> RewriteResult {
    RewriteResult result;
    auto const start_time = std::chrono::steady_clock::now();

    for (std::size_t iter = 0; iter < config.max_iterations; ++iter) {
      result.iterations = iter + 1;

      // Check timeout
      auto const elapsed = std::chrono::steady_clock::now() - start_time;
      if (elapsed >= config.timeout) {
        result.stop_reason = RewriteResult::StopReason::Timeout;
        return result;
      }

      // Check e-node limit
      if (egraph_->num_nodes() > config.max_enodes) {
        result.stop_reason = RewriteResult::StopReason::ENodeLimit;
        return result;
      }

      auto rewrites_this_iter = apply_once();
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

  /**
   * @brief Apply all rules once (single iteration)
   *
   * Useful for testing and debugging individual rewrite steps.
   * Automatically rebuilds the e-graph and refreshes the matcher
   * if any rewrites were applied.
   *
   * @return Total number of rewrites applied across all rules
   */
  auto apply_once() -> std::size_t {
    std::size_t total_rewrites = 0;

    for (auto const &rule : rules_) {
      total_rewrites += rule.template apply<Analysis>(*egraph_, matcher_, match_ctx_, proc_ctx_);
    }

    // Rebuild if any rewrites occurred
    if (total_rewrites > 0 && egraph_->needs_rebuild()) {
      egraph_->rebuild(proc_ctx_);
      matcher_.rebuild();
    }

    return total_rewrites;
  }

  /**
   * @brief Get the number of rules added to this rewriter
   */
  [[nodiscard]] auto num_rules() const -> std::size_t { return rules_.size(); }

  /**
   * @brief Refresh the matcher index
   *
   * Call after external modifications to the e-graph that bypass
   * the rewriter (e.g., manual merges).
   */
  void refresh_matcher() { matcher_.rebuild(); }

 private:
  EGraph<Symbol, Analysis> *egraph_;
  std::vector<RewriteRule<Symbol>> rules_;
  EMatcher<Symbol, Analysis> matcher_;
  EMatchContext match_ctx_;
  ProcessingContext<Symbol> proc_ctx_;
};

}  // namespace memgraph::planner::core
