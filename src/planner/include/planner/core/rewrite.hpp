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
#include "utils/small_vector.hpp"

import memgraph.planner.core.concepts;

namespace memgraph::planner::core {

/**
 * @brief A unified match across all patterns in a multi-pattern rule
 *
 * When a rule has multiple patterns, UnifiedMatch represents a complete
 * match where all patterns found compatible matches (agreeing on shared
 * variable bindings). This avoids manual joins in apply functions.
 */
struct UnifiedMatch {
  /// The e-class where each pattern matched (one entry per pattern, in order)
  /// Uses small_vector to avoid heap allocation for 1-2 pattern rules (common case)
  utils::small_vector<EClassId> pattern_roots;

  /// Combined variable substitution from all patterns
  /// For shared variables, this contains the canonicalized e-class ID
  Substitution subst;
};

/**
 * @brief Reusable buffers for multi-pattern rule matching
 *
 * Uses double-buffering pattern (like EMatchContext) to avoid repeated
 * allocations when processing multi-pattern rules. Each rule iteration
 * swaps between current and next buffers.
 */
struct UnifiedMatchBuffers {
  std::vector<UnifiedMatch> current;   ///< Current set of unified matches
  std::vector<UnifiedMatch> next;      ///< Next set after extending with another pattern
  std::vector<Match> pattern_matches;  ///< Reusable buffer for pattern match results
  Substitution constraints;            ///< Reusable buffer for constraint building

  /**
   * @brief Swap current and next buffers
   */
  void swap() { std::swap(current, next); }

  /**
   * @brief Clear both buffers (keeps capacity for reuse)
   */
  void clear() {
    current.clear();
    next.clear();
    pattern_matches.clear();
    constraints.clear();
  }
};

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

  /// Number of rewrites applied by each rule (indexed by rule position in Rewriter)
  std::vector<std::size_t> rewrites_per_rule;

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
 *         for (auto& match : matches) {
 *           auto x = match.subst.at(PatternVar{0});
 *           eg.merge(match.pattern_roots[0], x);
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
   * @param matches Unified matches where each entry represents a complete match
   *                across all patterns with compatible variable bindings
   * @param proc_ctx Processing context for rebuild operations
   * @return Number of rewrites applied (typically number of merges)
   */
  template <typename Analysis>
  using ApplyFn = std::function<std::size_t(EGraph<Symbol, Analysis> &egraph, std::span<UnifiedMatch const> matches,
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
      // Precompute shared variables for each pattern
      auto shared_vars = compute_shared_variables(patterns_);
      return RewriteRule{std::move(patterns_),
                         std::move(pattern_names_),
                         std::move(shared_vars),
                         std::move(apply_fn_),
                         std::move(name)};
    }

   private:
    /**
     * @brief Compute shared variables for all patterns at build time
     *
     * For each pattern index p (1..N), computes which variables in pattern p
     * also appear in earlier patterns (0..p-1). Pattern 0 has no shared vars.
     */
    static auto compute_shared_variables(std::vector<Pattern<Symbol>> const &patterns)
        -> std::vector<std::vector<PatternVar>> {
      std::vector<std::vector<PatternVar>> result;
      result.reserve(patterns.size());

      boost::unordered_flat_set<PatternVar> earlier_vars;

      for (std::size_t p = 0; p < patterns.size(); ++p) {
        std::vector<PatternVar> shared;

        if (p > 0) {
          // Find variables in pattern p that also appear in earlier patterns
          for (auto const &node : patterns[p].nodes()) {
            if (node.is_variable() && earlier_vars.contains(node.variable())) {
              // Avoid duplicates
              if (std::find(shared.begin(), shared.end(), node.variable()) == shared.end()) {
                shared.push_back(node.variable());
              }
            }
          }
        }

        result.push_back(std::move(shared));

        // Collect variables from this pattern for subsequent iterations
        for (auto const &node : patterns[p].nodes()) {
          if (node.is_variable()) {
            earlier_vars.insert(node.variable());
          }
        }
      }

      return result;
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
   * computes unified matches by joining on shared variables, then invokes
   * the apply function with the unified results.
   *
   * The framework handles the join internally using constrained matching
   * for efficiency. This avoids O(n²) joins in apply functions.
   *
   * @tparam Analysis The e-graph analysis type
   * @param egraph The e-graph to rewrite
   * @param matcher Pre-built matcher for the e-graph
   * @param match_ctx Reusable match context
   * @param proc_ctx Processing context for rebuild operations
   * @param unified_buffers Optional pre-allocated buffers for multi-pattern matching.
   *                        If nullptr, local vectors are used (less efficient).
   * @return Number of rewrites applied
   */
  template <typename Analysis>
  auto apply(EGraph<Symbol, Analysis> &egraph, EMatcher<Symbol, Analysis> &matcher, EMatchContext &match_ctx,
             ProcessingContext<Symbol> &proc_ctx, UnifiedMatchBuffers *unified_buffers = nullptr) const -> std::size_t {
    if (patterns_.empty()) {
      return 0;
    }

    // Use provided buffers or fallback to local vectors
    UnifiedMatchBuffers local_buffers;
    UnifiedMatchBuffers &buffers = unified_buffers != nullptr ? *unified_buffers : local_buffers;
    buffers.clear();

    // Start with matches from the first pattern (use buffer to avoid allocation)
    match_ctx.clear();
    buffers.pattern_matches.clear();
    matcher.match_into(patterns_[0], match_ctx, buffers.pattern_matches);

    buffers.current.reserve(buffers.pattern_matches.size());
    for (auto &m : buffers.pattern_matches) {
      UnifiedMatch um;
      um.pattern_roots.push_back(m.matched_eclass);
      um.subst = std::move(m.subst);
      buffers.current.push_back(std::move(um));
    }

    // For each subsequent pattern, extend unified matches using double-buffering
    for (std::size_t p = 1; p < patterns_.size(); ++p) {
      auto const &shared_vars = shared_vars_per_pattern_[p];  // Use precomputed shared vars
      buffers.next.clear();                                   // Clear next buffer (keeps capacity)

      if (shared_vars.empty()) {
        // No shared variables - Cartesian product (use buffer to avoid allocation)
        match_ctx.clear();
        buffers.pattern_matches.clear();
        matcher.match_into(patterns_[p], match_ctx, buffers.pattern_matches);

        for (auto const &um : buffers.current) {
          for (auto const &m : buffers.pattern_matches) {
            UnifiedMatch extended;
            extended.pattern_roots = um.pattern_roots;
            extended.pattern_roots.push_back(m.matched_eclass);
            extended.subst = um.subst;
            for (auto const &[var, eclass] : m.subst) {
              extended.subst.insert_or_assign(var, eclass);
            }
            buffers.next.push_back(std::move(extended));
          }
        }
      } else {
        // Shared variables - use constrained matching for each unified match
        // Group by constraint key to avoid redundant matching
        boost::unordered_flat_map<Substitution, std::vector<std::size_t>> constraint_to_indices;
        for (std::size_t i = 0; i < buffers.current.size(); ++i) {
          buffers.constraints.clear();  // Reuse constraints buffer
          for (auto var : shared_vars) {
            auto it = buffers.current[i].subst.find(var);
            if (it != buffers.current[i].subst.end()) {
              buffers.constraints[var] = egraph.find(it->second);
            }
          }
          constraint_to_indices[buffers.constraints].push_back(i);
        }

        // Match once per unique constraint, then extend all unified matches with that constraint
        for (auto const &[constraints, indices] : constraint_to_indices) {
          match_ctx.clear();
          buffers.pattern_matches.clear();
          if (constraints.empty()) {
            matcher.match_into(patterns_[p], match_ctx, buffers.pattern_matches);
          } else {
            matcher.match_constrained_into(patterns_[p], constraints, match_ctx, buffers.pattern_matches);
          }

          for (std::size_t idx : indices) {
            auto const &um = buffers.current[idx];
            for (auto const &m : buffers.pattern_matches) {
              UnifiedMatch extended;
              extended.pattern_roots = um.pattern_roots;
              extended.pattern_roots.push_back(m.matched_eclass);
              extended.subst = um.subst;
              for (auto const &[var, eclass] : m.subst) {
                extended.subst.insert_or_assign(var, eclass);
              }
              buffers.next.push_back(std::move(extended));
            }
          }
        }
      }

      buffers.swap();  // current = next for next iteration
    }

    // Apply the rule
    auto *fn = std::any_cast<ApplyFn<Analysis>>(&apply_fn_);
    if (fn == nullptr) {
      return 0;  // Type mismatch or no apply function set
    }

    return (*fn)(egraph, buffers.current, proc_ctx);
  }

 private:
  RewriteRule(std::vector<Pattern<Symbol>> patterns, std::vector<std::string> pattern_names,
              std::vector<std::vector<PatternVar>> shared_vars_per_pattern, std::any apply_fn, std::string name)
      : patterns_(std::move(patterns)),
        pattern_names_(std::move(pattern_names)),
        shared_vars_per_pattern_(std::move(shared_vars_per_pattern)),
        apply_fn_(std::move(apply_fn)),
        name_(std::move(name)) {}

  std::vector<Pattern<Symbol>> patterns_;
  std::vector<std::string> pattern_names_;
  /// Precomputed shared variables for each pattern (index i contains variables
  /// in pattern i that also appear in patterns 0..i-1). Empty for pattern 0.
  std::vector<std::vector<PatternVar>> shared_vars_per_pattern_;
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
    result.rewrites_per_rule.resize(rules_.size(), 0);  // Initialize per-rule counters
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

      auto rewrites_this_iter = apply_once_with_stats(result.rewrites_per_rule);
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
      total_rewrites += rule.template apply<Analysis>(*egraph_, matcher_, match_ctx_, proc_ctx_, &unified_buffers_);
    }

    // Rebuild if any rewrites occurred (use incremental matcher rebuild)
    if (total_rewrites > 0 && egraph_->needs_rebuild()) {
      auto affected = egraph_->rebuild(proc_ctx_);
      affected_eclasses_buffer_.clear();
      affected_eclasses_buffer_.insert(affected_eclasses_buffer_.end(), affected.begin(), affected.end());
      matcher_.rebuild(std::span<EClassId const>{affected_eclasses_buffer_});
    }

    return total_rewrites;
  }

 private:
  /**
   * @brief Apply all rules once and accumulate per-rule statistics
   *
   * Internal helper used by saturate() to track per-rule rewrites.
   *
   * @param per_rule_stats Vector to accumulate per-rule counts (must be sized to rules_.size())
   * @return Total number of rewrites applied across all rules
   */
  auto apply_once_with_stats(std::vector<std::size_t> &per_rule_stats) -> std::size_t {
    std::size_t total_rewrites = 0;

    for (std::size_t i = 0; i < rules_.size(); ++i) {
      auto rule_rewrites =
          rules_[i].template apply<Analysis>(*egraph_, matcher_, match_ctx_, proc_ctx_, &unified_buffers_);
      per_rule_stats[i] += rule_rewrites;
      total_rewrites += rule_rewrites;
    }

    // Rebuild if any rewrites occurred (use incremental matcher rebuild)
    if (total_rewrites > 0 && egraph_->needs_rebuild()) {
      auto affected = egraph_->rebuild(proc_ctx_);
      affected_eclasses_buffer_.clear();
      affected_eclasses_buffer_.insert(affected_eclasses_buffer_.end(), affected.begin(), affected.end());
      matcher_.rebuild(std::span<EClassId const>{affected_eclasses_buffer_});
    }

    return total_rewrites;
  }

 public:
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
  std::vector<EClassId> affected_eclasses_buffer_;  ///< Buffer for incremental matcher rebuild
  UnifiedMatchBuffers unified_buffers_;             ///< Reusable buffers for multi-pattern matching
};

}  // namespace memgraph::planner::core
