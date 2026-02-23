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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <ranges>
#include <span>
#include <vector>

#include "planner/egraph/egraph.hpp"
#include "planner/egraph/processing_context.hpp"
#include "planner/pattern/matcher.hpp"
#include "planner/pattern/vm/executor.hpp"
#include "planner/rewrite/rule.hpp"

import memgraph.planner.core.concepts;
import memgraph.planner.core.eids;

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
 *   auto ruleset = RuleSet<Op, NoAnalysis>::Builder{}
 *       .add_rule(double_negation_rule)
 *       .add_rule(commutativity_rule)
 *       .build();
 *
 *   // RuleSet copy is cheap (shared_ptr increment)
 *   Rewriter<Op, NoAnalysis> rewriter(egraph, ruleset);
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
   * @brief Construct a rewriter with no rules
   *
   * Use set_rules() to configure rules before calling saturate().
   *
   * @param egraph Reference to the e-graph to rewrite (must remain valid)
   */
  explicit Rewriter(EGraph<Symbol, Analysis> &egraph) : egraph_(&egraph), matcher_(egraph), vm_executor_(egraph) {}

  /**
   * @brief Construct a rewriter with a shared rule set
   *
   * The RuleSet is copied (cheap - just shared_ptr increment), allowing
   * multiple rewriters to share the same rules efficiently.
   *
   * @param egraph Reference to the e-graph to rewrite (must remain valid)
   * @param rules Shared rule set to use
   */
  Rewriter(EGraph<Symbol, Analysis> &egraph, RuleSet<Symbol, Analysis> rules)
      : egraph_(&egraph), rules_(std::move(rules)), matcher_(egraph), vm_executor_(egraph) {}

  /**
   * @brief Set or replace the rule set
   *
   * @param rules New rule set to use
   */
  void set_rules(RuleSet<Symbol, Analysis> rules) { rules_ = std::move(rules); }

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
    result.rewrites_per_rule.resize(num_rules(), 0);  // Initialize per-rule counters
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
   * Rebuilds the e-graph for congruence closure if needed.
   * Does incremental matcher rebuild for any new e-classes created.
   *
   * Note: For per-rule statistics, use saturate() with max_iterations=1 and
   * check result.rewrites_per_rule.
   *
   * @return Total number of rewrites applied across all rules
   */
  auto iterate_once() -> std::size_t {
    std::vector<std::size_t> unused_stats(num_rules(), 0);
    return apply_once_with_stats(unused_stats);
  }

 private:
  /**
   * @brief Apply all rules once and accumulate per-rule statistics
   *
   * Internal helper used by saturate() to track per-rule rewrites.
   * Uses VM-based pattern matching for improved performance on single-pattern rules.
   *
   * @param per_rule_stats Vector to accumulate per-rule counts (must be sized to rules_.size())
   * @return Total number of rewrites applied across all rules
   */
  auto apply_once_with_stats(std::vector<std::size_t> &per_rule_stats) -> std::size_t {
    DMG_ASSERT(!egraph_->needs_rebuild(), "E-graph must be clean at start of rewrite iteration");

    ctx_.clear_new_eclasses();
    std::size_t total_rewrites = 0;
    std::size_t stat_idx = 0;

    for (auto const &rule_ptr : rules_.rules()) {
      // Use bottom-up VM matching by default (better for wide e-classes)
      // Falls back to top-down VM if no leaf variable, or EMatcher for multi-pattern rules
      auto rewrites = rule_ptr->apply_vm_bottomup(*egraph_, matcher_, vm_executor_, ctx_, candidates_buffer_);
      per_rule_stats[stat_idx++] += rewrites;
      total_rewrites += rewrites;
    }

    // Rebuild e-graph after all rules (restores congruence closure after merges)
    if (egraph_->needs_rebuild()) {
      egraph_->rebuild(proc_ctx_);
    }

    // Incremental matcher rebuild for new e-classes only
    if (!ctx_.new_eclasses.empty()) {
      // Canonicalize, sort, and deduplicate e-class IDs
      for (auto &id : ctx_.new_eclasses) {
        id = egraph_->find(id);
      }
      std::ranges::sort(ctx_.new_eclasses);
      ctx_.new_eclasses.erase(std::ranges::unique(ctx_.new_eclasses).begin(), ctx_.new_eclasses.end());
      matcher_.rebuild_index(std::span<EClassId const>{ctx_.new_eclasses});
    }

    return total_rewrites;
  }

 public:
  /**
   * @brief Get the number of rules in this rewriter
   */
  [[nodiscard]] auto num_rules() const -> std::size_t { return rules_.size(); }

  /**
   * @brief Rebuild the matcher index
   *
   * Call after external modifications to the e-graph that bypass
   * the rewriter (e.g., manual merges or adding nodes).
   */
  void rebuild_index() { matcher_.rebuild_index(); }

  /**
   * @brief Incrementally rebuild the matcher index for specific e-classes
   *
   * @param updated_eclasses E-classes that were modified
   */
  void rebuild_index(std::span<EClassId const> updated_eclasses) { matcher_.rebuild_index(updated_eclasses); }

 private:
  EGraph<Symbol, Analysis> *egraph_;
  RuleSet<Symbol, Analysis> rules_;  ///< Shared rules (cheap to copy)
  EMatcher<Symbol, Analysis> matcher_;
  vm::VMExecutorVerify<Symbol, Analysis> vm_executor_;  ///< VM pattern matcher
  ProcessingContext<Symbol> proc_ctx_;
  RewriteContext ctx_;
  std::vector<EClassId> candidates_buffer_;  ///< Reusable buffer for VM candidate lookup
};

}  // namespace memgraph::planner::core
