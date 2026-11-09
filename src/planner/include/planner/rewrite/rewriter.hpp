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
#include <cassert>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <ranges>
#include <span>
#include <utility>
#include <vector>

#include <boost/unordered/unordered_flat_set.hpp>

#include "planner/pattern/match_index.hpp"
#include "planner/pattern/vm/executor.hpp"
#include "planner/rewrite/pass_driver.hpp"
#include "planner/rewrite/rule.hpp"
#include "planner/rewrite/rule_latch.hpp"
#include "planner/rewrite/rule_set.hpp"
#include "utils/logging.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core::rewrite {

// Import specific types from pattern namespace
using pattern::MatcherContext;
using pattern::MatcherIndex;

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

/// Reusable buffers for the rewrite process.
template <RewritableGraph Graph>
class RewriteContext {
 public:
  explicit RewriteContext(Graph &graph) : rule_ctx_(graph, new_eclasses_) {}

  [[nodiscard]] auto matcher_ctx() -> MatcherContext & { return matcher_ctx_; }

  [[nodiscard]] auto matcher_ctx() const -> MatcherContext const & { return matcher_ctx_; }

  [[nodiscard]] auto new_eclasses() -> std::vector<EClassId> & { return new_eclasses_; }

  [[nodiscard]] auto new_eclasses() const -> std::vector<EClassId> const & { return new_eclasses_; }

  void clear_new_eclasses() { new_eclasses_.clear(); }

  /// Get rule context, resetting the rewrite counter.
  /// Call this once per rule application to get accurate per-rule statistics.
  auto rule_ctx() -> RuleContext<Graph> & {
    rule_ctx_.reset_rewrites();
    return rule_ctx_;
  }

 private:
  MatcherContext matcher_ctx_;
  std::vector<EClassId> new_eclasses_;
  RuleContext<Graph> rule_ctx_;
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
 *   MyTypedEGraph graph;
 *   // ... populate graph ...
 *
 *   auto ruleset = RuleSet<MyTypedEGraph>::Builder{}
 *       .add_rule(double_negation_rule)
 *       .add_rule(commutativity_rule)
 *       .build();
 *
 *   // RuleSet copy is cheap (shared_ptr increment); the graph type is deduced
 *   Rewriter rewriter(graph, ruleset);
 *
 *   auto result = rewriter.saturate_incremental(RewriteConfig::Default());
 *   if (result.saturated()) {
 *     // Fixed point reached
 *   }
 * @endcode
 *
 * @tparam Graph A RewritableGraph; Symbol and Analysis are derived from it
 */
template <RewritableGraph Graph>
class Rewriter {
  using Symbol = typename Graph::symbol_type;
  using Analysis = typename Graph::analysis_type;
  using EGraph = typename Graph::egraph_type;

 public:
  /**
   * @brief Construct a rewriter with no rules
   *
   * Use set_rules() to configure rules before calling saturate().
   *
   * @param graph The graph to rewrite; must remain valid
   */
  explicit Rewriter(Graph &graph) : Rewriter(graph, RuleSet<Graph>{}) {}

  /**
   * @brief Construct a rewriter with a shared rule set
   *
   * The RuleSet is copied (cheap - just shared_ptr increment), allowing
   * multiple rewriters to share the same rules efficiently.
   *
   * @param graph The TypedEGraph to rewrite; must remain valid
   * @param rules Shared rule set to use
   */
  Rewriter(Graph &graph, RuleSet<Graph> rules)
      : graph_(&graph), rules_(std::move(rules)), matcher_(graph.core()), vm_executor_(graph.core()), ctx_(graph) {
    reseat_latch();
  }

  /**
   * @brief Set or replace the rule set
   *
   * @param rules New rule set to use
   */
  void set_rules(RuleSet<Graph> rules) {
    rules_ = std::move(rules);
    reseat_latch();  // new rules: re-seed the latch so the next incremental saturate arms all once
  }

  /// Run equality saturation under a given pass driver, to fixpoint or a config
  /// limit (iterations, e-node count, timeout). The loop is driver-agnostic: the
  /// driver owns the touched-set lifecycle and supplies each pass's armed
  /// predicate and root restriction. This is the seam FullDriver and
  /// IncrementalDriver sit at; the convenience entry points below funnel through
  /// it. The e-graph is rebuilt and the matcher index refreshed after each pass.
  template <PassDriver<EGraph> Driver>
  auto saturate(RewriteConfig const &config, Driver &driver) -> RewriteResult {
    RewriteResult result;
    result.rewrites_per_rule.resize(num_rules(), 0);  // Initialize per-rule counters
    auto const start_time = std::chrono::steady_clock::now();

    // Facts the graph as handed over already implies, but which no refresh has
    // derived yet: a caller can build a term whose fact only `update` can
    // establish. Deriving them before the first arm is what lets a rule gated
    // on one fire in the first pass, since a pass derives facts at its end, too
    // late to arm anything for itself. A caller capping the pass count would
    // otherwise spend its whole budget before the rule was ever enabled.
    graph_->refresh_analysis(proc_ctx_);

    driver.begin(graph_->core());

    for (std::size_t iter = 0; iter < config.max_iterations; ++iter) {
      result.iterations = iter + 1;

      auto const elapsed = std::chrono::steady_clock::now() - start_time;
      if (elapsed >= config.timeout) {
        result.stop_reason = RewriteResult::StopReason::Timeout;
        return result;
      }
      if (graph_->core().num_nodes() > config.max_enodes) {
        result.stop_reason = RewriteResult::StopReason::ENodeLimit;
        return result;
      }

      driver.before_pass(graph_->core());

#ifndef NDEBUG
      auto const analysis_before = snapshot_analysis_for_audit();
#endif
      auto const pass = apply_once_with_stats(result.rewrites_per_rule, driver.armed(), driver.active());
#ifndef NDEBUG
      audit_moved_facts_were_touched(analysis_before);
#endif

      result.rewrites_applied += pass.rewrites;

      // A pass that rewrote nothing is a fixpoint only if it also learned
      // nothing. Its refresh can still derive a fact, and the rules gated on
      // that fact are armed for the pass after, which stopping here would never
      // run. Facts only widen, so the passes this keeps alive are bounded by
      // what is left to learn.
      if (pass.rewrites == 0 && pass.facts_moved == 0) {
        result.stop_reason = RewriteResult::StopReason::Saturated;
        return result;
      }

      driver.after_pass(graph_->core());
    }

    result.stop_reason = RewriteResult::StopReason::IterationLimit;
    return result;
  }

  /// Saturate running every rule every pass - the reference behaviour and
  /// differential oracle. Leaves the e-graph's touched-set intact.
  auto saturate_full(RewriteConfig const &config) -> RewriteResult {
    FullDriver driver;
    return saturate(config, driver);
  }

  /// Saturate running only the rules a pass could newly enable. Reaches the same
  /// fixpoint as saturate_full given enough iterations, but may need a few more
  /// passes; if max_iterations stops it first the e-graph is valid but less
  /// saturated, never incorrect. Drives the persistent latch, so a later
  /// saturate on the same rewriter arms from the touched-set left by the prior.
  auto saturate_incremental(RewriteConfig const &config) -> RewriteResult {
    IncrementalDriver<Symbol, Analysis> driver{latch_};
    return saturate(config, driver);
  }

  /**
   * @brief Apply every rule once (a single Full pass)
   *
   * One pass under FullDriver: every rule, every candidate, rebuilding the
   * e-graph and refreshing the matcher index. FullDriver leaves the touched-set
   * intact, so a change made here folds into the arm of a subsequent incremental
   * saturate - which is why this doubles as the differential oracle at the end of
   * an incremental run.
   *
   * @return Total number of rewrites applied across all rules
   */
  auto iterate_once() -> std::size_t {
    auto config = RewriteConfig::Unlimited();
    config.max_iterations = 1;
    return saturate_full(config).rewrites_applied;
  }

 private:
  /// Re-seed the latch from the current rule set's derived data. Called on
  /// construction and whenever set_rules replaces the rules.
  void reseat_latch() { latch_.reset(rules_.arming_index(), rules_.max_pattern_depth(), rules_.size()); }

#ifndef NDEBUG
  /// An analysis the audit below can compare across a pass. One that cannot be
  /// copied or compared is not audited, rather than refusing to compile.
  static constexpr bool kSupportsAnalysisAudit =
      std::copy_constructible<Analysis> && std::equality_comparable<Analysis>;

  using AnalysisSnapshot = std::vector<std::pair<EClassId, Analysis>>;

  /// The analysis of every canonical e-class, to compare after the pass.
  auto snapshot_analysis_for_audit() const -> AnalysisSnapshot {
    AnalysisSnapshot snapshot;
    if constexpr (kSupportsAnalysisAudit) {
      snapshot.reserve(graph_->core().num_classes());
      for (auto const id : graph_->core().canonical_eclass_ids())
        snapshot.emplace_back(id, graph_->core().analysis_of(id));
    }
    return snapshot;
  }

  /// Checks the requirement EClass states of a conforming analysis: that a fact
  /// never moves without its e-class being announced as touched, which is what
  /// incremental arming reads. A merge announces its survivor and the refresh
  /// announces each e-class it widens, so neither can trip this. Checked per
  /// pass, where the touched-set holds this pass's touches, and a merged-away id
  /// canonicalises to the survivor that was announced.
  void audit_moved_facts_were_touched(AnalysisSnapshot const &before) const {
    if constexpr (kSupportsAnalysisAudit) {
      boost::unordered_flat_set<EClassId> touched;
      graph_->core().touched_eclasses_into(touched);
      for (auto const &[id, was] : before) {
        if (graph_->core().analysis_of(id) == was) continue;
        DMG_ASSERT(touched.contains(graph_->core().find(id)),
                   "analysis of an e-class moved during a pass that never touched it; incremental arming cannot "
                   "see the change and will skip a rule that full saturation would fire");
      }
    }
  }
#endif

  /// What one pass changed. Both halves gate the loop's fixpoint test, because
  /// either can enable a rule for the pass after.
  struct PassOutcome {
    std::size_t rewrites;
    std::size_t facts_moved;
  };

  /**
   * @brief Apply all rules once and accumulate per-rule statistics
   *
   * Internal helper used by saturate() to track per-rule rewrites.
   * Uses VM-based pattern matching for improved performance on single-pattern rules.
   *
   * @param per_rule_stats Vector to accumulate per-rule counts (must be sized to rules_.size())
   */
  auto apply_once_with_stats(std::vector<std::size_t> &per_rule_stats, std::vector<std::uint8_t> const *armed = nullptr,
                             RootRestriction active = RootRestriction::MatchAll()) -> PassOutcome {
    assert(!graph_->core().needs_rebuild() && "E-graph must be clean at start of rewrite iteration");

    ctx_.clear_new_eclasses();
    std::size_t total_rewrites = 0;

    auto const &rules = rules_.rules();
    for (std::size_t idx = 0; idx < rules.size(); ++idx) {
      if (armed != nullptr && (*armed)[idx] == 0) continue;  // incremental: skip un-armed rules
      // The root restriction is sound only for qualifying rules; others match
      // every candidate. See RewriteRule::supports_active_root_restriction().
      auto const rule_roots = rules[idx]->supports_active_root_restriction() ? active : RootRestriction::MatchAll();
      rules[idx]->match(matcher_, vm_executor_, ctx_.matcher_ctx(), rule_roots);
      auto rewrites = rules[idx]->apply(ctx_.rule_ctx(), ctx_.matcher_ctx());
      per_rule_stats[idx] += rewrites;
      total_rewrites += rewrites;
    }

    // Rebuild e-graph after all rules (restores congruence closure after merges)
    if (graph_->core().needs_rebuild()) {
      graph_->core().rebuild(proc_ctx_);
    }

    // Recompute facts the merges above enabled, inside the pass rather than
    // after the last one: a fact learned here still has a pass left in which to
    // arm the rules gated on it, which the loop's fixpoint test preserves.
    auto const facts_moved = graph_->refresh_analysis(proc_ctx_);

    // Incremental matcher rebuild for new e-classes only
    // rebuild_index handles canonicalization and duplicates internally
    if (!ctx_.new_eclasses().empty()) {
      matcher_.rebuild_index(std::span<EClassId const>{ctx_.new_eclasses()});
    }

    return {.rewrites = total_rewrites, .facts_moved = facts_moved};
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
  Graph *graph_;
  RuleSet<Graph> rules_;  ///< Shared rules (cheap to copy)
  MatcherIndex<Symbol, Analysis> matcher_;
  pattern::vm::VMExecutor<Symbol, Analysis> vm_executor_;  ///< VM pattern matcher
  ProcessingContext<Symbol> proc_ctx_;
  RewriteContext<Graph> ctx_;

  // The arming engine IncrementalDriver drives: decides the armed rule set and
  // active-set restriction each pass. Seeded from rules_ on construction and
  // set_rules; a long-lived member so its scratch is reused across passes.
  RuleLatch<Symbol, Analysis> latch_;
};

/// Deduce the graph type at the construction site, so callers write
/// `Rewriter{graph}` / `Rewriter{graph, rules}` without spelling the graph type.
template <RewritableGraph Graph>
Rewriter(Graph &) -> Rewriter<Graph>;
template <RewritableGraph Graph>
Rewriter(Graph &, RuleSet<Graph>) -> Rewriter<Graph>;

}  // namespace memgraph::planner::core::rewrite
