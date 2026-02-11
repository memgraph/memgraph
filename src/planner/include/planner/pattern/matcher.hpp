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

#include <optional>
#include <span>
#include <vector>

#include <boost/container/small_vector.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

#include "planner/egraph/egraph.hpp"
#include "planner/pattern/match.hpp"
#include "planner/pattern/pattern.hpp"

import memgraph.planner.core.eids;
import memgraph.planner.core.concepts;

namespace memgraph::planner::core {

/// Helper for visiting variants with overloaded lambdas
template <class... Ts>
struct Overload : Ts... {
  using Ts::operator()...;
};

/**
 * @brief E-matching engine for finding pattern matches in an e-graph
 *
 * Implements the core e-matching algorithm for equality saturation.
 * E-matching finds all ways a pattern can match expressions represented
 * in an e-graph, accounting for the equivalence classes.
 *
 * Key features:
 * - Symbol index for fast candidate lookup (O(1) to find e-classes containing a symbol)
 * - Incremental index updates (for saturation loop efficiency)
 * - Backtracking search over e-class nodes
 * - Variable consistency enforcement (same variable binds to same e-class)
 *
 * Index maintenance:
 * - rebuild() does a full index rebuild from the e-graph
 * - rebuild(span) does incremental update for new e-classes only
 * - Incremental updates may leave stale entries pointing to merged-away e-classes;
 *   this is safe (matching uses find() for canonical IDs) but may waste memory
 * - Call rebuild() periodically to compact if needed
 *
 * Usage:
 * @code
 *   EMatchContext ctx;
 *   EMatcher<Symbol, Analysis> ematcher(egraph);
 *
 *   // Create pattern Add(?x, ?x) with explicit root binding
 *   constexpr PatternVar kVarX{0};
 *   constexpr PatternVar kVarRoot{10};
 *   auto builder = Pattern<Symbol>::Builder{};
 *   auto x = builder.var(kVarX);
 *   builder.sym(Op::Add, {x, x}, kVarRoot);  // Bind root (last node)
 *   auto pattern = std::move(builder).build();
 *
 *   // Find all matches as slot-based PatternMatch offsets
 *   std::vector<PatternMatch> matches;
 *   ematcher.match_into(pattern, ctx, matches);
 *   for (auto match : matches) {
 *     // O(1) variable lookup via slot index
 *     auto root_slot = pattern.var_slot(kVarRoot);
 *     auto eclass_id = ctx.arena().get(match, root_slot);
 *     // Process match, apply rewrites...
 *   }
 *
 *   // After adding new e-classes, update index incrementally
 *   ematcher.rebuild_index(new_eclasses);
 * @endcode
 *
 * @tparam Symbol Must satisfy ENodeSymbol concept
 * @tparam Analysis E-graph analysis type (can be NoAnalysis)
 */
template <typename Symbol, typename Analysis>
class EMatcher {
 public:
  /**
   * @brief Construct EMatcher with e-graph reference and build initial index
   *
   * @param egraph The e-graph to match against (reference must remain valid)
   */
  explicit EMatcher(EGraph<Symbol, Analysis> const &egraph);

  /**
   * @brief Full rebuild of the symbol index
   *
   * Call after major e-graph changes or merges that may have invalidated
   * the index.
   */
  void rebuild_index();

  /**
   * @brief Incremental update for newly added e-classes
   *
   * More efficient than full rebuild during saturation loops.
   * Only updates index entries for the specified new e-classes.
   *
   * @note Index Staleness: This method adds new index entries but does not
   * remove stale entries pointing to e-classes that were merged away. This is
   * safe because match() always calls egraph.find() to get canonical IDs and
   * uses a processed set to deduplicate. However, over many iterations the
   * index may accumulate dead entries. Call rebuild_index() (no args) periodically
   * to compact the index if memory usage becomes a concern.
   *
   * @param new_eclasses E-class IDs that were added since last rebuild
   */
  void rebuild_index(std::span<EClassId const> new_eclasses);

  /**
   * @brief Find all matches as slot-based PatternMatch offsets
   *
   * Returns matches as PatternMatch offsets into ctx.arena(). Each match has
   * a fixed-size array of slots where each variable occupies its designated
   * slot (from Pattern::var_slot()). This enables O(1) variable lookup.
   *
   * @param pattern The pattern to match
   * @param ctx Reusable context with pre-allocated buffers and arena
   * @param results Output vector to append PatternMatch offsets to (not cleared)
   */
  void match_into(Pattern<Symbol> const &pattern, EMatchContext &ctx, std::vector<PatternMatch> &results) const;

 private:
  using IndexType = boost::unordered_flat_map<Symbol, boost::unordered_flat_set<EClassId>>;

  // MatchFrame is defined in match.hpp for reuse in EMatchContext

  /// Result of processing a single frame - tells the outer loop exactly what to do
  struct StepOutcome {
    enum class Action : uint8_t {
      YieldMatchAndContinue,  ///< Root match found, save to results, keep iterating (symbol nodes)
      YieldMatchAndPop,       ///< Root match found, save to results, pop frame (wildcards/variables)
      ChildYielded,           ///< Child completed, pop frame
      PushChild,              ///< Push child_frame onto stack
      PopAndContinue,         ///< Pop current frame, no retry needed
      PopAndRetryParent       ///< Pop current frame, parent should retry
    };
    Action action;
    std::optional<MatchFrame> child_frame;  ///< For PushChild action
  };

  /// Process a single stack frame, returning what action to take
  auto step_frame(Pattern<Symbol> const &pattern, MatchFrame &frame, PartialMatch &partial) const -> StepOutcome;

  // Type-specific step handlers (called by step_frame)
  auto step_wildcard(bool is_root) const -> StepOutcome;
  auto step_variable(Pattern<Symbol> const &pattern, MatchFrame &frame, PartialMatch &partial, PatternVar var,
                     bool is_root) const -> StepOutcome;
  auto step_symbol(Pattern<Symbol> const &pattern, MatchFrame &frame, PartialMatch &partial,
                   SymbolWithChildren<Symbol> const &sym_node, MatchFrame::ChildResult child_result, bool is_root) const
      -> StepOutcome;

  EGraph<Symbol, Analysis> const *egraph_;
  IndexType index_;
};

// ========================================================================
// EMatcher Implementation
// ========================================================================

template <typename Symbol, typename Analysis>
EMatcher<Symbol, Analysis>::EMatcher(EGraph<Symbol, Analysis> const &egraph) : egraph_(&egraph) {
  rebuild_index();
}

template <typename Symbol, typename Analysis>
void EMatcher<Symbol, Analysis>::rebuild_index() {
  index_.clear();

  // Scan all canonical e-classes
  for (auto const &[eclass_id, eclass] : egraph_->canonical_classes()) {
    // Each e-node in the e-class contributes its symbol to the index
    for (auto const &enode_id : eclass.nodes()) {
      auto const &enode = egraph_->get_enode(enode_id);
      index_[enode.symbol()].insert(eclass_id);
    }
  }
}

template <typename Symbol, typename Analysis>
void EMatcher<Symbol, Analysis>::rebuild_index(std::span<EClassId const> new_eclasses) {
#ifndef NDEBUG
  // Debug-only: verify new_eclasses are already canonical
  for (auto eclass_id : new_eclasses) {
    DMG_ASSERT(egraph_->find(eclass_id) == eclass_id, "new_eclasses must be canonical (call find() before passing)");
  }
#endif
  for (auto eclass_id : new_eclasses) {
    // eclass_id is guaranteed canonical by precondition (asserted above in debug builds)
    auto const &eclass = egraph_->eclass(eclass_id);
    for (auto const &enode_id : eclass.nodes()) {
      auto const &enode = egraph_->get_enode(enode_id);
      index_[enode.symbol()].insert(eclass_id);
    }
  }
}

template <typename Symbol, typename Analysis>
void EMatcher<Symbol, Analysis>::match_into(Pattern<Symbol> const &pattern, EMatchContext &ctx,
                                            std::vector<PatternMatch> &results) const {
  // Note: We intentionally allow matching on a dirty e-graph (needs_rebuild() == true).
  // During a rewrite iteration, multiple rules may fire, each doing merges. We batch
  // all rewrites and only rebuild once at the end of the iteration for efficiency.
  // The matcher uses egraph_->find() to canonicalize e-class IDs as needed.

  if (pattern.empty()) {
    return;
  }

  // No variables to bind means no meaningful matches to return.
  // Each "match" would be an identical empty binding set.
  if (pattern.num_vars() == 0) {
    return;
  }

  ctx.prepare_for_pattern(pattern.num_vars());
  results.clear();

  auto &partial = ctx.partial();
  auto &processed = ctx.processed();
  auto &stack = ctx.match_stack();

  // Process a single root candidate through the backtracking matcher
  auto process_candidate = [&](EClassId root_eclass) {
    using Action = StepOutcome::Action;

    auto canonical_root = egraph_->find(root_eclass);
    if (!processed.insert(canonical_root).second) {
      return;
    }

    stack.push_back(MatchFrame{.pnode_id = pattern.root(), .eclass_id = canonical_root, .binding_start = 0});

    // Simple dispatch loop - step_frame tells us exactly what to do
    while (!stack.empty()) {
      auto outcome = step_frame(pattern, stack.back(), partial);

      switch (outcome.action) {
        case Action::YieldMatchAndContinue:
          results.push_back(ctx.commit(partial));
          break;

        case Action::YieldMatchAndPop:
          results.push_back(ctx.commit(partial));
          partial.rewind_to(stack.back().binding_start);
          stack.pop_back();
          break;

        case Action::ChildYielded:
          // No binding transfer needed - bindings are in partial's undo log
          stack.pop_back();
          if (!stack.empty()) {
            stack.back().child_result = MatchFrame::ChildResult::Yielded;
          }
          break;

        case Action::PushChild:
          // Record current checkpoint for the new frame
          outcome.child_frame->binding_start = partial.checkpoint();
          stack.push_back(*outcome.child_frame);
          break;

        case Action::PopAndContinue:
          partial.rewind_to(stack.back().binding_start);
          stack.pop_back();
          break;

        case Action::PopAndRetryParent:
          partial.rewind_to(stack.back().binding_start);
          stack.pop_back();
          if (!stack.empty()) {
            stack.back().child_result = MatchFrame::ChildResult::Backtracked;
          }
          break;
      }
    }
  };

  // Iterate root candidates directly from source - no intermediate copy
  auto const &root_pnode = pattern[pattern.root()];
  std::visit(Overload{[&](Wildcard) {
                        for (auto const &[eclass_id, _] : egraph_->canonical_classes()) {
                          process_candidate(eclass_id);
                        }
                      },
                      [&](PatternVar) {
                        for (auto const &[eclass_id, _] : egraph_->canonical_classes()) {
                          process_candidate(eclass_id);
                        }
                      },
                      [&](SymbolWithChildren<Symbol> const &sym_node) {
                        if (auto it = index_.find(sym_node.sym); it != index_.end()) {
                          for (auto eclass_id : it->second) {
                            process_candidate(eclass_id);
                          }
                        }
                      }},
             root_pnode);
}

// ========================================================================
// step_frame: Process one stack frame (dispatches to type-specific handlers)
// ========================================================================

/// Wildcard: always matches, yields once then pops
template <typename Symbol, typename Analysis>
auto EMatcher<Symbol, Analysis>::step_wildcard(bool is_root) const -> StepOutcome {
  using Action = StepOutcome::Action;
  return {is_root ? Action::YieldMatchAndPop : Action::ChildYielded, std::nullopt};
}

/// Variable: bind or check consistency, yields once then pops
template <typename Symbol, typename Analysis>
auto EMatcher<Symbol, Analysis>::step_variable(Pattern<Symbol> const &pattern, MatchFrame &frame, PartialMatch &partial,
                                               PatternVar var, bool is_root) const -> StepOutcome {
  using Action = StepOutcome::Action;
  auto slot = pattern.var_slot(var);

  if (!partial.is_bound(slot)) {
    // New binding
    partial.bind(slot, frame.eclass_id);
    return {is_root ? Action::YieldMatchAndPop : Action::ChildYielded, std::nullopt};
  }
  // Already bound - check consistency
  if (egraph_->find(partial.get(slot)) == egraph_->find(frame.eclass_id)) {
    return {is_root ? Action::YieldMatchAndPop : Action::ChildYielded, std::nullopt};
  }
  // Consistency failure - parent should try alternative e-node
  return {Action::PopAndRetryParent, std::nullopt};
}

/// Symbol node: iterate e-nodes in e-class, recursively match children
template <typename Symbol, typename Analysis>
auto EMatcher<Symbol, Analysis>::step_symbol(Pattern<Symbol> const &pattern, MatchFrame &frame, PartialMatch &partial,
                                             SymbolWithChildren<Symbol> const &sym_node,
                                             MatchFrame::ChildResult child_result, bool is_root) const -> StepOutcome {
  using Action = StepOutcome::Action;
  using ChildResult = MatchFrame::ChildResult;

  // Helpers
  auto yield = [&] {
    return StepOutcome{is_root ? Action::YieldMatchAndContinue : Action::ChildYielded, std::nullopt};
  };

  auto push_child = [&] {
    return StepOutcome{Action::PushChild,
                       MatchFrame{.pnode_id = frame.pattern_children->front(),
                                  .eclass_id = egraph_->find(frame.enode_children->front())}};
  };

  // Handle child result
  switch (child_result) {
    case ChildResult::Backtracked:
      // Rewind bindings made during this e-node attempt
      partial.rewind_to(frame.binding_start);
      frame.advance_enode();
      break;
    case ChildResult::Yielded:
      if (frame.pattern_children) {
        frame.advance_child();
        if (frame.children_exhausted()) {
          frame.advance_enode();
          return yield();
        }
        return push_child();
      }
      break;
    case ChildResult::None:
      break;
  }

  // Initialize e-node iteration if needed
  if (!frame.enode_ids) {
    frame.init_enodes(egraph_->eclass(frame.eclass_id).nodes());
  }

  // Find next matching e-node
  auto binding = pattern.binding_for(frame.pnode_id);
  while (!frame.enode_ids->empty()) {
    auto const &enode = egraph_->get_enode(frame.current_enode_id());

    // Skip non-matching e-nodes
    if (sym_node.sym != enode.symbol() || sym_node.children.size() != enode.arity()) [[unlikely]] {
      frame.advance_enode();
      continue;
    }

    // Found match - rewind to frame start and bind if needed
    partial.rewind_to(frame.binding_start);
    if (binding) {
      partial.bind(pattern.var_slot(*binding), frame.eclass_id);
    }

    if (sym_node.children.empty()) {
      frame.advance_enode();
      return yield();
    }

    // Non-leaf: initialize children spans and push first child
    frame.init_children(sym_node.children, enode.children());
    return push_child();
  }

  // No more e-nodes to try
  return {Action::PopAndRetryParent, std::nullopt};
}

/// Main dispatcher - routes to type-specific handler via std::visit
template <typename Symbol, typename Analysis>
auto EMatcher<Symbol, Analysis>::step_frame(Pattern<Symbol> const &pattern, MatchFrame &frame,
                                            PartialMatch &partial) const -> StepOutcome {
  bool is_root = frame.pnode_id == pattern.root();
  auto child_result = std::exchange(frame.child_result, MatchFrame::ChildResult::None);

  return std::visit(Overload{[&](Wildcard) { return step_wildcard(is_root); },
                             [&](PatternVar const var) { return step_variable(pattern, frame, partial, var, is_root); },
                             [&](SymbolWithChildren<Symbol> const &sym) {
                               return step_symbol(pattern, frame, partial, sym, child_result, is_root);
                             }},
                    pattern[frame.pnode_id]);
}

}  // namespace memgraph::planner::core
