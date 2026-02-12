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

#include <boost/container/flat_map.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

#include "planner/core/egraph.hpp"
#include "planner/core/pattern.hpp"

import memgraph.planner.core.eids;
import memgraph.planner.core.concepts;

namespace memgraph::planner::core {

/**
 * @brief Variable bindings from a successful pattern match
 *
 * Maps pattern variables to the e-class IDs they matched during e-matching.
 * Uses flat_map for cache-friendly iteration during pattern instantiation.
 */
using Substitution = boost::container::flat_map<PatternVar, EClassId>;

/**
 * @brief Reusable buffers for e-matching operations
 *
 * Eliminates repeated vector allocations during recursive matching.
 * Each recursion depth level gets its own pair of buffers to avoid
 * conflicts between parent and child calls.
 *
 * Create once, pass to match operations, reuse across multiple patterns.
 *
 * Usage:
 * @code
 *   EMatchContext ctx;
 *   auto matches1 = ematcher.match(egraph, pattern1, ctx);
 *   ctx.clear();  // Optional: reset for clean state
 *   auto matches2 = ematcher.match(egraph, pattern2, ctx);
 * @endcode
 */
struct EMatchContext {
  /**
   * @brief Buffers for a single recursion depth level
   */
  struct DepthBuffers {
    std::vector<Substitution> current;  ///< Current set of partial substitutions
    std::vector<Substitution> next;     ///< Next set after matching a child
  };

  /**
   * @brief Ensure depth pool has capacity for given depth
   *
   * IMPORTANT: Call this BEFORE getting any references to depth buffers,
   * as resize may invalidate existing references.
   *
   * @param max_depth Maximum depth that will be accessed
   */
  void ensure_depth(std::size_t max_depth) {
    if (max_depth >= depth_pool_.size()) {
      depth_pool_.resize(max_depth + 1);
    }
  }

  /**
   * @brief Get buffers at a specific depth (must call ensure_depth first)
   * @param depth The depth level
   * @return Reference to buffers at that depth
   */
  auto buffers_at(std::size_t depth) -> DepthBuffers & { return depth_pool_[depth]; }

  /**
   * @brief Clear all buffers (keeps capacity for reuse)
   */
  void clear() {
    for (auto &bufs : depth_pool_) {
      bufs.current.clear();
      bufs.next.clear();
    }
    processed_.clear();
    child_matches_.clear();
  }

  /**
   * @brief Get current pool depth (for debugging/testing)
   */
  [[nodiscard]] auto pool_depth() const -> std::size_t { return depth_pool_.size(); }

  /**
   * @brief Reusable set for tracking processed e-classes in match()
   */
  auto processed() -> boost::unordered_flat_set<EClassId> & { return processed_; }

  /**
   * @brief Reusable buffer for child matching results in match()
   */
  auto child_matches() -> std::vector<Substitution> & { return child_matches_; }

 private:
  std::vector<DepthBuffers> depth_pool_;
  boost::unordered_flat_set<EClassId> processed_;
  std::vector<Substitution> child_matches_;
};

/**
 * @brief Result of a single pattern match
 *
 * Contains the e-class that matched the pattern root and the
 * variable bindings discovered during matching.
 */
struct Match {
  EClassId matched_eclass;  ///< E-class matching the pattern root
  Substitution subst;       ///< Variable → e-class bindings

  friend auto operator==(Match const &, Match const &) -> bool = default;
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
 * Usage:
 * @code
 *   EMatchContext ctx;
 *   EMatcher<Symbol, Analysis> ematcher(egraph);
 *
 *   // Create pattern Add(?x, ?x)
 *   auto builder = Pattern<Symbol>::Builder{};
 *   auto x = builder.var(0);
 *   auto add = builder.sym(Op::Add, {x, x});
 *   auto pattern = std::move(builder).build(add);
 *
 *   // Find all matches
 *   auto matches = ematcher.match(pattern, ctx);
 *   for (auto& [eclass_id, subst] : matches) {
 *     // Process match, apply rewrites...
 *   }
 *
 *   // After adding new e-classes, update index incrementally
 *   ematcher.rebuild(new_eclasses);
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
  void rebuild();

  /**
   * @brief Incremental update for newly added e-classes
   *
   * More efficient than full rebuild during saturation loops.
   * Only updates index entries for the specified new e-classes.
   *
   * @param new_eclasses E-class IDs that were added since last rebuild
   */
  void rebuild(std::span<EClassId const> new_eclasses);

  /**
   * @brief Find all matches of a pattern in the e-graph
   *
   * Searches the entire e-graph for expressions matching the pattern.
   * Returns all valid matches with their variable bindings.
   *
   * @param pattern The pattern to match
   * @param ctx Reusable context with pre-allocated buffers
   * @return Vector of all matches found
   */
  auto match(Pattern<Symbol> const &pattern, EMatchContext &ctx) const -> std::vector<Match>;

  /**
   * @brief Find matches with pre-bound variable constraints
   *
   * Like match(), but starts with existing variable bindings. Only returns
   * matches that are consistent with the constraints. Uses parent tracking
   * for optimization when a pattern child is a constrained variable.
   *
   * This is useful for multi-pattern rules where patterns share variables.
   * Instead of matching all instances and joining later, constrained matching
   * produces only correlated results.
   *
   * Example:
   * @code
   *   // Match Bind(?input, ?sym, ?expr)
   *   auto bind_matches = matcher.match(bind_pattern, ctx);
   *
   *   // For each bind match, find correlated Identifier(?sym)
   *   for (auto& bind_match : bind_matches) {
   *     Substitution constraints;
   *     constraints[PatternVar{1}] = bind_match.subst.at(PatternVar{1}); // ?sym
   *
   *     auto id_matches = matcher.match_constrained(identifier_pattern, constraints, ctx);
   *     // id_matches are already correlated with bind_match
   *   }
   * @endcode
   *
   * @param pattern The pattern to match
   * @param constraints Pre-bound variable values that must be respected
   * @param ctx Reusable context with pre-allocated buffers
   * @return Vector of matches consistent with constraints
   */
  auto match_constrained(Pattern<Symbol> const &pattern, Substitution const &constraints, EMatchContext &ctx) const
      -> std::vector<Match>;

 private:
  using IndexType = boost::unordered_flat_map<Symbol, boost::unordered_flat_set<EClassId>>;

  /**
   * @brief Get e-classes containing a specific symbol
   * @return Pointer to set of e-class IDs, or nullptr if symbol not found
   */
  [[nodiscard]] auto eclasses_with_symbol(Symbol const &sym) const -> boost::unordered_flat_set<EClassId> const * {
    auto it = index_.find(sym);
    return it != index_.end() ? &it->second : nullptr;
  }

  /**
   * @brief Match a pattern node and append results to output vector
   */
  void match_node_into(Pattern<Symbol> const &pattern, PatternNodeId pnode_id, EClassId eclass_id,
                       Substitution const &subst, std::vector<Substitution> &out, EMatchContext &ctx,
                       std::size_t depth) const;

  /**
   * @brief Match children of a pattern node against children of an e-node
   */
  void match_children_into(Pattern<Symbol> const &pattern, PatternNode<Symbol> const &pnode, ENode<Symbol> const &enode,
                           Substitution const &subst, std::vector<Substitution> &out, EMatchContext &ctx,
                           std::size_t depth) const;

  /**
   * @brief Check if parent-based matching can be used for optimization
   *
   * Returns the index of a constrained variable child if one exists, otherwise nullopt.
   */
  [[nodiscard]] auto find_constrained_child(Pattern<Symbol> const &pattern, PatternNode<Symbol> const &root,
                                            Substitution const &constraints) const -> std::optional<std::size_t>;

  /**
   * @brief Match using parent tracking (optimization for constrained variables)
   *
   * When a pattern's immediate child is a constrained variable, we can use the
   * parent list of that variable's e-class to find candidates, instead of scanning
   * all e-classes with the root symbol.
   */
  void match_via_parents(Pattern<Symbol> const &pattern, PatternNode<Symbol> const &root, EClassId anchor_eclass,
                         std::size_t anchor_child_index, Substitution const &constraints, std::vector<Match> &results,
                         EMatchContext &ctx) const;

  EGraph<Symbol, Analysis> const *egraph_;
  IndexType index_;
};

// ========================================================================
// EMatcher Implementation
// ========================================================================

template <typename Symbol, typename Analysis>
EMatcher<Symbol, Analysis>::EMatcher(EGraph<Symbol, Analysis> const &egraph) : egraph_(&egraph) {
  rebuild();
}

template <typename Symbol, typename Analysis>
void EMatcher<Symbol, Analysis>::rebuild() {
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
void EMatcher<Symbol, Analysis>::rebuild(std::span<EClassId const> new_eclasses) {
  for (auto eclass_id : new_eclasses) {
    // Get canonical form in case merges happened
    auto canonical_id = egraph_->find(eclass_id);
    if (!egraph_->has_class(canonical_id)) {
      continue;  // E-class was merged away
    }

    auto const &eclass = egraph_->eclass(canonical_id);
    for (auto const &enode_id : eclass.nodes()) {
      auto const &enode = egraph_->get_enode(enode_id);
      index_[enode.symbol()].insert(canonical_id);
    }
  }
}

template <typename Symbol, typename Analysis>
auto EMatcher<Symbol, Analysis>::match(Pattern<Symbol> const &pattern, EMatchContext &ctx) const -> std::vector<Match> {
  if (pattern.empty()) {
    return {};
  }

  // Pre-allocate depth buffers to avoid invalidation during recursion
  // Pattern size is an upper bound on recursion depth
  ctx.ensure_depth(pattern.size());

  std::vector<Match> results;
  auto const &root_pnode = pattern[pattern.root()];

  if (root_pnode.is_variable()) {
    // Variable pattern matches any e-class
    for (auto const &[eclass_id, _] : egraph_->canonical_classes()) {
      Substitution subst;
      subst[root_pnode.variable()] = eclass_id;
      results.push_back({eclass_id, std::move(subst)});
    }
    return results;
  }

  // Symbol pattern: use index to find candidate e-classes
  auto const *candidates = eclasses_with_symbol(root_pnode.symbol());
  if (candidates == nullptr) {
    return {};  // No e-classes contain this symbol
  }

  // Get reusable buffers from context
  auto &processed = ctx.processed();
  auto &child_matches = ctx.child_matches();
  processed.clear();

  for (auto eclass_id : *candidates) {
    auto canonical_id = egraph_->find(eclass_id);
    if (!processed.insert(canonical_id).second) {
      continue;  // Already processed this canonical e-class
    }

    auto const &eclass = egraph_->eclass(canonical_id);

    // Inline root matching: iterate e-nodes filtering by symbol
    // (We know from index this e-class has at least one match)
    for (auto enode_id : eclass.nodes()) {
      auto const &enode = egraph_->get_enode(enode_id);

      // Symbol must match
      if (root_pnode.symbol() != enode.symbol()) {
        continue;
      }

      // Arity must match
      if (root_pnode.arity() != enode.arity()) {
        continue;
      }

      // Leaf node with matching symbol: immediate match
      if (root_pnode.is_leaf()) {
        results.push_back({canonical_id, Substitution{}});
        continue;
      }

      // Non-leaf root: match children
      child_matches.clear();
      match_children_into(pattern, root_pnode, enode, Substitution{}, child_matches, ctx, 0);

      for (auto &subst : child_matches) {
        results.push_back({canonical_id, std::move(subst)});
      }
    }
  }

  return results;
}

template <typename Symbol, typename Analysis>
void EMatcher<Symbol, Analysis>::match_node_into(Pattern<Symbol> const &pattern, PatternNodeId pnode_id,
                                                 EClassId eclass_id, Substitution const &subst,
                                                 std::vector<Substitution> &out, EMatchContext &ctx,
                                                 std::size_t depth) const {
  auto const &pnode = pattern[pnode_id];

  if (pnode.is_variable()) {
    // Variable node: check for consistent binding or create new binding
    auto var = pnode.variable();
    auto it = subst.find(var);
    if (it != subst.end()) {
      // Variable already bound: must match same e-class (canonical comparison)
      if (egraph_->find(it->second) == egraph_->find(eclass_id)) {
        out.push_back(subst);  // Consistent binding
      }
      // Else: inconsistent binding, don't add anything
      return;
    }
    // New variable binding
    Substitution new_subst = subst;
    new_subst[var] = eclass_id;
    out.push_back(std::move(new_subst));
    return;
  }

  // Symbol node: try to match against each e-node in the e-class
  auto const &eclass = egraph_->eclass(eclass_id);

  for (auto enode_id : eclass.nodes()) {
    auto const &enode = egraph_->get_enode(enode_id);

    // Symbol must match
    if (pnode.symbol() != enode.symbol()) {
      continue;
    }

    // Arity must match
    if (pnode.arity() != enode.arity()) {
      continue;
    }

    // Leaf node with matching symbol: immediate match
    if (pnode.is_leaf()) {
      out.push_back(subst);
      continue;
    }

    // Non-leaf: match children
    match_children_into(pattern, pnode, enode, subst, out, ctx, depth);
  }
}

template <typename Symbol, typename Analysis>
void EMatcher<Symbol, Analysis>::match_children_into(Pattern<Symbol> const &pattern, PatternNode<Symbol> const &pnode,
                                                     ENode<Symbol> const &enode, Substitution const &subst,
                                                     std::vector<Substitution> &out, EMatchContext &ctx,
                                                     std::size_t depth) const {
  // Get reusable buffers for this depth level (already ensured by caller)
  auto &bufs = ctx.buffers_at(depth);
  bufs.current.clear();
  bufs.current.push_back(subst);

  for (std::size_t i = 0; i < pnode.arity(); ++i) {
    auto pattern_child_id = pnode.children[i];
    auto enode_child_eclass = egraph_->find(enode.children()[i]);

    bufs.next.clear();  // Reuse existing capacity
    for (auto const &current_subst : bufs.current) {
      match_node_into(pattern, pattern_child_id, enode_child_eclass, current_subst, bufs.next, ctx, depth + 1);
    }
    std::swap(bufs.current, bufs.next);

    if (bufs.current.empty()) {
      return;  // No way to match this e-node
    }
  }

  // All children matched: add successful substitutions to output
  for (auto &s : bufs.current) {
    out.push_back(std::move(s));
  }
}

template <typename Symbol, typename Analysis>
auto EMatcher<Symbol, Analysis>::find_constrained_child(Pattern<Symbol> const &pattern, PatternNode<Symbol> const &root,
                                                        Substitution const &constraints) const
    -> std::optional<std::size_t> {
  for (std::size_t i = 0; i < root.arity(); ++i) {
    auto const &child = pattern[root.children[i]];
    if (child.is_variable()) {
      auto it = constraints.find(child.variable());
      if (it != constraints.end()) {
        // Found a constrained variable child - verify the e-class exists
        auto canonical = egraph_->find(it->second);
        if (egraph_->has_class(canonical)) {
          return i;
        }
      }
    }
  }
  return std::nullopt;
}

template <typename Symbol, typename Analysis>
void EMatcher<Symbol, Analysis>::match_via_parents(Pattern<Symbol> const &pattern, PatternNode<Symbol> const &root,
                                                   EClassId anchor_eclass, std::size_t anchor_child_index,
                                                   Substitution const &constraints, std::vector<Match> &results,
                                                   EMatchContext &ctx) const {
  auto &processed = ctx.processed();
  auto &child_matches = ctx.child_matches();
  processed.clear();

  // Get parents of the anchor e-class
  auto const &anchor_class = egraph_->eclass(anchor_eclass);

  for (auto parent_enode_id : anchor_class.parents()) {
    auto const &parent_enode = egraph_->get_enode(parent_enode_id);

    // Check symbol matches
    if (parent_enode.symbol() != root.symbol()) {
      continue;
    }

    // Check arity matches
    if (parent_enode.arity() != root.arity()) {
      continue;
    }

    // Check the anchor child position actually points to our anchor e-class
    if (egraph_->find(parent_enode.children()[anchor_child_index]) != anchor_eclass) {
      continue;
    }

    // Get the e-class containing this parent e-node
    auto parent_eclass = egraph_->find(parent_enode_id);
    if (!processed.insert(parent_eclass).second) {
      continue;  // Already processed this e-class
    }

    // Match all children with the constraints as initial substitution
    if (root.is_leaf()) {
      results.push_back({parent_eclass, constraints});
      continue;
    }

    child_matches.clear();
    match_children_into(pattern, root, parent_enode, constraints, child_matches, ctx, 0);

    for (auto &subst : child_matches) {
      results.push_back({parent_eclass, std::move(subst)});
    }
  }
}

template <typename Symbol, typename Analysis>
auto EMatcher<Symbol, Analysis>::match_constrained(Pattern<Symbol> const &pattern, Substitution const &constraints,
                                                   EMatchContext &ctx) const -> std::vector<Match> {
  if (pattern.empty()) {
    return {};
  }

  // Pre-allocate depth buffers
  ctx.ensure_depth(pattern.size());

  std::vector<Match> results;
  auto const &root_pnode = pattern[pattern.root()];

  if (root_pnode.is_variable()) {
    // Variable pattern root
    auto var = root_pnode.variable();
    auto it = constraints.find(var);
    if (it != constraints.end()) {
      // Variable is constrained - only match if the e-class exists
      auto constrained_eclass = egraph_->find(it->second);
      if (egraph_->has_class(constrained_eclass)) {
        Substitution subst = constraints;
        subst[var] = constrained_eclass;
        results.push_back({constrained_eclass, std::move(subst)});
      }
    } else {
      // Unconstrained variable - matches any e-class (but start with constraints)
      for (auto const &[eclass_id, _] : egraph_->canonical_classes()) {
        Substitution subst = constraints;
        subst[var] = eclass_id;
        results.push_back({eclass_id, std::move(subst)});
      }
    }
    return results;
  }

  // Symbol pattern root - check if we can use parent-based optimization
  auto constrained_child_idx = find_constrained_child(pattern, root_pnode, constraints);
  if (constrained_child_idx.has_value()) {
    // Use parent tracking for efficient matching
    auto const &child = pattern[root_pnode.children[*constrained_child_idx]];
    auto anchor_eclass = egraph_->find(constraints.at(child.variable()));
    match_via_parents(pattern, root_pnode, anchor_eclass, *constrained_child_idx, constraints, results, ctx);
    return results;
  }

  // Fall back to index-based matching (same as match() but with constraints)
  auto const *candidates = eclasses_with_symbol(root_pnode.symbol());
  if (candidates == nullptr) {
    return {};
  }

  auto &processed = ctx.processed();
  auto &child_matches = ctx.child_matches();
  processed.clear();

  for (auto eclass_id : *candidates) {
    auto canonical_id = egraph_->find(eclass_id);
    if (!processed.insert(canonical_id).second) {
      continue;
    }

    auto const &eclass = egraph_->eclass(canonical_id);

    for (auto enode_id : eclass.nodes()) {
      auto const &enode = egraph_->get_enode(enode_id);

      if (root_pnode.symbol() != enode.symbol()) {
        continue;
      }

      if (root_pnode.arity() != enode.arity()) {
        continue;
      }

      if (root_pnode.is_leaf()) {
        results.push_back({canonical_id, constraints});
        continue;
      }

      child_matches.clear();
      match_children_into(pattern, root_pnode, enode, constraints, child_matches, ctx, 0);

      for (auto &subst : child_matches) {
        results.push_back({canonical_id, std::move(subst)});
      }
    }
  }

  return results;
}

}  // namespace memgraph::planner::core
