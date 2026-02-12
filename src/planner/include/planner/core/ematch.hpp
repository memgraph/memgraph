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
 *   EMatcher<Symbol, Analysis> ematcher;
 *   ematcher.build_index(egraph);
 *
 *   // Create pattern Add(?x, ?x)
 *   auto builder = Pattern<Symbol>::Builder{};
 *   auto x = builder.var(0);
 *   auto add = builder.sym(Op::Add, {x, x});
 *   auto pattern = std::move(builder).build(add);
 *
 *   // Find all matches
 *   auto matches = ematcher.match(egraph, pattern);
 *   for (auto& [eclass_id, subst] : matches) {
 *     // Process match...
 *   }
 * @endcode
 *
 * @tparam Symbol Must satisfy ENodeSymbol concept
 * @tparam Analysis E-graph analysis type (can be NoAnalysis)
 */
template <typename Symbol, typename Analysis>
class EMatcher {
 public:
  using IndexType = boost::unordered_flat_map<Symbol, boost::unordered_flat_set<EClassId>>;

  /**
   * @brief Build symbol index from entire e-graph (initial setup)
   *
   * Scans all e-nodes in the e-graph and builds a mapping from symbols
   * to the e-classes containing nodes with that symbol.
   *
   * @param egraph The e-graph to index
   * @note Call this once before matching, or after major e-graph changes
   */
  void build_index(EGraph<Symbol, Analysis> const &egraph);

  /**
   * @brief Incrementally update index with newly added e-classes
   *
   * More efficient than rebuilding when adding small numbers of new e-classes
   * during a saturation loop.
   *
   * @param egraph The e-graph (must contain the new e-classes)
   * @param new_eclasses E-class IDs that were added since last index update
   */
  void update_index(EGraph<Symbol, Analysis> const &egraph, std::span<EClassId const> new_eclasses);

  /**
   * @brief Clear the symbol index
   */
  void clear_index() { index_.clear(); }

  /**
   * @brief Find all matches of a pattern in the e-graph
   *
   * Searches the entire e-graph for expressions matching the pattern.
   * Returns all valid matches with their variable bindings.
   *
   * @param egraph The e-graph to search
   * @param pattern The pattern to match
   * @return Vector of all matches found
   */
  auto match(EGraph<Symbol, Analysis> const &egraph, Pattern<Symbol> const &pattern) const -> std::vector<Match>;

  /**
   * @brief Find all matches of a pattern in the e-graph (with reusable context)
   *
   * More efficient for repeated calls - reuses internal buffers from context.
   *
   * @param egraph The e-graph to search
   * @param pattern The pattern to match
   * @param ctx Reusable context with pre-allocated buffers
   * @return Vector of all matches found
   */
  auto match(EGraph<Symbol, Analysis> const &egraph, Pattern<Symbol> const &pattern, EMatchContext &ctx) const
      -> std::vector<Match>;

  /**
   * @brief Find matches of a pattern starting from a specific e-class
   *
   * Only searches within the specified e-class and its descendants.
   * Useful for targeted matching during rule application.
   *
   * @param egraph The e-graph to search
   * @param pattern The pattern to match
   * @param root The e-class to match against the pattern root
   * @return Vector of matches found (all will have matched_eclass == root's canonical form)
   */
  auto match_in(EGraph<Symbol, Analysis> const &egraph, Pattern<Symbol> const &pattern, EClassId root) const
      -> std::vector<Match>;

  /**
   * @brief Find matches of a pattern starting from a specific e-class (with reusable context)
   *
   * More efficient for repeated calls - reuses internal buffers from context.
   *
   * @param egraph The e-graph to search
   * @param pattern The pattern to match
   * @param root The e-class to match against the pattern root
   * @param ctx Reusable context with pre-allocated buffers
   * @return Vector of matches found
   */
  auto match_in(EGraph<Symbol, Analysis> const &egraph, Pattern<Symbol> const &pattern, EClassId root,
                EMatchContext &ctx) const -> std::vector<Match>;

  /**
   * @brief Get the symbol index (for debugging/testing)
   */
  [[nodiscard]] auto index() const -> IndexType const & { return index_; }

  /**
   * @brief Check if the index contains a symbol
   */
  [[nodiscard]] auto has_symbol(Symbol const &sym) const -> bool { return index_.contains(sym); }

  /**
   * @brief Get e-classes containing a specific symbol
   * @return Pointer to set of e-class IDs, or nullptr if symbol not found
   */
  [[nodiscard]] auto eclasses_with_symbol(Symbol const &sym) const -> boost::unordered_flat_set<EClassId> const * {
    auto it = index_.find(sym);
    return it != index_.end() ? &it->second : nullptr;
  }

 private:
  /**
   * @brief Try to match a pattern node against an e-class, collecting all valid substitutions
   *
   * Core recursive algorithm that tries to match a pattern node against all e-nodes
   * in an e-class. For variables, binds or checks consistency. For symbols, tries
   * each e-node and recursively matches children.
   *
   * @param egraph The e-graph
   * @param pattern The full pattern
   * @param pnode_id Pattern node to match
   * @param eclass_id E-class to match against (should be canonical)
   * @param subst Current variable bindings
   * @return All valid substitutions that extend the input substitution
   */
  auto match_node(EGraph<Symbol, Analysis> const &egraph, Pattern<Symbol> const &pattern, PatternNodeId pnode_id,
                  EClassId eclass_id, Substitution const &subst) const -> std::vector<Substitution>;

  /**
   * @brief Match a pattern node and append results to output vector
   *
   * Optimized version that uses context buffers and appends directly to output,
   * eliminating per-call allocations.
   *
   * @param egraph The e-graph
   * @param pattern The full pattern
   * @param pnode_id Pattern node to match
   * @param eclass_id E-class to match against (should be canonical)
   * @param subst Current variable bindings
   * @param out Output vector to append results to
   * @param ctx Context with reusable buffers
   * @param depth Current recursion depth (for buffer pool indexing)
   */
  void match_node_into(EGraph<Symbol, Analysis> const &egraph, Pattern<Symbol> const &pattern, PatternNodeId pnode_id,
                       EClassId eclass_id, Substitution const &subst, std::vector<Substitution> &out,
                       EMatchContext &ctx, std::size_t depth) const;

  /**
   * @brief Match children of a pattern node against children of an e-node
   *
   * Helper that handles the child matching loop: iterates through pattern children,
   * recursively matches each against the corresponding e-node child's e-class,
   * and collects all valid substitution extensions.
   *
   * @param egraph The e-graph
   * @param pattern The full pattern
   * @param pnode Pattern node (must be non-leaf symbol node)
   * @param enode E-node to match against (symbol and arity already verified)
   * @param subst Starting variable bindings
   * @param out Output vector to append results to
   * @param ctx Context with reusable buffers
   * @param depth Current depth for buffer indexing
   */
  void match_children_into(EGraph<Symbol, Analysis> const &egraph, Pattern<Symbol> const &pattern,
                           PatternNode<Symbol> const &pnode, ENode<Symbol> const &enode, Substitution const &subst,
                           std::vector<Substitution> &out, EMatchContext &ctx, std::size_t depth) const;

  IndexType index_;
};

// ========================================================================
// EMatcher Implementation
// ========================================================================

template <typename Symbol, typename Analysis>
void EMatcher<Symbol, Analysis>::build_index(EGraph<Symbol, Analysis> const &egraph) {
  index_.clear();

  // Scan all canonical e-classes
  for (auto const &[eclass_id, eclass] : egraph.canonical_classes()) {
    // Each e-node in the e-class contributes its symbol to the index
    for (auto const &enode_id : eclass.nodes()) {
      auto const &enode = egraph.get_enode(enode_id);
      index_[enode.symbol()].insert(eclass_id);
    }
  }
}

template <typename Symbol, typename Analysis>
void EMatcher<Symbol, Analysis>::update_index(EGraph<Symbol, Analysis> const &egraph,
                                              std::span<EClassId const> new_eclasses) {
  for (auto eclass_id : new_eclasses) {
    // Get canonical form in case merges happened
    auto canonical_id = egraph.find(eclass_id);
    if (!egraph.has_class(canonical_id)) {
      continue;  // E-class was merged away
    }

    auto const &eclass = egraph.eclass(canonical_id);
    for (auto const &enode_id : eclass.nodes()) {
      auto const &enode = egraph.get_enode(enode_id);
      index_[enode.symbol()].insert(canonical_id);
    }
  }
}

template <typename Symbol, typename Analysis>
auto EMatcher<Symbol, Analysis>::match(EGraph<Symbol, Analysis> const &egraph, Pattern<Symbol> const &pattern) const
    -> std::vector<Match> {
  // Non-context version: create temporary context
  EMatchContext ctx;
  return match(egraph, pattern, ctx);
}

template <typename Symbol, typename Analysis>
auto EMatcher<Symbol, Analysis>::match(EGraph<Symbol, Analysis> const &egraph, Pattern<Symbol> const &pattern,
                                       EMatchContext &ctx) const -> std::vector<Match> {
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
    for (auto const &[eclass_id, _] : egraph.canonical_classes()) {
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
    auto canonical_id = egraph.find(eclass_id);
    if (!processed.insert(canonical_id).second) {
      continue;  // Already processed this canonical e-class
    }

    auto const &eclass = egraph.eclass(canonical_id);

    // Inline root matching: iterate e-nodes filtering by symbol
    // (We know from index this e-class has at least one match)
    for (auto enode_id : eclass.nodes()) {
      auto const &enode = egraph.get_enode(enode_id);

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
      match_children_into(egraph, pattern, root_pnode, enode, Substitution{}, child_matches, ctx, 0);

      for (auto &subst : child_matches) {
        results.push_back({canonical_id, std::move(subst)});
      }
    }
  }

  return results;
}

template <typename Symbol, typename Analysis>
auto EMatcher<Symbol, Analysis>::match_in(EGraph<Symbol, Analysis> const &egraph, Pattern<Symbol> const &pattern,
                                          EClassId root) const -> std::vector<Match> {
  // Non-context version: create temporary context
  EMatchContext ctx;
  return match_in(egraph, pattern, root, ctx);
}

template <typename Symbol, typename Analysis>
auto EMatcher<Symbol, Analysis>::match_in(EGraph<Symbol, Analysis> const &egraph, Pattern<Symbol> const &pattern,
                                          EClassId root, EMatchContext &ctx) const -> std::vector<Match> {
  if (pattern.empty()) {
    return {};
  }

  auto canonical_root = egraph.find(root);
  if (!egraph.has_class(canonical_root)) {
    return {};
  }

  // Pre-allocate depth buffers to avoid invalidation during recursion
  ctx.ensure_depth(pattern.size());

  std::vector<Match> results;
  std::vector<Substitution> substitutions;
  Substitution empty_subst;
  match_node_into(egraph, pattern, pattern.root(), canonical_root, empty_subst, substitutions, ctx, 0);

  for (auto &subst : substitutions) {
    results.push_back({canonical_root, std::move(subst)});
  }

  return results;
}

template <typename Symbol, typename Analysis>
auto EMatcher<Symbol, Analysis>::match_node(EGraph<Symbol, Analysis> const &egraph, Pattern<Symbol> const &pattern,
                                            PatternNodeId pnode_id, EClassId eclass_id, Substitution const &subst) const
    -> std::vector<Substitution> {
  // Legacy API: use temporary context
  EMatchContext ctx;
  std::vector<Substitution> results;
  match_node_into(egraph, pattern, pnode_id, eclass_id, subst, results, ctx, 0);
  return results;
}

template <typename Symbol, typename Analysis>
void EMatcher<Symbol, Analysis>::match_node_into(EGraph<Symbol, Analysis> const &egraph, Pattern<Symbol> const &pattern,
                                                 PatternNodeId pnode_id, EClassId eclass_id, Substitution const &subst,
                                                 std::vector<Substitution> &out, EMatchContext &ctx,
                                                 std::size_t depth) const {
  auto const &pnode = pattern[pnode_id];

  if (pnode.is_variable()) {
    // Variable node: check for consistent binding or create new binding
    auto var = pnode.variable();
    auto it = subst.find(var);
    if (it != subst.end()) {
      // Variable already bound: must match same e-class (canonical comparison)
      if (egraph.find(it->second) == egraph.find(eclass_id)) {
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
  auto const &eclass = egraph.eclass(eclass_id);

  for (auto enode_id : eclass.nodes()) {
    auto const &enode = egraph.get_enode(enode_id);

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
    match_children_into(egraph, pattern, pnode, enode, subst, out, ctx, depth);
  }
}

template <typename Symbol, typename Analysis>
void EMatcher<Symbol, Analysis>::match_children_into(EGraph<Symbol, Analysis> const &egraph,
                                                     Pattern<Symbol> const &pattern, PatternNode<Symbol> const &pnode,
                                                     ENode<Symbol> const &enode, Substitution const &subst,
                                                     std::vector<Substitution> &out, EMatchContext &ctx,
                                                     std::size_t depth) const {
  // Get reusable buffers for this depth level (already ensured by caller)
  auto &bufs = ctx.buffers_at(depth);
  bufs.current.clear();
  bufs.current.push_back(subst);

  for (std::size_t i = 0; i < pnode.arity(); ++i) {
    auto pattern_child_id = pnode.children[i];
    auto enode_child_eclass = egraph.find(enode.children()[i]);

    bufs.next.clear();  // Reuse existing capacity
    for (auto const &current_subst : bufs.current) {
      match_node_into(egraph, pattern, pattern_child_id, enode_child_eclass, current_subst, bufs.next, ctx, depth + 1);
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

}  // namespace memgraph::planner::core
