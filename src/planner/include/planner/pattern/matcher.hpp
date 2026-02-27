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
#include <boost/dynamic_bitset.hpp>
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

/// Bindings collected during recursive matching.
/// Simpler than PartialMatch - no undo log, just slot values.
class CollectedBindings {
 public:
  explicit CollectedBindings(std::size_t num_slots) : slots_(num_slots), bound_(num_slots) {}

  void bind(std::size_t slot, EClassId eclass) {
    slots_[slot] = eclass;
    bound_.set(slot);
  }

  [[nodiscard]] auto is_bound(std::size_t slot) const -> bool { return bound_.test(slot); }

  [[nodiscard]] auto get(std::size_t slot) const -> EClassId { return slots_[slot]; }

  [[nodiscard]] auto num_slots() const -> std::size_t { return slots_.size(); }

  [[nodiscard]] auto slots() const -> boost::container::small_vector<EClassId, 8> const & { return slots_; }

 private:
  boost::container::small_vector<EClassId, 8> slots_;
  boost::dynamic_bitset<> bound_;
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

  /**
   * @brief Get candidate e-classes for a given symbol
   *
   * Returns all e-classes that contain at least one e-node with the given symbol.
   * This is useful for VM-based pattern matching to get the initial candidates
   * for index-driven iteration.
   *
   * @param sym The symbol to look up
   * @param candidates Output vector to append candidate e-class IDs to (cleared first)
   */
  void candidates_for_symbol(Symbol sym, std::vector<EClassId> &candidates) const {
    candidates.clear();
    if (auto it = index_.find(sym); it != index_.end()) {
      candidates.reserve(it->second.size());
      for (auto id : it->second) {
        candidates.push_back(id);
      }
    }
  }

  /**
   * @brief Get all e-classes for wildcard/variable pattern matching
   *
   * Returns all canonical e-classes. Used when a pattern's root is a variable
   * or wildcard (not a symbol) and thus cannot use the symbol index.
   *
   * @param candidates Output vector to append candidate e-class IDs to (cleared first)
   */
  void all_candidates(std::vector<EClassId> &candidates) const {
    auto ids = egraph_->canonical_eclass_ids();
    candidates.assign(ids.begin(), ids.end());
  }

 private:
  using IndexType = boost::unordered_flat_map<Symbol, boost::unordered_flat_set<EClassId>>;

  /// Recursively collect all matches for a pattern node at a given e-class.
  /// Returns all valid binding combinations (Cartesian product over choices).
  void collect_matches_impl(Pattern<Symbol> const &pattern, PatternNodeId pnode, EClassId eclass,
                            std::vector<CollectedBindings> &results) const;

  /// Try to merge two binding sets. Returns nullopt if variable conflict.
  auto try_merge_bindings(CollectedBindings const &a, CollectedBindings const &b) const
      -> std::optional<CollectedBindings>;

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

  auto &processed = ctx.processed();

  // Process a single root candidate using recursive Cartesian product matching.
  // This approach correctly handles self-referential e-classes by enumerating
  // ALL valid combinations of e-nodes at each pattern position.
  auto process_candidate = [&](EClassId root_eclass) {
    auto canonical_root = egraph_->find(root_eclass);
    if (!processed.insert(canonical_root).second) {
      return;
    }

    // Collect all matches from this root
    std::vector<CollectedBindings> matches;
    collect_matches_impl(pattern, pattern.root(), canonical_root, matches);

    // Commit each match to the arena
    for (auto const &bindings : matches) {
      results.push_back(ctx.arena().intern(bindings.slots()));
    }
  };

  // Iterate root candidates directly from source - no intermediate copy
  auto const &root_pnode = pattern[pattern.root()];
  std::visit(Overload{[&](Wildcard) {
                        for (auto eclass_id : egraph_->canonical_eclass_ids()) {
                          process_candidate(eclass_id);
                        }
                      },
                      [&](PatternVar) {
                        for (auto eclass_id : egraph_->canonical_eclass_ids()) {
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
// Recursive Cartesian Product Matching
// ========================================================================

/// Try to merge two binding sets. Returns nullopt if variable conflict.
template <typename Symbol, typename Analysis>
auto EMatcher<Symbol, Analysis>::try_merge_bindings(CollectedBindings const &a, CollectedBindings const &b) const
    -> std::optional<CollectedBindings> {
  CollectedBindings result(a.num_slots());

  // Copy all bindings from a
  for (std::size_t i = 0; i < a.num_slots(); ++i) {
    if (a.is_bound(i)) {
      result.bind(i, a.get(i));
    }
  }

  // Merge bindings from b, checking for conflicts
  for (std::size_t i = 0; i < b.num_slots(); ++i) {
    if (b.is_bound(i)) {
      if (result.is_bound(i)) {
        // Both bound - check consistency (same e-class after canonicalization)
        if (egraph_->find(result.get(i)) != egraph_->find(b.get(i))) {
          return std::nullopt;  // Conflict
        }
      } else {
        result.bind(i, b.get(i));
      }
    }
  }

  return result;
}

/// Recursively collect all matches for a pattern node at a given e-class.
/// Uses Cartesian product over e-node choices and child matches.
template <typename Symbol, typename Analysis>
void EMatcher<Symbol, Analysis>::collect_matches_impl(Pattern<Symbol> const &pattern, PatternNodeId pnode,
                                                      EClassId eclass, std::vector<CollectedBindings> &results) const {
  auto const &pnode_data = pattern[pnode];

  std::visit(Overload{[&](Wildcard) {
                        // Wildcard matches anything with no binding
                        results.emplace_back(pattern.num_vars());
                      },

                      [&](PatternVar var) {
                        // Variable binds to eclass
                        CollectedBindings bindings(pattern.num_vars());
                        bindings.bind(pattern.var_slot(var), eclass);
                        results.push_back(std::move(bindings));
                      },

                      [&](SymbolWithChildren<Symbol> const &sym_node) {
                        // For each e-node in the e-class with matching symbol and arity
                        for (auto enode_id : egraph_->eclass(eclass).nodes()) {
                          auto const &enode = egraph_->get_enode(enode_id);

                          // Check symbol and arity match
                          if (enode.symbol() != sym_node.sym || enode.arity() != sym_node.children.size()) {
                            continue;
                          }

                          if (sym_node.children.empty()) {
                            // Leaf symbol node - existence check only, one match per e-class
                            CollectedBindings bindings(pattern.num_vars());
                            if (auto binding = pattern.binding_for(pnode)) {
                              bindings.bind(pattern.var_slot(*binding), eclass);
                            }
                            results.push_back(std::move(bindings));
                            break;  // Don't iterate remaining e-nodes - one match per e-class
                          } else {
                            // Non-leaf: collect matches from each child, then Cartesian product
                            std::vector<std::vector<CollectedBindings>> child_matches;
                            child_matches.reserve(sym_node.children.size());
                            bool all_children_matched = true;

                            for (std::size_t i = 0; i < sym_node.children.size(); ++i) {
                              std::vector<CollectedBindings> child_results;
                              auto child_eclass = egraph_->find(enode.children()[i]);
                              collect_matches_impl(pattern, sym_node.children[i], child_eclass, child_results);

                              if (child_results.empty()) {
                                all_children_matched = false;
                                break;
                              }
                              child_matches.push_back(std::move(child_results));
                            }

                            if (!all_children_matched) {
                              continue;
                            }

                            // Cartesian product of child matches
                            // Start with first child's matches
                            std::vector<CollectedBindings> current = std::move(child_matches[0]);

                            for (std::size_t i = 1; i < child_matches.size(); ++i) {
                              std::vector<CollectedBindings> next;
                              for (auto const &left : current) {
                                for (auto const &right : child_matches[i]) {
                                  if (auto merged = try_merge_bindings(left, right)) {
                                    next.push_back(std::move(*merged));
                                  }
                                }
                              }
                              current = std::move(next);

                              // Early exit if no valid combinations
                              if (current.empty()) {
                                break;
                              }
                            }

                            // Add this node's binding to each result
                            auto binding = pattern.binding_for(pnode);
                            for (auto &m : current) {
                              if (binding) {
                                m.bind(pattern.var_slot(*binding), eclass);
                              }
                              results.push_back(std::move(m));
                            }
                          }
                        }
                      }},
             pnode_data);
}

}  // namespace memgraph::planner::core
