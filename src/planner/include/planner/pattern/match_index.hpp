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

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>
#include <range/v3/all.hpp>

#include "planner/pattern/match.hpp"
#include "planner/pattern/pattern.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core {

/**
 * @brief Symbol index for fast candidate lookup in e-matching
 *
 * Maintains an index from symbols to e-classes containing that symbol.
 * Used by VM-based pattern matching (VMExecutor) to get initial candidates
 * for index-driven iteration.
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
 *   MatcherIndex<Symbol, Analysis> ematcher(egraph);
 *   vm::VMExecutor<Symbol, Analysis> vm_executor(egraph);
 *   EMatchContext ctx;
 *   std::vector<PatternMatch> matches;
 *   vm_executor.execute(compiled_pattern, ematcher, ctx, matches);
 *
 *   // After adding new e-classes, update index incrementally
 *   ematcher.rebuild_index(new_eclasses);
 * @endcode
 *
 * @tparam Symbol Must satisfy ENodeSymbol concept
 * @tparam Analysis E-graph analysis type (can be NoAnalysis)
 */
template <typename Symbol, typename Analysis>
class MatcherIndex {
 public:
  /**
   * @brief Construct MatcherIndex with e-graph reference and build initial index
   *
   * @param egraph The e-graph to match against (reference must remain valid)
   */
  explicit MatcherIndex(EGraph<Symbol, Analysis> const &egraph);

  /**
   * @brief Full rebuild of the symbol index
   *
   * Call after major e-graph changes or merges that may have invalidated
   * the index.
   */
  void rebuild_index() { rebuild_index_full(); }

  /**
   * @brief Incremental update for newly added e-classes
   *
   * More efficient than full rebuild during saturation loops.
   * Only updates index entries for the specified new e-classes.
   * Handles canonicalization and duplicates internally.
   *
   * @note Index Staleness: This method adds new index entries but does not
   * remove stale entries pointing to e-classes that were merged away. This is
   * safe because the VM executor always calls egraph.find() to get canonical
   * IDs. However, over many iterations the index may accumulate dead entries.
   * Call rebuild_index() (no args) periodically to compact if needed.
   *
   * @param new_eclasses E-class IDs that were added since last rebuild (may be non-canonical)
   */
  void rebuild_index(std::span<EClassId const> new_eclasses);

  /**
   * @brief Get candidate e-classes for a given symbol
   *
   * Returns all e-classes that contain at least one e-node with the given symbol.
   * Used by VMExecutor for index-driven candidate lookup. Candidates are
   * canonicalized to handle stale index entries from merged e-classes.
   *
   * @param sym The symbol to look up
   * @param candidates Output vector to append candidate e-class IDs to (cleared first)
   */
  void candidates_for_symbol(Symbol sym, std::vector<EClassId> &candidates) const {
    candidates.clear();
    if (auto it = index_.find(sym); it != index_.end()) {
      candidates.reserve(it->second.size());
      for (auto id : it->second) {
        candidates.push_back(egraph_->find(id));
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
  void rebuild_index_full();

  using IndexType = boost::unordered_flat_map<Symbol, boost::unordered_flat_set<EClassId>>;

  EGraph<Symbol, Analysis> const *egraph_;
  IndexType index_;
};

// ========================================================================
// MatcherIndex Implementation
// ========================================================================

template <typename Symbol, typename Analysis>
MatcherIndex<Symbol, Analysis>::MatcherIndex(EGraph<Symbol, Analysis> const &egraph) : egraph_(&egraph) {
  rebuild_index();
}

template <typename Symbol, typename Analysis>
void MatcherIndex<Symbol, Analysis>::rebuild_index_full() {
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
void MatcherIndex<Symbol, Analysis>::rebuild_index(std::span<EClassId const> new_eclasses) {
  for (auto eclass_id : new_eclasses) {
    // Canonicalize - input may contain stale IDs from before rebuild
    auto canonical_id = egraph_->find(eclass_id);
    auto const &eclass = egraph_->eclass(canonical_id);
    for (auto const &enode_id : eclass.nodes()) {
      auto const &enode = egraph_->get_enode(enode_id);
      // Set insert handles duplicates naturally
      index_[enode.symbol()].insert(canonical_id);
    }
  }
}

}  // namespace memgraph::planner::core
