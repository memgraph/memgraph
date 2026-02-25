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

import memgraph.planner.core.eids;

#include <span>

#include <boost/container/small_vector.hpp>
#include <boost/unordered/unordered_flat_map.hpp>

#include "planner/egraph/egraph.hpp"

namespace memgraph::planner::core::vm {

/// Parent index organized by symbol for efficient pattern matching.
///
/// This index allows O(1) lookup of parents with a specific symbol,
/// rather than iterating all parents and filtering.
///
/// Two modes of operation:
/// 1. "Verify" mode: Use basic EClass::parents() and verify child consistency
/// 2. "Clean" mode: Use this index which is kept clean (no stale entries)
template <typename Symbol, typename Analysis>
class ParentSymbolIndex {
 public:
  using EGraphType = EGraph<Symbol, Analysis>;
  using ParentList = boost::container::small_vector<ENodeId, 4>;

  /// Per-eclass index: symbol -> list of parent e-nodes with that symbol
  using EClassIndex = boost::unordered_flat_map<Symbol, ParentList>;

  explicit ParentSymbolIndex(EGraphType const &egraph) : egraph_(&egraph) {}

  /// Full rebuild of the index from the e-graph
  void rebuild() {
    index_.clear();

    for (auto const &[eclass_id, eclass] : egraph_->canonical_classes()) {
      auto &eclass_index = index_[eclass_id];

      for (auto parent_enode_id : eclass.parents()) {
        auto const &parent_enode = egraph_->get_enode(parent_enode_id);
        eclass_index[parent_enode.symbol()].push_back(parent_enode_id);
      }
    }

    ++stats_.full_rebuilds;
  }

  /// Incremental update after e-graph rebuild
  /// @param affected_eclasses E-classes that were modified during rebuild
  void update(boost::unordered_flat_set<EClassId> const &affected_eclasses) {
    for (auto eclass_id : affected_eclasses) {
      auto canonical_id = egraph_->find(eclass_id);

      // Remove old index entry if it exists
      // TODO: can we have a better control over this. Can we maintain and present the set of obsolete eclasses
      // (non-canonical after merge)
      //      vs those that need to be rebuilt?
      index_.erase(eclass_id);
      if (eclass_id != canonical_id) {
        index_.erase(canonical_id);  // TODO: why erase + rebuild can we do an inplace update?
      }

      // Rebuild for this e-class
      if (egraph_->has_class(canonical_id)) {  // TODO: this is canonical so should be true
        auto const &eclass = egraph_->eclass(canonical_id);
        auto &eclass_index = index_[canonical_id];
        eclass_index.clear();  // TODO: why? we already erased it

        // TODO: this is duplicate from rebuild()
        for (auto parent_enode_id : eclass.parents()) {
          auto const &parent_enode = egraph_->get_enode(parent_enode_id);
          eclass_index[parent_enode.symbol()].push_back(parent_enode_id);
        }
      }
    }

    ++stats_.incremental_updates;
  }

  /// Get parents of an e-class with a specific symbol
  /// Returns empty span if no parents with that symbol exist
  [[nodiscard]] auto parents_with_symbol(EClassId eclass_id, Symbol sym) const -> std::span<ENodeId const> {
    auto canonical_id = egraph_->find(eclass_id);
    auto it = index_.find(canonical_id);
    if (it == index_.end()) {
      return {};
    }

    auto sym_it = it->second.find(sym);
    if (sym_it == it->second.end()) {
      return {};
    }

    return sym_it->second;
  }

  /// Check if an e-class has any parents with a specific symbol
  // TODO: why do we need this? It would be cleaner and more performant just to fetch the span
  [[nodiscard]] auto has_parents_with_symbol(EClassId eclass_id, Symbol sym) const -> bool {
    return !parents_with_symbol(eclass_id, sym).empty();
  }

  /// Statistics for benchmarking
  struct Stats {
    std::size_t full_rebuilds{0};
    std::size_t incremental_updates{0};
  };

  // TODO: Do we really need stats here?
  [[nodiscard]] auto stats() const -> Stats const & { return stats_; }

 private:
  EGraphType const *egraph_;
  boost::unordered_flat_map<EClassId, EClassIndex> index_;
  Stats stats_;
};

}  // namespace memgraph::planner::core::vm
