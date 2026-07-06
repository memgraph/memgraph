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

module;

#include <algorithm>
#include <cassert>
#include <span>
#include <type_traits>
#include <utility>

#include <boost/container/small_vector.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

export module memgraph.planner.core.egraph:eclass;

import memgraph.planner.core.eids;

export namespace memgraph::planner::core {

namespace detail {
struct EClassBase {
  explicit EClassBase(ENodeId initial_enode_id);

  /**
   * @brief Add a parent reference (needed for congruence maintenance)
   */
  void add_parent(ENodeId parent_enode_id) { parents_.insert(parent_enode_id); }

  /**
   * @brief Remove a parent reference
   */
  void remove_parent(ENodeId parent_enode_id) { parents_.erase(parent_enode_id); }

  /**
   * @brief Remove an e-node from this class (used for duplicate removal)
   */
  void remove_node(ENodeId enode_id);

  /**
   * @brief Get the number of e-nodes in this class
   */
  [[nodiscard]] auto size() const -> size_t { return nodes_.size(); }

  [[nodiscard]] auto nodes() const -> std::span<ENodeId const> { return nodes_; }

  [[nodiscard]] auto representative() const -> ENodeId { return nodes_.front(); }

  // TODO: does this need to be a set? do we use O(1) contains/lookup?
  //       maybe needed for ematching later, leave for now
  [[nodiscard]] auto parents() const -> boost::unordered_flat_set<ENodeId> const & { return parents_; }

  void merge_with(EClassBase &other);

 private:
  boost::container::small_vector<ENodeId, 4> nodes_;
  boost::unordered_flat_set<ENodeId> parents_;
};
}  // namespace detail

/**
 * @brief Equivalence class containing semantically equivalent e-nodes
 *
 * @details
 * An E-class represents a set of e-nodes that are known to be semantically
 * equivalent.
 */
template <typename Analysis>
struct EClass : private detail::EClassBase {
  explicit EClass(ENodeId initial_enode_id) : EClassBase(initial_enode_id) {}

  EClass(ENodeId initial_enode_id, Analysis analysis)
      : EClassBase(initial_enode_id), analysis_data(std::move(analysis)) {}

  /// The e-class's analysis facts. Set once at construction (the make half) and
  /// thereafter only by merge; there is deliberately no mutable accessor, so
  /// the variant arm cannot be changed out from under the merge soundness guard.
  [[nodiscard]] auto analysis() const -> Analysis const & { return analysis_data; }

  using EClassBase::add_parent;
  using EClassBase::nodes;
  using EClassBase::parents;
  using EClassBase::remove_node;
  using EClassBase::remove_parent;
  using EClassBase::representative;
  using EClassBase::size;

  /**
   * @brief Merge of another e-class into this one
   */
  void merge_with(EClass &&other) {
    EClassBase::merge_with(other);

    // Combine per-e-class facts and flag any contradiction. Analyses that carry
    // no facts (e.g. NoAnalysis) supply a no-op merge.
    analysis_data.merge(other.analysis_data);
  }

 private:
  Analysis analysis_data;
};

}  // namespace memgraph::planner::core
