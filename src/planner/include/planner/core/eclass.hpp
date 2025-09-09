// Copyright 2025 Memgraph Ltd.
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

#include "planner/core/eids.hpp"
#include "planner/core/enode.hpp"

#include <boost/unordered/unordered_flat_set.hpp>

namespace memgraph::planner::core {

namespace detail {
struct EClassBase {
  explicit EClassBase(ENodeId initial_enode_id) {
    nodes_.reserve(8);
    parents_.reserve(16);

    nodes_.emplace(initial_enode_id);
  }

  /**
   * @brief Add a parent reference for congruence tracking
   *
   * Records that the given parent ENodeId (in the specified parent e-class)
   * has this e-class as one of its children.
   */
  void add_parent(ENodeId parent_enode_id, EClassId parent_class_id);

  /**
   * @brief Get the number of e-nodes in this class
   */
  [[nodiscard]] auto size() const -> size_t { return nodes_.size(); }

  /**
   * @brief Get a representative ENodeId from this class
   */
  [[nodiscard]] auto representative_id() const -> ENodeId;

  auto nodes() const -> boost::unordered_flat_set<ENodeId> const & { return nodes_; }
  auto parents() const -> std::vector<std::pair<ENodeId, EClassId>> const & { return parents_; }

  auto merge_with(EClassBase &other) {
    // Simple optimization: swap if other is significantly larger
    if (other.nodes_.size() > nodes_.size()) {
      nodes_.swap(other.nodes_);
      parents_.swap(other.parents_);
    }

    nodes_.insert(other.nodes_.begin(), other.nodes_.end());
    parents_.insert(parents_.end(), other.parents_.begin(), other.parents_.end());
  }

 private:
  boost::unordered_flat_set<ENodeId> nodes_;
  /**
   * @brief Parent references for congruence closure
   *
   * Maintains a list of "parent" e-node IDs that have this e-class as a child.
   * Each entry is a pair of (parent_enode_id, parent_class_id)
   */
  std::vector<std::pair<ENodeId, EClassId>> parents_;  // TODO: is the right way to track parents
};
}  // namespace detail

/**
 * @brief Equivalence class containing semantically equivalent e-nodes
 *
 * @details
 * An E-class represents a set of e-nodes that are known to be semantically
 * equivalent.
 *
 * @tparam Analysis Optional analysis type for attaching domain-specific data
 *
 * @par Thread Safety:
 * Not thread-safe.
 */
template <typename Analysis>
struct EClass : private detail::EClassBase {
  explicit EClass(ENodeId initial_enode_id) : detail::EClassBase(initial_enode_id) {}

  using EClassBase::add_parent;
  using EClassBase::nodes;
  using EClassBase::parents;
  using EClassBase::representative_id;
  using EClassBase::size;

  /**
   * @brief Merge of another e-class into this one
   *
   * @param other The e-class to merge into this one (may be modified)
   */
  void merge_with(EClass &&other) {
    EClassBase::merge_with(other);

    // Merge analysis data if present
    if constexpr (!std::is_same_v<Analysis, void>) {
      // This would need to be implemented based on the analysis type
      // For now, just a placeholder
    }
  }

 private:
  /**
   * @brief Domain-specific analysis data
   *
   * Stores analysis-specific information attached to this e-class.
   * Only present when Analysis template parameter is not void.

   * @par Analysis Framework:
   * The analysis data is automatically maintained by the e-graph
   * using the Analysis template parameter's merge and modify methods.
   */
  Analysis analysis_data;
};

}  // namespace memgraph::planner::core
