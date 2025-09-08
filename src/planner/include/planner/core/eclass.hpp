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

/**
 * @brief Equivalence class containing semantically equivalent e-nodes
 *
 * @details
 * An E-class represents a set of e-nodes that are known to be semantically
 * equivalent.
 *
 * @tparam Symbol The type used for operation symbols in e-nodes
 * @tparam Analysis Optional analysis type for attaching domain-specific data
 *
 * @par Thread Safety:
 * Not thread-safe.
 */
template <typename Symbol, typename Analysis>
struct EClass {
  explicit EClass(ENodeId initial_enode_id) {
    nodes.reserve(8);
    parents.reserve(16);

    nodes.emplace(initial_enode_id);
  }

  /**
   * @brief Add a parent reference for congruence tracking
   *
   * Records that the given parent ENodeId (in the specified parent e-class)
   * has this e-class as one of its children.
   */
  void add_parent(ENodeId parent_enode_id, EClassId parent_class_id);

  /**
   * @brief Merge of another e-class into this one
   *
   * @param other The e-class to merge into this one (may be modified)

   * @complexity O(min(this.size(), other.size())) amortized
   */
  void merge_with(EClass &&other);

  /**
   * @brief Get the number of e-nodes in this class
   */
  [[nodiscard]] auto size() const -> size_t { return nodes.size(); }

  /**
   * @brief Get a representative ENodeId from this class
   */
  [[nodiscard]] auto representative_id() const -> ENodeId;

  /**
   * @brief Get a representative e-node value from this class
   *
   * @warning Less efficient due to copying. Consider using representative_id()
   */
  template <typename EGraphType>
  [[nodiscard]] auto representative_node(const EGraphType &egraph) const -> ENode<Symbol>;

 private:
  boost::unordered_flat_set<ENodeId> nodes;

  /**
   * @brief Parent references for congruence closure
   *
   * Maintains a list of "parent" e-node IDs that have this e-class as a child.
   * Each entry is a pair of (parent_enode_id, parent_class_id), enabling
   * efficient propagation during congruence closure algorithms.
   * Uses ENodeId for memory efficiency and fast integer-based comparisons.
   */
  std::vector<std::pair<ENodeId, EClassId>> parents;  // TODO: is the right way to track parents

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

template <typename Symbol, typename Analysis>
void EClass<Symbol, Analysis>::add_parent(ENodeId parent_enode_id, EClassId parent_class_id) {
  parents.emplace_back(parent_enode_id, parent_class_id);
}

template <typename Symbol, typename Analysis>
void EClass<Symbol, Analysis>::merge_with(EClass &&other) {
  // Simple optimization: swap if other is significantly larger
  if (other.nodes.size() > nodes.size()) {
    nodes.swap(other.nodes);
    parents.swap(other.parents);
  }

  nodes.insert(other.nodes.begin(), other.nodes.end());
  parents.insert(parents.end(), other.parents.begin(), other.parents.end());

  // Merge analysis data if present
  if constexpr (!std::is_same_v<Analysis, void>) {
    // This would need to be implemented based on the analysis type
    // For now, just a placeholder
  }
}

template <typename Symbol, typename Analysis>
auto EClass<Symbol, Analysis>::representative_id() const -> ENodeId {
  // Every e-class has at least one e-node
  return *nodes.begin();
}

template <typename Symbol, typename Analysis>
template <typename EGraphType>
auto EClass<Symbol, Analysis>::representative_node(const EGraphType &egraph) const -> ENode<Symbol> {
  return egraph.get_enode(representative_id());
}

}  // namespace memgraph::planner::core
