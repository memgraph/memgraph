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

#include <boost/unordered/unordered_flat_set.hpp>
#include "planner/core/eids.hpp"
#include "planner/core/enode.hpp"

namespace memgraph::planner::core {

/**
 * @brief Equivalence class containing semantically equivalent e-nodes
 *
 * @details
 * An E-class represents a set of e-nodes that are known to be semantically
 * equivalent. This is the fundamental unit of an e-graph that enables
 * compact representation of many equivalent expressions in a single container.
 *
 * @tparam Symbol The type used for operation symbols in e-nodes
 * @tparam Analysis Optional analysis type for attaching domain-specific data
 *
 * @par Core Concept:
 * When expressions are determined to be equivalent (through rewrite rules,
 * axiomatic equalities, or congruence), their e-nodes are placed in the same
 * e-class. This allows the e-graph to represent exponentially many equivalent
 * expressions compactly.
 *
 * @par Performance Characteristics:
 * - **Node addition**: O(1) average, O(n) worst case for hash conflicts
 * - **Node lookup**: O(1) average with boost::unordered_flat_set
 * - **Parent addition**: O(1) amortized with std::vector
 * - **Merge operations**: O(min(|this|, |other|)) by moving smaller to larger
 *
 * @par Thread Safety:
 * Not thread-safe. External synchronization required for concurrent access.
 * Designed for single-threaded e-graph algorithms.
 *
 * @complexity
 * - Space: O(nodes.size() + parents.size())
 * - Node operations: O(1) average for add/lookup
 * - Merge: O(smaller_class.size())
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
   * @brief Move-optimized merge of another e-class into this one
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
   *
   */
  [[nodiscard]] auto representative_id() const -> ENodeId;

  /**
   * @brief Get a representative e-node value from this class
   *
   * Returns a copy of an arbitrary e-node from this e-class.
   * Requires EGraph reference to resolve the ENodeId to actual data.
   * By design, every e-class is guaranteed to have at least one e-node.
   *
   * @param egraph The e-graph containing the e-node data
   * @return Copy of a representative e-node
   *
   * @warning Less efficient due to copying. Consider using representative_id()
   *
   * @complexity O(1) plus copy cost
   * @threadsafety Thread-safe for reads
   */
  template <typename EGraphType>
  [[nodiscard]] auto representative_node(const EGraphType &egraph) const -> ENode<Symbol>;

  /**
   * @brief Get a representative ENodeId (backward compatible version)
   *
   * Backward compatible alias for representative_id(). No longer throws
   * since by design every e-class has at least one e-node.
   *
   * @return ENodeId referencing a representative e-node
   *
   * @complexity O(1)
   * @threadsafety Thread-safe for reads
   */
  [[nodiscard]] auto representative_id_or_throw() const -> ENodeId {
    // By design, every e-class has at least one e-node
    return representative_id();
  }

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
   * Common uses include cost models, type information, or abstract values.
   *
   * @par Examples:
   * - Cost analysis: minimum cost among all equivalent expressions
   * - Type analysis: inferred type for expressions in this class
   * - Constant folding: computed constant value if determinable
   * - Abstract interpretation: abstract domain values
   *
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
  // By design, every e-class has at least one e-node
  return egraph.get_enode(representative_id());  // Get the e-node (guaranteed to exist)
}

}  // namespace memgraph::planner::core
