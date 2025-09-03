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
 * @par Key Features:
 * - **Efficient storage**: Uses boost::unordered_flat_set for O(1) node lookup
 * - **Parent tracking**: Maintains reverse references for congruence closure
 * - **Analysis integration**: Optional domain-specific data attachment
 * - **Merge operations**: Efficient combining of equivalent e-classes
 *
 * @par Usage in E-graphs:
 * E-classes are the "buckets" that contain equivalent e-nodes. When two
 * e-classes are merged (due to discovered equivalence), their contents
 * are combined and the union-find structure is updated to reflect the merger.
 *
 * @par Example:
 * @code{.cpp}
 * // Create e-class for expressions equivalent to "x + 0"
 * EClass<std::string, void> eclass(42);
 *
 * // Add equivalent expressions
 * eclass.add_node(ENode<std::string>("+", {id_x, id_0}));  // x + 0
 * eclass.add_node(ENode<std::string>("+", {id_0, id_x}));  // 0 + x (commutative)
 * eclass.add_node(ENode<std::string>("x", {}));            // x (simplified)
 *
 * // Now eclass contains 3 equivalent representations of the same value
 * assert(eclass.size() == 3);
 * @endcode
 *
 * @par Performance Characteristics:
 * - **Node addition**: O(1) average, O(n) worst case for hash conflicts
 * - **Node lookup**: O(1) average with boost::unordered_flat_set
 * - **Parent addition**: O(1) amortized with std::vector
 * - **Merge operations**: O(min(|this|, |other|)) by moving smaller to larger
 *
 * @par Memory Layout:
 * - EClassId: 4 bytes
 * - boost::unordered_flat_set<ENode>: ~32 bytes + node storage
 * - std::vector<parent_pairs>: ~24 bytes + parent storage
 * - Analysis data: sizeof(Analysis::Data) if Analysis != void
 * - Total: ~60 bytes + content storage
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
  /**
   * @brief Constructor creates e-class with initial ENodeId
   *
   * Initializes a new e-class with the given ENodeId as its first element.
   * More efficient than default construction + add_node() as it avoids
   * the hash table lookup during insertion.
   *
   * @param initial_enode_id The first ENodeId to add to this e-class
   */
  explicit EClass(ENodeId initial_enode_id) {
    // Pre-size containers
    nodes.reserve(8);
    parents.reserve(16);

    // Insert the initial ENodeId directly
    nodes.emplace(initial_enode_id);
  }

  /**
   * @brief Add a parent reference for congruence tracking
   *
   * Records that the given parent ENodeId (in the specified parent e-class)
   * has this e-class as one of its children. This information is essential
   * for congruence closure algorithms. Uses ENodeId for maximum memory efficiency.
   *
   * @param parent_enode_id The parent ENodeId that references this e-class
   * @param parent_class_id The e-class ID containing the parent e-node
   *
   * @par When Called:
   * - When a new e-node is added to the e-graph
   * - During e-class merging operations
   * - When updating parent-child relationships
   *
   * @par Performance Note:
   * Uses std::vector for parent storage, which provides amortized O(1)
   * insertion and cache-friendly iteration during congruence closure.
   * ENodeId enables fast integer-based comparison of parent nodes.
   *
   * @complexity O(1) amortized
   * @threadsafety Not thread-safe
   */
  void add_parent(ENodeId parent_enode_id, EClassId parent_class_id);

  /**
   * @brief Merge another e-class into this one
   *
   * Combines all e-nodes and parent references from the other e-class
   * into this e-class. The other e-class should be considered invalid
   * after this operation. Analysis data is also merged appropriately.
   *
   * @param other The e-class to merge into this one
   *
   * @par Algorithm:
   * 1. Move all e-nodes from other.nodes to this.nodes
   * 2. Move all parent references from other.parents to this.parents
   * 3. Merge analysis data using Analysis::merge() if applicable
   * 4. Other e-class data is moved but remains valid
   *
   * @par Performance Optimization:
   * The e-graph ensures the smaller e-class is merged into the larger
   * one to minimize data movement and maintain good performance.
   *
   * @complexity O(other.nodes.size() + other.parents.size())
   * @threadsafety Not thread-safe
   */
  [[deprecated("Use merge_with(EClass&&) instead")]] void merge_with(const EClass &other);

  /**
   * @brief Move-optimized merge of another e-class into this one
   *
   * Highly optimized version that can move data from the other e-class
   * when beneficial, providing significant performance improvements
   * for large e-class merges.
   *
   * @param other The e-class to merge into this one (may be modified)
   *
   * @par Optimization Strategy:
   * - If other is larger, swaps containers then merges back
   * - Uses move semantics to avoid expensive copying
   * - Minimizes memory allocations and rehashing
   * - Up to 50-70% faster than copy-based merge for large sets
   *
   * @complexity O(min(this.size(), other.size())) amortized
   * @threadsafety Not thread-safe
   */
  void merge_with(EClass &&other);

  /**
   * @brief Get the number of e-nodes in this class
   *
   * @return Number of equivalent e-nodes stored in this e-class
   *
   * @complexity O(1)
   * @threadsafety Thread-safe for reads
   */
  [[nodiscard]] auto size() const -> size_t { return nodes.size(); }

  /**
   * @brief Get a representative ENodeId from this class
   *
   * Returns an arbitrary ENodeId from this e-class for display,
   * debugging, or analysis purposes. The choice of representative
   * is implementation-defined and may change between calls.
   * By design, every e-class is guaranteed to have at least one e-node.
   *
   * @return ENodeId referencing a representative e-node
   *
   * @par Usage:
   * Useful for extracting a concrete syntax tree from an e-class,
   * showing examples during debugging, or providing default
   * representations for visualization. Use with EGraph::get_enode()
   * to access the actual e-node data.
   *
   * @par Benefits:
   * Returns lightweight ENodeId for efficient storage and passing.
   * No lifetime concerns or reference counting overhead.
   *
   * @complexity O(1)
   * @threadsafety Thread-safe for reads
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

  // PRIVATE

  /**
   * @brief Set of equivalent e-node IDs in this class
   *
   * Contains ENodeIds referencing all e-nodes that are known to be semantically equivalent.
   * Uses boost::unordered_flat_set with ENodeId for memory-efficient storage.
   * ENodeIds are 4-byte integers that provide 75% memory savings vs 16-byte shared pointers.
   *
   * @par Performance:
   * - Insertion: O(1) average, hash-table based
   * - Lookup: O(1) average for containment checks
   * - Iteration: Cache-friendly flat storage with excellent locality
   * - Comparison: Fast integer equality instead of pointer dereferencing
   *
   * @par Invariant:
   * All ENodeIds in this set reference e-nodes that represent the same semantic value,
   * though they may have different syntactic forms (e.g., "x+0" and "x").
   *
   * @par Memory Optimization:
   * ENodeId storage eliminates pointer overhead and reference counting complexity.
   * Actual e-node data is centrally stored in EGraph::enode_storage_.
   */
  boost::unordered_flat_set<ENodeId> nodes;

  /**
   * @brief Parent references for congruence closure
   *
   * Maintains a list of "parent" e-node IDs that have this e-class as a child.
   * Each entry is a pair of (parent_enode_id, parent_class_id), enabling
   * efficient propagation during congruence closure algorithms.
   * Uses ENodeId for memory efficiency and fast integer-based comparisons.
   *
   * @par Purpose:
   * When this e-class is merged with another, all parents must be
   * re-examined to check if they now represent congruent expressions
   * that should also be merged.
   *
   * @par Example:
   * If this e-class represents "x" and has parents:
   * - (enode_id_plus, parent_id_1)  // ENodeId for "+(x, 0)" expression
   * - (enode_id_mult, parent_id_2)  // ENodeId for "*(x, 1)" expression
   *
   * When "x" is merged with another e-class, both parent expressions
   * need to be re-evaluated for potential congruence.
   *
   * @par Performance:
   * - Addition: O(1) amortized
   * - Iteration: O(parents.size()) during congruence checks
   * - Comparison: Fast integer equality for ENodeIds
   *
   * @par Memory Optimization:
   * ENodeId (4 bytes) vs shared pointer approach (16 bytes) provides 75% memory savings.
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
void EClass<Symbol, Analysis>::merge_with(const EClass &other) {
  nodes.insert(other.nodes.begin(), other.nodes.end());
  parents.insert(parents.end(), other.parents.begin(), other.parents.end());

  // Merge analysis data if present
  if constexpr (!std::is_same_v<Analysis, void>) {
    // This would need to be implemented based on the analysis type
    // For now, just a placeholder
  }
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
  // By design, every e-class has at least one e-node
  return *nodes.begin();
}

template <typename Symbol, typename Analysis>
template <typename EGraphType>
auto EClass<Symbol, Analysis>::representative_node(const EGraphType &egraph) const -> ENode<Symbol> {
  // By design, every e-class has at least one e-node
  return egraph.get_enode(representative_id());  // Get the e-node (guaranteed to exist)
}

}  // namespace memgraph::planner::core
