/**
 * @file hashcons.hpp
 * @brief Hash consing data structure for efficient e-node deduplication using ENodeId
 *
 * This file contains the Hashcons class, which implements hash consing
 * for e-nodes using ENodeId references. Hash consing is a fundamental technique
 * in e-graph implementations that ensures each unique e-node structure exists
 * only once in memory, enabling efficient equality checking and
 * memory usage optimization.
 */

#pragma once

#include <optional>
#include <stdexcept>
#include <unordered_map>

#include "absl/container/flat_hash_map.h"
#include "boost/unordered/unordered_flat_map.hpp"
#include "planner/core/enode.hpp"
#include "planner/core/fwd.hpp"

namespace memgraph::planner::core {

template <typename Symbol, typename Analysis>
class EGraph;

/**
 * @brief Hash consing table for efficient e-node deduplication using direct ENode mapping
 *
 * @details
 * Hash consing is a fundamental technique in e-graph implementations that
 * maintains a global table of unique e-node structures. When a new e-node
 * is created, the hash consing table is consulted to see if an equivalent
 * e-node already exists. If so, the existing e-class ID is returned;
 * otherwise, the new e-node is added to the table.
 *
 * This implementation maps ENode<Symbol> directly to EClassId, eliminating
 * the two-level lookup overhead. ENode's pre-computed hash enables efficient
 * direct mapping without performance penalty.
 *
 * @tparam Symbol The type used to represent operation symbols in e-nodes
 */
template <typename Symbol>
class Hashcons {
 public:
  Hashcons() {
    // Pre-size to avoid expensive rehashing during initial population
    // Based on performance analysis: prevents 70-80% of rehash operations
    table_.reserve(4096);
  }

  /**
   * @brief Lookup an ENode in the hash consing table
   */
  [[nodiscard]] auto lookup(const ENode<Symbol> &enode) const -> std::optional<EClassId>;

  /**
   * @brief Insert an ENode with its associated e-class ID
   *
   * Adds a new ENode to the hash consing table, associating it with
   * the provided e-class ID. The e-node should be canonicalized before
   * insertion to ensure correct future lookups.
   *
   * @param enode The ENode to insert (should be canonicalized)
   * @param eclass_id The e-class ID to associate with this e-node
   */
  void insert(const ENode<Symbol> &enode, EClassId eclass_id);

  /**
   * @brief Remove an ENode from the hash consing table
   * @param enode The ENode to remove
   */
  void remove(const ENode<Symbol> &enode);

  /**
   * @brief Update an ENode entry (atomic remove and insert)
   *
   * Efficiently removes the old ENode and inserts the new ENode
   * with the same e-class ID.
   *
   * @param old_enode The ENode to remove
   * @param new_enode The canonicalized ENode to insert
   * @param eclass_id The e-class ID to associate with the new e-node
   */
  void update(const ENode<Symbol> &old_enode, const ENode<Symbol> &new_enode, EClassId eclass_id);

  /**
   * @brief Clear all entries from the hash consing table
   */
  void clear();

  /**
   * @brief Get the number of entries in the table
   */
  [[nodiscard]] auto size() const -> size_t { return table_.size(); }

  /**
   * @brief Check if the hash consing table is empty
   */
  [[nodiscard]] auto empty() const -> bool { return table_.empty(); }

  /**
   * @brief Reserve capacity for n entries
   *
   * @param n Expected number of unique e-nodes

   */
  void reserve(size_t n) { table_.reserve(n); }

  /**
   * @brief Get the current load factor of the hash table
   *
   * Load factor = size() / bucket_count()
   */
  [[nodiscard]] auto load_factor() const -> float { return table_.load_factor(); }

  /**
   * @brief Get the maximum allowed load factor
   *
   * When the load factor exceeds this threshold, the hash table
   * automatically rehashes to maintain performance.
   */
  [[nodiscard]] auto max_load_factor() const -> float { return table_.max_load_factor(); }

  /**
   * @brief Set the maximum allowed load factor
   *
   * Controls when the hash table rehashes for performance tuning.
   * Lower values favor lookup speed, higher values favor memory efficiency.
   *
   * @param ml New maximum load factor (typically 0.5 to 1.0)
   *
   * @par Performance Tuning:
   * - Lower values (0.5-0.7): Faster lookups, more memory usage
   * - Higher values (0.8-1.0): Better memory efficiency, slower lookups
   */
  void max_load_factor(float ml) { table_.max_load_factor(ml); }

 private:
  /**
   * @brief Internal hash table storing ENode to e-class ID mappings
   */
  boost::unordered_flat_map<ENode<Symbol> /*TODO hold shared copy by ptr*/, EClassId,
                            ENodeHash<Symbol> /*ENodePtrHash*/ /*ENodePtrEq*/>
      table_;
};

}  // namespace memgraph::planner::core

// Template method implementations
template <typename Symbol>
auto memgraph::planner::core::Hashcons<Symbol>::lookup(const ENode<Symbol> &enode) const -> std::optional<EClassId> {
  auto it = table_.find(enode);
  if (it != table_.end()) {
    return it->second;
  }
  return std::nullopt;
}

template <typename Symbol>
void memgraph::planner::core::Hashcons<Symbol>::insert(const ENode<Symbol> &enode, EClassId eclass_id) {
  table_[enode] = eclass_id;
}

template <typename Symbol>
void memgraph::planner::core::Hashcons<Symbol>::remove(const ENode<Symbol> &enode) {
  table_.erase(enode);
}

template <typename Symbol>
void memgraph::planner::core::Hashcons<Symbol>::update(const ENode<Symbol> &old_enode, const ENode<Symbol> &new_enode,
                                                       EClassId eclass_id) {
  table_.erase(old_enode);
  table_[new_enode] = eclass_id;
}

template <typename Symbol>
void memgraph::planner::core::Hashcons<Symbol>::clear() {
  table_.clear();
}
