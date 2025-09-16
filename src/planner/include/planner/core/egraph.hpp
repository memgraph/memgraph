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

#include "planner/core/eclass.hpp"
#include "planner/core/eids.hpp"
#include "planner/core/enode.hpp"
#include "planner/core/processing_context.hpp"
#include "planner/core/union_find.hpp"

#include <boost/container/flat_set.hpp>
#include <boost/unordered/unordered_flat_map.hpp>

#include <range/v3/all.hpp>

#ifdef ASSERT_FUZZ

#undef assert

#define assert(expr)                                                                                   \
  ((expr) ? (void)0                                                                                    \
          : throw std::runtime_error(std::string("Assertion failed: ") + #expr + " at " __FILE__ ":" + \
                                     std::to_string(__LINE__)))

#endif

namespace memgraph::planner::core {

// Forward declaration for friend access
template <typename Symbol, typename Analysis>
class EGraphProxy;

/**
 * @brief e-graph with defered invariant maintenance
 *
 * @tparam Symbol The type used for operation symbols in expressions
 * @tparam Analysis Optional analysis type for domain-specific data (void for none)
 */
template <typename Symbol, typename Analysis>
struct EGraph {
  EGraph() : EGraph(256 /*an ok default capacity*/) {}

  explicit EGraph(size_t capacity) {
    classes_.reserve(capacity);
    hashcons_.reserve(capacity);
  }

  EGraph(const EGraph &other);
  EGraph(EGraph &&) noexcept = default;
  auto operator=(const EGraph &other) -> EGraph &;
  auto operator=(EGraph &&) -> EGraph & = default;

  /**
   * @brief Emplace an e-node directly with canonical children
   *
   * More efficient alternative to add() that constructs the e-node in-place
   * with pre-canonicalized children, avoiding the need for canonicalize_inplace.
   * This is the preferred method for constructing new expressions.
   */
  auto emplace(Symbol symbol, utils::small_vector<EClassId> children) -> EClassId;

  /**
   * @brief Convenience overload for leaf nodes with optional disambiguator
   *
   * Creates a leaf node with a disambiguator to distinguish between different
   * instances of the same symbol (e.g., different variables named "x").
   * Example: egraph.emplace(Symbol::Var) for variable with default ID 0
   * Example: egraph.emplace(Symbol::Var, 42) for variable with ID 42
   */
  auto emplace(Symbol symbol, uint64_t disambiguator = 0) -> EClassId;

  /**
   * @brief Convenience overload accepting initializer list for children
   *
   * Allows inline specification of children without explicit vector construction.
   * Example: egraph.emplace(Symbol::Plus, {a, b})
   */
  auto emplace(Symbol symbol, std::initializer_list<EClassId> children) -> EClassId {
    return emplace(std::move(symbol), utils::small_vector(children));
  }

  /**
   * @brief Find canonical representative of an e-class
   *
   * Returns the canonical e-class ID for the given ID, following union-find
   * path compression for optimal performance. This is the core lookup operation
   * used throughout e-graph algorithms.
   */
  auto find(EClassId id) const -> EClassId;

  /**
   * @brief Merge two e-classes with external context
   *
   * Version of merge() that uses external ProcessingContext for temporary storage.
   * This eliminates the need for mutable object pools and gives users control
   * over memory allocation.
   */
  auto merge(EClassId a, EClassId b) -> EClassId;

  /**
   * @brief Get e-class by canonical ID (const access)
   *
   * Returns a const reference to the e-class with the given canonical ID.
   * The ID should be canonical (from find()) for correct results.
   */
  auto eclass(EClassId id) const -> const EClass<Analysis> &;

  /**
   * @brief Get e-class by canonical ID (mutable access)
   *
   * Returns a mutable reference to the e-class with the given canonical ID.
   * Use for modifications like adding nodes or updating analysis data.
   */
  auto eclass(EClassId id) -> EClass<Analysis> & { return *classes_.find(id)->second; }

  /**
   * @brief Check if an e-class exists
   *
   * Tests whether the given e-class ID corresponds to an existing e-class.
   * The ID should be canonical for accurate results.
   */
  auto has_class(EClassId id) const -> bool;

  /**
   * @brief Get the number of distinct e-classes
   *
   * Returns the count of unique equivalence classes currently in the e-graph.
   * This represents the number of distinct semantic values stored.
   */
  auto num_classes() const -> size_t;

  /**
   * @brief Get the total number of e-nodes across all e-classes
   *
   * Counts all e-node instances stored in the e-graph, providing a measure
   * of the total syntactic diversity represented.
   */
  auto num_nodes() const -> size_t;

  /**
   * @brief Check if the e-graph contains no e-classes
   */
  auto empty() const -> bool { return classes_.empty(); }

  /**
   * @brief Remove all e-classes and reset to empty state
   *
   * Clears all e-classes, union-find structure, and hash consing table.
   * Resets ID generation to start from 0.
   */
  void clear();

  /**
   * @brief Get range of all canonical e-class IDs for efficient iteration
   *
   * Provides direct access to canonical e-class IDs without scanning
   * the entire union-find structure. This eliminates the O(n) overhead
   * of checking has_class() for every possible ID.
   */
  auto canonical_class_ids() const { return classes_ | ranges::views::keys; }

  /**
   * @brief Get range of all canonical e-classes for efficient processing
   *
   * Provides direct access to e-class objects without individual lookups.
   * Combines e-class ID iteration with direct object access for maximum
   * efficiency in algorithms that need both.
   */
  auto canonical_classes() const {
    return ranges::views::transform(
        classes_, [](const auto &pair) { return std::make_pair(pair.first, std::cref(*pair.second)); });
  }

  /**
   * @brief Reserve capacity for expected number of e-classes
   *
   * Pre-allocates memory to avoid reallocations during e-graph construction.
   * Useful when the approximate final size is known.
   */
  void reserve(size_t num_classes) {
    classes_.reserve(num_classes);
    hashcons_.reserve(num_classes);
  }

  /**
   * @brief Get direct access to union-find structure
   *
   * Provides const access to the underlying union-find structure for
   * testing, debugging, and advanced use cases that need direct access.
   */
  auto union_find() const -> const UnionFind & { return union_find_; }
  auto union_find() -> UnionFind & { return union_find_; }

  // ========================================================================================
  // ENodeId API - Lightweight ENode Storage and Management
  // ========================================================================================

  /**
   * @brief Get e-node by ID with const reference access
   *
   * Provides efficient access to stored e-nodes via their ENodeId.
   * Returns a const reference to avoid unnecessary copying.
   */
  auto get_enode(ENodeId id) -> ENode<Symbol> &;

  /**
   * @brief Get total number of stored e-nodes
   */
  auto num_enodes() const -> size_t { return enode_storage_.size(); }

  // ========================================================================
  // Rebuilding Algorithm (egg paper optimization)
  // ========================================================================

  /**
   * @brief Check if rebuilding is needed
   *
   * @return true if there are pending congruences to process
   */
  [[nodiscard]] auto needs_rebuild() const -> bool { return !rebuild_worklist_.empty(); }

  /**
   * @brief Get the number of e-classes awaiting rebuilding
   *
   * Returns the size of the rebuild worklist, indicating how much deferred
   * work is pending. Useful for performance analysis and debugging.
   *
   * @return Number of e-classes in the rebuild worklist
   *
   * @par Performance Insight:
   * Larger worklist sizes indicate more batched work, which generally
   * leads to better amortized performance during rebuilding.
   *
   * @complexity O(1)
   * @threadsafety Thread-safe for reads
   */
  [[nodiscard]] auto worklist_size() const -> size_t { return rebuild_worklist_.size(); }

  /**
   * @brief Restore all e-graph invariants using rebuilding algorithm
   *
   * Processes the worklist of e-classes that need congruence checking
   * in O(N log N) time. This is the core innovation from the egg paper.
   *
   * @par Algorithm:
   * 1. Make a batch copy of the worklist
   * 2. Process the batch in chunks using ranges (chunk size: REBUILD_BATCH_SIZE)
   * 3. For each chunk:
   *    - Deduplicate by canonicalizing e-class IDs
   *    - Repair hash consing for each e-class
   *    - Check for and merge congruent parents
   * 4. Repeat until fixpoint
   *
   * @par Chunked Batching Optimization:
   * The chunked batching strategy processes items in configurable groups,
   * providing better memory locality and allowing for more efficient deduplication.
   * Each chunk is deduplicated independently, reducing memory pressure and
   * improving cache performance during processing.
   *
   * @complexity O(N log N) for full congruence closure
   */
  void rebuild(ProcessingContext<Symbol> &ctx);

 protected:
  // Allow EGraphProxy access to protected members for union-find indirection
  template <typename S, typename A>
  friend class EGraphProxy;

  /**
   * @brief Union-find structure for managing e-class equivalences
   *
   * Core data structure that tracks which e-classes have been merged together.
   * Provides the canonical ID lookup that forms the foundation of all e-graph
   * operations requiring equivalence class normalization.
   *
   * @par Performance:
   * Uses path compression and union-by-rank for optimal amortized performance.
   * Critical for scaling to large e-graphs with many equivalence relationships.
   */
  mutable UnionFind union_find_;

  /**
   * @brief Storage for all e-class objects
   *
   * Maps canonical e-class IDs to their corresponding e-class objects.
   * Uses boost::unordered_flat_map for cache-friendly iteration and lookup performance.
   * E-classes are stored via unique_ptr to enable polymorphism and stable addresses.
   *
   * @par Memory Management:
   * unique_ptr provides automatic cleanup and enables safe e-class references
   * that remain valid across hash map reallocations.
   */
  // boost::unordered_flat_map<EClassId, std::unique_ptr<EClass<Analysis>>> classes_;
  std::unordered_map<EClassId, std::unique_ptr<EClass<Analysis>>> classes_;

  /**
   * @brief Hash consing table for e-node deduplication
   *
   * Ensures that each unique canonical e-node appears only once in the e-graph.
   * Maps canonical e-nodes to their containing e-class IDs for O(1) lookup
   * during add operations.
   */
  boost::unordered_flat_map<ENodeRef<Symbol>, EClassId> hashcons_;

  /**
   * @brief Storage for all e-node instances within the e-graph
   *
   * Central storage that owns all ENode instances, indexed by ENodeId.
   * This provides the foundational ownership model where EGraph controls
   * all ENode lifetimes, similar to how it controls EClass lifetimes.
   */
  std::deque<ENode<Symbol>> enode_storage_;

  // ========================================================================
  // Rebuilding Algorithm Infrastructure (egg paper optimization)
  // ========================================================================

  /**
   * @brief Worklist of e-classes needing congruence processing
   *
   * Used by the rebuilding algorithm to track which e-classes need their
   * parents processed for congruences after merge operations.
   */
  mutable boost::unordered_flat_set<EClassId> rebuild_worklist_;

  /**
   * @brief Batch size for memory-efficient rebuilding
   */
  static constexpr size_t REBUILD_BATCH_SIZE = 100;

  /**
   * @brief Interval for memory pressure checks during rebuilding
   */
  static constexpr size_t MEMORY_CHECK_INTERVAL = 50;

  /**
   * @brief Process parent expressions for congruence checking
   *
   * Examines all parent expressions of the given e-class to find newly
   * congruent expressions. to enable optimized batch processing
   * in derived classes.
   */
  void process_parents(EClassId eclass_id, ProcessingContext<Symbol> &ctx);

  // ========================================================================
  // Rebuilding Algorithm Helper Methods
  // ========================================================================

  /**
   * @brief Repair hash consing for an e-class during rebuilding
   *
   * Updates hash consing entries to use canonical representatives
   * after merges have occurred.
   */
  void repair_hashcons_eclass(EClass<Analysis> const &eclass, EClassId eclass_id);

  void repair_hashcons_enode(ENode<Symbol> &enode, EClassId eclass_id) {
    // NOTE: the node maybe non-canonicalize, if so it will not be found
    auto it = hashcons_.find(ENodeRef{enode});

    // Canonicalize the enode in place
    auto const changed = enode.canonicalize_in_place(union_find_);
    if (changed) {
      if (it != hashcons_.end()) {
        hashcons_.erase(it);
      }
      hashcons_[ENodeRef{enode}] = eclass_id;
    } else {
      it->second = eclass_id;
    }
  }

  /**
   * @brief Process parents of an e-class during rebuilding
   *
   * Optimized version for rebuild that avoids recursive calls.
   */
  void process_class_parents_for_rebuild(EClass<Analysis> const &eclass, ProcessingContext<Symbol> &ctx);

  void merge_eclasses(EClass<Analysis> &destination, EClassId other_id) {
    if (auto it = classes_.find(other_id); it != classes_.end()) {
      destination.merge_with(std::move(*it->second));
      classes_.erase(it);
    }
  }

  /**
   * @brief Store a new e-node and return its ENodeId
   *
   * Creates a new ENodeId for the given e-node and stores it in the e-graph.
   * This is the primary method for creating new e-nodes with ENodeId ownership.
   */
  auto intern_enode(ENode<Symbol> enode) -> ENodeRef<Symbol>;
};

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::emplace(Symbol symbol, uint64_t disambiguator) -> EClassId {
  // Construct leaf e-node with disambiguator
  auto canonical_node = ENode{std::move(symbol), disambiguator};

  // Use direct O(1) ENode lookup in hashcons for single-level mapping
  auto it = hashcons_.find(ENodeRef{canonical_node});
  if (it != hashcons_.end()) {
    // TODO: check if we nees to do find here, hashcons_ should be correct? Maybe
    return union_find_.Find(it->second);
  }

  // No existing equivalent node
  // Create new EClass with single ENode

  // Add new id to unionfind, eclass and enode will be initially the same id
  EClassId new_eclass_id = union_find_.MakeSet();
  ENodeId new_enode_id = new_eclass_id;

  // Intern ENode
  auto enode_ref = intern_enode(std::move(canonical_node));
  // Add to hashcons
  hashcons_[enode_ref] = new_eclass_id;
  // Create EClass owning the ENode
  classes_.emplace(new_eclass_id, std::make_unique<EClass<Analysis>>(new_enode_id));

  // No children to update parent lists for (it's a leaf)
  return new_eclass_id;
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::emplace(Symbol symbol, utils::small_vector<EClassId> children) -> EClassId {
  // Canonicalize all children first
  for (auto &child_id : children) {
    child_id = union_find_.Find(child_id);
  }

  // Construct e-node directly with canonical children (no canonicalize_inplace needed)
  auto canonical_node = ENode{std::move(symbol), std::move(children)};

  // Use direct O(1) ENode lookup in hashcons for single-level mapping
  auto it = hashcons_.find(ENodeRef{canonical_node});
  if (it != hashcons_.end()) {
    // We may have merges
    return union_find_.Find(it->second);
  }

  // No existing equivalent node
  // Create new EClass with single ENode

  // Add new id to unionfind, eclass and enode will be initially the same id
  EClassId new_eclass_id = union_find_.MakeSet();
  ENodeId new_enode_id = new_eclass_id;

  // Intern ENode
  auto enode_ref = intern_enode(std::move(canonical_node));
  // Add to hashcons
  hashcons_[enode_ref] = new_eclass_id;
  // Create EClass owning the ENode
  classes_.emplace(new_eclass_id, std::make_unique<EClass<Analysis>>(new_enode_id));

  // Update parent lists for children - ESSENTIAL for congruence closure
  for (EClassId child_id : enode_ref.value().children()) {
    assert(union_find_.Find(child_id) == child_id);
    auto child_it = classes_.find(child_id);
    assert(child_it != classes_.end());
    child_it->second->add_parent(new_enode_id, new_eclass_id);
  }

  return new_eclass_id;
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::merge(EClassId a, EClassId b) -> EClassId {
  EClassId canonical_a = union_find_.Find(a);
  EClassId canonical_b = union_find_.Find(b);

  if (canonical_a == canonical_b) {
    return canonical_a;
  }

  // Union the sets
  EClassId merged_id = union_find_.UnionSets(canonical_a, canonical_b);

  // Merge the e-classes
  EClassId other_id = (merged_id == canonical_a) ? canonical_b : canonical_a;

  merge_eclasses(*classes_[merged_id], other_id);

  // Handle congruence processing based on mode
  // Deferred mode: add both classes to worklist for later batch processing
  // This is crucial - we need to process parents of both classes
  rebuild_worklist_.insert(merged_id);
  // Still need to update hashcons for correctness

  return merged_id;
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::find(EClassId id) const -> EClassId {
  return union_find_.Find(id);
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::eclass(EClassId id) const -> const EClass<Analysis> & {
  // Use ConstAccess proxy for path compression optimization
  EClassId canonical_id = union_find_.Find(id);

  auto it = classes_.find(canonical_id);
  if (it == classes_.end()) {
    throw std::out_of_range("E-class not found: " + std::to_string(canonical_id));
  }
  return *it->second;
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::has_class(EClassId id) const -> bool {
  // Check if the ID is valid in union-find first
  if (id >= union_find_.Size()) {
    return false;
  }

  try {
    // Use ConstAccess proxy for path compression optimization
    EClassId canonical_id = union_find_.Find(id);
    return classes_.find(canonical_id) != classes_.end();
  } catch (const std::out_of_range &) {
    return false;
  }
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::num_classes() const -> size_t {
  return classes_.size();
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::num_nodes() const -> size_t {
  // ENodes are never deleted
  return union_find_.Size();
}

template <typename Symbol, typename Analysis>
void EGraph<Symbol, Analysis>::clear() {
  union_find_.Clear();
  classes_.clear();
  hashcons_.clear();
  enode_storage_.clear();     // Clear ENode storage
  rebuild_worklist_.clear();  // Clear rebuild worklist
}

// New context-aware method implementations

template <typename Symbol, typename Analysis>
void EGraph<Symbol, Analysis>::process_parents(EClassId eclass_id, ProcessingContext<Symbol> &ctx) {
  // Delegate to recursive version starting at depth 0
  process_parents_recursive(eclass_id, ctx, 0);
}

// ========================================================================
// Rebuilding Algorithm Implementation
// ========================================================================

template <typename Symbol, typename Analysis>
void EGraph<Symbol, Analysis>::rebuild(ProcessingContext<Symbol> &ctx) {
  if (rebuild_worklist_.empty()) return;

  auto &canonicalized_chunk = ctx.canonicalized_chunk;
  canonicalized_chunk.reserve(REBUILD_BATCH_SIZE);

  while (!rebuild_worklist_.empty()) {
    auto batch = std::exchange(rebuild_worklist_, {});
    auto chunked_batches = batch | ranges::views::chunk(REBUILD_BATCH_SIZE);

    for (auto chunk : chunked_batches) {
      canonicalized_chunk.clear();

      // Deduplicate eclasses in this chunk
      for (EClassId eclass_id : chunk) {
        EClassId canonical_id = union_find_.Find(eclass_id);
        canonicalized_chunk.insert(canonical_id);
      }

      for (EClassId eclass_id : canonicalized_chunk) {
        auto it = classes_.find(eclass_id);
        if (it == classes_.end()) {
          // This is possible if during process_class_parents_for_rebuild we have merged
          // eclass_id with another eclass. In that case the merged eclass will exist
          // in the rebuild_worklist_ for the next iteration of todos
          continue;
        }
        const auto &eclass = *it->second;
        repair_hashcons_eclass(eclass, eclass_id);
        process_class_parents_for_rebuild(eclass, ctx);
      }
    }
  }
}

template <typename Symbol, typename Analysis>
void EGraph<Symbol, Analysis>::repair_hashcons_eclass(EClass<Analysis> const &eclass, EClassId eclass_id) {
  // Update hash consing for all nodes in this e-class
  for (const auto &enode_id : eclass.nodes()) {
    repair_hashcons_enode(get_enode(enode_id), eclass_id);
  }
}

template <typename Symbol, typename Analysis>
void EGraph<Symbol, Analysis>::process_class_parents_for_rebuild(EClass<Analysis> const &eclass,
                                                                 ProcessingContext<Symbol> &ctx) {
  // OPTIMIZATION: Reuse ProcessingContext map instead of creating new one
  // TODO: what if the vector was a set?
  auto &canonical_to_parents = ctx.enode_to_parents;
  canonical_to_parents.clear();

  // Group by canonical child being used by a parent + intern new connonical parent
  for (const auto &[parent_enode_id, parent_class_id] : eclass.parents()) {
    // parent_class_id is never updated, hence could be stale, use union find to get correct class
    EClassId canonical_parent_class = union_find_.Find(parent_class_id);
    // Group by canonical enode
    auto &canonical_to_parent = canonical_to_parents[get_enode(parent_enode_id).canonicalize(union_find_)];
    canonical_to_parent.eclass_ids.push_back(canonical_parent_class);
    canonical_to_parent.enode_ids.push_back(parent_class_id);
  }

  // Merge congruent parents using bulk operations for optimal performance
  for (auto &[canonical_enode, parents_collection] : canonical_to_parents) {
    auto &[eclass_ids, enode_ids] = parents_collection;
    // deduplicate eclass_ids
    std::sort(eclass_ids.begin(), eclass_ids.end());
    eclass_ids.erase(std::unique(eclass_ids.begin(), eclass_ids.end()), eclass_ids.end());
    if (eclass_ids.size() > 1) {
      // TODO: can we cheaply detect that UnionSet did anything?
      //       if it did nothing we can skip the reset of parent rebuilding
      EClassId merged_root = union_find_.UnionSets(eclass_ids, ctx.union_find_context);
      rebuild_worklist_.insert(merged_root);

      auto merged_it = classes_.find(merged_root);
      if (merged_it == classes_.end()) [[unlikely]] {
        throw std::runtime_error("Failed to find merged e-class");
      }
      EClass<Analysis> &merged_eclass = *merged_it->second;

      // Merge e-class contents for all merged classes
      for (EClassId parent_id : eclass_ids) {
        if (parent_id != merged_root) {
          merge_eclasses(merged_eclass, parent_id);
        }
      }
      // hashcons update can be defered to the next rebuild iteration because we inserted into the rebuild worklist
    } else if (eclass_ids.size() == 1) {
      // NOTE: we can NOT add to rebuild_worklist_, we must avoid infinate processing bugs (where parent is yourself)
      for (auto enode_id : enode_ids) {
        repair_hashcons_enode(get_enode(enode_id), eclass_ids[0]);
      }
    }
  }
}

// ========================================================================================
// ENodeId Method Implementations
// ========================================================================================

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::get_enode(ENodeId id) -> ENode<Symbol> & {
  assert(id < enode_storage_.size());
  return enode_storage_[id];
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::intern_enode(ENode<Symbol> enode) -> ENodeRef<Symbol> {
  return ENodeRef{enode_storage_.emplace_back(std::move(enode))};
}

// EGraph copy constructor and assignment operator implementations
template <typename Symbol, typename Analysis>
EGraph<Symbol, Analysis>::EGraph(const EGraph &other)
    : union_find_(other.union_find_), rebuild_worklist_(other.rebuild_worklist_) {
  // Copy all e-nodes first
  enode_storage_.reserve(other.enode_storage_.size());
  enode_storage_ = other.enode_storage_;

  // Copy all e-classes
  classes_.reserve(other.classes_.size());
  for (const auto &[id, eclass_ptr] : other.classes_) {
    classes_[id] = std::make_unique<EClass<Analysis>>(*eclass_ptr);
  }

  // Copy hashcons table
  hashcons_ = other.hashcons_;
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::operator=(const EGraph &other) -> EGraph & {
  if (this == &other) return *this;

  // Clear current state
  clear();

  // Copy all fields
  union_find_ = other.union_find_;
  rebuild_worklist_ = other.rebuild_worklist_;

  // Copy all e-nodes first
  enode_storage_.reserve(other.enode_storage_.size());
  enode_storage_ = other.enode_storage_;

  // Copy all e-classes
  classes_.reserve(other.classes_.size());
  for (const auto &[id, eclass_ptr] : other.classes_) {
    classes_[id] = std::make_unique<EClass<Analysis>>(*eclass_ptr);
  }

  // Copy hashcons table
  hashcons_ = other.hashcons_;

  return *this;
}

}  // namespace memgraph::planner::core
