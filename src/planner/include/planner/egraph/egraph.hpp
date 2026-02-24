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
#include "utils/logging.hpp"

// Uncomment to enable debug output for rebuild
// #define EGRAPH_DEBUG_REBUILD
#ifdef EGRAPH_DEBUG_REBUILD
#include <iostream>
#endif

// When fuzzing we always want assert regardless of Debug vs Release
#ifdef ASSERT_FUZZ

#undef assert

#define assert(expr)                                                                                   \
  ((expr) ? (void)0                                                                                    \
          : throw std::runtime_error(std::string("Assertion failed: ") + #expr + " at " __FILE__ ":" + \
                                     std::to_string(__LINE__)))

#endif

#include "planner/egraph/eclass.hpp"
import memgraph.planner.core.eids;
#include "planner/egraph/enode.hpp"
#include "planner/egraph/processing_context.hpp"
import memgraph.planner.core.union_find;

#include <optional>

#include <boost/unordered/unordered_flat_map.hpp>
#include <range/v3/all.hpp>

namespace memgraph::planner::core {

inline auto canonical_eclass(UnionFind &uf, EClassId id) -> EClassId { return EClassId{uf.Find(id.value_of())}; }

inline auto canonical_eclass(UnionFind &uf, ENodeId id) -> EClassId { return EClassId{uf.Find(id.value_of())}; }

namespace detail {
struct EGraphBase {
  /**
   * @brief Find canonical representative of an e-class
   */
  auto find(EClassId id) const -> EClassId { return canonical_eclass(union_find_, id); }

  auto find(ENodeId id) const -> EClassId { return canonical_eclass(union_find_, id); }

  /**
   * @brief Get the total number of e-nodes ever created
   */
  auto num_nodes() const -> size_t { return union_find_.Size(); }

  /**
   * @brief Get the number of dead (duplicate) e-nodes removed during rebuild
   */
  auto num_dead_nodes() const -> size_t { return num_dead_nodes_; }

  /**
   * @brief Get the number of live e-nodes (total - dead)
   */
  auto num_live_nodes() const -> size_t { return num_nodes() - num_dead_nodes_; }

  /**
   * @brief Check if rebuilding is needed
   */
  [[nodiscard]] auto needs_rebuild() const -> bool { return !rebuild_worklist_.empty(); }

  /**
   * @brief Get the number of e-classes awaiting rebuilding
   */
  [[nodiscard]] auto worklist_size() const -> size_t { return rebuild_worklist_.size(); }

 protected:
  mutable UnionFind union_find_;
  boost::unordered_flat_set<EClassId> rebuild_worklist_;
  size_t num_dead_nodes_ = 0;
};
}  // namespace detail

/// Result of emplace operation
struct EmplaceResult {
  EClassId eclass_id;  // current e-class ID (can be invalidated by merges)
  ENodeId enode_id;    // e-node identifier, valid until next rebuild() which may remove duplicates
  bool did_insert;     // true if a new e-node was created
};

/// Result of merge operation
struct MergeResult {
  EClassId eclass_id;  // canonical e-class ID after merge
  bool did_merge;      // true if merge was performed (false if already equivalent)
};

/// Internal type for hashcons storage
struct ENodeInfo {
  // Returns info with canonical e-class ID
  auto UpdatedInfo(UnionFind &uf) const -> ENodeInfo { return {canonical_eclass(uf, current_eclassid), enode_id}; }

  EClassId current_eclassid;
  ENodeId enode_id;
};

/**
 * @brief e-graph with defered invariant maintenance
 *
 * @tparam Symbol The type used for operation symbols in expressions
 * @tparam Analysis Analysis type for domain-specific data
 */
template <typename Symbol, typename Analysis>
struct EGraph : private detail::EGraphBase {
  EGraph() : EGraph(256 /*an ok default capacity*/) {}

  explicit EGraph(size_t capacity) {
    classes_.reserve(capacity);
    hashcons_.reserve(capacity);
  }

  EGraph(EGraph &&) noexcept = default;
  auto operator=(EGraph &&) -> EGraph & = default;

  using EGraphBase::find;
  using EGraphBase::needs_rebuild;
  using EGraphBase::num_dead_nodes;
  using EGraphBase::num_live_nodes;
  using EGraphBase::num_nodes;
  using EGraphBase::worklist_size;

  /**
   * @brief Emplace an e-node directly with canonical children
   * @return EmplaceResult - (eclass_id, enode_id, did_insert)
   */
  auto emplace(Symbol symbol, utils::small_vector<EClassId> children, uint64_t disambiguator = 0) -> EmplaceResult;

  /**
   * @brief Emplace leaf nodes with optional disambiguator
   * @return EmplaceResult - (eclass_id, enode_id, did_insert)
   */
  auto emplace(Symbol symbol, uint64_t disambiguator = 0) -> EmplaceResult;

  /**
   * @brief Convenience overload accepting initializer list for children
   *
   * Allows inline specification of children without explicit vector construction.
   * Example: egraph.emplace(Symbol::Plus, {a, b})
   * @return EmplaceResult - (eclass_id, enode_id, did_insert)
   */
  auto emplace(Symbol symbol, std::initializer_list<EClassId> children, uint64_t disambiguator = 0) -> EmplaceResult {
    return emplace(std::move(symbol), utils::small_vector(children), disambiguator);
  }

  /**
   * @brief Merge two e-classes
   * @return MergeResult with canonical e-class ID and whether merge was performed
   *         If both IDs were already in the same e-class, returns {canonical, false}
   */
  auto merge(EClassId a, EClassId b) -> MergeResult;

  /**
   * @brief Get e-class by canonical ID
   */
  auto eclass(EClassId id) -> EClass<Analysis> & {
    DMG_ASSERT(classes_.contains(id), "id needs to be canonical");
    return *classes_.find(id)->second;
  }

  /**
   * @brief Get e-class by canonical ID
   */
  auto eclass(EClassId id) const -> EClass<Analysis> const & {
    DMG_ASSERT(classes_.contains(id), "id needs to be canonical");
    return *classes_.find(id)->second;
  }

  /**
   * @brief Check if an e-class exists and is canonical
   */
  auto has_class(EClassId id) const -> bool;

  /**
   * @brief Get the number of distinct e-classes
   */
  auto num_classes() const -> size_t { return classes_.size(); }

  /**
   * @brief Check if the e-graph contains no e-classes
   */
  auto empty() const -> bool { return classes_.empty(); }

  /**
   * @brief Remove all e-classes and reset to empty state
   */
  void clear();

  /**
   * @brief Get range of all canonical e-class IDs
   */
  auto canonical_class_ids() const { return classes_ | ranges::views::keys; }

  /**
   * @brief Get range of all canonical e-classes
   */
  auto canonical_classes() const {
    return ranges::views::transform(
        classes_, [](const auto &pair) { return std::make_pair(pair.first, std::cref(*pair.second)); });
  }

  /**
   * @brief Reserve capacity for expected number of e-classes
   */
  void reserve(size_t num_classes) {
    classes_.reserve(num_classes);
    hashcons_.reserve(num_classes);
  }

  /**
   * @brief Get e-node by ID with reference access
   *
   * @warning Only use with IDs obtained from eclass.nodes() iteration or from
   * results returned before any subsequent rebuild(). The rebuild() operation
   * may remove duplicate e-nodes, making previously valid ENodeIds refer to
   * dead storage. Safe usage pattern:
   *
   *   for (auto enode_id : eclass.nodes()) {
   *     auto const& enode = egraph.get_enode(enode_id);  // Always safe
   *   }
   */
  auto get_enode(ENodeId id) -> ENode<Symbol> & {
    assert(id.value_of() < enode_storage_.size());
    return enode_storage_[id.value_of()];
  }

  /// @copydoc get_enode(ENodeId)
  auto get_enode(ENodeId id) const -> ENode<Symbol> const & {
    assert(id.value_of() < enode_storage_.size());
    return enode_storage_[id.value_of()];
  }

  /**
   * @brief Restore all e-graph invariants using rebuilding algorithm
   *
   * This operation may remove duplicate e-nodes (e-nodes with identical canonical
   * form within the same e-class). Any ENodeId obtained before rebuild may become
   * invalid after this call. Always re-obtain ENodeIds from eclass.nodes() after
   * rebuilding.
   *
   * @return Set of all canonical e-class IDs that were processed (affected by merges).
   *         This can be used for incremental EMatcher::rebuild().
   */
  auto rebuild(ProcessingContext<Symbol> &ctx) -> boost::unordered_flat_set<EClassId>;

  /**
   * @brief checks if a rebuild is required
   */
  auto needs_rebuild() const -> bool { return !rebuild_worklist_.empty(); }

  /**
   * @brief Checks that congruence has been maintained post rebuild. Only used in test code.
   */
  bool ValidateCongruenceClosure();

 protected:
  auto repair_hashcons_eclass(EClass<Analysis> const &eclass, EClassId eclass_id) -> std::optional<EClassId>;

  auto repair_hashcons_enode(ENodeId enode_id, EClassId eclass_id) -> std::optional<EClassId>;

  void process_parents(EClass<Analysis> const &eclass, ProcessingContext<Symbol> &ctx);

  void merge_eclasses(EClass<Analysis> &destination, EClassId other_id);

  void remove_duplicate_enode(ENode<Symbol> const &enode, ENodeId enode_id, EClassId eclass_id);

  auto intern_enode(ENode<Symbol> enode) -> ENodeRef<Symbol>;

  boost::unordered_flat_map<EClassId, std::unique_ptr<EClass<Analysis>>> classes_;
  boost::unordered_flat_map<ENodeRef<Symbol>, ENodeInfo> hashcons_;
  std::deque<ENode<Symbol>> enode_storage_;
};

// ========================================================================
// Egraph methods
// ========================================================================

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::emplace(Symbol symbol, uint64_t disambiguator) -> EmplaceResult {
  // construct leaf e-node with disambiguator
  auto canonical_node = ENode{std::move(symbol), {}, disambiguator};

  auto it = hashcons_.find(ENodeRef{canonical_node});
  if (it != hashcons_.end()) {
    auto updated = it->second.UpdatedInfo(union_find_);
    return {updated.current_eclassid, updated.enode_id, false};
  }

  auto new_id = union_find_.MakeSet();
  auto new_eclass_id = EClassId{new_id};
  auto new_enode_id = ENodeId{new_id};

  auto enode_ref = intern_enode(std::move(canonical_node));
  hashcons_[enode_ref] = ENodeInfo{new_eclass_id, new_enode_id};
  classes_.emplace(new_eclass_id, std::make_unique<EClass<Analysis>>(new_enode_id));

  return {new_eclass_id, new_enode_id, true};
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::emplace(Symbol symbol, utils::small_vector<EClassId> children, uint64_t disambiguator)
    -> EmplaceResult {
  for (auto &child_id : children) {
    child_id = canonical_eclass(union_find_, child_id);
  }
  auto canonical_node = ENode{std::move(symbol), std::move(children), disambiguator};

  auto it = hashcons_.find(ENodeRef{canonical_node});
  if (it != hashcons_.end()) {
    auto updated = it->second.UpdatedInfo(union_find_);
    return {updated.current_eclassid, updated.enode_id, false};
  }

#ifdef EGRAPH_DEBUG_REBUILD
  std::cerr << "[emplace] Creating new node -> ";
  for (auto c : canonical_node.children()) {
    std::cerr << c << " ";
  }
  std::cerr << "\n";
#endif

  auto new_id = union_find_.MakeSet();
  auto new_eclass_id = EClassId{new_id};
  auto new_enode_id = ENodeId{new_id};

  auto enode_ref = intern_enode(std::move(canonical_node));
  hashcons_[enode_ref] = ENodeInfo{new_eclass_id, new_enode_id};
  classes_.emplace(new_eclass_id, std::make_unique<EClass<Analysis>>(new_enode_id));

  // Update parent lists for children - ESSENTIAL for congruence closure
  for (EClassId child_id : enode_ref.value().children()) {
    assert(canonical_eclass(union_find_, child_id) == child_id);
    auto child_it = classes_.find(child_id);
    assert(child_it != classes_.end());
#ifdef EGRAPH_DEBUG_REBUILD
    std::cerr << "[emplace]   Adding " << new_enode_id << " as parent of class " << child_id << " (had "
              << child_it->second->parents().size() << " parents)\n";
#endif
    child_it->second->add_parent(new_enode_id);
#ifdef EGRAPH_DEBUG_REBUILD
    std::cerr << "[emplace]   Class " << child_id << " now has " << child_it->second->parents().size() << " parents\n";
#endif
  }

  return {new_eclass_id, new_enode_id, true};
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::intern_enode(ENode<Symbol> enode) -> ENodeRef<Symbol> {
  return ENodeRef{enode_storage_.emplace_back(std::move(enode))};
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::merge(EClassId a, EClassId b) -> MergeResult {
  auto canonical_a = union_find_.Find(a.value_of());
  auto canonical_b = union_find_.Find(b.value_of());

  if (canonical_a == canonical_b) {
    return {EClassId{canonical_a}, false};
  }

  auto merged_result = union_find_.UnionSets(canonical_a, canonical_b);
  auto merged_id = EClassId{merged_result};
  auto other_id = EClassId{(merged_result == canonical_a) ? canonical_b : canonical_a};

#ifdef EGRAPH_DEBUG_REBUILD
  std::cerr << "[merge] Merging " << a << " and " << b << " -> merged_id=" << merged_id << ", other_id=" << other_id
            << "\n";
  std::cerr << "[merge]   Before merge, class " << merged_id << " has " << classes_[merged_id]->parents().size()
            << " parents\n";
  for (auto p : classes_[merged_id]->parents()) {
    std::cerr << "[merge]     parent: " << p << "\n";
  }
  std::cerr << "[merge]   Before merge, class " << other_id << " has " << classes_[other_id]->parents().size()
            << " parents\n";
  for (auto p : classes_[other_id]->parents()) {
    std::cerr << "[merge]     parent: " << p << "\n";
  }
#endif

  // defer hashcons and congruence processing
  rebuild_worklist_.insert(merged_id);

  merge_eclasses(*classes_[merged_id], other_id);

#ifdef EGRAPH_DEBUG_REBUILD
  std::cerr << "[merge]   After merge, class " << merged_id << " has " << classes_[merged_id]->parents().size()
            << " parents\n";
  for (auto p : classes_[merged_id]->parents()) {
    std::cerr << "[merge]     parent: " << p << "\n";
  }
#endif

  return {merged_id, true};
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::has_class(EClassId id) const -> bool {
  // Check if the ID is valid in union-find first
  // NOTE: This code AFAICT should not be used in a production usecase, only exists for tests and fuzzing
  //       The bounds checking inefficency is acceptable there.
  if (id.value_of() >= union_find_.Size()) {
    return false;
  }

  return classes_.contains(id);
}

template <typename Symbol, typename Analysis>
void EGraph<Symbol, Analysis>::clear() {
  union_find_.Clear();
  rebuild_worklist_.clear();
  classes_.clear();
  hashcons_.clear();
  enode_storage_.clear();
  num_dead_nodes_ = 0;
}

// ========================================================================
// Rebuilding Algorithm Implementation
// ========================================================================

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::rebuild(ProcessingContext<Symbol> &ctx) -> boost::unordered_flat_set<EClassId> {
  boost::unordered_flat_set<EClassId> affected_eclasses;

  if (rebuild_worklist_.empty()) [[unlikely]]
    return affected_eclasses;

#ifdef EGRAPH_DEBUG_REBUILD
  std::cerr << "[rebuild] Starting rebuild with " << rebuild_worklist_.size() << " worklist entries\n";
  for (auto id : rebuild_worklist_) {
    std::cerr << "[rebuild]   worklist: " << id << "\n";
  }
  std::cerr << "[rebuild] E-graph state before rebuild:\n";
  for (auto const &[class_id, eclass] : classes_) {
    std::cerr << "[rebuild]   class " << class_id << ": " << eclass->size() << " nodes, " << eclass->parents().size()
              << " parents\n";
    for (auto p : eclass->parents()) {
      std::cerr << "[rebuild]     parent: " << p << " (canonical: " << canonical_eclass(union_find_, p) << ")\n";
    }
  }
#endif

  auto &canonicalized_chunk = ctx.rebuild_canonicalized_chunk_container();

  auto const chunk_processor = [&] {
    for (EClassId eclass_id : canonicalized_chunk) {
      auto it = classes_.find(eclass_id);
      if (it == classes_.end()) [[unlikely]] {
        // This is possible if during process_parents we have merged
        // eclass_id with another eclass. In that case the merged eclass will exist
        // in the rebuild_worklist_ for the next iteration of todos
        continue;
      }
      const auto &eclass = *it->second;
      auto merge_target = repair_hashcons_eclass(eclass, eclass_id);

      // If a duplicate was detected during hashcons repair, merge the classes
      if (merge_target) {
        auto target_canonical = canonical_eclass(union_find_, *merge_target);
        auto current_canonical = canonical_eclass(union_find_, eclass_id);
        if (target_canonical != current_canonical) {
#ifdef EGRAPH_DEBUG_REBUILD
          std::cerr << "[chunk_processor] Merging class " << current_canonical << " with " << target_canonical
                    << " due to duplicate detection\n";
#endif
          auto merged = union_find_.UnionSets(target_canonical.value_of(), current_canonical.value_of());
          auto merged_id = EClassId{merged};
          auto other_id = (merged_id == target_canonical) ? current_canonical : target_canonical;
          rebuild_worklist_.insert(merged_id);
          merge_eclasses(*classes_[merged_id], other_id);
        }
      }

      // Re-lookup the class since it might have been merged
      it = classes_.find(canonical_eclass(union_find_, eclass_id));
      if (it == classes_.end()) {
        continue;
      }
      process_parents(*it->second, ctx);
    }
  };

  while (!rebuild_worklist_.empty()) {
    auto todo = std::exchange(rebuild_worklist_, {});
    for (EClassId eclass_id : todo) {
      // canonical + deduplication
      auto canonical = canonical_eclass(union_find_, eclass_id);
      auto [_, inserted] = canonicalized_chunk.insert(canonical);

      // Track all affected canonical e-classes across all iterations
      if (inserted) {
        affected_eclasses.insert(canonical);
      }

      if (inserted && canonicalized_chunk.size() == REBUILD_BATCH_SIZE) [[unlikely]] {
        chunk_processor();
        canonicalized_chunk.clear();
      }
    }
    // ensure we process remaining incomplete chunk
    chunk_processor();
  }

  return affected_eclasses;
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::repair_hashcons_eclass(EClass<Analysis> const &eclass, EClassId eclass_id)
    -> std::optional<EClassId> {
  std::optional<EClassId> merge_target = std::nullopt;
  for (const auto &enode_id : eclass.nodes()) {
    auto result = repair_hashcons_enode(enode_id, eclass_id);
    if (result && !merge_target) {
      merge_target = result;
    } else if (result && merge_target) {
      // Multiple merge targets - they should all be the same after canonicalization
      // due to how we're processing, but we need to merge them too
      auto target1 = canonical_eclass(union_find_, *merge_target);
      auto target2 = canonical_eclass(union_find_, *result);
      if (target1 != target2) {
        // Need to merge these targets too
        auto merged = union_find_.UnionSets(target1.value_of(), target2.value_of());
        merge_target = EClassId{merged};
        rebuild_worklist_.insert(*merge_target);
        merge_eclasses(*classes_[*merge_target], target1 == *merge_target ? target2 : target1);
      }
    }
  }
  return merge_target;
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::repair_hashcons_enode(ENodeId enode_id, EClassId eclass_id) -> std::optional<EClassId> {
  auto &enode = get_enode(enode_id);
  // NOTE: the node maybe non-canonicalize, if so it will not be found
  auto old_it = hashcons_.find(ENodeRef{enode});

  // Canonicalize the enode in place
  auto const changed = enode.canonicalize_in_place(union_find_);
  if (changed) {
    // Hash + value have changed, need to remove stale hashcons entry
    if (old_it != hashcons_.end()) {
      hashcons_.erase(old_it);
    }

    // Check if canonical form already exists (duplicate detection)
    auto existing_it = hashcons_.find(ENodeRef{enode});
    if (existing_it != hashcons_.end()) {
      // Duplicate! This enode canonicalizes to the same form as an existing one.
      // The duplicate's e-class should be merged with the existing e-node's e-class.
      auto existing_eclass = canonical_eclass(union_find_, existing_it->second.current_eclassid);
      auto duplicate_eclass = canonical_eclass(union_find_, eclass_id);

#ifdef EGRAPH_DEBUG_REBUILD
      std::cerr << "[repair_hashcons_enode] Duplicate detected: enode " << enode_id << " in class " << eclass_id
                << " matches existing enode " << existing_it->second.enode_id << " in class " << existing_eclass
                << "\n";
#endif

      if (existing_eclass != duplicate_eclass) {
        // Remove this enode - the existing hashcons entry is correct.
        remove_duplicate_enode(enode, enode_id, eclass_id);
        // Return the e-class that needs to be merged
        return existing_eclass;
      } else {
        // Already in the same class, just remove the duplicate
        remove_duplicate_enode(enode, enode_id, eclass_id);
        return std::nullopt;
      }
    }

    hashcons_[ENodeRef{enode}] = ENodeInfo{eclass_id, enode_id};
  } else {
    if (old_it != hashcons_.end()) {
      // ensure updated eclass
      old_it->second.current_eclassid = eclass_id;
    } else {
      // TODO: coverage...do we get here? Can we get here
      DMG_ASSERT(false, "not sure if this should be possible");
      hashcons_[ENodeRef{enode}] = ENodeInfo{eclass_id, enode_id};
    }
  }
  return std::nullopt;
}

template <typename Symbol, typename Analysis>
void EGraph<Symbol, Analysis>::process_parents(EClass<Analysis> const &eclass, ProcessingContext<Symbol> &ctx) {
  auto &canonical_to_parents = ctx.rebuild_enode_to_parents_container();

#ifdef EGRAPH_DEBUG_REBUILD
  std::cerr << "[process_parents] Processing eclass with " << eclass.parents().size() << " parents\n";
  for (auto p : eclass.parents()) {
    std::cerr << "[process_parents]   parent enode: " << p
              << " -> canonical eclass: " << canonical_eclass(union_find_, p) << "\n";
  }
#endif

  // Step 1: Group by canonical
  for (const auto &parent_enode_id : eclass.parents()) {
    canonical_to_parents[get_enode(parent_enode_id).canonicalize(union_find_)].push_back(parent_enode_id);
  }

  // Merge congruent parents using bulk operations for optimal performance
  auto &canonical_eclass_ids = ctx.canonical_eclass_ids;
#ifdef EGRAPH_DEBUG_REBUILD
  std::cerr << "[process_parents] Found " << canonical_to_parents.size() << " canonical groups\n";
#endif
  for (auto &[canonical_enode, enode_ids] : canonical_to_parents) {
    // parents are non-canonical, use union find to get correct canonical class
    // this must be done per group, previous grouping congruence merging can change what are the canonical classes
    canonical_eclass_ids.clear();
    canonical_eclass_ids.reserve(enode_ids.size());
    for (auto enode_id : enode_ids) {
      canonical_eclass_ids.push_back(canonical_eclass(union_find_, enode_id).value_of());
    }
#ifdef EGRAPH_DEBUG_REBUILD
    std::cerr << "[process_parents]   group with " << enode_ids.size() << " enodes -> classes: [";
    for (size_t i = 0; i < canonical_eclass_ids.size(); ++i) {
      if (i > 0) std::cerr << ", ";
      std::cerr << canonical_eclass_ids[i];
    }
    std::cerr << "]\n";
#endif
    // deduplicate
    std::sort(canonical_eclass_ids.begin(), canonical_eclass_ids.end());
    canonical_eclass_ids.erase(std::unique(canonical_eclass_ids.begin(), canonical_eclass_ids.end()),
                               canonical_eclass_ids.end());
    if (canonical_eclass_ids.size() > 1) {
#ifdef EGRAPH_DEBUG_REBUILD
      std::cerr << "[process_parents]   MERGING classes: [";
      for (size_t i = 0; i < canonical_eclass_ids.size(); ++i) {
        if (i > 0) std::cerr << ", ";
        std::cerr << canonical_eclass_ids[i];
      }
      std::cerr << "]\n";
#endif
      auto merged_root = EClassId{union_find_.UnionSets(canonical_eclass_ids, ctx.union_find_context)};
      rebuild_worklist_.insert(merged_root);

      auto merged_it = classes_.find(merged_root);
      assert(merged_it != classes_.end());
      EClass<Analysis> &merged_eclass = *merged_it->second;
      for (auto parent_id : canonical_eclass_ids) {
        if (EClassId{parent_id} != merged_root) {
          merge_eclasses(merged_eclass, EClassId{parent_id});
        }
      }
      // hashcons update deferred to next rebuild iteration - duplicates will be
      // detected and removed then via repair_hashcons_enode
    } else if (canonical_eclass_ids.size() == 1) {
      // NOTE: we can NOT add to rebuild_worklist_, we must avoid infinate processing bugs (where parent is yourself)
      // repair_hashcons_enode detects and removes duplicates when inserting into hashcons
      for (auto enode_id : enode_ids) {
        repair_hashcons_enode(enode_id, EClassId{canonical_eclass_ids[0]});
      }
    }
  }
}

template <typename Symbol, typename Analysis>
void EGraph<Symbol, Analysis>::merge_eclasses(EClass<Analysis> &destination, EClassId other_id) {
  auto it = classes_.find(other_id);
  assert(it != classes_.end());
  destination.merge_with(std::move(*it->second));
  classes_.erase(it);
}

template <typename Symbol, typename Analysis>
void EGraph<Symbol, Analysis>::remove_duplicate_enode(ENode<Symbol> const &enode, ENodeId enode_id,
                                                      EClassId eclass_id) {
  // This e-node is a duplicate (same canonical form as another e-node in hashcons).
  // Remove it from the e-class and from children's parent sets.
  // Note: enode has already been canonicalized via canonicalize_in_place(), so children are canonical.

  // Remove from each child's parent set
  for (auto child_id : enode.children()) {
    DMG_ASSERT(child_id == canonical_eclass(union_find_, child_id), "enode children must be canonical");
    auto child_it = classes_.find(child_id);
    if (child_it != classes_.end()) {
      child_it->second->remove_parent(enode_id);
    }
  }

  // Remove from the e-class's nodes list
  auto eclass_it = classes_.find(eclass_id);
  assert(eclass_it != classes_.end());
  eclass_it->second->remove_node(enode_id);

  ++num_dead_nodes_;
}

// ========================================================================================
// Test Validation helpers
// ========================================================================================

template <typename Symbol, typename Analysis>
bool EGraph<Symbol, Analysis>::ValidateCongruenceClosure() {
  std::unordered_map<ENode<Symbol>, EClassId> canonical_forms;

  for (auto const &[class_id, eclass] : canonical_classes()) {
    for (auto enode_id : eclass.nodes()) {
      auto canonical_enode = get_enode(enode_id).canonicalize(union_find_);
      auto [it, inserted] = canonical_forms.try_emplace(canonical_enode, class_id);
      if (!inserted && it->second != class_id) {
        return false;  // Same canonical form in different e-classes
      }
    }
  }
  return true;
}

}  // namespace memgraph::planner::core
