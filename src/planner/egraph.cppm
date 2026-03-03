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

#include <cassert>

#include "utils/small_vector.hpp"

// When fuzzing we always want assert regardless of Debug vs Release
#ifdef ASSERT_FUZZ

#undef assert

#define assert(expr)                                                                                   \
  ((expr) ? (void)0                                                                                    \
          : throw std::runtime_error(std::string("Assertion failed: ") + #expr + " at " __FILE__ ":" + \
                                     std::to_string(__LINE__)))

#endif

#include <deque>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <unordered_map>
#include <vector>

#include <boost/container/flat_set.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>
#include <range/v3/all.hpp>
#include <strong_type/strong_type.hpp>

export module memgraph.planner.core.egraph;

// Export partitions
export import :eids;
export import :concepts;
export import :enode;
export import :eclass;

// Internal dependencies (not exported to consumers)
import memgraph.planner.core.constants;
import memgraph.planner.core.union_find;

export namespace memgraph::planner::core {

inline auto canonical_eclass(UnionFind &uf, EClassId id) -> EClassId { return EClassId{uf.Find(id.value_of())}; }

inline auto canonical_eclass(UnionFind &uf, ENodeId id) -> EClassId { return EClassId{uf.Find(id.value_of())}; }

/// Set of e-class IDs with O(1) add, remove, and contains operations.
/// Uses swap-with-last removal and dense index tracking.
class EClassSet {
 public:
  static constexpr std::size_t kNotPresent = std::numeric_limits<std::size_t>::max();

  void clear() {
    buffer_.clear();
    id_to_index_.clear();
  }

  void add(EClassId id) {
    assert(static_cast<std::size_t>(id.value_of()) == id_to_index_.size());
    id_to_index_.emplace_back(buffer_.size());
    buffer_.push_back(id);
  }

  void remove(EClassId id) {
    auto idx = static_cast<std::size_t>(id.value_of());
    std::size_t buf_idx = id_to_index_[idx];

    if (buf_idx != buffer_.size() - 1) {
      EClassId moved = buffer_.back();
      buffer_[buf_idx] = moved;
      id_to_index_[static_cast<std::size_t>(moved.value_of())] = buf_idx;
    }

    buffer_.pop_back();
    id_to_index_[idx] = kNotPresent;
  }

  [[nodiscard]] auto contains(EClassId id) const -> bool {
    auto idx = static_cast<std::size_t>(id.value_of());
    return idx < id_to_index_.size() && id_to_index_[idx] != kNotPresent;
  }

  [[nodiscard]] auto data() const -> std::span<EClassId const> { return buffer_; }

  [[nodiscard]] auto size() const -> std::size_t { return buffer_.size(); }

  [[nodiscard]] auto empty() const -> bool { return buffer_.empty(); }

 private:
  std::vector<EClassId> buffer_;
  std::vector<std::size_t> id_to_index_;  // indexed by EClassId value, kNotPresent = not in buffer
};

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
 * Contains reusable buffers and state that don't depend on Symbol type.
 */
struct BaseProcessingContext {
  ENodeContext enode_context;
  UnionFindContext union_find_context;
  std::vector<strong::underlying_type_t<EClassId>> canonical_eclass_ids;
  boost::container::flat_set<EClassId> canonicalized_chunk;
};

/**
 * Contains reusable buffers and state
 */
template <typename Symbol>
struct ProcessingContext : BaseProcessingContext {
  ProcessingContext() { canonicalized_chunk.reserve(REBUILD_BATCH_SIZE); }

  auto rebuild_canonicalized_chunk_container() -> boost::container::flat_set<EClassId> & {
    canonicalized_chunk.clear();
    return canonicalized_chunk;
  }

  auto rebuild_enode_to_parents_container() -> std::unordered_map<ENode<Symbol>, std::vector<ENodeId>> & {
    enode_to_parents.clear();
    return enode_to_parents;
  }

 private:
  std::unordered_map<ENode<Symbol>, std::vector<ENodeId>> enode_to_parents;
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
    assert(classes_.contains(id) && "id needs to be canonical");
    return *classes_.find(id)->second;
  }

  /**
   * @brief Get e-class by canonical ID
   */
  auto eclass(EClassId id) const -> EClass<Analysis> const & {
    assert(classes_.contains(id) && "id needs to be canonical");
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
   * @brief Get span of all canonical e-class IDs
   *
   * Returns a contiguous span suitable for efficient iteration.
   * Maintained incrementally during emplace() and merge operations.
   */
  auto canonical_eclass_ids() const -> std::span<EClassId const> { return canonical_eclasses_.data(); }

  /**
   * @brief Get range of all canonical e-classes
   *
   * Uses std::views instead of range-v3 so ADL works with C++20 modules.
   */
  auto canonical_classes() const {
    return std::views::transform(classes_,
                                 [](const auto &pair) { return std::make_pair(pair.first, std::cref(*pair.second)); });
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
   * @note Accessing a dead (duplicate-removed) e-node is memory-safe but logically
   * incorrect - the e-node's data still exists in storage but is no longer part of
   * any e-class. Always re-obtain ENodeIds from eclass.nodes() after rebuild().
   *
   * Safe usage pattern:
   * @code
   *   for (auto enode_id : eclass.nodes()) {
   *     auto const& enode = egraph.get_enode(enode_id);  // Always valid
   *   }
   * @endcode
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
   * @note Invalidation: This operation may remove duplicate e-nodes (e-nodes with
   * identical canonical form). ENodeIds obtained before rebuild may refer to dead
   * storage after this call. Re-obtain ENodeIds from eclass.nodes() after rebuilding.
   */
  void rebuild(ProcessingContext<Symbol> &ctx);

  /**
   * @brief checks if a rebuild is required
   */
  auto needs_rebuild() const -> bool { return !rebuild_worklist_.empty(); }

  /**
   * @brief Checks that congruence has been maintained post rebuild. Only used in test code.
   */
  bool ValidateCongruenceClosure();

 protected:
  /**
   * @brief Repair hashcons entries for all e-nodes in an e-class
   * @return The canonical e-class ID after any merges (may differ from eclass_id if
   *         duplicates were found that required merging with other e-classes)
   */
  auto repair_hashcons_eclass(EClass<Analysis> const &eclass, EClassId eclass_id, BaseProcessingContext &ctx)
      -> EClassId;

  /**
   * @brief Repair hashcons entry for a single e-node
   * @return A canonical e-class ID to merge with if a duplicate was found in a
   *         different e-class; nullopt if no merge is needed. The returned ID
   *         is already canonical (no need to call find() on it).
   */
  auto repair_hashcons_enode(ENodeId enode_id, EClassId eclass_id) -> std::optional<EClassId>;

  void process_parents(EClass<Analysis> const &eclass, ProcessingContext<Symbol> &ctx);

  /// Bulk merge multiple e-classes. Takes sorted, deduplicated canonical IDs.
  /// Returns the merged canonical e-class ID.
  auto merge_all(std::span<const uint32_t> canonical_ids, UnionFindContext &uf_ctx) -> EClassId;

  void merge_eclasses(EClass<Analysis> &destination, EClassId other_id);

  void remove_duplicate_enode(ENode<Symbol> const &enode, ENodeId enode_id, EClassId eclass_id);

  auto intern_enode(ENode<Symbol> enode) -> ENodeRef<Symbol>;

  boost::unordered_flat_map<EClassId, std::unique_ptr<EClass<Analysis>>> classes_;
  boost::unordered_flat_map<ENodeRef<Symbol>, ENodeInfo> hashcons_;
  std::deque<ENode<Symbol>> enode_storage_;
  EClassSet canonical_eclasses_;
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
  canonical_eclasses_.add(new_eclass_id);

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
    child_it->second->add_parent(new_enode_id);
  }

  canonical_eclasses_.add(new_eclass_id);

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

  // defer hashcons and congruence processing
  rebuild_worklist_.insert(merged_id);

  merge_eclasses(*classes_[merged_id], other_id);

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
  canonical_eclasses_.clear();
  num_dead_nodes_ = 0;
}

// ========================================================================
// Rebuilding Algorithm Implementation
// ========================================================================

template <typename Symbol, typename Analysis>
void EGraph<Symbol, Analysis>::rebuild(ProcessingContext<Symbol> &ctx) {
  if (rebuild_worklist_.empty()) [[unlikely]]
    return;

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
      auto canonical_after_repair = repair_hashcons_eclass(eclass, eclass_id, ctx);

      // Re-lookup the class since it might have been merged during repair
      if (canonical_after_repair != eclass_id) {
        it = classes_.find(canonical_after_repair);
        assert(it != classes_.end());
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

      if (inserted && canonicalized_chunk.size() == REBUILD_BATCH_SIZE) [[unlikely]] {
        chunk_processor();
        canonicalized_chunk.clear();
      }
    }
    // ensure we process remaining incomplete chunk
    chunk_processor();
  }
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::repair_hashcons_eclass(EClass<Analysis> const &eclass, EClassId eclass_id,
                                                      BaseProcessingContext &ctx) -> EClassId {
  // Collect canonical IDs to merge. We must iterate fully before merging to
  // avoid iterator invalidation - merge() can modify/move the eclass.
  auto &canonical_ids = ctx.canonical_eclass_ids;
  canonical_ids.clear();

  for (const auto &enode_id : eclass.nodes()) {
    if (auto merge_target = repair_hashcons_enode(enode_id, eclass_id)) {
      // repair_hashcons_enode returns canonical e-class IDs
      canonical_ids.push_back(merge_target->value_of());
    }
  }

  if (canonical_ids.empty()) {
    return eclass_id;
  }

  // Add eclass_id, deduplicate, and merge
  canonical_ids.push_back(eclass_id.value_of());
  std::ranges::sort(canonical_ids);
  auto [first, last] = std::ranges::unique(canonical_ids);
  canonical_ids.erase(first, last);

  return merge_all(canonical_ids, ctx.union_find_context);
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::repair_hashcons_enode(ENodeId enode_id, EClassId eclass_id) -> std::optional<EClassId> {
  auto &enode = get_enode(enode_id);
  auto old_it = hashcons_.find(ENodeRef{enode});
  assert(old_it != hashcons_.end() && "canonical e-node must exist in hashcons");
  assert(canonical_eclass(union_find_, eclass_id) == eclass_id);

  // Canonicalize the enode in place
  auto const changed = enode.canonicalize_in_place(union_find_);
  if (changed) {
    // remove stale hashcons entry
    hashcons_.erase(old_it);

    // Check if canonical form already exists (duplicate detection)
    auto existing_it = hashcons_.find(ENodeRef{enode});
    if (existing_it != hashcons_.end()) {
      // Duplicate! This enode canonicalizes to the same form as an existing one.
      // The duplicate's e-class should be merged with the existing e-node's e-class.
      auto existing_eclass = canonical_eclass(union_find_, existing_it->second.current_eclassid);

      // Remove this enode, the existing hashcons entry is prefered.
      remove_duplicate_enode(enode, enode_id, eclass_id);
      // Return the e-class that needs to be merged
      return (existing_eclass != eclass_id) ? std::optional{existing_eclass} : std::nullopt;
    }

    hashcons_[ENodeRef{enode}] = ENodeInfo{eclass_id, enode_id};
  } else {
    // ensure updated eclass
    old_it->second.current_eclassid = eclass_id;
  }

  return std::nullopt;
}

template <typename Symbol, typename Analysis>
void EGraph<Symbol, Analysis>::process_parents(EClass<Analysis> const &eclass, ProcessingContext<Symbol> &ctx) {
  auto &canonical_to_parents = ctx.rebuild_enode_to_parents_container();

  // Step 1: Group by canonical
  for (const auto &parent_enode_id : eclass.parents()) {
    canonical_to_parents[get_enode(parent_enode_id).canonicalize(union_find_)].push_back(parent_enode_id);
  }

  // Merge congruent parents using bulk operations for optimal performance
  auto &canonical_eclass_ids = ctx.canonical_eclass_ids;
  for (auto &[canonical_enode, enode_ids] : canonical_to_parents) {
    // parents are non-canonical, use union find to get correct canonical class
    // this must be done per group, previous grouping congruence merging can change what are the canonical classes
    canonical_eclass_ids.clear();
    canonical_eclass_ids.reserve(enode_ids.size());
    for (auto enode_id : enode_ids) {
      canonical_eclass_ids.push_back(canonical_eclass(union_find_, enode_id).value_of());
    }
    // deduplicate
    std::ranges::sort(canonical_eclass_ids);
    auto [first, last] = std::ranges::unique(canonical_eclass_ids);
    canonical_eclass_ids.erase(first, last);
    if (canonical_eclass_ids.size() > 1) {
      merge_all(canonical_eclass_ids, ctx.union_find_context);
      // hashcons update deferred to next rebuild iteration - duplicates will be
      // detected and removed then via repair_hashcons_enode
    } else if (canonical_eclass_ids.size() == 1) {
      // All parent e-nodes in this group belong to the same e-class after canonicalization.
      // We still need to check for congruence with e-nodes in OTHER e-classes via hashcons lookup.
      // repair_hashcons_enode returns a merge target if it finds a duplicate in a different e-class.
      auto current_eclass = EClassId{canonical_eclass_ids[0]};
      for (auto enode_id : enode_ids) {
        auto merge_target = repair_hashcons_enode(enode_id, current_eclass);
        if (merge_target) {
          current_eclass = merge(current_eclass, *merge_target).eclass_id;
        }
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
  canonical_eclasses_.remove(other_id);
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::merge_all(std::span<const uint32_t> canonical_ids, UnionFindContext &uf_ctx)
    -> EClassId {
  assert(!canonical_ids.empty());

  if (canonical_ids.size() == 1) {
    return EClassId{canonical_ids[0]};
  }

  auto merged_root = EClassId{union_find_.UnionSets(canonical_ids, uf_ctx)};
  rebuild_worklist_.insert(merged_root);

  auto merged_it = classes_.find(merged_root);
  assert(merged_it != classes_.end());
  auto &merged_eclass = *merged_it->second;
  for (auto id : canonical_ids) {
    if (EClassId{id} != merged_root) {
      merge_eclasses(merged_eclass, EClassId{id});
    }
  }

  return merged_root;
}

template <typename Symbol, typename Analysis>
void EGraph<Symbol, Analysis>::remove_duplicate_enode(ENode<Symbol> const &enode, ENodeId enode_id,
                                                      EClassId eclass_id) {
  // This e-node is a duplicate (same canonical form as another e-node in hashcons).
  // Remove it from the e-class and from children's parent sets.
  // Note: enode has already been canonicalized via canonicalize_in_place(), so children are canonical.

  // Remove from each child's parent set
  for (auto child_id : enode.children()) {
    assert(child_id == canonical_eclass(union_find_, child_id) && "enode children must be canonical");
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
