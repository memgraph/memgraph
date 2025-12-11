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

// When fuzzing we always want assert regardless of Debug vs Release
#ifdef ASSERT_FUZZ

#undef assert

#define assert(expr)                                                                                   \
  ((expr) ? (void)0                                                                                    \
          : throw std::runtime_error(std::string("Assertion failed: ") + #expr + " at " __FILE__ ":" + \
                                     std::to_string(__LINE__)))

#endif

#include <ranges>
#include "planner/core/eclass.hpp"
#include "planner/core/eids.hpp"
#include "planner/core/enode.hpp"
#include "planner/core/processing_context.hpp"
#include "planner/core/union_find.hpp"

#include <boost/unordered/unordered_flat_map.hpp>

namespace r = std::ranges;
namespace rv = r::views;

namespace memgraph::planner::core {
namespace detail {
struct EGraphBase {
  /**
   * @brief Find canonical representative of an e-class
   */
  auto find(EClassId id) const -> EClassId { return union_find_.Find(id); }

  /**
   * @brief Get the total number of e-nodes across all e-classes
   */
  auto num_nodes() const -> size_t { return union_find_.Size(); }

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
};
}  // namespace detail

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
  using EGraphBase::num_nodes;
  using EGraphBase::worklist_size;

  /**
   * @brief Emplace an e-node directly with canonical children
   */
  auto emplace(Symbol symbol, utils::small_vector<EClassId> children) -> EClassId;

  /**
   * @brief Emplace leaf nodes with optional disambiguator
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
   * @brief Merge two e-classes
   */
  auto merge(EClassId a, EClassId b) -> EClassId;

  /**
   * @brief Get e-class by canonical ID
   */
  auto eclass(EClassId id) -> EClass<Analysis> & { return *classes_.find(id)->second; }

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
  auto canonical_class_ids() const { return classes_ | rv::keys; }

  /**
   * @brief Get range of all canonical e-classes
   */
  auto canonical_classes() const {
    return rv::transform(classes_,
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
   */
  auto get_enode(ENodeId id) -> ENode<Symbol> & {
    assert(id < enode_storage_.size());
    return enode_storage_[id];
  }

  /**
   * @brief Restore all e-graph invariants using rebuilding algorithm
   */
  void rebuild(ProcessingContext<Symbol> &ctx);

  /**
   * @brief Checks that congruence has been maintained post rebuild. Only used in test code.
   */
  bool ValidateCongruenceClosure();

 protected:
  void repair_hashcons_eclass(EClass<Analysis> const &eclass, EClassId eclass_id);

  void repair_hashcons_enode(ENode<Symbol> &enode, EClassId eclass_id);

  void process_parents(EClass<Analysis> const &eclass, ProcessingContext<Symbol> &ctx);

  void merge_eclasses(EClass<Analysis> &destination, EClassId other_id);

  auto intern_enode(ENode<Symbol> enode) -> ENodeRef<Symbol>;

  boost::unordered_flat_map<EClassId, std::unique_ptr<EClass<Analysis>>> classes_;
  boost::unordered_flat_map<ENodeRef<Symbol>, EClassId> hashcons_;
  std::deque<ENode<Symbol>> enode_storage_;
};

// ========================================================================
// Egraph methods
// ========================================================================

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::emplace(Symbol symbol, uint64_t disambiguator) -> EClassId {
  // construct leaf e-node with disambiguator
  auto canonical_node = ENode{std::move(symbol), disambiguator};

  auto it = hashcons_.find(ENodeRef{canonical_node});
  if (it != hashcons_.end()) {
    // TODO: check if we need to do find here
    //       When we build the rewrite engine we can fuzz that and test if this is needed
    return union_find_.Find(it->second);
  }

  EClassId new_eclass_id = union_find_.MakeSet();
  ENodeId new_enode_id = new_eclass_id;

  auto enode_ref = intern_enode(std::move(canonical_node));
  hashcons_[enode_ref] = new_eclass_id;
  classes_.emplace(new_eclass_id, std::make_unique<EClass<Analysis>>(new_enode_id));

  return new_eclass_id;
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::emplace(Symbol symbol, utils::small_vector<EClassId> children) -> EClassId {
  for (auto &child_id : children) {
    child_id = union_find_.Find(child_id);
  }
  auto canonical_node = ENode{std::move(symbol), std::move(children)};

  auto it = hashcons_.find(ENodeRef{canonical_node});
  if (it != hashcons_.end()) {
    // TODO: check if we need to do find here
    //       When we build the rewrite engine we can fuzz that and test if this is needed
    return union_find_.Find(it->second);
  }

  EClassId new_eclass_id = union_find_.MakeSet();
  ENodeId new_enode_id = new_eclass_id;

  auto enode_ref = intern_enode(std::move(canonical_node));
  hashcons_[enode_ref] = new_eclass_id;
  classes_.emplace(new_eclass_id, std::make_unique<EClass<Analysis>>(new_enode_id));

  // Update parent lists for children - ESSENTIAL for congruence closure
  for (EClassId child_id : enode_ref.value().children()) {
    assert(union_find_.Find(child_id) == child_id);
    auto child_it = classes_.find(child_id);
    assert(child_it != classes_.end());
    child_it->second->add_parent(new_enode_id);
  }

  return new_eclass_id;
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::intern_enode(ENode<Symbol> enode) -> ENodeRef<Symbol> {
  return ENodeRef{enode_storage_.emplace_back(std::move(enode))};
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::merge(EClassId a, EClassId b) -> EClassId {
  EClassId canonical_a = union_find_.Find(a);
  EClassId canonical_b = union_find_.Find(b);

  if (canonical_a == canonical_b) {
    return canonical_a;
  }

  EClassId merged_id = union_find_.UnionSets(canonical_a, canonical_b);
  EClassId other_id = (merged_id == canonical_a) ? canonical_b : canonical_a;

  // defer hashcons and congruence processing
  rebuild_worklist_.insert(merged_id);

  merge_eclasses(*classes_[merged_id], other_id);

  return merged_id;
}

template <typename Symbol, typename Analysis>
auto EGraph<Symbol, Analysis>::has_class(EClassId id) const -> bool {
  // Check if the ID is valid in union-find first
  // NOTE: This code AFAICT should not be used in a production usecase, only exists for tests and fuzzing
  //       The bounds checking inefficency is acceptable there.
  if (id >= union_find_.Size()) {
    return false;
  }

  return classes_.find(id) != classes_.end();
}

template <typename Symbol, typename Analysis>
void EGraph<Symbol, Analysis>::clear() {
  union_find_.Clear();
  rebuild_worklist_.clear();
  classes_.clear();
  hashcons_.clear();
  enode_storage_.clear();
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
      repair_hashcons_eclass(eclass, eclass_id);
      process_parents(eclass, ctx);
    }
  };

  while (!rebuild_worklist_.empty()) {
    auto todo = std::exchange(rebuild_worklist_, {});
    for (EClassId eclass_id : todo) {
      // canonical + deduplication
      auto [_, inserted] = canonicalized_chunk.insert(union_find_.Find(eclass_id));

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
void EGraph<Symbol, Analysis>::repair_hashcons_eclass(EClass<Analysis> const &eclass, EClassId eclass_id) {
  for (const auto &enode_id : eclass.nodes()) {
    repair_hashcons_enode(get_enode(enode_id), eclass_id);
  }
}

template <typename Symbol, typename Analysis>
void EGraph<Symbol, Analysis>::repair_hashcons_enode(ENode<Symbol> &enode, EClassId eclass_id) {
  // NOTE: the node maybe non-canonicalize, if so it will not be found
  auto it = hashcons_.find(ENodeRef{enode});

  // Canonicalize the enode in place
  auto const changed = enode.canonicalize_in_place(union_find_);
  if (changed) {
    // Hash + value have changed, need to remove stale hashcons entry
    if (it != hashcons_.end()) {
      hashcons_.erase(it);
    }
    hashcons_[ENodeRef{enode}] = eclass_id;
  } else {
    it->second = eclass_id;
  }
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
      canonical_eclass_ids.push_back(union_find_.Find(enode_id));
    }
    // deduplicate
    std::sort(canonical_eclass_ids.begin(), canonical_eclass_ids.end());
    canonical_eclass_ids.erase(std::unique(canonical_eclass_ids.begin(), canonical_eclass_ids.end()),
                               canonical_eclass_ids.end());
    if (canonical_eclass_ids.size() > 1) {
      EClassId merged_root = union_find_.UnionSets(canonical_eclass_ids, ctx.union_find_context);
      rebuild_worklist_.insert(merged_root);

      auto merged_it = classes_.find(merged_root);
      assert(merged_it != classes_.end());
      EClass<Analysis> &merged_eclass = *merged_it->second;
      for (EClassId parent_id : canonical_eclass_ids) {
        if (parent_id != merged_root) {
          merge_eclasses(merged_eclass, parent_id);
        }
      }
      // hashcons update can be defered to the next rebuild iteration because we inserted into the rebuild worklist
    } else if (canonical_eclass_ids.size() == 1) {
      // NOTE: we can NOT add to rebuild_worklist_, we must avoid infinate processing bugs (where parent is yourself)
      for (auto enode_id : enode_ids) {
        repair_hashcons_enode(get_enode(enode_id), canonical_eclass_ids[0]);
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
