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

module;

#include <bit>
#include <cassert>
#include <cstdint>
#include <deque>
#include <limits>
#include <optional>
#include <span>
#include <vector>

export module memgraph.planner.core.union_find;

export namespace memgraph::planner::core {

/**
 * @brief Processing context for UnionFind operations
 *
 * @details
 * Provides reusable temporary storage specifically for UnionFind operations,
 * eliminating allocation overhead in hot paths.
 */
struct UnionFindContext {
  UnionFindContext() = default;

 private:
  friend struct UnionFind;

  auto GetReusableVector(std::size_t n) -> std::vector<uint32_t> & {
    roots_.clear();
    roots_.reserve(n);
    return roots_;
  }

  std::vector<uint32_t> roots_;
};

/**
 * @brief Union-Find data structure
 *
 * @details
 * Implements a disjoint-set data structure.
 *
 * @par Implementation Notes:
 * - **Path halving**: Amortized O(α(n)) find operations with single-pass optimization
 * - **Union by rank**: Keeps trees balanced for optimal performance
 * - **Bulk operations**: Zero-allocation UnionSets()
 * - **8-bit rank storage**: Memory-optimized rank storage (1 byte vs 8 bytes)
 * - **Chunked deque storage**: Eliminates vector reallocation overhead
 *
 * @par Thread Safety:
 * Not thread-safe. External synchronization required for concurrent access.
 *
 * @complexity
 * - Space: O(n) where n is the number of sets created
 * - Time: O(α(n)) amortized for find/union operations
 */
struct UnionFind {
  using id_t = uint32_t;
  using rank_t = uint8_t;

  UnionFind() = default;

  /**
   * @brief Create a new set containing only the returned ID
   *
   * @return ID of the new set
   */
  auto MakeSet() -> id_t;

  /**
   * @brief Find root id of the set
   *
   * @param id The node ID to find the root for
   * @return root id of the set
   *
   * @complexity O(α(n)) amortized where α is inverse Ackermann function
   */
  auto Find(id_t id) -> id_t;

  /**
   * @brief Find root id of the set without path halving
   *
   * @param id The node ID to find the root for
   * @return root id of the set
   */
  [[deprecated("Use the non-const version that performs path halving")]] [[nodiscard]] auto Find(id_t id) const -> id_t;

  /**
   * @brief Merges two sets
   *
   * Merges the sets containing the two given IDs and returns the root
   * id of the merged set. Uses union-by-rank to maintain balanced trees.
   *
   * @param a ID from first set to union
   * @param b ID from second set to union
   * @return root id of the merged set
   *
   * @complexity O(α(n)) amortized
   */
  auto UnionSets(id_t a, id_t b) -> id_t;

  /**
   * @brief Efficiently merges multiple sets
   *
   * This is the recommended approach for frequent bulk operations.
   *
   * @param ids Span of IDs whose sets should be merged
   * @param ctx UnionFindContext providing reusable temporary storage
   * @return root id of the merged set
   *
   * @complexity O(k × α(n)) where k is ids.size()
   */
  auto UnionSets(std::span<const id_t> ids, UnionFindContext &ctx) -> id_t;

  /**
   * @brief Test if two elements are in the same set
   *
   * @param a First set id
   * @param b Second set id
   * @return true if a and b are in the same set
   *
   * @complexity O(α(n)) amortized
   */
  auto Connected(id_t a, id_t b) -> bool;

  /**
   * @brief Get total number of elements
   *
   * Returns the total number of elements created, including those that
   * have been merged. This represents the total ID space, not the
   * number of distinct sets.
   *
   * @return Total number of IDs that have been created
   *
   * @complexity O(1)
   */
  [[nodiscard]] auto Size() const -> std::size_t { return parent_.size(); }

  /**
   * @brief Get number of distinct set
   *
   * Count of the actual number of separate sets,
   * which decreases as sets are unioned together.
   *
   * @return Number of distinct sets
   *
   * @complexity O(1)
   */
  [[nodiscard]] auto ComponentCount() const -> size_t;

  /**
   * @brief Reset to an empty union find structure
   *
   * @complexity O(1)
   */
  void Clear();

 private:
  /**
   * Uses std::deque to eliminate expensive vector reallocations and copying.
   * Deque stores elements in contiguous chunks (typically 512 bytes) and never
   * moves existing elements when growing, providing optimal performance for
   * UnionFind's never-shrinking requirement.
   **/
  std::deque<id_t> parent_;

  /**
   * Uses std::deque with 8-bit rank storage instead of the typical
   * size_t or int. This provides significant memory savings (8 bytes → 1 byte
   * per element) while supporting trees up to depth 255, which is more than
   * sufficient for practical use cases.
   */
  std::deque<rank_t> rank_;

  size_t num_of_sets_ = 0;
};
}  // namespace memgraph::planner::core

module : private;

namespace memgraph::planner::core {

// Static assertion to ensure UnionFind::rank_t rank storage is sufficient
// Maximum rank in a union-find is O(log n)
static_assert(std::bit_width(std::numeric_limits<UnionFind::id_t>::max()) <
                  std::numeric_limits<UnionFind::rank_t>::max(),
              "8-bit rank storage insufficient for expected maximum tree depth");

auto UnionFind::MakeSet() -> id_t {
  auto id = static_cast<id_t>(parent_.size());
  parent_.push_back(id);
  rank_.push_back(0);
  num_of_sets_++;
  return id;
}

auto UnionFind::Find(id_t id) -> id_t {
  // TODO: C++26 contract
  assert(id < parent_.size());

  // Single-pass path halving algorithm with branch prediction hints
  // Path halving is faster than full compression for find-heavy workloads
  auto current = id;
  while (parent_[current] != current) [[likely]] {
      auto const next = parent_[current];
      parent_[current] = parent_[next];
      current = next;
    }

  return current;
}

auto UnionFind::Find(id_t id) const -> id_t {
  // TODO: C++26 contract
  assert(id < parent_.size());

  // Non-modifying find
  auto current = id;
  while (parent_[current] != current) {
    // Always skip to grandparent for faster traversal
    // This works even when parent is root (grandparent == parent)
    current = parent_[parent_[current]];
  }
  return current;
}

auto UnionFind::UnionSets(id_t a, id_t b) -> id_t {
  auto root_a = Find(a);
  auto root_b = Find(b);

  if (root_a == root_b) [[unlikely]] {
    return root_a;
  }

  // Decrement component count when merging
  num_of_sets_--;

  auto const rank_comparison = rank_[root_a] <=> rank_[root_b];
  auto const a_is_parent = rank_comparison == std::strong_ordering::greater;
  auto const parent_root = a_is_parent ? root_a : root_b;
  auto const child_root = a_is_parent ? root_b : root_a;

  parent_[child_root] = parent_root;
  if (rank_comparison == std::strong_ordering::equivalent) {
    ++rank_[parent_root];
  }
  return parent_root;
}

auto UnionFind::UnionSets(std::span<id_t const> ids, UnionFindContext &ctx) -> id_t {
  // TODO: C++26 contract
  assert(!ids.empty());

  if (ids.size() == 1) [[unlikely]] {
    return Find(ids[0]);
  }

  // Use reusable vector to eliminate allocation overhead
  // Pass 1: Find the root for each id
  auto &roots = ctx.GetReusableVector(ids.size());

  for (auto id : ids) {
    roots.push_back(Find(id));
  }

  // Pass 2: Find the root with the highest rank (best root)
  auto best_root = roots[0];
  auto best_rank = rank_[best_root];
  bool unique_best = true;

  for (size_t i = 1; i < roots.size(); ++i) {
    auto root = roots[i];
    auto current_rank = rank_[root];

    auto rank_diff = static_cast<int64_t>(current_rank) - static_cast<int64_t>(best_rank);
    auto is_higher_rank = rank_diff > 0;
    auto is_equal_rank = rank_diff == 0;
    auto is_different_root = root != best_root;

    best_root = is_higher_rank ? root : best_root;
    best_rank = is_higher_rank ? current_rank : best_rank;
    unique_best = is_higher_rank || (unique_best && !(is_equal_rank && is_different_root));
  }

  // Pass 3: Union all to be the best_root
  size_t merges_performed = 0;
  for (auto root : roots) {
    parent_[root] = best_root;
    merges_performed += (root != best_root);
  }
  num_of_sets_ -= merges_performed;

  // If the best rank was not unique, increase the rank of best_root
  if (!unique_best) {
    ++rank_[best_root];
  }

  return best_root;
}

auto UnionFind::Connected(id_t a, id_t b) -> bool {
  // TODO: C++26 contract
  assert(a < parent_.size());
  assert(b < parent_.size());
  return Find(a) == Find(b);
}

auto UnionFind::ComponentCount() const -> size_t { return num_of_sets_; }

void UnionFind::Clear() {
  parent_.clear();
  rank_.clear();
  num_of_sets_ = 0;
}
}  // namespace memgraph::planner::core
