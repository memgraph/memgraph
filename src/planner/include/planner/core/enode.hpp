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

#include <concepts>
#include <cstdint>
#include <functional>
#include <type_traits>

#include <boost/functional/hash.hpp>

#include "planner/core/eids.hpp"
#include "utils/small_vector.hpp"

namespace memgraph::planner::core {

/// Concept: Symbol must be hashable, trivially copyable, and equality comparable
template <typename T>
concept ENodeSymbol = requires(T a, T b) {
  { std::hash<T>{}(a) } -> std::convertible_to<std::size_t>;
  { a == b } -> std::convertible_to<bool>;
} && std::is_trivially_copyable_v<T>;

struct UnionFind;

namespace detail {

struct ENodeBase {
  explicit ENodeBase(utils::small_vector<EClassId> children) : children_(std::move(children)) {}
  explicit ENodeBase(uint64_t disambiguator) : disambiguator_(disambiguator) {}

  friend bool operator==(ENodeBase const &lhs, ENodeBase const &rhs) = default;

  /// @return Number of child e-classes (0 for leaf nodes)
  [[nodiscard]] auto arity() const -> std::size_t { return children_.size(); }

  /// @return true if this e-node has no children (variable/constant)
  [[nodiscard]] auto is_leaf() const -> bool { return children_.empty(); }

  /// Returns copy with children updated to canonical e-class IDs via union-find
  /// @param uf Union-find structure (will be modified for path compression)
  auto canonicalize(UnionFind &uf) const -> ENodeBase;

  auto children() const -> utils::small_vector<EClassId> const & { return children_; }
  auto disambiguator() const -> uint64_t { return disambiguator_; }

  [[nodiscard]] auto compute_hash() const -> std::size_t;

 private:
  /// Unique ID for leaf nodes to distinguish identical symbols with different values
  uint64_t disambiguator_ = 0;

  /// E-class IDs of child nodes. Empty for leaves, N elements for N-ary operators.
  /// SmallVector avoids heap allocation for ≤2 children.
  utils::small_vector<EClassId> children_{};
};

}  // namespace detail

/**
 * @brief E-node: Expression node in an e-graph with symbol and child e-class references
 *
 * Represents expressions as flat structures: symbol + list of child e-class IDs.
 * Example: "f(g(x), y)" = ENode{symbol: "f", children: [id_of_g_x, id_of_y]}
 *
 * @tparam Symbol Must satisfy ENodeSymbol concept (hashable, trivially copyable, ==)
 *
 * Key features:
 * - Leaf nodes: Use disambiguator field for unique constants/variables
 * - Non-leaf: Store child e-class IDs (SmallVector optimized for ≤2 children)
 * - Pre-computed hash for O(1) hash table operations
 * - Canonicalize() builds new ENode with updated child IDs after e-class merging
 */
template <typename Symbol>
  requires ENodeSymbol<Symbol>
struct ENode : private detail::ENodeBase {
  ENode(Symbol sym, uint64_t disambig) : ENodeBase{disambig}, symbol_(std::move(sym)) {}

  ENode(Symbol sym, utils::small_vector<EClassId> kids) : ENodeBase(std::move(kids)), symbol_(std::move(sym)) {}

  // Convenience constructor with initializer_list for easier construction
  ENode(Symbol sym, std::initializer_list<EClassId> kids)
      : ENodeBase(utils::small_vector<EClassId>(kids)), symbol_(std::move(sym)) {}

  ENode(ENode const &other) = default;
  ENode(ENode &&other) noexcept = default;
  auto operator=(ENode const &other) -> ENode & = default;
  auto operator=(ENode &&other) noexcept -> ENode & = default;

  friend auto operator==(ENode const &lhs, ENode const &rhs) -> bool = default;

  using detail::ENodeBase::arity;
  using detail::ENodeBase::children;
  using detail::ENodeBase::disambiguator;
  using detail::ENodeBase::is_leaf;

  /// Returns copy with canonical child e-class IDs (modifies uf for path compression)
  auto canonicalize(UnionFind &uf) const -> ENode { return ENode{symbol_, ENodeBase::canonicalize(uf)}; }

  /// @return Pre-computed hash value (computed once at construction)
  /// @note Hash uses non-canonical children; canonicalize first for hash-consing
  [[nodiscard]] auto hash() const -> std::size_t { return hash_value_; }

  auto symbol() const -> Symbol const & { return symbol_; }

 private:
  ENode(Symbol sym, ENodeBase base) : ENodeBase(std::move(base)), symbol_(sym) {}

  /// Computes hash using boost::hash_combine (called once during construction)
  [[nodiscard]] auto compute_hash() const -> std::size_t {
    std::size_t seed = detail::ENodeBase::compute_hash();
    boost::hash_combine(seed, std::hash<Symbol>{}(symbol_));
    return seed;
  }

  /// Operation symbol: "+", "*", "f", variable names, etc.
  Symbol symbol_;

  /// Hash value computed once at construction for O(1) hash() calls
  /// MUST be declared after symbol_ to ensure correct initialization order
  std::size_t hash_value_ = compute_hash();
};

}  // namespace memgraph::planner::core

namespace std {
template <typename Symbol>
struct hash<memgraph::planner::core::ENode<Symbol>> {
  std::size_t operator()(memgraph::planner::core::ENode<Symbol> const &node) const noexcept { return node.hash(); }
};
}  // namespace std
