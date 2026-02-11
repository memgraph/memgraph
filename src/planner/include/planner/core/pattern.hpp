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

#include <cstdint>
#include <variant>
#include <vector>

#include <boost/functional/hash.hpp>

#include "utils/small_vector.hpp"

import memgraph.planner.core.concepts;
import rollbear.strong_type;

namespace memgraph::planner::core {

/**
 * @brief Strong type for pattern node indices within a Pattern
 *
 * Represents positions in the pattern's internal node vector.
 * Uses value semantics with strong typing to prevent confusion with other integer types.
 */
using PatternNodeId = strong::type<uint32_t, struct PatternNodeId_, strong::regular, strong::hashable, strong::ordered>;

/**
 * @brief Pattern variable for binding during e-matching
 *
 * Variables represent wildcard positions in patterns that can match any e-class.
 * Each variable has a unique ID within a pattern; multiple occurrences of the same ID
 * must bind to the same e-class during matching.
 *
 * Example: In pattern Add(?x, ?x), both ?x refer to the same variable ID and
 * will only match when both children are in the same e-class.
 */
struct PatternVar {
  uint32_t id;

  friend auto operator==(PatternVar, PatternVar) -> bool = default;
  friend auto operator<=>(PatternVar, PatternVar) = default;
};

// Boost hash support via ADL
inline std::size_t hash_value(PatternVar const &var) { return std::hash<uint32_t>{}(var.id); }

}  // namespace memgraph::planner::core

namespace std {
template <>
struct hash<memgraph::planner::core::PatternVar> {
  std::size_t operator()(memgraph::planner::core::PatternVar const &var) const noexcept {
    return std::hash<uint32_t>{}(var.id);
  }
};
}  // namespace std

namespace memgraph::planner::core {

/**
 * @brief A node in a pattern AST
 *
 * Represents either:
 * - A concrete symbol with children (e.g., Add with two child patterns)
 * - A variable that matches any e-class (e.g., ?x)
 *
 * The pattern is stored in a flat vector form similar to egg's RecExpr.
 * Children are referenced by PatternNodeId indices into the owning Pattern.
 *
 * @tparam Symbol Must satisfy ENodeSymbol concept (hashable, trivially copyable, ==)
 */
template <typename Symbol>
  requires ENodeSymbol<Symbol>
struct PatternNode {
  using ContentType = std::variant<Symbol, PatternVar>;

  ContentType content;
  utils::small_vector<PatternNodeId> children;

  /**
   * @brief Construct a variable pattern node (leaf)
   */
  explicit PatternNode(PatternVar var) : content(var) {}

  /**
   * @brief Construct a symbol pattern node with children
   */
  PatternNode(Symbol sym, utils::small_vector<PatternNodeId> kids) : content(sym), children(std::move(kids)) {}

  /**
   * @brief Construct a symbol pattern node with initializer list
   */
  PatternNode(Symbol sym, std::initializer_list<PatternNodeId> kids) : content(sym), children(kids) {}

  /**
   * @brief Construct a leaf symbol pattern node (no children)
   */
  explicit PatternNode(Symbol sym) : content(sym) {}

  /**
   * @return true if this node is a variable
   */
  [[nodiscard]] auto is_variable() const -> bool { return std::holds_alternative<PatternVar>(content); }

  /**
   * @return true if this node is a concrete symbol
   */
  [[nodiscard]] auto is_symbol() const -> bool { return std::holds_alternative<Symbol>(content); }

  /**
   * @return The symbol (requires is_symbol() == true)
   */
  [[nodiscard]] auto symbol() const -> Symbol const & { return std::get<Symbol>(content); }

  /**
   * @return The variable (requires is_variable() == true)
   */
  [[nodiscard]] auto variable() const -> PatternVar { return std::get<PatternVar>(content); }

  /**
   * @return Number of children (0 for variables and leaf symbols)
   */
  [[nodiscard]] auto arity() const -> std::size_t { return children.size(); }

  /**
   * @return true if this node has no children
   */
  [[nodiscard]] auto is_leaf() const -> bool { return children.empty(); }

  friend auto operator==(PatternNode const &, PatternNode const &) -> bool = default;
};

/**
 * @brief A pattern for matching against e-graph expressions
 *
 * Patterns are stored as flat vectors of PatternNodes (similar to egg's RecExpr).
 * The last element is the root, and children are referenced by index.
 *
 * Construction is done via the Builder nested class for safe, validated patterns.
 *
 * Example: Pattern for "Add(?x, Const(0))" stored as:
 *   [0]: PatternVar{id=0}           // ?x
 *   [1]: Symbol{Const}, children=[] // Const(0) leaf
 *   [2]: Symbol{Add}, children=[0,1] // Add(?x, Const(0)) - root
 *
 * @tparam Symbol Must satisfy ENodeSymbol concept
 */
template <typename Symbol>
  requires ENodeSymbol<Symbol>
class Pattern {
 public:
  /**
   * @brief Builder for constructing patterns programmatically
   *
   * The builder ensures patterns are well-formed by tracking node indices
   * and providing helper methods for common patterns.
   *
   * Example usage:
   * @code
   *   auto builder = Pattern<Op>::Builder{};
   *   auto x = builder.var(0);           // ?x
   *   auto zero = builder.sym(Op::Const); // Const leaf
   *   auto add = builder.sym(Op::Add, {x, zero}); // Add(?x, Const)
   *   auto pattern = builder.build(add);
   * @endcode
   */
  class Builder {
   public:
    /**
     * @brief Add a variable node to the pattern
     * @param var_id Unique variable identifier within this pattern
     * @return Index of the newly added node
     */
    auto var(uint32_t var_id) -> PatternNodeId {
      auto id = PatternNodeId{static_cast<uint32_t>(nodes_.size())};
      nodes_.emplace_back(PatternVar{var_id});
      return id;
    }

    /**
     * @brief Add a leaf symbol node (no children)
     * @param symbol The operation symbol
     * @return Index of the newly added node
     */
    auto sym(Symbol symbol) -> PatternNodeId {
      auto id = PatternNodeId{static_cast<uint32_t>(nodes_.size())};
      nodes_.emplace_back(symbol);
      return id;
    }

    /**
     * @brief Add a symbol node with children
     * @param symbol The operation symbol
     * @param children Child node indices (must all be < current size)
     * @return Index of the newly added node
     */
    auto sym(Symbol symbol, utils::small_vector<PatternNodeId> children) -> PatternNodeId {
      auto id = PatternNodeId{static_cast<uint32_t>(nodes_.size())};
      nodes_.emplace_back(symbol, std::move(children));
      return id;
    }

    /**
     * @brief Add a symbol node with children (initializer list variant)
     * @param symbol The operation symbol
     * @param children Child node indices
     * @return Index of the newly added node
     */
    auto sym(Symbol symbol, std::initializer_list<PatternNodeId> children) -> PatternNodeId {
      auto id = PatternNodeId{static_cast<uint32_t>(nodes_.size())};
      nodes_.emplace_back(symbol, children);
      return id;
    }

    /**
     * @brief Build the final pattern with specified root
     * @param root The root node index
     * @return The constructed pattern
     */
    auto build(PatternNodeId root) && -> Pattern { return Pattern{std::move(nodes_), root}; }

    /**
     * @brief Build the final pattern with the last added node as root
     * @return The constructed pattern
     * @pre At least one node has been added
     */
    auto build() && -> Pattern {
      assert(!nodes_.empty());
      auto root = PatternNodeId{static_cast<uint32_t>(nodes_.size() - 1)};
      return Pattern{std::move(nodes_), root};
    }

    /**
     * @brief Get current number of nodes in the builder
     */
    [[nodiscard]] auto size() const -> std::size_t { return nodes_.size(); }

    /**
     * @brief Check if builder is empty
     */
    [[nodiscard]] auto empty() const -> bool { return nodes_.empty(); }

   private:
    std::vector<PatternNode<Symbol>> nodes_;
  };

  /**
   * @brief Get the root node ID
   */
  [[nodiscard]] auto root() const -> PatternNodeId { return root_; }

  /**
   * @brief Access a pattern node by ID
   */
  [[nodiscard]] auto operator[](PatternNodeId id) const -> PatternNode<Symbol> const & { return nodes_[id.value_of()]; }

  /**
   * @brief Get the number of nodes in this pattern
   */
  [[nodiscard]] auto size() const -> std::size_t { return nodes_.size(); }

  /**
   * @brief Check if the pattern is empty
   */
  [[nodiscard]] auto empty() const -> bool { return nodes_.empty(); }

  /**
   * @brief Get all nodes as a span
   */
  [[nodiscard]] auto nodes() const -> std::span<PatternNode<Symbol> const> { return nodes_; }

  /**
   * @brief Check if the root is a variable (pattern matches any e-class)
   */
  [[nodiscard]] auto is_variable_pattern() const -> bool {
    return !nodes_.empty() && nodes_[root_.value_of()].is_variable();
  }

  friend auto operator==(Pattern const &, Pattern const &) -> bool = default;

 private:
  Pattern(std::vector<PatternNode<Symbol>> nodes, PatternNodeId root) : nodes_(std::move(nodes)), root_(root) {}

  std::vector<PatternNode<Symbol>> nodes_;
  PatternNodeId root_{0};
};

}  // namespace memgraph::planner::core
