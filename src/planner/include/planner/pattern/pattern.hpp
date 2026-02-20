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

#include <cassert>
#include <cstdint>
#include <memory>
#include <optional>
#include <variant>
#include <vector>

#include <boost/functional/hash.hpp>
#include <boost/unordered/unordered_flat_map.hpp>

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

// Boost hash support via ADL
inline std::size_t hash_value(PatternNodeId const &id) { return std::hash<uint32_t>{}(id.value_of()); }

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
 * @brief Wildcard pattern node that matches any e-class without binding
 *
 * Use in patterns where you need a placeholder for arity but don't need to
 * access the matched e-class. Unlike named variables, wildcards:
 * - Don't create bindings in the substitution
 * - Don't enforce consistency (multiple wildcards can match different e-classes)
 *
 * Can be used directly in fluent DSL:
 *   Pattern<Op>::build(Op::Add, {Wildcard{}, Var{kVarX}})
 */
struct Wildcard {
  friend auto operator==(Wildcard, Wildcard) -> bool = default;
};

/**
 * @brief Variable spec for fluent pattern DSL
 *
 * Represents a pattern variable that captures a binding during matching.
 * Use in fluent DSL:
 *   Pattern<Op>::build(Op::Add, {Var{kVarX}, Var{kVarY}})
 */
struct Var {
  PatternVar pvar;
};

// Forward declaration for recursive ChildSpec
template <typename Symbol>
struct SymNode;

/**
 * @brief Child specification for fluent pattern DSL
 *
 * Can be:
 * - PatternNodeId: reference to an already-built node (for Builder)
 * - Wildcard: matches any e-class without binding
 * - Var: pattern variable that captures a binding
 * - shared_ptr<SymNode>: nested symbol with children (for recursive patterns)
 */
template <typename Symbol>
using ChildSpec = std::variant<PatternNodeId, Wildcard, Var, std::shared_ptr<SymNode<Symbol>>>;

/**
 * @brief Symbol node for fluent pattern DSL with nested children
 *
 * Enables building nested patterns without explicit Builder:
 *   Pattern<Op>::build(Op::Add, {Sym(Op::Neg, Var{kVarX}), Var{kVarY}})
 *
 * The shared_ptr indirection breaks the circular dependency with ChildSpec.
 */
template <typename Symbol>
struct SymNode {
  Symbol symbol;
  std::vector<ChildSpec<Symbol>> children;
  std::optional<PatternVar> binding;

  SymNode(Symbol s, std::vector<ChildSpec<Symbol>> c = {}, std::optional<PatternVar> b = std::nullopt)
      : symbol(s), children(std::move(c)), binding(b) {}
};

/**
 * @brief Helper to create a nested symbol spec for fluent DSL
 *
 * Example usage:
 *   Sym(Op::Neg, Var{kVarX})                             // unary: Neg(?x)
 *   Sym(Op::Add, Var{kVarX}, Var{kVarY})                 // binary: Add(?x, ?y)
 *   Sym(Op::Add, Sym(Op::Neg, Var{kVarX}), Var{kVarY})   // nested: Add(Neg(?x), ?y)
 *
 * For binding at the root, use Pattern::build(..., binding) instead.
 * Symbol is deduced from the first argument, children can be any ChildSpec-convertible type.
 */
template <typename Symbol, typename... Children>
  requires(std::convertible_to<Children, ChildSpec<Symbol>> && ...)
auto Sym(Symbol symbol, Children &&...children) -> ChildSpec<Symbol> {
  return std::make_shared<SymNode<Symbol>>(
      symbol, std::vector<ChildSpec<Symbol>>{ChildSpec<Symbol>(std::forward<Children>(children))...}, std::nullopt);
}

/**
 * @brief Symbol with its children in a pattern
 *
 * Only symbol nodes have children; variables and wildcards are always leaves.
 */
template <typename Symbol>
  requires ENodeSymbol<Symbol>
struct SymbolWithChildren {
  Symbol sym;
  utils::small_vector<PatternNodeId> children;

  friend auto operator==(SymbolWithChildren const &, SymbolWithChildren const &) -> bool = default;
};

/**
 * @brief A node in a pattern AST
 *
 * Variant of:
 * - SymbolWithChildren: concrete symbol with child pattern references
 * - PatternVar: variable that captures a binding
 * - Wildcard: matches anything without capturing
 *
 * Use std::visit or std::get_if to dispatch on node type.
 */
template <typename Symbol>
  requires ENodeSymbol<Symbol>
using PatternNode = std::variant<SymbolWithChildren<Symbol>, PatternVar, Wildcard>;

/**
 * @brief A pattern for matching against e-graph expressions
 *
 * Patterns are stored as flat vectors of PatternNodes.
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
   *   constexpr PatternVar kVarX{0};
   *   auto builder = Pattern<Op>::Builder{};
   *   auto x = builder.var(kVarX);        // ?x
   *   auto zero = builder.sym(Op::Const); // Const leaf
   *   builder.sym(Op::Add, {x, zero});    // Add(?x, Const) - root (last node)
   *   auto pattern = std::move(builder).build();
   * @endcode
   */
  class Builder {
   public:
    /**
     * @brief Add a variable node to the pattern
     * @param pvar Pattern variable (use PatternVar{id} for type safety)
     * @return Index of the newly added node
     */
    auto var(PatternVar pvar) -> PatternNodeId {
      auto id = PatternNodeId{static_cast<uint32_t>(nodes_.size())};
      nodes_.push_back(pvar);
      return id;
    }

    /**
     * @brief Add a wildcard node that matches any e-class without binding
     *
     * Use this when you need a placeholder for arity but don't need to access
     * the matched e-class in the apply function.
     *
     * @return Index of the newly added wildcard node
     */
    auto wildcard() -> PatternNodeId {
      auto id = PatternNodeId{static_cast<uint32_t>(nodes_.size())};
      nodes_.push_back(Wildcard{});
      return id;
    }

    /**
     * @brief Add a leaf symbol node (no children)
     * @param symbol The operation symbol
     * @param binding Optional variable to bind the matched e-class to
     * @return Index of the newly added node
     */
    auto sym(Symbol symbol, std::optional<PatternVar> binding = std::nullopt) -> PatternNodeId {
      auto id = PatternNodeId{static_cast<uint32_t>(nodes_.size())};
      nodes_.push_back(SymbolWithChildren<Symbol>{symbol, {}});
      if (binding) {
        bindings_[id] = *binding;
      }
      return id;
    }

    /**
     * @brief Add a symbol node with children
     * @param symbol The operation symbol
     * @param children Child node indices (must all be < current size)
     * @param binding Optional variable to bind the matched e-class to
     * @return Index of the newly added node
     */
    auto sym(Symbol symbol, utils::small_vector<PatternNodeId> children,
             std::optional<PatternVar> binding = std::nullopt) -> PatternNodeId {
      auto id = PatternNodeId{static_cast<uint32_t>(nodes_.size())};
      nodes_.push_back(SymbolWithChildren<Symbol>{symbol, std::move(children)});
      if (binding) {
        bindings_[id] = *binding;
      }
      return id;
    }

    /**
     * @brief Add a symbol node with children (initializer list variant)
     *
     * Accepts PatternNodeId, Wildcard{}, Var{pvar}, or Sym(...) as children.
     * Example: builder.sym(Op::Add, {x, y})
     * Example: builder.sym(Op::Add, {Wildcard{}, Var{kVarX}})
     * Example: builder.sym(Op::Add, {Sym(Op::Neg, Var{kVarX}), Var{kVarY}})
     */
    auto sym(Symbol symbol, std::initializer_list<core::ChildSpec<Symbol>> children,
             std::optional<PatternVar> binding = std::nullopt) -> PatternNodeId {
      utils::small_vector<PatternNodeId> child_ids;
      for (auto const &spec : children) {
        child_ids.push_back(build_child(spec));
      }
      return sym(symbol, std::move(child_ids), binding);
    }

   private:
    /**
     * @brief Recursively build a child spec into pattern nodes
     */
    auto build_child(core::ChildSpec<Symbol> const &spec) -> PatternNodeId {
      return std::visit(
          [this](auto const &s) -> PatternNodeId {
            using T = std::decay_t<decltype(s)>;
            if constexpr (std::is_same_v<T, PatternNodeId>) {
              return s;
            } else if constexpr (std::is_same_v<T, Wildcard>) {
              return wildcard();
            } else if constexpr (std::is_same_v<T, Var>) {
              return var(s.pvar);
            } else if constexpr (std::is_same_v<T, std::shared_ptr<SymNode<Symbol>>>) {
              // Recursively build nested symbol
              utils::small_vector<PatternNodeId> child_ids;
              for (auto const &child : s->children) {
                child_ids.push_back(build_child(child));
              }
              return sym(s->symbol, std::move(child_ids), s->binding);
            }
          },
          spec);
    }

   public:
    /**
     * @brief Build the final pattern (last added node becomes root)
     *
     * Patterns are built bottom-up: children first, then parents.
     * The root is always the last node added.
     *
     * @return The constructed pattern
     * @pre At least one node has been added
     */
    auto build() && -> Pattern {
      assert(!nodes_.empty() && "Pattern must have at least one node");
      auto root = PatternNodeId{static_cast<uint32_t>(nodes_.size() - 1)};
      return Pattern{std::move(nodes_), root, std::move(bindings_)};
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
    boost::unordered_flat_map<PatternNodeId, PatternVar> bindings_;
  };

  /**
   * @brief Fluent DSL: build a pattern with nested children
   *
   * Example (simple):
   *   auto pattern = Pattern<Op>::build(Op::Add, {Var{kVarX}, Var{kVarY}});
   *
   * Example (nested):
   *   auto pattern = Pattern<Op>::build(Op::Add, {Sym(Op::Neg, Var{kVarX}), Var{kVarY}});
   *
   * Example (deeply nested):
   *   auto pattern = Pattern<Op>::build(Op::Mul, {
   *       Sym(Op::Add, Var{kVarX}, Var{kVarY}),
   *       Sym(Op::Neg, Var{kVarZ})
   *   });
   */
  static auto build(Symbol symbol, std::initializer_list<core::ChildSpec<Symbol>> children,
                    std::optional<PatternVar> binding = std::nullopt) -> Pattern {
    Builder builder;
    builder.sym(symbol, children, binding);
    return std::move(builder).build();
  }

  /**
   * @brief Fluent DSL: build a leaf pattern (symbol with no children)
   */
  static auto build(Symbol symbol, std::optional<PatternVar> binding = std::nullopt) -> Pattern {
    Builder builder;
    builder.sym(symbol, binding);
    return std::move(builder).build();
  }

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
   * @brief Get the maximum depth of this pattern (distance from root to deepest leaf)
   *
   * Depth is 0 for a single-node pattern (leaf or variable).
   * Depth is 1 for a symbol with only leaf/variable children.
   * Precalculated at construction time for O(1) access.
   */
  [[nodiscard]] auto depth() const -> std::size_t { return depth_; }

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
    return !nodes_.empty() && std::holds_alternative<PatternVar>(nodes_[root_.value_of()]);
  }

  /**
   * @brief Get the binding variable for a pattern node, if any
   * @param id The pattern node to check
   * @return The bound variable, or nullopt if the node has no binding
   */
  [[nodiscard]] auto binding_for(PatternNodeId id) const -> std::optional<PatternVar> {
    auto it = bindings_.find(id);
    return it != bindings_.end() ? std::optional{it->second} : std::nullopt;
  }

  /**
   * @brief Check if any nodes have bindings
   */
  [[nodiscard]] auto has_bindings() const -> bool { return !bindings_.empty(); }

  /**
   * @brief Get all node bindings
   */
  [[nodiscard]] auto bindings() const -> boost::unordered_flat_map<PatternNodeId, PatternVar> const & {
    return bindings_;
  }

  /**
   * @brief Get number of unique variables in this pattern
   *
   * This is the size of the binding buffer needed for pattern matches.
   */
  [[nodiscard]] auto num_vars() const -> std::size_t { return var_slots_.size(); }

  /**
   * @brief Get the slot index for a variable
   *
   * Slot indices are assigned sequentially (0, 1, 2, ...) to unique variables
   * in the order they're encountered during pattern traversal.
   *
   * @param var The pattern variable
   * @return Slot index in the binding buffer
   * @pre var must exist in this pattern
   */
  [[nodiscard]] auto var_slot(PatternVar var) const -> uint8_t {
    auto it = var_slots_.find(var);
    assert(it != var_slots_.end() && "var_slot: variable not found in pattern");
    return it->second;
  }

  /**
   * @brief Check if a variable exists in this pattern
   */
  [[nodiscard]] auto has_var(PatternVar var) const -> bool { return var_slots_.contains(var); }

  /**
   * @brief Get all variable slot mappings
   */
  [[nodiscard]] auto var_slots() const -> boost::unordered_flat_map<PatternVar, uint8_t> const & { return var_slots_; }

  friend auto operator==(Pattern const &, Pattern const &) -> bool = default;

 private:
  Pattern(std::vector<PatternNode<Symbol>> nodes, PatternNodeId root,
          boost::unordered_flat_map<PatternNodeId, PatternVar> bindings)
      : nodes_(std::move(nodes)),
        root_(root),
        bindings_(std::move(bindings)),
        depth_(compute_depth()),
        var_slots_(compute_var_slots()) {}

  /**
   * @brief Compute maximum depth from root to any leaf
   *
   * Uses iterative approach with explicit stack to avoid recursion.
   * Called once at construction time.
   */
  [[nodiscard]] auto compute_depth() const -> std::size_t {
    if (nodes_.empty()) return 0;

    std::size_t max_depth = 0;
    // Stack of (node_id, current_depth)
    std::vector<std::pair<PatternNodeId, std::size_t>> stack;
    stack.emplace_back(root_, 0);

    while (!stack.empty()) {
      auto [node_id, current_depth] = stack.back();
      stack.pop_back();

      max_depth = std::max(max_depth, current_depth);

      auto const &node = nodes_[node_id.value_of()];
      if (auto const *sym = std::get_if<SymbolWithChildren<Symbol>>(&node)) {
        for (auto child_id : sym->children) {
          stack.emplace_back(child_id, current_depth + 1);
        }
      }
      // PatternVar and Wildcard are leaves - no children to process
    }

    return max_depth;
  }

  /**
   * @brief Collect all unique variables and assign slot indices
   *
   * Variables come from two sources:
   * 1. PatternVar nodes in the pattern tree
   * 2. Bindings on symbol nodes (via bindings_ map)
   *
   * Called once at construction time.
   */
  [[nodiscard]] auto compute_var_slots() const -> boost::unordered_flat_map<PatternVar, uint8_t> {
    boost::unordered_flat_map<PatternVar, uint8_t> slots;
    uint8_t next_slot = 0;

    auto assign_slot = [&](PatternVar var) {
      if (!slots.contains(var)) {
        slots[var] = next_slot++;
      }
    };

    // Collect from pattern nodes (PatternVar nodes)
    for (auto const &node : nodes_) {
      if (auto const *var = std::get_if<PatternVar>(&node)) {
        assign_slot(*var);
      }
    }

    // Collect from symbol node bindings
    for (auto const &[_, var] : bindings_) {
      assign_slot(var);
    }

    return slots;
  }

  std::vector<PatternNode<Symbol>> nodes_;
  PatternNodeId root_{0};
  boost::unordered_flat_map<PatternNodeId, PatternVar> bindings_;  ///< Node-to-variable bindings
  std::size_t depth_{0};                                           ///< Cached maximum depth (root to deepest leaf)
  boost::unordered_flat_map<PatternVar, uint8_t> var_slots_;       ///< Variable to slot index mapping
};

}  // namespace memgraph::planner::core
