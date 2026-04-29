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
#include <concepts>
#include <cstdint>
#include <functional>
#include <utility>

namespace memgraph::query::plan::v2 {

enum struct symbol : std::uint8_t {
  Once,
  Bind,
  Symbol,
  Literal,
  Identifier,
  Output,
  NamedOutput,
  ParamLookup,
  // Arithmetic operators (binary)
  Add,
  Sub,
  Mul,
  Div,
  Mod,
  Exp,
  // Comparison operators (binary)
  Eq,
  Neq,
  Lt,
  Lte,
  Gt,
  Gte,
  // Boolean operators
  And,
  Or,
  Xor,
  Not,
  // Unary operators
  UnaryMinus,
  UnaryPlus,
};

// ============================================================================
// Symbol descriptors — one source of truth for each symbol's properties.
// ============================================================================
//
// Adding a new symbol requires:
//   1. an entry in the `symbol` enum above,
//   2. a `symbol_descriptor<symbol::Foo>` specialisation below,
//   3. a `symbol_make_traits<symbol::Foo>` specialisation in symbol_make_traits.hpp
//      (mostly auto-derived for binary/unary via the requires clauses there),
//   4. a `Build(tag<symbol::Foo>, ...)` overload in egraph_converter.cpp,
//   5. an ast_converter visitor pair (PreVisit/PostVisit) for the AST node.
//
// (1) and (2) are the *categorical* declaration — what kind of symbol is this?
// They live next to each other so a missing descriptor is a compile error at
// the first template instantiation that needs it (predicates, cost-class
// lookup, etc.).  (3)-(5) are the *behavioural* declarations — true forks
// because each operator has a unique lowering and AST mapping.

/// Arity / shape category for a symbol.  Drives Make* signatures and the
/// classification predicates below.
enum class Arity : std::uint8_t {
  Leaf,     ///< Once, Symbol, Literal, ParamLookup — no children.
  Unary,    ///< Identifier, Not, UnaryMinus, UnaryPlus — exactly one child.
  Binary,   ///< Add, Sub, ..., Eq, ..., And, Or, Xor — exactly two children.
  Special,  ///< Bind (3 children), Output (variable), NamedOutput (2 children).
};

/// Cost class for PlanCostModel.  Symbols of the same class share an expression
/// cost constant from expression_cost.hpp; structural symbols are scored
/// directly by PlanCostModel rather than via a constant.
enum class CostClass : std::uint8_t {
  Arithmetic,  ///< Add, Sub, Mul, Div, Mod, Exp — expression_cost::kArithmetic.
  Comparison,  ///< Eq, Neq, Lt, Lte, Gt, Gte — expression_cost::kComparison.
  Boolean,     ///< And, Or, Xor — expression_cost::kBoolean.
  Unary,       ///< Not, UnaryMinus, UnaryPlus — expression_cost::kUnary.
  Identifier,  ///< Identifier — expression_cost::kIdentifier (+ child cost).
  Structural,  ///< Bind, Output, NamedOutput — scored by PlanCostModel directly.
  Leaf,        ///< Once, Symbol, Literal, ParamLookup — bind::kSymbolCost.
};

/// Per-symbol descriptor.  Each enum value MUST have a specialisation; missing
/// ones produce a compile error the first time the descriptor is queried
/// (e.g. via is_binary_op_v).
template <symbol S>
struct symbol_descriptor;  // primary template intentionally undefined

// clang-format off
// Structural / leaf symbols.
template<> struct symbol_descriptor<symbol::Once>        { static constexpr Arity arity = Arity::Leaf;    static constexpr CostClass cost_class = CostClass::Leaf;       };
template<> struct symbol_descriptor<symbol::Symbol>      { static constexpr Arity arity = Arity::Leaf;    static constexpr CostClass cost_class = CostClass::Leaf;       };
template<> struct symbol_descriptor<symbol::Literal>     { static constexpr Arity arity = Arity::Leaf;    static constexpr CostClass cost_class = CostClass::Leaf;       };
template<> struct symbol_descriptor<symbol::ParamLookup> { static constexpr Arity arity = Arity::Leaf;    static constexpr CostClass cost_class = CostClass::Leaf;       };
template<> struct symbol_descriptor<symbol::Identifier>  { static constexpr Arity arity = Arity::Unary;   static constexpr CostClass cost_class = CostClass::Identifier; };
template<> struct symbol_descriptor<symbol::Bind>        { static constexpr Arity arity = Arity::Special; static constexpr CostClass cost_class = CostClass::Structural; };
template<> struct symbol_descriptor<symbol::Output>      { static constexpr Arity arity = Arity::Special; static constexpr CostClass cost_class = CostClass::Structural; };
template<> struct symbol_descriptor<symbol::NamedOutput> { static constexpr Arity arity = Arity::Special; static constexpr CostClass cost_class = CostClass::Structural; };

// Arithmetic operators (binary).
template<> struct symbol_descriptor<symbol::Add>         { static constexpr Arity arity = Arity::Binary;  static constexpr CostClass cost_class = CostClass::Arithmetic; };
template<> struct symbol_descriptor<symbol::Sub>         { static constexpr Arity arity = Arity::Binary;  static constexpr CostClass cost_class = CostClass::Arithmetic; };
template<> struct symbol_descriptor<symbol::Mul>         { static constexpr Arity arity = Arity::Binary;  static constexpr CostClass cost_class = CostClass::Arithmetic; };
template<> struct symbol_descriptor<symbol::Div>         { static constexpr Arity arity = Arity::Binary;  static constexpr CostClass cost_class = CostClass::Arithmetic; };
template<> struct symbol_descriptor<symbol::Mod>         { static constexpr Arity arity = Arity::Binary;  static constexpr CostClass cost_class = CostClass::Arithmetic; };
template<> struct symbol_descriptor<symbol::Exp>         { static constexpr Arity arity = Arity::Binary;  static constexpr CostClass cost_class = CostClass::Arithmetic; };

// Comparison operators (binary).
template<> struct symbol_descriptor<symbol::Eq>          { static constexpr Arity arity = Arity::Binary;  static constexpr CostClass cost_class = CostClass::Comparison; };
template<> struct symbol_descriptor<symbol::Neq>         { static constexpr Arity arity = Arity::Binary;  static constexpr CostClass cost_class = CostClass::Comparison; };
template<> struct symbol_descriptor<symbol::Lt>          { static constexpr Arity arity = Arity::Binary;  static constexpr CostClass cost_class = CostClass::Comparison; };
template<> struct symbol_descriptor<symbol::Lte>         { static constexpr Arity arity = Arity::Binary;  static constexpr CostClass cost_class = CostClass::Comparison; };
template<> struct symbol_descriptor<symbol::Gt>          { static constexpr Arity arity = Arity::Binary;  static constexpr CostClass cost_class = CostClass::Comparison; };
template<> struct symbol_descriptor<symbol::Gte>         { static constexpr Arity arity = Arity::Binary;  static constexpr CostClass cost_class = CostClass::Comparison; };

// Boolean operators.
template<> struct symbol_descriptor<symbol::And>         { static constexpr Arity arity = Arity::Binary;  static constexpr CostClass cost_class = CostClass::Boolean;    };
template<> struct symbol_descriptor<symbol::Or>          { static constexpr Arity arity = Arity::Binary;  static constexpr CostClass cost_class = CostClass::Boolean;    };
template<> struct symbol_descriptor<symbol::Xor>         { static constexpr Arity arity = Arity::Binary;  static constexpr CostClass cost_class = CostClass::Boolean;    };

// Unary operators.
template<> struct symbol_descriptor<symbol::Not>         { static constexpr Arity arity = Arity::Unary;   static constexpr CostClass cost_class = CostClass::Unary;      };
template<> struct symbol_descriptor<symbol::UnaryMinus>  { static constexpr Arity arity = Arity::Unary;   static constexpr CostClass cost_class = CostClass::Unary;      };
template<> struct symbol_descriptor<symbol::UnaryPlus>   { static constexpr Arity arity = Arity::Unary;   static constexpr CostClass cost_class = CostClass::Unary;      };

// clang-format on

// ============================================================================
// Classification predicates — derived from descriptors.
// ============================================================================
//
// These are the entry points used by the rest of the planner-v2 code to ask
// "is this symbol binary?" / "is this symbol unary?".  They derive from
// symbol_descriptor<S>::arity, so adding a new symbol's descriptor automatically
// flips the right predicate.

template <symbol S>
constexpr bool is_binary_op_v = symbol_descriptor<S>::arity == Arity::Binary;

template <symbol S>
constexpr bool is_unary_op_v = symbol_descriptor<S>::arity == Arity::Unary;

template <symbol S>
constexpr bool is_leaf_v = symbol_descriptor<S>::arity == Arity::Leaf;

// ============================================================================
// Canonical enumeration of all symbols.
// ============================================================================
//
// AllSymbolsSeq is the single source of truth for "what symbols exist."  It
// drives:
//   * the exhaustiveness check below (every symbol must have a descriptor),
//   * `symbol_storage` in symbol_make_traits.hpp (combined storage type),
//   * any runtime symbol → property dispatch (e.g. CostClassOf in PlanCostModel).
//
// Adding a new symbol requires appending it here AND in the `symbol` enum
// above AND specialising symbol_descriptor.  All three are co-located so a
// missing entry is a compile error at the first query that needs it.
//
// We roll our own pack holder rather than using std::integer_sequence because
// the latter requires an integral underlying type, not a scoped enum.

template <symbol... Ss>
struct symbol_sequence {};

using AllSymbolsSeq =
    symbol_sequence<symbol::Once, symbol::Bind, symbol::Symbol, symbol::Literal, symbol::Identifier, symbol::Output,
                    symbol::NamedOutput, symbol::ParamLookup, symbol::Add, symbol::Sub, symbol::Mul, symbol::Div,
                    symbol::Mod, symbol::Exp, symbol::Eq, symbol::Neq, symbol::Lt, symbol::Lte, symbol::Gt, symbol::Gte,
                    symbol::And, symbol::Or, symbol::Xor, symbol::Not, symbol::UnaryMinus, symbol::UnaryPlus>;

// ============================================================================
// Exhaustiveness check — every enum value MUST have a descriptor.
// ============================================================================
//
// If you add a value to `enum class symbol` and forget the descriptor, the
// static_assert below fails at this site (clear named error) rather than at
// some random predicate query miles away.

namespace detail {

/// A symbol has a descriptor if `symbol_descriptor<S>::arity` names a value.
template <symbol S>
concept HasDescriptor = requires {
  { symbol_descriptor<S>::arity } -> std::convertible_to<Arity>;
  { symbol_descriptor<S>::cost_class } -> std::convertible_to<CostClass>;
};

/// Fold the concept check across the canonical sequence.  Defined as a
/// function template so the fold is *not* instantiated by merely including
/// this header — only the TU that actually evaluates this function pays the
/// cost.  The check fires from src/query/plan_v2/private_symbol.cpp.
template <symbol... Ss>
constexpr auto AllHaveDescriptorsImpl(symbol_sequence<Ss...>) -> bool {
  return (HasDescriptor<Ss> && ...);
}

}  // namespace detail

// ============================================================================
// Runtime symbol → CostClass dispatch.
// ============================================================================
//
// Maps a runtime symbol value to the descriptor's cost_class.  Used by
// PlanCostModel to dispatch expression operators uniformly: one switch arm in
// the cost model handles all binary expression operators by routing through
// this helper, instead of three separate case-groups (arithmetic / comparison /
// boolean) that would each need updating per new operator.
//
// Implemented as a fold over AllSymbolsSeq so adding a symbol's descriptor is
// the only edit needed; this function picks up the new entry automatically.

namespace detail {

template <symbol... Ss>
constexpr auto CostClassOfImpl(symbol s, symbol_sequence<Ss...>) -> CostClass {
  CostClass result{};
  bool const found = (((s == Ss) && ((result = symbol_descriptor<Ss>::cost_class), true)) || ...);
  // `found` should always be true — AllSymbolsSeq is exhaustive over the enum.
  // If a symbol is added to the enum but not to AllSymbolsSeq, this returns the
  // default-initialised CostClass; the static_assert above wouldn't fire (it
  // checks descriptors, not sequence membership).  Belt-and-braces:
  assert(found && "CostClassOf: symbol missing from AllSymbolsSeq — see private_symbol.hpp");
  return result;
}

}  // namespace detail

constexpr auto CostClassOf(symbol s) -> CostClass { return detail::CostClassOfImpl(s, AllSymbolsSeq{}); }

}  // namespace memgraph::query::plan::v2

namespace std {
using std::hash;

template <>
struct hash<memgraph::query::plan::v2::symbol> {
  size_t operator()(memgraph::query::plan::v2::symbol const &value) const noexcept { return std::to_underlying(value); }
};
}  // namespace std
