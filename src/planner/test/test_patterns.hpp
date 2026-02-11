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

#include "planner/pattern/pattern.hpp"
#include "test_symbols.hpp"

namespace memgraph::planner::core::test {

// ============================================================================
// Common Pattern Variables
// ============================================================================
//
// Named pattern variables for use in tests. Using named constants instead of
// raw PatternVar{0} improves readability and prevents ID collisions.
//
// Naming conventions:
//   kVarX, kVarY, kVarZ: generic variables (?x, ?y, ?z)
//   kVarA, kVarB, kVarC: alternative naming when x/y/z are awkward
//   kVarRoot*: for binding pattern roots to variables
// ============================================================================

// Pattern variables (?x, ?y, ?z, ?w)
inline constexpr PatternVar kVarX{0};
inline constexpr PatternVar kVarY{1};
inline constexpr PatternVar kVarZ{2};
inline constexpr PatternVar kVarW{3};

// Root binding variables
inline constexpr PatternVar kVarRoot{10};
inline constexpr PatternVar kVarDoubleNegRoot{11};
inline constexpr PatternVar kVarAddRoot{12};
inline constexpr PatternVar kVarMulRoot{13};

// Chain/join test variables
inline constexpr PatternVar kVarRootP1{20};
inline constexpr PatternVar kVarRootP2{21};
inline constexpr PatternVar kVarRootP3{22};

// Alternative generic variables (?a, ?b, ?c) for multi-pattern tests
inline constexpr PatternVar kVarA{30};
inline constexpr PatternVar kVarB{31};
inline constexpr PatternVar kVarC{32};

// Root bindings for join/multi-pattern tests
inline constexpr PatternVar kVarRootA{33};
inline constexpr PatternVar kVarRootB{34};
inline constexpr PatternVar kVarRootC{35};
inline constexpr PatternVar kVarRootConst{36};
inline constexpr PatternVar kVarRootNeg{37};

// Arbitrary IDs for testing variable ID handling
inline constexpr PatternVar kVarArbitrary{42};
inline constexpr PatternVar kTestRoot{100};

// ============================================================================
// Type Aliases
// ============================================================================

using TestPattern = Pattern<Op>;

// ============================================================================
// Pattern Helpers
// ============================================================================

/**
 * @brief Create a variable-only pattern: ?var
 *
 * Variable patterns match any e-class and are useful for testing
 * wildcard matching behavior.
 */
inline auto make_var_pattern(PatternVar var) -> TestPattern {
  auto builder = TestPattern::Builder{};
  builder.var(var);
  return std::move(builder).build();
}

/**
 * @brief Create a leaf symbol pattern: Op()
 *
 * @param op The operation symbol
 * @param root_binding Optional variable to bind the matched e-class
 */
inline auto make_leaf_pattern(Op op, std::optional<PatternVar> root_binding = std::nullopt) -> TestPattern {
  return TestPattern::build(op, root_binding);
}

/**
 * @brief Create a unary pattern: Op(?var)
 */
inline auto make_unary_pattern(Op op, PatternVar var, std::optional<PatternVar> root_binding = std::nullopt)
    -> TestPattern {
  return TestPattern::build(op, {Var{var}}, root_binding);
}

/**
 * @brief Create a binary pattern: Op(?var_x, ?var_y)
 */
inline auto make_binary_pattern(Op op, PatternVar var_x, PatternVar var_y,
                                std::optional<PatternVar> root_binding = std::nullopt) -> TestPattern {
  return TestPattern::build(op, {Var{var_x}, Var{var_y}}, root_binding);
}

/**
 * @brief Create a double negation pattern: Neg(Neg(?x))
 *
 * Binds the outer Neg to kVarDoubleNegRoot and the inner variable to kVarX.
 * Used for double negation elimination: Neg(Neg(?x)) -> ?x
 */
inline auto make_double_neg_pattern() -> TestPattern {
  return TestPattern::build(Op::Neg, {Sym(Op::Neg, Var{kVarX})}, kVarDoubleNegRoot);
}

}  // namespace memgraph::planner::core::test
