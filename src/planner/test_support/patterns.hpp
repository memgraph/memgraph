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
#include "test_support/types.hpp"

namespace memgraph::planner::core::test {

using pattern::PatternVar;
using pattern::dsl::BoundSym;
using pattern::dsl::Sym;
using pattern::dsl::Var;
using pattern::dsl::Wildcard;

// ============================================================================
// Pattern Variable IDs
// ============================================================================
// Named PatternVar constants for tests and benchmarks. Using named constants
// instead of raw PatternVar{N} improves readability and prevents ID collisions.

// Generic variables (?x, ?y, ?z, ?w)
inline constexpr PatternVar kVarX{0};
inline constexpr PatternVar kVarY{1};
inline constexpr PatternVar kVarZ{2};
inline constexpr PatternVar kVarW{3};

// Root binding variables
inline constexpr PatternVar kVarRoot{10};
inline constexpr PatternVar kVarDoubleNegRoot{11};
inline constexpr PatternVar kVarAddRoot{12};
inline constexpr PatternVar kVarMulRoot{13};
inline constexpr PatternVar kVarNegRoot{14};

// Chain/join test variables
inline constexpr PatternVar kVarRootP1{20};
inline constexpr PatternVar kVarRootP2{21};
inline constexpr PatternVar kVarRootP3{22};

// Bind/Ident join variables
inline constexpr PatternVar kVarSym{30};
inline constexpr PatternVar kVarExpr{31};
inline constexpr PatternVar kBindRoot{32};
inline constexpr PatternVar kIdentRoot{33};

// Alternative generic variables (?a, ?b, ?c) for multi-pattern tests
inline constexpr PatternVar kVarA{40};
inline constexpr PatternVar kVarB{41};
inline constexpr PatternVar kVarC{42};

// Eclass-level hoisting variables
inline constexpr PatternVar kHoistR{50};  // ?r = F(?x) root binding
inline constexpr PatternVar kHoistY{51};  // Mul(?r, ?y) sibling binding

// Root bindings for join/multi-pattern tests
inline constexpr PatternVar kVarRootA{63};
inline constexpr PatternVar kVarRootB{64};
inline constexpr PatternVar kVarRootC{65};
inline constexpr PatternVar kVarRootConst{66};
inline constexpr PatternVar kVarRootNeg{67};

// Arbitrary IDs for testing variable ID handling
inline constexpr PatternVar kVarArbitrary{99};
inline constexpr PatternVar kTestRoot{100};

// ============================================================================
// Pattern Helpers
// ============================================================================

inline auto make_var_pattern(PatternVar var) -> TestPattern {
  auto builder = TestPattern::Builder{};
  builder.var(var);
  return std::move(builder).build();
}

inline auto make_wildcard_pattern() -> TestPattern {
  auto builder = TestPattern::Builder{};
  builder.wildcard();
  return std::move(builder).build();
}

/// Neg(Neg(?x)) bound to kVarDoubleNegRoot. Used for double-neg elimination.
inline auto make_double_neg_pattern() -> TestPattern {
  return TestPattern::build(kVarDoubleNegRoot, Op::Neg, {Sym(Op::Neg, Var{kVarX})});
}

// Named pattern factories used by benchmarks.

inline auto PatternAdd() { return TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}}); }

inline auto PatternAddSameVar() { return TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarX}}); }

inline auto PatternDoubleNeg() { return TestPattern::build(kVarDoubleNegRoot, Op::Neg, {Sym(Op::Neg, Var{kVarX})}); }

inline auto PatternSelective() { return TestPattern::build(Op::Add, {Sym(Op::Neg, Var{kVarX}), Var{kVarY}}); }

inline auto PatternNestedNeg(int depth) -> TestPattern {
  auto b = TestPattern::Builder{};
  auto cur = b.var(kVarX);
  for (int i = 0; i < depth; ++i) cur = b.sym(Op::Neg, {cur});
  return std::move(b).build();
}

inline auto PatternNeg() { return TestPattern::build(Op::Neg, {Var{kVarX}}); }

inline auto PatternNestedF() { return TestPattern::build(Op::F, {Sym(Op::F, Var{kVarX})}); }

inline auto PatternShallowF() { return TestPattern::build(Op::F, {Var{kVarX}}); }

inline auto PatternDeepNestedF() { return TestPattern::build(Op::F, {Sym(Op::F, Sym(Op::F, Sym(Op::F, Var{kVarX})))}); }

inline auto PatternBind() { return TestPattern::build(kBindRoot, Op::Bind, {Wildcard{}, Var{kVarSym}, Var{kVarExpr}}); }

inline auto PatternIdent() { return TestPattern::build(kIdentRoot, Op::Ident, {Var{kVarSym}}); }

inline auto PatternHoistAnchor() { return TestPattern::build(kHoistR, Op::F, {Var{kVarX}}); }

inline auto PatternHoistJoined() { return TestPattern::build(Op::Mul, {Var{kHoistR}, Var{kHoistY}}); }

}  // namespace memgraph::planner::core::test
