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
#include <format>
#include <string_view>

namespace memgraph::planner::core::test {

// ============================================================================
// Test Symbols
// ============================================================================
//
// Common symbol enum for planner unit tests. Represents a minimal expression
// language for testing e-graph and pattern matching operations.
// ============================================================================

/**
 * @brief Test symbols for e-graph and pattern tests
 *
 * Includes:
 *   - Add, Mul: binary operators
 *   - Neg: unary operator
 *   - Var, Const: generic terminals
 *   - A, B, C, D, X, Y: named terminals (when tests need distinguishable leaves)
 *   - F, F2, F3: N-ary functions (semantically neutral, for testing infrastructure)
 */
enum class Op : uint8_t {
  // Binary operators (with mathematical meaning - use for semantic tests)
  Add,
  Mul,
  // Unary operator (with mathematical meaning - use for semantic tests)
  Neg,
  // Generic terminals
  Var,
  Const,
  // Named terminals
  A,
  B,
  C,
  D,
  X,
  Y,
  Z,
  // N-ary functions (semantically neutral - use for infrastructure tests)
  F,
  F2,
  F3,
  G,
  H,
  Plus,
  // For multi-pattern join tests
  Bind,   // Bind(_, ?sym, ?expr) - 3 children
  Ident,  // Ident(?sym) - 1 child
  // Misc
  Test,
};

/// Empty analysis type for tests that don't need e-class analysis
struct NoAnalysis {};

/// Convert Op to string for debugging
constexpr auto op_to_string(Op op) -> std::string_view {
  switch (op) {
    case Op::Add:
      return "Add";
    case Op::Mul:
      return "Mul";
    case Op::Neg:
      return "Neg";
    case Op::Var:
      return "Var";
    case Op::Const:
      return "Const";
    case Op::A:
      return "A";
    case Op::B:
      return "B";
    case Op::C:
      return "C";
    case Op::D:
      return "D";
    case Op::X:
      return "X";
    case Op::Y:
      return "Y";
    case Op::Z:
      return "Z";
    case Op::F:
      return "F";
    case Op::F2:
      return "F2";
    case Op::F3:
      return "F3";
    case Op::G:
      return "G";
    case Op::H:
      return "H";
    case Op::Plus:
      return "Plus";
    case Op::Bind:
      return "Bind";
    case Op::Ident:
      return "Ident";
    case Op::Test:
      return "Test";
  }
  return "Unknown";
}

}  // namespace memgraph::planner::core::test

template <>
struct std::formatter<memgraph::planner::core::test::Op> : std::formatter<std::string_view> {
  auto format(memgraph::planner::core::test::Op op, std::format_context &ctx) const {
    return std::formatter<std::string_view>::format(memgraph::planner::core::test::op_to_string(op), ctx);
  }
};
