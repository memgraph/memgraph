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
  // N-ary functions (semantically neutral - use for infrastructure tests)
  F,
  F2,
  F3,
  // For multi-pattern join tests
  Bind,   // Bind(_, ?sym, ?expr) - 3 children
  Ident,  // Ident(?sym) - 1 child
  // Misc
  Test,
};

/// Empty analysis type for tests that don't need e-class analysis
struct NoAnalysis {};

}  // namespace memgraph::planner::core::test
