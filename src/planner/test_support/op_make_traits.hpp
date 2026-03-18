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

#include <vector>

#include "test_support/symbols.hpp"
#include "utils/small_vector.hpp"

import memgraph.planner.core.egraph;
import memgraph.planner.core.typed_egraph;

/// Trait specialisations for the test/benchmark `Op` symbol set. All Ops
/// lower as "just children, no storage, no disambiguator"; the toy
/// planner has no interning or scope semantics. Different arities are
/// modelled by the number of `EClassId` parameters in `make()`.
namespace memgraph::planner::core::test {

template <Op S>
struct op_make_traits;

// Nullary leaves: no children, no disambiguator.
template <Op S>
  requires(S == Op::Var || S == Op::Const || S == Op::A || S == Op::B || S == Op::C || S == Op::D || S == Op::X ||
           S == Op::Y || S == Op::Z || S == Op::Test)
struct op_make_traits<S> {
  struct storage_type {};

  static auto make(storage_type & /*s*/) -> LoweredNode { return {.children = {}, .disambiguator = std::nullopt}; }
};

// Unary nodes: one child.
template <Op S>
  requires(S == Op::Neg || S == Op::Ident)
struct op_make_traits<S> {
  struct storage_type {};

  static auto make(storage_type & /*s*/, EClassId child) -> LoweredNode {
    return {.children = utils::small_vector<EClassId>{child}, .disambiguator = std::nullopt};
  }
};

// Binary nodes: two children.
template <Op S>
  requires(S == Op::Add || S == Op::Mul || S == Op::Plus)
struct op_make_traits<S> {
  struct storage_type {};

  static auto make(storage_type & /*s*/, EClassId lhs, EClassId rhs) -> LoweredNode {
    return {.children = utils::small_vector<EClassId>{lhs, rhs}, .disambiguator = std::nullopt};
  }
};

// Ternary nodes (used by multi-pattern join tests).
template <Op S>
  requires(S == Op::Bind || S == Op::F3)
struct op_make_traits<S> {
  struct storage_type {};

  static auto make(storage_type & /*s*/, EClassId a, EClassId b, EClassId c) -> LoweredNode {
    return {.children = utils::small_vector<EClassId>{a, b, c}, .disambiguator = std::nullopt};
  }
};

// Unary-or-N-ary infrastructure-test symbols: take an explicit child list.
template <Op S>
  requires(S == Op::F || S == Op::F2 || S == Op::G || S == Op::H)
struct op_make_traits<S> {
  struct storage_type {};

  static auto make(storage_type & /*s*/, std::vector<EClassId> args) -> LoweredNode {
    return {.children = utils::small_vector<EClassId>(args.begin(), args.end()), .disambiguator = std::nullopt};
  }
};

using AllOpsSeq =
    SymbolSequence<Op, Op::Add, Op::Mul, Op::Neg, Op::Var, Op::Const, Op::A, Op::B, Op::C, Op::D, Op::X, Op::Y, Op::Z,
                   Op::F, Op::F2, Op::F3, Op::G, Op::H, Op::Plus, Op::Bind, Op::Ident, Op::Test>;

using TypedTestEGraph = TypedEGraph<Op, NoAnalysis, AllOpsSeq, op_make_traits>;

}  // namespace memgraph::planner::core::test
