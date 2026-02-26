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

#include <gtest/gtest.h>

#include "test_egraph.hpp"

import memgraph.planner.core.eids;

namespace memgraph::planner::core::test {

// ============================================================================
// E-Graph Test Fixture Base
// ============================================================================
//
// Provides a declarative DSL for building e-graphs in tests:
//
//   node(Op::F,
//     node(Op::Add, leaf(Op::Var, 1), leaf(Op::Var, 2)),
//     node(Op::Mul, leaf(Op::Var, 3), leaf(Op::Var, 4)));
//
// Produces:
//            F
//          /   \
//        Add   Mul
//       / \    / \
//      v1  v2 v3  v4

class EGraphTestBase : public ::testing::Test {
 protected:
  TestEGraph egraph;
  TestEMatcher matcher{egraph};
  ProcessingContext<Op> proc_ctx_;

  // ---------------------------------------------------------------------------
  // E-Graph Building DSL
  // ---------------------------------------------------------------------------

  /// Create a leaf node with the given op and optional disambiguator.
  auto leaf(Op op, int disambiguator = 0) -> EClassId { return egraph.emplace(op, disambiguator).eclass_id; }

  /// Create a node with the given op and children.
  template <typename... Children>
  auto node(Op op, Children... children) -> EClassId {
    return egraph.emplace(op, {children...}).eclass_id;
  }

  /// Merge two e-classes. Returns the canonical e-class ID.
  auto merge(EClassId a, EClassId b) -> EClassId { return egraph.merge(a, b).eclass_id; }

  /// Rebuild the e-graph and matcher index to restore invariants after merges.
  void rebuild_egraph() {
    egraph.rebuild(proc_ctx_);
    matcher.rebuild_index();
  }
};

}  // namespace memgraph::planner::core::test
