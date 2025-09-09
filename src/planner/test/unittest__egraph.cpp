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

#include <gtest/gtest.h>

#include <unordered_map>

#include "planner/core/egraph.hpp"
#include "planner/core/union_find.hpp"

namespace memgraph::planner::core {

// Simple test symbol type using enum to be trivially copyable
// TODO: both unittest__enode and this unittest__egraph use TestSymbols, we should unify into a common test helper
// header
enum class TestSymbol : uint32_t {
  A,
  B,
  C,
  D,
  X,
  Y,
  Plus,
  Mul,
  F,
  Test,
  Node,
  // For numbered nodes
  Node0 = 1000,
  // We can generate more by adding to Node0
};

struct NoAnalysis {};

// Simple processing context for testing
// TODO: this should be moved into a production header this is not test code, but actual required for enode usage
template <typename Symbol>
class ProcessingContext {
 public:
  // Storage for enode to parents mapping used during rebuilding
  std::unordered_map<ENodeRef<Symbol>, std::vector<EClassId>> enode_to_parents;

  // Context for union-find operations
  UnionFindContext union_find_context;
};

TEST(EGraphBasicOperations, EmptyEGraph) {
  EGraph<TestSymbol, NoAnalysis> egraph;

  EXPECT_TRUE(egraph.empty());
  EXPECT_EQ(egraph.num_classes(), 0);
  EXPECT_EQ(egraph.num_nodes(), 0);
}

TEST(EGraphBasicOperations, AddSimpleENodes) {
  EGraph<TestSymbol, NoAnalysis> egraph;

  // Add leaf nodes
  auto id1 = egraph.emplace(TestSymbol::A);
  auto id2 = egraph.emplace(TestSymbol::B);

  EXPECT_EQ(egraph.num_classes(), 2);
  EXPECT_EQ(egraph.num_nodes(), 2);
  EXPECT_NE(id1, id2);
  EXPECT_TRUE(egraph.has_class(id1));
  EXPECT_TRUE(egraph.has_class(id2));
}

TEST(EGraphBasicOperations, AddNodesWithChildren) {
  EGraph<TestSymbol, NoAnalysis> egraph;

  // Add leaf nodes first
  auto a = egraph.emplace(TestSymbol::A);
  auto b = egraph.emplace(TestSymbol::B);

  // Add node with children
  EClassId plus = egraph.emplace(TestSymbol::Plus, {a, b});

  EXPECT_EQ(egraph.num_classes(), 3);
  EXPECT_EQ(egraph.num_nodes(), 3);
  EXPECT_TRUE(egraph.has_class(plus));
  auto const &enode = egraph.get_enode(plus);
  EXPECT_EQ(enode.arity(), 2);

  auto const &eclass = egraph.eclass(plus);
  EXPECT_EQ(eclass.size(), 1);
  EXPECT_EQ(eclass.representative_id(), plus);
}

TEST(EGraphBasicOperations, DuplicateNodesReturnSameEClass) {
  EGraph<TestSymbol, NoAnalysis> egraph;

  // Add same node twice
  auto id1 = egraph.emplace(TestSymbol::A, 0);
  auto id2 = egraph.emplace(TestSymbol::A, 0);

  EXPECT_EQ(id1, id2);
  EXPECT_EQ(egraph.num_classes(), 1);
  EXPECT_EQ(egraph.num_nodes(), 1);
}

TEST(EGraphMergingOperations, MergeTwoDifferentEClasses) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  auto id1 = egraph.emplace(TestSymbol::A);
  auto id2 = egraph.emplace(TestSymbol::B);

  EXPECT_EQ(egraph.num_classes(), 2);

  auto merged = egraph.merge(id1, id2);

  EXPECT_EQ(egraph.num_classes(), 1);
  EXPECT_EQ(egraph.find(id1), egraph.find(id2));
  EXPECT_TRUE(merged == id1 || merged == id2);
}

TEST(EGraphMergingOperations, MergeSameEClassIsNoOp) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  auto id1 = egraph.emplace(TestSymbol::A);
  auto merged = egraph.merge(id1, id1);

  EXPECT_EQ(merged, id1);
  EXPECT_EQ(egraph.num_classes(), 1);
}

TEST(EGraphMergingOperations, CongruenceAfterMergeAndRebuild) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  // Create: f(a), f(b), merge a and b
  auto a = egraph.emplace(TestSymbol::A);
  auto b = egraph.emplace(TestSymbol::B);
  auto fa = egraph.emplace(TestSymbol::F, {a});
  auto fb = egraph.emplace(TestSymbol::F, {b});

  EXPECT_EQ(egraph.num_classes(), 4);
  EXPECT_NE(egraph.find(fa), egraph.find(fb));

  // Merge a and b
  egraph.merge(a, b);

  // a and b should be same e-class
  EXPECT_EQ(egraph.find(a), egraph.find(b));

  // f(a) and f(b) should not yet be congruent
  EXPECT_NE(egraph.find(fa), egraph.find(fb));

  // Need to rebuild to update congruence
  egraph.rebuild(ctx);

  // f(a) and f(b) should now be congruent
  EXPECT_EQ(egraph.find(fa), egraph.find(fb));

  EXPECT_EQ(egraph.num_classes(), 2);  // One for a=b, one for f(a)=f(b)
}

TEST(EGraphEClassAccess, GetEClassByID) {
  EGraph<TestSymbol, NoAnalysis> egraph;

  auto id = egraph.emplace(TestSymbol::Test);

  const auto &eclass = egraph.eclass(id);
  EXPECT_EQ(eclass.size(), 1);

  auto repr_id = eclass.representative_id();
  const auto &repr = egraph.get_enode(repr_id);
  EXPECT_EQ(repr.symbol(), TestSymbol::Test);
}

TEST(EGraphClearAndReserve, ClearEmptiesTheGraph) {
  EGraph<TestSymbol, NoAnalysis> egraph;

  egraph.emplace(TestSymbol::A);
  egraph.emplace(TestSymbol::B);

  EXPECT_FALSE(egraph.empty());

  egraph.clear();

  EXPECT_TRUE(egraph.empty());
  EXPECT_EQ(egraph.num_classes(), 0);
  EXPECT_EQ(egraph.num_nodes(), 0);
}

}  // namespace memgraph::planner::core
