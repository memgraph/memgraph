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
#include "utils/small_vector.hpp"

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
  std::unordered_map<ENodeId, std::vector<EClassId>> enode_to_parents;

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
  auto id1 = egraph.emplace(TestSymbol::A, utils::small_vector<EClassId>{});
  auto id2 = egraph.emplace(TestSymbol::B, utils::small_vector<EClassId>{});

  EXPECT_EQ(egraph.num_classes(), 2);
  EXPECT_EQ(egraph.num_nodes(), 2);
  EXPECT_NE(id1, id2);
  EXPECT_TRUE(egraph.has_class(id1));
  EXPECT_TRUE(egraph.has_class(id2));
}

TEST(EGraphBasicOperations, AddNodesWithChildren) {
  EGraph<TestSymbol, NoAnalysis> egraph;

  // Add leaf nodes first
  auto a = egraph.emplace(TestSymbol::A, utils::small_vector<EClassId>{});
  auto b = egraph.emplace(TestSymbol::B, utils::small_vector<EClassId>{});

  // Add node with children
  auto plus = egraph.emplace(TestSymbol::Plus, utils::small_vector<EClassId>{a, b});

  EXPECT_EQ(egraph.num_classes(), 3);
  EXPECT_EQ(egraph.num_nodes(), 3);
  EXPECT_TRUE(egraph.has_class(plus));
}

TEST(EGraphBasicOperations, DuplicateNodesReturnSameEClass) {
  EGraph<TestSymbol, NoAnalysis> egraph;

  // Add same node twice
  auto id1 = egraph.emplace(TestSymbol::A, utils::small_vector<EClassId>{});
  auto id2 = egraph.emplace(TestSymbol::A, utils::small_vector<EClassId>{});

  EXPECT_EQ(id1, id2);
  EXPECT_EQ(egraph.num_classes(), 1);
  EXPECT_EQ(egraph.num_nodes(), 1);
}

TEST(EGraphMergingOperations, MergeTwoDifferentEClasses) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  auto id1 = egraph.emplace(TestSymbol::A, utils::small_vector<EClassId>{});
  auto id2 = egraph.emplace(TestSymbol::B, utils::small_vector<EClassId>{});

  EXPECT_EQ(egraph.num_classes(), 2);

  auto merged = egraph.merge(id1, id2, ctx);

  EXPECT_EQ(egraph.num_classes(), 1);
  EXPECT_EQ(egraph.find(id1, ctx), egraph.find(id2, ctx));
  EXPECT_TRUE(merged == id1 || merged == id2);
}

TEST(EGraphMergingOperations, MergeSameEClassIsNoOp) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  auto id1 = egraph.emplace(TestSymbol::A, utils::small_vector<EClassId>{});
  auto merged = egraph.merge(id1, id1, ctx);

  EXPECT_EQ(merged, id1);
  EXPECT_EQ(egraph.num_classes(), 1);
}

TEST(EGraphMergingOperations, CongruenceAfterMerge) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  // Create: f(a), f(b), merge a and b
  auto a = egraph.emplace(TestSymbol::A, utils::small_vector<EClassId>{});
  auto b = egraph.emplace(TestSymbol::B, utils::small_vector<EClassId>{});
  auto fa = egraph.emplace(TestSymbol::F, utils::small_vector<EClassId>{a});
  auto fb = egraph.emplace(TestSymbol::F, utils::small_vector<EClassId>{b});

  EXPECT_EQ(egraph.num_classes(), 4);
  EXPECT_NE(egraph.find(fa, ctx), egraph.find(fb, ctx));

  // Merge a and b
  egraph.merge(a, b, ctx);

  // f(a) and f(b) should now be congruent
  EXPECT_EQ(egraph.find(fa, ctx), egraph.find(fb, ctx));
  EXPECT_EQ(egraph.num_classes(), 2);  // One for a=b, one for f(a)=f(b)
}

TEST(EGraphEClassAccess, GetEClassByID) {
  EGraph<TestSymbol, NoAnalysis> egraph;

  auto id = egraph.emplace(TestSymbol::Test, utils::small_vector<EClassId>{});

  const auto &eclass = egraph.eclass(id);
  EXPECT_EQ(eclass.size(), 1);

  auto repr_id = eclass.representative_id();
  const auto &repr = egraph.get_enode(repr_id);
  EXPECT_EQ(repr.symbol(), TestSymbol::Test);
}

TEST(EGraphCongruenceDetailed, CongruenceWithParentTracking) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  // This covers debug_congruence.cpp scenarios
  auto a = egraph.emplace(TestSymbol::A, utils::small_vector<EClassId>{});
  auto b = egraph.emplace(TestSymbol::B, utils::small_vector<EClassId>{});
  auto fa = egraph.emplace(TestSymbol::F, utils::small_vector<EClassId>{a});
  auto fb = egraph.emplace(TestSymbol::F, utils::small_vector<EClassId>{b});

  // Verify initial state
  EXPECT_EQ(egraph.num_classes(), 4);
  EXPECT_NE(egraph.find(fa, ctx), egraph.find(fb, ctx));

  // Check parent tracking before merge
  const auto &eclass_a = egraph.eclass(a);
  bool has_parent_fa = false;
  for (const auto &[parent_enode_id, parent_id] : eclass_a.parents()) {
    if (parent_id == fa) has_parent_fa = true;
  }
  EXPECT_TRUE(has_parent_fa);

  // Merge and verify congruence
  egraph.merge(a, b, ctx);
  EXPECT_EQ(egraph.find(fa, ctx), egraph.find(fb, ctx));
  EXPECT_EQ(egraph.num_classes(), 2);
}

TEST(EGraphCongruenceDetailed, RebuildingAfterMerge) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  // This covers debug_rebuilding.cpp scenarios
  auto a = egraph.emplace(TestSymbol::A, utils::small_vector<EClassId>{});
  auto b = egraph.emplace(TestSymbol::B, utils::small_vector<EClassId>{});
  auto fa = egraph.emplace(TestSymbol::F, utils::small_vector<EClassId>{a});
  auto fb = egraph.emplace(TestSymbol::F, utils::small_vector<EClassId>{b});

  // Merge triggers rebuild internally
  egraph.merge(a, b, ctx);

  // Verify congruence is maintained
  EXPECT_EQ(egraph.find(fa, ctx), egraph.find(fb, ctx));
}

TEST(EGraphCongruenceDetailed, EnodeCountingAccuracy) {
  EGraph<TestSymbol, NoAnalysis> egraph;

  // This covers debug_enode_count.cpp scenarios
  EXPECT_EQ(egraph.num_enodes(), 0);

  auto a = egraph.emplace(TestSymbol::A, utils::small_vector<EClassId>{});
  EXPECT_EQ(egraph.num_enodes(), 1);

  [[maybe_unused]] auto b = egraph.emplace(TestSymbol::B, utils::small_vector<EClassId>{});
  EXPECT_EQ(egraph.num_enodes(), 2);

  auto c = egraph.emplace(TestSymbol::C, utils::small_vector<EClassId>{});
  auto d = egraph.emplace(TestSymbol::D, utils::small_vector<EClassId>{});
  EXPECT_EQ(egraph.num_enodes(), 4);

  auto f = egraph.emplace(TestSymbol::F, utils::small_vector<EClassId>{c, d});
  EXPECT_EQ(egraph.num_enodes(), 5);

  // Adding duplicate should not increase count
  auto f2 = egraph.emplace(TestSymbol::F, utils::small_vector<EClassId>{c, d});
  EXPECT_EQ(f, f2);
  EXPECT_EQ(egraph.num_enodes(), 5);
}

TEST(EGraphClearAndReserve, ClearEmptiesTheGraph) {
  EGraph<TestSymbol, NoAnalysis> egraph;

  egraph.emplace(TestSymbol::A, utils::small_vector<EClassId>{});
  egraph.emplace(TestSymbol::B, utils::small_vector<EClassId>{});

  EXPECT_FALSE(egraph.empty());

  egraph.clear();

  EXPECT_TRUE(egraph.empty());
  EXPECT_EQ(egraph.num_classes(), 0);
  EXPECT_EQ(egraph.num_nodes(), 0);
}

TEST(EGraphClearAndReserve, ReserveAllocatesCapacity) {
  EGraph<TestSymbol, NoAnalysis> egraph;

  egraph.reserve(1000);

  // Add many nodes - should not trigger reallocations
  for (int i = 0; i < 100; ++i) {
    egraph.emplace(static_cast<TestSymbol>(static_cast<uint32_t>(TestSymbol::Node0) + i),
                   utils::small_vector<EClassId>{});
  }

  EXPECT_EQ(egraph.num_classes(), 100);
}

// Note: to_string() method is not yet implemented in the EGraph class
// These tests are commented out until the method is added
/*
TEST(EGraphStringRepresentation, EmptyGraphString) {
  EGraph<TestSymbol, NoAnalysis> egraph;

  auto str = egraph.to_string();
  EXPECT_NE(str.find("0 classes"), std::string::npos);
  EXPECT_NE(str.find("0 nodes"), std::string::npos);
}

TEST(EGraphStringRepresentation, NonEmptyGraphString) {
  EGraph<TestSymbol, NoAnalysis> egraph;

  egraph.emplace(TestSymbol::Test, {});

  auto str = egraph.to_string();
  EXPECT_NE(str.find("1 classes"), std::string::npos);
  EXPECT_NE(str.find("1 nodes"), std::string::npos);
  EXPECT_NE(str.find("test"), std::string::npos);
}
*/

TEST(EGraphComplexOperations, BuildArithmeticExpressionTree) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  // Build: (x + y) * (x + y)
  auto x = egraph.emplace(TestSymbol::X, utils::small_vector<EClassId>{});
  auto y = egraph.emplace(TestSymbol::Y, utils::small_vector<EClassId>{});
  auto plus1 = egraph.emplace(TestSymbol::Plus, utils::small_vector<EClassId>{x, y});
  auto plus2 = egraph.emplace(TestSymbol::Plus, utils::small_vector<EClassId>{x, y});
  auto mult = egraph.emplace(TestSymbol::Mul, utils::small_vector<EClassId>{plus1, plus2});

  EXPECT_EQ(egraph.num_classes(), 4);                           // x, y, +(x,y), *(+(x,y), +(x,y))
  EXPECT_EQ(egraph.find(plus1, ctx), egraph.find(plus2, ctx));  // Congruent +

  // Verify structure
  const auto &mult_class = egraph.eclass(mult);
  auto mult_node_id = mult_class.representative_id();
  const auto &mult_node = egraph.get_enode(mult_node_id);
  EXPECT_EQ(mult_node.symbol(), TestSymbol::Mul);
  EXPECT_EQ(mult_node.arity(), 2);

  // Both children should point to the same e-class
  EXPECT_EQ(egraph.find(mult_node.children()[0], ctx), egraph.find(mult_node.children()[1], ctx));
}

TEST(EGraphComplexOperations, AssociativityMerge) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  // Build: (a + b) + c and a + (b + c)
  auto a = egraph.emplace(TestSymbol::A, utils::small_vector<EClassId>{});
  auto b = egraph.emplace(TestSymbol::B, utils::small_vector<EClassId>{});
  auto c = egraph.emplace(TestSymbol::C, utils::small_vector<EClassId>{});

  auto ab = egraph.emplace(TestSymbol::Plus, utils::small_vector<EClassId>{a, b});
  auto abc1 = egraph.emplace(TestSymbol::Plus, utils::small_vector<EClassId>{ab, c});

  auto bc = egraph.emplace(TestSymbol::Plus, utils::small_vector<EClassId>{b, c});
  auto abc2 = egraph.emplace(TestSymbol::Plus, utils::small_vector<EClassId>{a, bc});

  EXPECT_NE(egraph.find(abc1, ctx), egraph.find(abc2, ctx));

  // Merge them to represent associativity
  egraph.merge(abc1, abc2, ctx);

  EXPECT_EQ(egraph.find(abc1, ctx), egraph.find(abc2, ctx));
}

TEST(EGraphComplexOperations, NextClassIdIsMonotonic) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  // next_class_id should never decrease, even after merges reduce num_classes

  // Initially empty
  auto initial_next = egraph.next_class_id();
  EXPECT_EQ(initial_next, EClassId(0));  // 0 is the first ID that will be assigned

  // Add first class
  auto id1 = egraph.emplace(TestSymbol::A, utils::small_vector<EClassId>{});
  auto next1 = egraph.next_class_id();
  EXPECT_EQ(next1, EClassId(1));  // Next ID after 0
  EXPECT_EQ(egraph.num_classes(), 1);

  // Add second class
  auto id2 = egraph.emplace(TestSymbol::B, utils::small_vector<EClassId>{});
  auto next2 = egraph.next_class_id();
  EXPECT_EQ(next2, EClassId(2));  // Next ID after 0,1
  EXPECT_GT(next2, next1);        // Monotonic increase
  EXPECT_EQ(egraph.num_classes(), 2);

  // Add third class
  auto id3 = egraph.emplace(TestSymbol::C, utils::small_vector<EClassId>{});
  auto next3 = egraph.next_class_id();
  EXPECT_EQ(next3, EClassId(3));  // Next ID after 0,1,2
  EXPECT_GT(next3, next2);        // Monotonic increase
  EXPECT_EQ(egraph.num_classes(), 3);

  // Merge classes - this reduces num_classes but next_class_id should NOT decrease
  egraph.merge(id1, id2, ctx);
  auto next_after_merge1 = egraph.next_class_id();
  EXPECT_EQ(next_after_merge1, next3);  // Should still be 3, not decreased!
  EXPECT_EQ(egraph.num_classes(), 2);   // Only 2 classes now

  // Another merge
  egraph.merge(id2, id3, ctx);
  auto next_after_merge2 = egraph.next_class_id();
  EXPECT_EQ(next_after_merge2, next3);  // Should still be 3, not decreased!
  EXPECT_EQ(egraph.num_classes(), 1);   // Only 1 class now

  // Add a new class after merges
  auto id4 = egraph.emplace(TestSymbol::D, utils::small_vector<EClassId>{});
  auto next4 = egraph.next_class_id();
  EXPECT_EQ(next4, EClassId(4));        // Next ID after 0,1,2,3
  EXPECT_GT(next4, next_after_merge2);  // Monotonic increase from previous next
  EXPECT_EQ(egraph.num_classes(), 2);   // 2 classes again

  // Verify monotonic property held throughout
  EXPECT_LE(initial_next, next1);
  EXPECT_LE(next1, next2);
  EXPECT_LE(next2, next3);
  EXPECT_LE(next3, next_after_merge1);
  EXPECT_LE(next_after_merge1, next_after_merge2);
  EXPECT_LE(next_after_merge2, next4);
}

}  // namespace memgraph::planner::core
