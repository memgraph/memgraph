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

#include <gtest/gtest.h>

#include "test_symbols.hpp"
#include "utils/small_vector.hpp"

import memgraph.planner.core.egraph;
import memgraph.planner.core.union_find;

namespace memgraph::planner::core {

using namespace test;
using TestNode = ENode<Op>;

static_assert(ENodeSymbol<Op>);
static_assert(ENodeSymbol<int>);

// --- Leaf Node Tests ---

TEST(Core_ENode, LeafNodeHasNoChildren) {
  TestNode x(Op::Var, {}, 1);

  EXPECT_TRUE(x.is_leaf());
  EXPECT_EQ(x.arity(), 0);
  EXPECT_TRUE(x.children().empty());
}

TEST(Core_ENode, LeafNodePreservesSymbol) {
  TestNode x(Op::Var, {}, 1);

  EXPECT_EQ(x.symbol(), Op::Var);
}

TEST(Core_ENode, LeafNodePreservesDisambiguator) {
  TestNode x(Op::Var, {}, 42);

  EXPECT_EQ(x.disambiguator(), 42);
}

TEST(Core_ENode, LeafNodesEqualWithSameSymbolAndDisambiguator) {
  TestNode x1(Op::Var, {}, 1);
  TestNode x2(Op::Var, {}, 1);

  EXPECT_EQ(x1, x2);
}

TEST(Core_ENode, LeafNodesDifferWithDifferentDisambiguator) {
  TestNode x1(Op::Var, {}, 1);
  TestNode x2(Op::Var, {}, 2);

  EXPECT_NE(x1, x2);
}

TEST(Core_ENode, LeafNodesDifferWithDifferentSymbol) {
  TestNode x(Op::Var, {}, 1);
  TestNode y(Op::Const, {}, 1);

  EXPECT_NE(x, y);
}

// --- Non-Leaf Node Tests ---

TEST(Core_ENode, NonLeafNodeHasChildren) {
  auto id0 = EClassId{0};
  auto id1 = EClassId{1};
  TestNode add(Op::Add, {id0, id1});

  EXPECT_FALSE(add.is_leaf());
  EXPECT_EQ(add.arity(), 2);
  EXPECT_EQ(add.children().size(), 2);
}

TEST(Core_ENode, NonLeafNodePreservesChildOrder) {
  auto id0 = EClassId{0};
  auto id1 = EClassId{1};
  TestNode add(Op::Add, {id0, id1});

  EXPECT_EQ(add.children()[0], id0);
  EXPECT_EQ(add.children()[1], id1);
}

TEST(Core_ENode, NonLeafNodesEqualWithSameStructure) {
  auto id0 = EClassId{0};
  auto id1 = EClassId{1};
  TestNode add1(Op::Add, {id0, id1});
  TestNode add2(Op::Add, {id0, id1});

  EXPECT_EQ(add1, add2);
}

TEST(Core_ENode, NonLeafNodesDifferWithDifferentSymbol) {
  auto id0 = EClassId{0};
  auto id1 = EClassId{1};
  TestNode add(Op::Add, {id0, id1});
  TestNode mul(Op::Mul, {id0, id1});

  EXPECT_NE(add, mul);
}

TEST(Core_ENode, NonLeafNodesDifferWithDifferentChildOrder) {
  auto id0 = EClassId{0};
  auto id1 = EClassId{1};
  TestNode forward(Op::Add, {id0, id1});
  TestNode reverse(Op::Add, {id1, id0});

  EXPECT_NE(forward, reverse);
}

TEST(Core_ENode, UnaryNodeSupported) {
  auto id0 = EClassId{0};
  TestNode neg(Op::Neg, {id0});

  EXPECT_EQ(neg.arity(), 1);
  EXPECT_EQ(neg.children()[0], id0);
}

// --- Hash Tests ---

TEST(Core_ENode, EqualNodesHaveSameHash) {
  // Leaf nodes
  EXPECT_EQ(TestNode(Op::Var, {}, 42).hash(), TestNode(Op::Var, {}, 42).hash());

  // Non-leaf nodes
  auto id0 = EClassId{0};
  auto id1 = EClassId{1};
  EXPECT_EQ(TestNode(Op::Add, {id0, id1}).hash(), TestNode(Op::Add, {id0, id1}).hash());
}

TEST(Core_ENode, HashRespectsAllFields) {
  auto id0 = EClassId{0};
  auto id1 = EClassId{1};
  auto id2 = EClassId{2};

  // Disambiguator
  EXPECT_NE(TestNode(Op::Var, {}, 1).hash(), TestNode(Op::Var, {}, 2).hash());
  // Symbol
  EXPECT_NE(TestNode(Op::Add, {id0, id1}).hash(), TestNode(Op::Mul, {id0, id1}).hash());
  // Child order
  EXPECT_NE(TestNode(Op::Add, {id0, id1}).hash(), TestNode(Op::Add, {id1, id0}).hash());
  // Child identity
  EXPECT_NE(TestNode(Op::Add, {id0, id1}).hash(), TestNode(Op::Add, {id0, id2}).hash());
}

TEST(Core_ENode, StdHashMatchesNodeHash) {
  std::hash<TestNode> hasher;

  TestNode leaf(Op::Var, {}, 1);
  EXPECT_EQ(hasher(leaf), leaf.hash());

  TestNode non_leaf(Op::Add, {EClassId{0}, EClassId{1}});
  EXPECT_EQ(hasher(non_leaf), non_leaf.hash());
}

// --- Canonicalization Tests ---

TEST(Core_ENode, LeafNodeCanonicalizationIsIdentity) {
  UnionFind uf;  // does not matter about union find, cononicalization won't use it
  TestNode x(Op::Var, {}, 42);

  auto canonical = x.canonicalize(uf);

  EXPECT_EQ(canonical, x);
}

TEST(Core_ENode, NonLeafNodeCanonicalizesChildren) {
  UnionFind uf;
  auto id0 = uf.MakeSet();
  auto id1 = uf.MakeSet();

  // Merge id1 and id0
  auto merged = EClassId{uf.UnionSets(id0, id1)};

  TestNode add(Op::Add, {EClassId{id0}, EClassId{id1}});
  auto canonical = add.canonicalize(uf);

  // Symbol preserved
  EXPECT_EQ(canonical.symbol(), Op::Add);
  // First child canonicalized from id1 to id0
  EXPECT_EQ(canonical.children()[0], merged);
  // Second child unchanged
  EXPECT_EQ(canonical.children()[1], merged);
}

TEST(Core_ENode, CanonicalizationDoesNotMutateOriginal) {
  UnionFind uf;
  auto id0 = uf.MakeSet();
  auto id1 = uf.MakeSet();

  uf.UnionSets(id0, id1);

  TestNode add(Op::Add, {EClassId{id0}, EClassId{id1}});
  auto before_child0 = add.children()[0];
  auto before_child1 = add.children()[1];

  add.canonicalize(uf);  // Ignore result

  EXPECT_EQ(add.children()[0], before_child0);
  EXPECT_EQ(add.children()[1], before_child1);
}

// --- Edge Cases ---

TEST(Core_ENode, ZeroArityNodeIsLeaf) {
  TestNode nullary(Op::Add, {});  // Empty initializer list

  EXPECT_TRUE(nullary.is_leaf());
  EXPECT_EQ(nullary.disambiguator(), 0);
}

TEST(Core_ENode, LargeAritySupported) {
  utils::small_vector<EClassId> children;
  for (uint32_t i = 0; i < 5; ++i) {
    children.push_back(EClassId{i});
  }

  TestNode f(Op::F, children);

  EXPECT_EQ(f.arity(), 5);
  for (uint32_t i = 0; i < 5; ++i) {
    EXPECT_EQ(f.children()[i], EClassId{i});
  }
}

// --- Move Semantics ---

TEST(Core_ENode, MoveConstructionPreservesData) {
  TestNode x(Op::F, {EClassId{42}, EClassId{43}, EClassId{44}});
  auto original_hash = x.hash();
  auto original_symbol = x.symbol();

  TestNode moved(std::move(x));

  EXPECT_EQ(moved.symbol(), original_symbol);
  EXPECT_EQ(moved.arity(), 3);
  EXPECT_EQ(moved.children()[0], EClassId{42});
  EXPECT_EQ(moved.children()[1], EClassId{43});
  EXPECT_EQ(moved.children()[2], EClassId{44});
  EXPECT_EQ(moved.hash(), original_hash);
}

TEST(Core_ENode, MoveAssignmentWorks) {
  TestNode target(Op::Var, {}, 1);
  TestNode source(Op::F, {EClassId{42}, EClassId{43}, EClassId{44}});
  auto original_hash = source.hash();

  target = std::move(source);

  EXPECT_EQ(target.symbol(), Op::F);
  EXPECT_EQ(target.arity(), 3);
  EXPECT_EQ(target.children()[0], EClassId{42});
  EXPECT_EQ(target.children()[1], EClassId{43});
  EXPECT_EQ(target.children()[2], EClassId{44});
  EXPECT_EQ(target.hash(), original_hash);
}

}  // namespace memgraph::planner::core
