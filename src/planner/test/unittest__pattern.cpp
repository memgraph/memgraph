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

#include "planner/core/pattern.hpp"

namespace memgraph::planner::core {

// Test symbol enum (same as used in other planner tests)
enum class Op : uint8_t {
  Add,
  Mul,
  Neg,
  Var,
  Const,
  F,
};

using TestPattern = Pattern<Op>;
using TestPatternNode = PatternNode<Op>;

// --- PatternVar Tests ---

TEST(PatternVar, EqualityWithSameId) {
  PatternVar a{0};
  PatternVar b{0};

  EXPECT_EQ(a, b);
}

TEST(PatternVar, InequalityWithDifferentId) {
  PatternVar a{0};
  PatternVar b{1};

  EXPECT_NE(a, b);
}

TEST(PatternVar, OrderingWorks) {
  PatternVar a{0};
  PatternVar b{1};

  EXPECT_LT(a, b);
  EXPECT_GT(b, a);
}

TEST(PatternVar, HashEqualForSameId) {
  PatternVar a{42};
  PatternVar b{42};

  EXPECT_EQ(std::hash<PatternVar>{}(a), std::hash<PatternVar>{}(b));
}

TEST(PatternVar, HashDifferentForDifferentId) {
  PatternVar a{0};
  PatternVar b{1};

  // Different IDs should have different hashes (very likely)
  EXPECT_NE(std::hash<PatternVar>{}(a), std::hash<PatternVar>{}(b));
}

// --- PatternNode Tests ---

TEST(PatternNode, VariableNodeConstruction) {
  TestPatternNode node{PatternVar{0}};

  EXPECT_TRUE(node.is_variable());
  EXPECT_FALSE(node.is_symbol());
  EXPECT_TRUE(node.is_leaf());
  EXPECT_EQ(node.arity(), 0);
  EXPECT_EQ(node.variable().id, 0);
}

TEST(PatternNode, LeafSymbolConstruction) {
  TestPatternNode node{Op::Const};

  EXPECT_FALSE(node.is_variable());
  EXPECT_TRUE(node.is_symbol());
  EXPECT_TRUE(node.is_leaf());
  EXPECT_EQ(node.arity(), 0);
  EXPECT_EQ(node.symbol(), Op::Const);
}

TEST(PatternNode, NonLeafSymbolConstruction) {
  TestPatternNode node{Op::Add, {PatternNodeId{0}, PatternNodeId{1}}};

  EXPECT_FALSE(node.is_variable());
  EXPECT_TRUE(node.is_symbol());
  EXPECT_FALSE(node.is_leaf());
  EXPECT_EQ(node.arity(), 2);
  EXPECT_EQ(node.symbol(), Op::Add);
  EXPECT_EQ(node.children[0], PatternNodeId{0});
  EXPECT_EQ(node.children[1], PatternNodeId{1});
}

TEST(PatternNode, UnarySymbolConstruction) {
  TestPatternNode node{Op::Neg, {PatternNodeId{0}}};

  EXPECT_EQ(node.arity(), 1);
  EXPECT_EQ(node.children[0], PatternNodeId{0});
}

TEST(PatternNode, EqualityVariableNodes) {
  TestPatternNode a{PatternVar{0}};
  TestPatternNode b{PatternVar{0}};

  EXPECT_EQ(a, b);
}

TEST(PatternNode, InequalityDifferentVariables) {
  TestPatternNode a{PatternVar{0}};
  TestPatternNode b{PatternVar{1}};

  EXPECT_NE(a, b);
}

TEST(PatternNode, EqualitySymbolNodes) {
  TestPatternNode a{Op::Add, {PatternNodeId{0}, PatternNodeId{1}}};
  TestPatternNode b{Op::Add, {PatternNodeId{0}, PatternNodeId{1}}};

  EXPECT_EQ(a, b);
}

TEST(PatternNode, InequalityDifferentSymbols) {
  TestPatternNode a{Op::Add, {PatternNodeId{0}, PatternNodeId{1}}};
  TestPatternNode b{Op::Mul, {PatternNodeId{0}, PatternNodeId{1}}};

  EXPECT_NE(a, b);
}

TEST(PatternNode, InequalityDifferentChildren) {
  TestPatternNode a{Op::Add, {PatternNodeId{0}, PatternNodeId{1}}};
  TestPatternNode b{Op::Add, {PatternNodeId{0}, PatternNodeId{2}}};

  EXPECT_NE(a, b);
}

TEST(PatternNode, InequalityVariableVsSymbol) {
  TestPatternNode var_node{PatternVar{0}};
  TestPatternNode sym_node{Op::Const};

  EXPECT_NE(var_node, sym_node);
}

// --- Pattern Builder Tests ---

TEST(PatternBuilder, SingleVariablePattern) {
  auto builder = TestPattern::Builder{};
  auto x = builder.var(0);
  auto pattern = std::move(builder).build(x);

  EXPECT_EQ(pattern.size(), 1);
  EXPECT_EQ(pattern.root(), PatternNodeId{0});
  EXPECT_TRUE(pattern[pattern.root()].is_variable());
  EXPECT_TRUE(pattern.is_variable_pattern());
}

TEST(PatternBuilder, SingleSymbolPattern) {
  auto builder = TestPattern::Builder{};
  auto c = builder.sym(Op::Const);
  auto pattern = std::move(builder).build(c);

  EXPECT_EQ(pattern.size(), 1);
  EXPECT_EQ(pattern.root(), PatternNodeId{0});
  EXPECT_TRUE(pattern[pattern.root()].is_symbol());
  EXPECT_EQ(pattern[pattern.root()].symbol(), Op::Const);
  EXPECT_FALSE(pattern.is_variable_pattern());
}

TEST(PatternBuilder, BinaryPatternAddXY) {
  // Pattern: Add(?x, ?y)
  auto builder = TestPattern::Builder{};
  auto x = builder.var(0);  // ?x
  auto y = builder.var(1);  // ?y
  auto add = builder.sym(Op::Add, {x, y});
  auto pattern = std::move(builder).build(add);

  EXPECT_EQ(pattern.size(), 3);
  EXPECT_EQ(pattern.root(), PatternNodeId{2});

  // Check root node
  auto const &root = pattern[pattern.root()];
  EXPECT_TRUE(root.is_symbol());
  EXPECT_EQ(root.symbol(), Op::Add);
  EXPECT_EQ(root.arity(), 2);
  EXPECT_EQ(root.children[0], PatternNodeId{0});
  EXPECT_EQ(root.children[1], PatternNodeId{1});

  // Check children
  EXPECT_TRUE(pattern[PatternNodeId{0}].is_variable());
  EXPECT_EQ(pattern[PatternNodeId{0}].variable().id, 0);
  EXPECT_TRUE(pattern[PatternNodeId{1}].is_variable());
  EXPECT_EQ(pattern[PatternNodeId{1}].variable().id, 1);
}

TEST(PatternBuilder, PatternWithRepeatedVariable) {
  // Pattern: Add(?x, ?x) - same variable used twice
  auto builder = TestPattern::Builder{};
  auto x = builder.var(0);  // ?x
  auto add = builder.sym(Op::Add, {x, x});
  auto pattern = std::move(builder).build(add);

  EXPECT_EQ(pattern.size(), 2);

  auto const &root = pattern[pattern.root()];
  EXPECT_EQ(root.children[0], root.children[1]);
}

TEST(PatternBuilder, NestedPattern) {
  // Pattern: Add(Mul(?x, ?y), ?z)
  auto builder = TestPattern::Builder{};
  auto x = builder.var(0);
  auto y = builder.var(1);
  auto mul = builder.sym(Op::Mul, {x, y});
  auto z = builder.var(2);
  auto add = builder.sym(Op::Add, {mul, z});
  auto pattern = std::move(builder).build(add);

  EXPECT_EQ(pattern.size(), 5);

  // Root is Add
  auto const &root = pattern[pattern.root()];
  EXPECT_EQ(root.symbol(), Op::Add);
  EXPECT_EQ(root.arity(), 2);

  // First child is Mul
  auto const &mul_node = pattern[root.children[0]];
  EXPECT_TRUE(mul_node.is_symbol());
  EXPECT_EQ(mul_node.symbol(), Op::Mul);
  EXPECT_EQ(mul_node.arity(), 2);

  // Second child is variable
  auto const &z_node = pattern[root.children[1]];
  EXPECT_TRUE(z_node.is_variable());
  EXPECT_EQ(z_node.variable().id, 2);
}

TEST(PatternBuilder, UnaryPattern) {
  // Pattern: Neg(?x)
  auto builder = TestPattern::Builder{};
  auto x = builder.var(0);
  auto neg = builder.sym(Op::Neg, {x});
  auto pattern = std::move(builder).build(neg);

  EXPECT_EQ(pattern.size(), 2);

  auto const &root = pattern[pattern.root()];
  EXPECT_EQ(root.symbol(), Op::Neg);
  EXPECT_EQ(root.arity(), 1);
}

TEST(PatternBuilder, MixedSymbolsAndVariables) {
  // Pattern: Add(?x, Const) - variable + leaf symbol
  auto builder = TestPattern::Builder{};
  auto x = builder.var(0);
  auto c = builder.sym(Op::Const);
  auto add = builder.sym(Op::Add, {x, c});
  auto pattern = std::move(builder).build(add);

  EXPECT_EQ(pattern.size(), 3);

  auto const &root = pattern[pattern.root()];
  EXPECT_EQ(root.symbol(), Op::Add);

  EXPECT_TRUE(pattern[root.children[0]].is_variable());
  EXPECT_TRUE(pattern[root.children[1]].is_symbol());
  EXPECT_EQ(pattern[root.children[1]].symbol(), Op::Const);
}

TEST(PatternBuilder, BuildWithoutExplicitRoot) {
  // Build without specifying root - should use last node
  auto builder = TestPattern::Builder{};
  auto x = builder.var(0);
  [[maybe_unused]] auto neg = builder.sym(Op::Neg, {x});
  auto pattern = std::move(builder).build();

  EXPECT_EQ(pattern.root(), PatternNodeId{1});  // neg is the last node
}

TEST(PatternBuilder, EmptyBuilderState) {
  auto builder = TestPattern::Builder{};

  EXPECT_TRUE(builder.empty());
  EXPECT_EQ(builder.size(), 0);

  builder.var(0);

  EXPECT_FALSE(builder.empty());
  EXPECT_EQ(builder.size(), 1);
}

// --- Pattern Tests ---

TEST(Pattern, EmptyPatternHandling) {
  auto builder = TestPattern::Builder{};
  // Note: We don't actually create an empty pattern since build() requires nodes
  // This test verifies that the empty() method works correctly
  auto x = builder.var(0);
  auto pattern = std::move(builder).build(x);

  EXPECT_FALSE(pattern.empty());
}

TEST(Pattern, NodesAccessor) {
  auto builder = TestPattern::Builder{};
  auto x = builder.var(0);
  auto y = builder.var(1);
  auto add = builder.sym(Op::Add, {x, y});
  auto pattern = std::move(builder).build(add);

  auto nodes = pattern.nodes();
  EXPECT_EQ(nodes.size(), 3);
}

TEST(Pattern, IsVariablePattern) {
  // Variable pattern
  auto builder1 = TestPattern::Builder{};
  auto x = builder1.var(0);
  auto var_pattern = std::move(builder1).build(x);
  EXPECT_TRUE(var_pattern.is_variable_pattern());

  // Symbol pattern
  auto builder2 = TestPattern::Builder{};
  auto c = builder2.sym(Op::Const);
  auto sym_pattern = std::move(builder2).build(c);
  EXPECT_FALSE(sym_pattern.is_variable_pattern());
}

TEST(Pattern, Equality) {
  // Same pattern structure
  auto builder1 = TestPattern::Builder{};
  auto x1 = builder1.var(0);
  auto add1 = builder1.sym(Op::Add, {x1, x1});
  auto pattern1 = std::move(builder1).build(add1);

  auto builder2 = TestPattern::Builder{};
  auto x2 = builder2.var(0);
  auto add2 = builder2.sym(Op::Add, {x2, x2});
  auto pattern2 = std::move(builder2).build(add2);

  EXPECT_EQ(pattern1, pattern2);
}

TEST(Pattern, Inequality) {
  // Different structure
  auto builder1 = TestPattern::Builder{};
  auto x1 = builder1.var(0);
  auto y1 = builder1.var(1);
  auto add1 = builder1.sym(Op::Add, {x1, y1});
  auto pattern1 = std::move(builder1).build(add1);

  auto builder2 = TestPattern::Builder{};
  auto x2 = builder2.var(0);
  auto mul2 = builder2.sym(Op::Mul, {x2, x2});
  auto pattern2 = std::move(builder2).build(mul2);

  EXPECT_NE(pattern1, pattern2);
}

// --- Complex Pattern Tests ---

TEST(Pattern, DeeplyNestedPattern) {
  // Pattern: Add(Mul(Neg(?x), ?y), Const)
  auto builder = TestPattern::Builder{};
  auto x = builder.var(0);
  auto neg = builder.sym(Op::Neg, {x});
  auto y = builder.var(1);
  auto mul = builder.sym(Op::Mul, {neg, y});
  auto c = builder.sym(Op::Const);
  auto add = builder.sym(Op::Add, {mul, c});
  auto pattern = std::move(builder).build(add);

  EXPECT_EQ(pattern.size(), 6);

  // Verify structure by traversing
  auto const &root = pattern[pattern.root()];
  EXPECT_EQ(root.symbol(), Op::Add);

  auto const &left = pattern[root.children[0]];
  EXPECT_EQ(left.symbol(), Op::Mul);

  auto const &neg_node = pattern[left.children[0]];
  EXPECT_EQ(neg_node.symbol(), Op::Neg);

  auto const &x_node = pattern[neg_node.children[0]];
  EXPECT_TRUE(x_node.is_variable());
  EXPECT_EQ(x_node.variable().id, 0);
}

TEST(Pattern, NaryFunctionPattern) {
  // Pattern: F(?a, ?b, ?c, ?d) - 4-ary function
  auto builder = TestPattern::Builder{};
  auto a = builder.var(0);
  auto b = builder.var(1);
  auto c = builder.var(2);
  auto d = builder.var(3);
  auto f = builder.sym(Op::F, {a, b, c, d});
  auto pattern = std::move(builder).build(f);

  EXPECT_EQ(pattern.size(), 5);

  auto const &root = pattern[pattern.root()];
  EXPECT_EQ(root.symbol(), Op::F);
  EXPECT_EQ(root.arity(), 4);
}

}  // namespace memgraph::planner::core
