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

// Fused/joined pattern compilation and execution tests.

#include <map>
#include <set>
#include <sstream>

#include <gtest/gtest.h>

#include "planner/pattern/vm/compiler.hpp"
#include "planner/pattern/vm/executor.hpp"
#include "planner/pattern/vm/tracer.hpp"
#include "test_egraph_fixture.hpp"
#include "test_patterns.hpp"

namespace memgraph::planner::core {

using namespace test;
using namespace vm;

// ============================================================================
// Fused Pattern Compiler Tests
// ============================================================================

class FusedCompilerTest : public EGraphTestBase {
 protected:
  EMatchContext ctx;
  std::vector<PatternMatch> results;
};

// Test: Bind(_, ?sym, ?expr) joined with ?id = Ident(?sym)
// This tests parent traversal: after matching Bind, traverse UP from ?sym to find Ident parents
TEST_F(FusedCompilerTest, SimpleJoinWithParentTraversal) {
  // Build e-graph:
  //   sym_val = Const(1)
  //   expr_val = Const(2)
  //   bind_node = Bind(Const(0), sym_val, expr_val)
  //   ident_node = Ident(sym_val)  <- shares sym_val with bind_node
  auto placeholder = leaf(Op::Const, 0);
  auto sym_val = leaf(Op::Const, 1);
  auto expr_val = leaf(Op::Const, 2);
  auto bind_node = node(Op::Bind, placeholder, sym_val, expr_val);
  auto ident_node = node(Op::Ident, sym_val);  // Uses same sym_val!

  rebuild_egraph();

  // Anchor pattern: Bind(_, ?sym, ?expr)
  constexpr PatternVar kVarSym{1};
  constexpr PatternVar kVarExpr{2};
  auto anchor = Pattern<Op>::build(Op::Bind, {Wildcard{}, Var{kVarSym}, Var{kVarExpr}}, kTestRoot);

  // Joined pattern: ?id = Ident(?sym)
  constexpr PatternVar kVarId{3};
  auto joined = Pattern<Op>::build(Op::Ident, {Var{kVarSym}}, kVarId);

  // Compile with PatternCompiler
  PatternCompiler<Op> compiler;
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);

  ASSERT_TRUE(compiled.has_value()) << "Fused compilation should succeed";

  auto bytecode = disassemble<Op>(compiled->code(), compiled->symbols());

  // Verify bytecode has parent traversal instructions
  bool has_iter_parents = false;
  bool has_next_parent = false;
  bool has_get_enode_eclass = false;

  for (auto const &instr : compiled->code()) {
    if (instr.op == VMOp::IterParents) {
      has_iter_parents = true;
    }
    if (instr.op == VMOp::NextParent) {
      has_next_parent = true;
    }
    if (instr.op == VMOp::GetENodeEClass) {
      has_get_enode_eclass = true;
    }
  }

  EXPECT_TRUE(has_iter_parents) << "Fused bytecode should have IterParents instruction\nBytecode:\n" << bytecode;
  EXPECT_TRUE(has_next_parent) << "Fused bytecode should have NextParent instruction\nBytecode:\n" << bytecode;
  EXPECT_TRUE(has_get_enode_eclass) << "Fused bytecode should have GetENodeEClass for ?id binding\nBytecode:\n"
                                    << bytecode;

  // Execute and verify matches
  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  // Only use the Bind e-class as candidate (anchor pattern entry)
  std::vector<EClassId> candidates = {egraph.find(bind_node)};

  EMatchContext ctx;
  std::vector<PatternMatch> results;
  executor.execute(*compiled, candidates, ctx, results);

  // Should find 1 match: Bind paired with Ident (both reference same sym_val)
  EXPECT_EQ(results.size(), 1) << "Should find exactly 1 match where Bind and Ident share ?sym";

  if (!results.empty()) {
    // The match should contain bindings for kVarSym, kVarExpr, kVarId, and kTestRoot
    // Note: Slot ordering depends on unordered map iteration, so we check all slots
    // to find which one contains each expected value
    auto &match = results[0];
    auto num_slots = compiled->num_slots();

    bool found_sym = false, found_expr = false, found_id = false;
    for (std::size_t i = 0; i < num_slots; ++i) {
      auto bound = ctx.arena().get(match, i);
      if (egraph.find(bound) == egraph.find(sym_val)) found_sym = true;
      if (egraph.find(bound) == egraph.find(expr_val)) found_expr = true;
      if (egraph.find(bound) == egraph.find(ident_node)) found_id = true;
    }

    EXPECT_TRUE(found_sym) << "Match should contain binding for ?sym = sym_val";
    EXPECT_TRUE(found_expr) << "Match should contain binding for ?expr = expr_val";
    EXPECT_TRUE(found_id) << "Match should contain binding for ?id = ident_node";
  }
}

// Test: No matching join - Ident uses different symbol
TEST_F(FusedCompilerTest, NoMatchingJoin) {
  // Build e-graph:
  //   sym_val1 = Const(1)
  //   sym_val2 = Const(2)  <- different from sym_val1
  //   expr_val = Const(3)
  //   bind_node = Bind(Const(0), sym_val1, expr_val)
  //   ident_node = Ident(sym_val2)  <- uses DIFFERENT symbol!
  auto placeholder = leaf(Op::Const, 0);
  auto sym_val1 = leaf(Op::Const, 1);
  auto sym_val2 = leaf(Op::Const, 2);
  auto expr_val = leaf(Op::Const, 3);
  auto bind_node = node(Op::Bind, placeholder, sym_val1, expr_val);
  node(Op::Ident, sym_val2);  // Uses different sym!

  rebuild_egraph();

  // Anchor pattern: Bind(_, ?sym, ?expr)
  constexpr PatternVar kVarSym{1};
  constexpr PatternVar kVarExpr{2};
  auto anchor = Pattern<Op>::build(Op::Bind, {Wildcard{}, Var{kVarSym}, Var{kVarExpr}}, kTestRoot);

  // Joined pattern: ?id = Ident(?sym)
  constexpr PatternVar kVarId{3};
  auto joined = Pattern<Op>::build(Op::Ident, {Var{kVarSym}}, kVarId);

  // Compile and execute
  PatternCompiler<Op> compiler;
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);
  std::vector<EClassId> candidates = {egraph.find(bind_node)};

  EMatchContext ctx;
  std::vector<PatternMatch> results;
  executor.execute(*compiled, candidates, ctx, results);

  // Should find NO matches - Ident doesn't reference the same ?sym
  EXPECT_EQ(results.size(), 0) << "Should find no matches when Ident uses different symbol";
}

// Test: Multiple Ident references to same symbol
TEST_F(FusedCompilerTest, MultipleJoinMatches) {
  // Build e-graph:
  //   sym_val = Const(1)
  //   expr_val = Const(2)
  //   bind_node = Bind(Const(0), sym_val, expr_val)
  //   ident1 = Ident(sym_val)
  //   ident2 = Ident(sym_val)  <- second Ident referencing same symbol
  auto placeholder = leaf(Op::Const, 0);
  auto sym_val = leaf(Op::Const, 1);
  auto expr_val = leaf(Op::Const, 2);
  auto bind_node = node(Op::Bind, placeholder, sym_val, expr_val);
  auto ident1 = node(Op::Ident, sym_val);
  auto ident2 = node(Op::Ident, sym_val);  // Second Ident

  rebuild_egraph();

  // Anchor pattern: Bind(_, ?sym, ?expr)
  constexpr PatternVar kVarSym{1};
  constexpr PatternVar kVarExpr{2};
  auto anchor = Pattern<Op>::build(Op::Bind, {Wildcard{}, Var{kVarSym}, Var{kVarExpr}}, kTestRoot);

  // Joined pattern: ?id = Ident(?sym)
  constexpr PatternVar kVarId{3};
  auto joined = Pattern<Op>::build(Op::Ident, {Var{kVarSym}}, kVarId);

  // Compile and execute
  PatternCompiler<Op> compiler;
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value());

  auto bytecode = disassemble<Op>(compiled->code(), compiled->symbols());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);
  std::vector<EClassId> candidates = {egraph.find(bind_node)};

  EMatchContext ctx;
  std::vector<PatternMatch> results;
  executor.execute(*compiled, candidates, ctx, results);

  // Note: If both ident nodes have the same structure Ident(sym_val), they might
  // be in the same e-class due to structural sharing. In that case, there's only
  // 1 Ident e-class, so only 1 match.
  bool idents_same_eclass = (egraph.find(ident1) == egraph.find(ident2));
  std::size_t expected_matches = idents_same_eclass ? 1 : 2;

  // Build debug info for failure message
  std::ostringstream debug_ss;
  auto sym_canonical = egraph.find(sym_val);
  auto const &sym_eclass = egraph.eclass(sym_canonical);
  debug_ss << "sym_val e-class " << sym_canonical << " has " << sym_eclass.parents().size() << " parents\n";
  for (auto parent_id : sym_eclass.parents()) {
    auto const &parent = egraph.get_enode(parent_id);
    debug_ss << "  Parent: enode " << parent_id << " symbol=" << static_cast<int>(parent.symbol()) << "\n";
  }
  debug_ss << "ident1 e-class: " << egraph.find(ident1) << "\n";
  debug_ss << "ident2 e-class: " << egraph.find(ident2) << "\n";

  EXPECT_EQ(results.size(), expected_matches)
      << "Should find " << expected_matches << " matches (idents same e-class: " << idents_same_eclass
      << ")\nBytecode:\n"
      << bytecode << "\nDebug info:\n"
      << debug_ss.str();
}

// ============================================================================
// Nested Fused Pattern Tests
// ============================================================================

class FusedPatternNestedTest : public EGraphTestBase {};

TEST_F(FusedPatternNestedTest, NestedJoinedPatternViaParentTraversal) {
  // Build e-graph:
  // We want to test nested symbol patterns in joined patterns.
  // Anchor: Bind(_, ?sym, _) matches bind_node
  // Joined: F(?sym, Neg(?x)) - has nested Neg with shared ?sym as direct child
  //
  // This tests that when the joined pattern has nested symbols, they are
  // compiled correctly via emit_joined_child recursion.

  constexpr PatternVar kVarSym{0};
  constexpr PatternVar kVarX{1};
  constexpr PatternVar kTestRoot{2};

  // Build: sym_val, x_val, Neg(x_val), F(sym_val, Neg(x_val)), Bind(_, sym_val, _)
  auto sym_val = leaf(Op::Const, 1);
  auto x_val = leaf(Op::Const, 2);
  auto neg_x = node(Op::Neg, x_val);
  auto f_node = node(Op::F, sym_val, neg_x);
  auto bind_node = node(Op::Bind, leaf(Op::A), sym_val, leaf(Op::B));

  // Anchor pattern: Bind(_, ?sym, _)
  auto anchor = Pattern<Op>::build(Op::Bind, {Wildcard{}, Var{kVarSym}, Wildcard{}}, kTestRoot);

  // Joined pattern: F(?sym, Neg(?x)) - nested Neg symbol with shared ?sym
  auto joined = Pattern<Op>::build(Op::F, {Var{kVarSym}, Sym(Op::Neg, Var{kVarX})});

  PatternCompiler<Op> compiler;
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value()) << "Fused compilation should succeed";

  auto bytecode = disassemble<Op>(compiled->code(), compiled->symbols());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);
  std::vector<EClassId> candidates = {egraph.find(bind_node)};

  EMatchContext ctx;
  std::vector<PatternMatch> results;
  executor.execute(*compiled, candidates, ctx, results);

  EXPECT_EQ(results.size(), 1) << "Should find 1 match for nested pattern\nBytecode:\n" << bytecode;

  // Check that we found all the right bindings - collect bound values
  if (!results.empty()) {
    std::set<EClassId> bound_values;
    auto num_slots = compiled->num_slots();
    for (std::size_t i = 0; i < num_slots; ++i) {
      bound_values.insert(ctx.arena().get(results[0], i));
    }
    // Should have bound: bind_node (root), sym_val, x_val
    EXPECT_TRUE(bound_values.count(egraph.find(bind_node)) > 0 || bound_values.count(egraph.find(sym_val)) > 0)
        << "Should have bound sym_val or bind_node";
    EXPECT_TRUE(bound_values.count(egraph.find(x_val)) > 0) << "Should have bound x_val";
  }
}

// Test PatternsCompiler with deeply nested symbols in Cartesian product join
TEST_F(FusedPatternNestedTest, DeeplyNestedCartesianPattern) {
  // Build e-graph with deeply nested structure
  // The joined pattern has no shared variable at direct child level,
  // so it uses Cartesian product. This tests emit_joined_child recursion
  // in the Cartesian product path.

  constexpr PatternVar kVarX{0};
  constexpr PatternVar kVarY{1};
  constexpr PatternVar kVarZ{2};

  // Create: x, Neg(x), Neg(Neg(x)), y, Add(y, Neg(Neg(x)))
  auto x = leaf(Op::Const, 1);
  auto y = leaf(Op::Const, 2);
  auto neg_x = node(Op::Neg, x);
  auto neg_neg_x = node(Op::Neg, neg_x);
  auto add_node = node(Op::Add, y, neg_neg_x);

  // Anchor pattern: Add(?y, ?x) - ?x will bind to Neg(Neg(x))
  auto anchor = Pattern<Op>::build(Op::Add, {Var{kVarY}, Var{kVarX}});

  // Joined pattern: Neg(Neg(?z)) - deeply nested, no direct shared var with anchor
  // This forces Cartesian product with recursive compilation
  auto joined = Pattern<Op>::build(Op::Neg, {Sym(Op::Neg, Var{kVarZ})});

  // Compiler will detect shared variables automatically
  PatternCompiler<Op> compiler;
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value()) << "Fused compilation should succeed";

  auto bytecode = disassemble<Op>(compiled->code(), compiled->symbols());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);
  std::vector<EClassId> candidates = {egraph.find(add_node)};

  EMatchContext ctx;
  std::vector<PatternMatch> results;
  executor.execute(*compiled, candidates, ctx, results);

  // Should find at least one match
  EXPECT_GE(results.size(), 1) << "Should find matches for nested pattern\nBytecode:\n" << bytecode;
}

// Test that verifies bytecode structure for Cartesian product joins
// This tests that IterAllEClasses and NextEClass instructions are generated
TEST_F(FusedPatternNestedTest, CartesianProductBytecodeStructure) {
  // Build minimal e-graph
  auto x = leaf(Op::Const, 1);
  auto y = leaf(Op::Const, 2);
  auto add_node = node(Op::Add, x, y);

  // Anchor pattern: Add(?x, ?y)
  constexpr PatternVar kVarX{0};
  constexpr PatternVar kVarY{1};
  constexpr PatternVar kVarZ{2};
  auto anchor = Pattern<Op>::build(Op::Add, {Var{kVarX}, Var{kVarY}});

  // Joined pattern: Neg(?z) - no shared variable with anchor
  // This forces Cartesian product join
  auto joined = Pattern<Op>::build(Op::Neg, {Var{kVarZ}});

  PatternCompiler<Op> compiler;
  // No shared vars between patterns forces Cartesian product
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value()) << "Fused compilation should succeed";

  auto bytecode = disassemble<Op>(compiled->code(), compiled->symbols());

  // Verify bytecode has IterAllEClasses and NextEClass instructions
  bool has_iter_all_eclasses = false;
  bool has_next_eclass = false;

  for (auto const &instr : compiled->code()) {
    if (instr.op == VMOp::IterAllEClasses) {
      has_iter_all_eclasses = true;
    }
    if (instr.op == VMOp::NextEClass) {
      has_next_eclass = true;
    }
  }

  EXPECT_TRUE(has_iter_all_eclasses)
      << "Cartesian product bytecode should have IterAllEClasses instruction\nBytecode:\n"
      << bytecode;
  EXPECT_TRUE(has_next_eclass) << "Cartesian product bytecode should have NextEClass instruction\nBytecode:\n"
                               << bytecode;
}

// Test the simplified compile(patterns) API that computes join order internally
TEST_F(FusedPatternNestedTest, SimplifiedCompileAPI) {
  // Build e-graph with shared variables between patterns
  auto sym_val = leaf(Op::Const, 1);
  auto expr_val = leaf(Op::Const, 2);
  auto bind_node = node(Op::Bind, leaf(Op::A), sym_val, expr_val);
  auto ident_node = node(Op::Ident, sym_val);

  // Create patterns - order doesn't matter, compiler figures out optimal join order
  constexpr PatternVar kVarSym{1};
  constexpr PatternVar kVarExpr{2};
  constexpr PatternVar kVarId{3};
  constexpr PatternVar kTestRoot{0};

  auto bind_pattern = Pattern<Op>::build(Op::Bind, {Wildcard{}, Var{kVarSym}, Var{kVarExpr}}, kTestRoot);
  auto ident_pattern = Pattern<Op>::build(Op::Ident, {Var{kVarSym}}, kVarId);

  // Use simplified API - just pass all patterns
  std::array<Pattern<Op>, 2> patterns = {bind_pattern, ident_pattern};

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(patterns);

  ASSERT_TRUE(compiled.has_value()) << "Simplified compile API should succeed";

  auto bytecode = disassemble<Op>(compiled->code(), compiled->symbols());

  // Execute and verify matches work
  VMExecutorVerify<Op, NoAnalysis> executor(egraph);
  std::vector<EClassId> candidates = {egraph.find(bind_node)};

  EMatchContext ctx;
  std::vector<PatternMatch> results;
  executor.execute(*compiled, candidates, ctx, results);

  EXPECT_EQ(results.size(), 1) << "Should find 1 match using simplified API\nBytecode:\n" << bytecode;
}

// Test that verifies bytecode structure for variable-only joined patterns
TEST_F(FusedPatternNestedTest, VariableOnlyJoinedPatternBytecode) {
  // Build minimal e-graph
  auto x = leaf(Op::Const, 1);
  auto add_node = node(Op::Add, x, x);

  // Anchor pattern: Add(?x, ?y)
  constexpr PatternVar kVarX{0};
  constexpr PatternVar kVarY{1};
  constexpr PatternVar kVarZ{2};
  auto anchor = Pattern<Op>::build(Op::Add, {Var{kVarX}, Var{kVarY}});

  // Joined pattern: ?z - just a variable, no symbol
  // This tests variable-only pattern handling
  auto joined_builder = TestPattern::Builder{};
  joined_builder.var(kVarZ);
  auto joined = std::move(joined_builder).build();

  PatternCompiler<Op> compiler;
  // No shared vars between patterns
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value()) << "Fused compilation should succeed for variable-only joined pattern";

  auto bytecode = disassemble<Op>(compiled->code(), compiled->symbols());

  // Verify bytecode has IterAllEClasses for the variable-only pattern
  bool has_iter_all_eclasses = false;
  bool has_next_eclass = false;

  for (auto const &instr : compiled->code()) {
    if (instr.op == VMOp::IterAllEClasses) {
      has_iter_all_eclasses = true;
    }
    if (instr.op == VMOp::NextEClass) {
      has_next_eclass = true;
    }
  }

  EXPECT_TRUE(has_iter_all_eclasses) << "Variable-only joined pattern should use IterAllEClasses\nBytecode:\n"
                                     << bytecode;
  EXPECT_TRUE(has_next_eclass) << "Variable-only joined pattern should use NextEClass\nBytecode:\n" << bytecode;
}

// ============================================================================
// Potential Bug Tests
// ============================================================================

// Test: Multiple parent traversals from same variable (single parent each)
// This is the simple case with single parents that's easy to verify.
TEST_F(FusedPatternNestedTest, MultipleParentTraversalsFromSameVariable) {
  // Build e-graph:
  //   sym_val = Const(1)
  //   ident_node = Ident(sym_val)
  //   neg_node = Neg(sym_val)
  //   bind_node = Bind(Const(0), sym_val, Const(2))

  auto placeholder = leaf(Op::Const, 0);
  auto sym_val = leaf(Op::Const, 1);
  auto expr_val = leaf(Op::Const, 2);
  auto bind_node = node(Op::Bind, placeholder, sym_val, expr_val);
  auto ident_node = node(Op::Ident, sym_val);
  auto neg_node = node(Op::Neg, sym_val);

  rebuild_egraph();

  constexpr PatternVar kVarSym{1};
  constexpr PatternVar kTestRoot{0};
  constexpr PatternVar kVarIdent{2};
  constexpr PatternVar kVarNeg{3};

  auto anchor = Pattern<Op>::build(Op::Bind, {Wildcard{}, Var{kVarSym}, Wildcard{}}, kTestRoot);
  auto joined_ident = Pattern<Op>::build(Op::Ident, {Var{kVarSym}}, kVarIdent);
  auto joined_neg = Pattern<Op>::build(Op::Neg, {Var{kVarSym}}, kVarNeg);

  PatternCompiler<Op> compiler;
  std::array patterns = {anchor, joined_ident, joined_neg};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);
  std::vector<EClassId> candidates = {egraph.find(bind_node)};

  EMatchContext ctx;
  std::vector<PatternMatch> results;
  executor.execute(*compiled, candidates, ctx, results);

  EXPECT_EQ(results.size(), 1) << "Should find 1 match";
}

// Test: Nested parent traversals work correctly
// Historical note: Before the IterParents + CheckSymbol approach, IterParentsSym
// used a shared buffer that could be corrupted by nested iterations. This test
// verifies that nested parent traversals produce correct results.
//
// Setup: Create distinct F2 e-classes that all reference the same sym_val,
// and distinct Neg e-classes that also reference sym_val.
// This avoids structural sharing merging them.
TEST_F(FusedPatternNestedTest, MultipleParentTraversalsManyParents) {
  // Build e-graph with multiple DISTINCT parents of sym_val:
  //   sym_val = Const(1)
  //   For structural distinctness, create F2(sym_val, Const(i)) for different i
  //   These won't merge because they have different second children.
  //
  //   f2_1 = F2(sym_val, Const(101))
  //   f2_2 = F2(sym_val, Const(102))
  //   f2_3 = F2(sym_val, Const(103))
  //
  //   neg_1 = Neg(sym_val)  <- All Neg(sym_val) will merge (structural sharing)
  //
  //   bind_node = Bind(Const(0), sym_val, Const(2))
  //
  // Patterns:
  //   Anchor: Bind(_, ?sym, _)
  //   Joined1: F2(?sym, _)  <- 3 distinct F2 parents
  //   Joined2: Neg(?sym)    <- 1 Neg parent
  //
  // Expected: 3 * 1 = 3 matches

  auto placeholder = leaf(Op::Const, 0);
  auto sym_val = leaf(Op::Const, 1);
  auto expr_val = leaf(Op::Const, 2);
  auto bind_node = node(Op::Bind, placeholder, sym_val, expr_val);

  // Create 3 structurally distinct F2 parents of sym_val
  auto f2_1 = node(Op::F2, sym_val, leaf(Op::Const, 101));
  auto f2_2 = node(Op::F2, sym_val, leaf(Op::Const, 102));
  auto f2_3 = node(Op::F2, sym_val, leaf(Op::Const, 103));

  // Create one Neg parent (will be single e-class due to structural sharing)
  auto neg_1 = node(Op::Neg, sym_val);

  rebuild_egraph();

  // Verify we have 3 distinct F2 parents and 1 Neg parent of sym_val
  auto sym_class = egraph.find(sym_val);
  auto const &sym_eclass = egraph.eclass(sym_class);

  std::set<EClassId> f2_classes, neg_classes;
  for (auto parent_id : sym_eclass.parents()) {
    auto const &parent = egraph.get_enode(parent_id);
    if (parent.symbol() == Op::F2) {
      f2_classes.insert(egraph.find(parent_id));
    }
    if (parent.symbol() == Op::Neg) {
      neg_classes.insert(egraph.find(parent_id));
    }
  }

  ASSERT_EQ(f2_classes.size(), 3) << "Should have 3 distinct F2 e-classes";
  ASSERT_EQ(neg_classes.size(), 1) << "Should have 1 Neg e-class";

  constexpr PatternVar kVarSym{1};
  constexpr PatternVar kTestRoot{0};
  constexpr PatternVar kVarF2{2};
  constexpr PatternVar kVarNeg{3};

  auto anchor = Pattern<Op>::build(Op::Bind, {Wildcard{}, Var{kVarSym}, Wildcard{}}, kTestRoot);
  auto joined_f2 = Pattern<Op>::build(Op::F2, {Var{kVarSym}, Wildcard{}}, kVarF2);
  auto joined_neg = Pattern<Op>::build(Op::Neg, {Var{kVarSym}}, kVarNeg);

  PatternCompiler<Op> compiler;
  std::array patterns = {anchor, joined_f2, joined_neg};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value()) << "Three-pattern fused compilation should succeed";

  auto bytecode = disassemble<Op>(compiled->code(), compiled->symbols());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);
  std::vector<EClassId> candidates = {egraph.find(bind_node)};

  EMatchContext ctx;
  std::vector<PatternMatch> results;
  executor.execute(*compiled, candidates, ctx, results);

  // Expected: 3 F2 e-classes * 1 Neg e-class = 3 matches
  std::size_t expected_matches = f2_classes.size() * neg_classes.size();
  EXPECT_EQ(results.size(), expected_matches) << "Expected " << f2_classes.size() << " F2 * " << neg_classes.size()
                                              << " Neg = " << expected_matches << " matches\nBytecode:\n"
                                              << bytecode;

  // Verify all matches bind to correct F2 e-classes
  if (results.size() == expected_matches) {
    std::set<EClassId> matched_f2_classes;
    for (auto const &match : results) {
      auto num_slots = compiled->num_slots();
      for (std::size_t i = 0; i < num_slots; ++i) {
        auto bound = ctx.arena().get(match, i);
        if (f2_classes.count(egraph.find(bound))) {
          matched_f2_classes.insert(egraph.find(bound));
        }
      }
    }
    EXPECT_EQ(matched_f2_classes.size(), 3) << "All 3 F2 e-classes should appear in matches";
  }
}

// Test: Nested parent traversals with asymmetric counts
// Historical note: This test was originally designed to detect a buffer sharing bug
// in the old IterParentsSym implementation. Now that we use IterParents + CheckSymbol,
// there's no shared buffer, but this test still verifies correct behavior for nested
// parent iterations with different iteration counts.
//
// Setup:
// - Outer loop: 2 F2 parents of sym_val
// - Inner loop: 5 F parents of sym_val (MORE than outer)
//
// Expected: 2 F2 * 5 F = 10 matches
TEST_F(FusedPatternNestedTest, FilteredParentsBufferOverwriteBug) {
  // Create sym_val with 2 F2 parents and 5 Neg parents
  auto sym_val = leaf(Op::Const, 1);

  // 2 structurally distinct F2 parents (different second children prevent merging)
  auto f2_1 = node(Op::F2, sym_val, leaf(Op::Const, 201));
  auto f2_2 = node(Op::F2, sym_val, leaf(Op::Const, 202));

  // 5 structurally distinct Neg parents
  // Neg is unary, so we need to make them distinct via merging different structures
  // Actually, Neg(sym_val) will all merge. Let's use F instead with distinct second children.
  // Wait, let me use a different approach: create Neg of different children, then those children
  // reference sym_val.
  //
  // Actually simpler: use F (binary) for inner loop instead of Neg (unary)
  // F(sym_val, Const(i)) for i = 301..305 will be 5 distinct F parents

  std::vector<EClassId> f_parents;
  for (int i = 0; i < 5; ++i) {
    f_parents.push_back(node(Op::F, sym_val, leaf(Op::Const, 301 + i)));
  }

  // Create anchor pattern's target
  auto bind_node = node(Op::Bind, leaf(Op::Const, 0), sym_val, leaf(Op::Const, 2));

  rebuild_egraph();

  // Verify parent counts
  auto sym_class = egraph.find(sym_val);
  auto const &sym_eclass = egraph.eclass(sym_class);

  std::size_t f2_count = 0, f_count = 0;
  std::cerr << "sym_val e-class: " << sym_class << "\n";
  std::cerr << "Parents of sym_val:\n";
  for (auto parent_id : sym_eclass.parents()) {
    auto const &parent = egraph.get_enode(parent_id);
    auto parent_eclass = egraph.find(parent_id);
    std::cerr << "  ENode " << parent_id << " (e-class " << parent_eclass
              << ") symbol=" << static_cast<int>(parent.symbol());
    if (parent.symbol() == Op::F2) {
      std::cerr << " [F2]";
      ++f2_count;
    }
    if (parent.symbol() == Op::F) {
      std::cerr << " [F]";
      ++f_count;
    }
    if (parent.symbol() == Op::Bind) {
      std::cerr << " [Bind]";
    }
    std::cerr << "\n";
  }

  ASSERT_EQ(f2_count, 2) << "Should have 2 F2 parents";
  ASSERT_EQ(f_count, 5) << "Should have 5 F parents";

  // Patterns:
  // Anchor: Bind(_, ?sym, _)
  // Joined1 (outer): F2(?sym, _)  <- 2 parents, fills buffer first
  // Joined2 (inner): F(?sym, _)   <- 5 parents, overwrites buffer
  constexpr PatternVar kVarSym{1};
  constexpr PatternVar kTestRoot{0};
  constexpr PatternVar kVarF2{2};
  constexpr PatternVar kVarF{3};

  auto anchor = Pattern<Op>::build(Op::Bind, {Wildcard{}, Var{kVarSym}, Wildcard{}}, kTestRoot);
  auto joined_f2 = Pattern<Op>::build(Op::F2, {Var{kVarSym}, Wildcard{}}, kVarF2);
  auto joined_f = Pattern<Op>::build(Op::F, {Var{kVarSym}, Wildcard{}}, kVarF);

  PatternCompiler<Op> compiler;
  std::array patterns = {anchor, joined_f2, joined_f};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value());

  auto bytecode = disassemble<Op>(compiled->code(), compiled->symbols());

  // Count IterParents to verify nested parent traversals are used
  int iter_parents_count = 0;
  for (auto const &instr : compiled->code()) {
    if (instr.op == VMOp::IterParents) {
      ++iter_parents_count;
    }
  }

  // Print bytecode and symbol table for debugging
  std::cerr << "FilteredParentsBufferOverwriteBug bytecode:\n" << bytecode << "\n";
  std::cerr << "Symbol table:\n";
  for (std::size_t i = 0; i < compiled->symbols().size(); ++i) {
    auto sym = compiled->symbols()[i];
    std::cerr << "  sym[" << i << "] = " << static_cast<int>(sym);
    if (sym == Op::Bind) std::cerr << " [Bind]";
    if (sym == Op::F2) std::cerr << " [F2]";
    if (sym == Op::F) std::cerr << " [F]";
    std::cerr << "\n";
  }
  std::cerr << "num_enode_regs=" << compiled->num_enode_regs() << ", num_eclass_regs=" << compiled->num_eclass_regs()
            << "\n";
  std::cerr << "Op enum values: F2=" << static_cast<int>(Op::F2) << ", F=" << static_cast<int>(Op::F)
            << ", Bind=" << static_cast<int>(Op::Bind) << "\n";
  std::cerr << "IterParents count: " << iter_parents_count << "\n";

  // CRITICAL: We need 2 IterParents (with CheckSymbol) to test nested parent traversals
  ASSERT_GE(iter_parents_count, 2) << "Need 2 IterParents for this test to be meaningful";

  // Find which slots are used for F2 and F by examining bytecode.
  // The bytecode pattern is: IterParents dst, src -> Jump -> NextParent dst -> CheckSymbol src, sym_idx
  // So we collect IterParents dst registers, then match CheckSymbol by src register.
  // NOTE: The compiler may reorder patterns, so we can't assume r1=F2, r2=F.
  int f2_slot = -1, f_slot = -1;
  std::map<uint8_t, Op> reg_to_symbol;  // Which symbol each enode register filters

  // First pass: collect all IterParents destination registers
  std::set<uint8_t> iter_parents_regs;
  for (auto const &instr : compiled->code()) {
    if (instr.op == VMOp::IterParents) {
      iter_parents_regs.insert(instr.dst);
    }
  }

  // Second pass: for each CheckSymbol, if its src is an IterParents reg, record the symbol
  for (auto const &instr : compiled->code()) {
    if (instr.op == VMOp::CheckSymbol && iter_parents_regs.count(instr.src)) {
      auto sym = compiled->symbols()[instr.arg];
      reg_to_symbol[instr.src] = sym;
      std::cerr << "Register r" << static_cast<int>(instr.src) << " filters symbol " << static_cast<int>(sym);
      if (sym == Op::F2) std::cerr << " [F2]";
      if (sym == Op::F) std::cerr << " [F]";
      std::cerr << "\n";
    }
  }

  // Second pass: find which slot is bound from which register
  for (std::size_t i = 1; i < compiled->code().size(); ++i) {
    auto const &instr = compiled->code()[i];
    if (instr.op == VMOp::BindSlot && instr.arg != 0 && instr.arg != 1) {
      auto const &prev = compiled->code()[i - 1];
      if (prev.op == VMOp::GetENodeEClass) {
        auto src_reg = prev.src;
        if (reg_to_symbol.count(src_reg)) {
          auto sym = reg_to_symbol[src_reg];
          if (sym == Op::F2) {
            f2_slot = static_cast<int>(instr.arg);
            std::cerr << "F2 bound to slot[" << f2_slot << "] (via r" << static_cast<int>(src_reg) << ")\n";
          }
          if (sym == Op::F) {
            f_slot = static_cast<int>(instr.arg);
            std::cerr << "F bound to slot[" << f_slot << "] (via r" << static_cast<int>(src_reg) << ")\n";
          }
        }
      }
    }
  }

  ASSERT_NE(f2_slot, -1) << "Could not find F2 slot in bytecode";
  ASSERT_NE(f_slot, -1) << "Could not find F slot in bytecode";

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);
  std::vector<EClassId> candidates = {egraph.find(bind_node)};
  std::cerr << "Candidate bind_node e-class: " << egraph.find(bind_node) << "\n";

  EMatchContext ctx;
  std::vector<PatternMatch> results;
  executor.execute(*compiled, candidates, ctx, results);

  // Expected: 2 F2 parents * 5 F parents = 10 matches
  std::size_t expected_matches = 2 * 5;
  EXPECT_EQ(results.size(), expected_matches) << "Expected 2 F2 * 5 F = 10 matches.\n"
                                              << "IterParents count: " << iter_parents_count << "\n"
                                              << "Bytecode:\n"
                                              << bytecode;

  // Additional verification: check that both F2 e-classes appear in results
  auto f2_1_class = egraph.find(f2_1);
  auto f2_2_class = egraph.find(f2_2);

  // Collect e-class IDs of all F parents
  std::set<EClassId> f_classes;
  for (auto const &f_parent : f_parents) {
    f_classes.insert(egraph.find(f_parent));
  }

  std::cerr << "F2_1 e-class: " << f2_1_class << ", F2_2 e-class: " << f2_2_class << "\n";
  std::cerr << "F e-classes: ";
  for (auto fc : f_classes) std::cerr << fc << " ";
  std::cerr << "\n";

  std::set<EClassId> matched_f2_in_f2_slot;
  std::set<EClassId> wrong_in_f2_slot;  // Non-F2 e-classes found in f2_slot
  int match_idx = 0;
  for (auto const &match : results) {
    auto f2_slot_value = ctx.arena().get(match, static_cast<std::size_t>(f2_slot));
    auto canonical = egraph.find(f2_slot_value);

    std::cerr << "Match " << match_idx++ << ": slot[" << f2_slot << "]=" << f2_slot_value << " (canonical=" << canonical
              << ")";

    if (canonical == f2_1_class) {
      matched_f2_in_f2_slot.insert(canonical);
      std::cerr << " -> F2_1 (correct)\n";
    } else if (canonical == f2_2_class) {
      matched_f2_in_f2_slot.insert(canonical);
      std::cerr << " -> F2_2 (correct)\n";
    } else if (f_classes.count(canonical)) {
      wrong_in_f2_slot.insert(canonical);
      std::cerr << " -> F (WRONG!)\n";
    } else {
      wrong_in_f2_slot.insert(canonical);
      std::cerr << " -> Unknown (WRONG!)\n";
    }
  }

  EXPECT_TRUE(wrong_in_f2_slot.empty()) << "F2 slot (slot[" << f2_slot
                                        << "]) should only contain F2 e-classes, but found " << wrong_in_f2_slot.size()
                                        << " wrong e-classes.";

  EXPECT_EQ(matched_f2_in_f2_slot.size(), 2) << "Both F2 e-classes should appear in F2 slot (slot[" << f2_slot << "]). "
                                             << "If only 1, the second F2 was missed.";
}

// Test: Sequential parent traversals (no nesting - should work)
// This is the non-buggy case: two parent traversals that happen sequentially,
// not nested. The buffer gets reused but each traversal completes before the next.
TEST_F(FusedPatternNestedTest, SequentialParentTraversals) {
  // Similar setup but patterns are structured so traversals are sequential
  auto sym_val = leaf(Op::Const, 1);
  auto ident1 = node(Op::Ident, sym_val);
  auto ident2 = node(Op::Ident, sym_val);
  auto neg1 = node(Op::Neg, sym_val);

  rebuild_egraph();

  // Pattern: Ident(?sym) - find all Ident nodes
  constexpr PatternVar kVarSym{0};
  auto pattern = Pattern<Op>::build(Op::Ident, {Var{kVarSym}});

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  // Get all Ident e-classes as candidates
  std::vector<EClassId> candidates;
  for (auto id : egraph.canonical_class_ids()) {
    candidates.push_back(id);
  }

  EMatchContext ctx;
  std::vector<PatternMatch> results;
  executor.execute(*compiled, candidates, ctx, results);

  // ident1 and ident2 may be same e-class (structural sharing)
  // Should find matches for unique Ident e-classes
  bool same_class = egraph.find(ident1) == egraph.find(ident2);
  std::size_t expected = same_class ? 1 : 2;
  EXPECT_EQ(results.size(), expected) << "Should find " << expected << " Ident matches";
}

}  // namespace memgraph::planner::core
