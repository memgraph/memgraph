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
  for (auto parent_id : sym_eclass.parents()) {
    auto const &parent = egraph.get_enode(parent_id);
    if (parent.symbol() == Op::F2) {
      ++f2_count;
    }
    if (parent.symbol() == Op::F) {
      ++f_count;
    }
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
    }
  }

  // Third pass: find which slot is bound from which register
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
          }
          if (sym == Op::F) {
            f_slot = static_cast<int>(instr.arg);
          }
        }
      }
    }
  }

  ASSERT_NE(f2_slot, -1) << "Could not find F2 slot in bytecode";
  ASSERT_NE(f_slot, -1) << "Could not find F slot in bytecode";

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);
  std::vector<EClassId> candidates = {egraph.find(bind_node)};

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

  std::set<EClassId> matched_f2_in_f2_slot;
  std::set<EClassId> wrong_in_f2_slot;  // Non-F2 e-classes found in f2_slot
  for (auto const &match : results) {
    auto f2_slot_value = ctx.arena().get(match, static_cast<std::size_t>(f2_slot));
    auto canonical = egraph.find(f2_slot_value);

    if (canonical == f2_1_class) {
      matched_f2_in_f2_slot.insert(canonical);
    } else if (canonical == f2_2_class) {
      matched_f2_in_f2_slot.insert(canonical);
    } else {
      wrong_in_f2_slot.insert(canonical);
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

// ============================================================================
// Deduplication Logic Tests
// ============================================================================

// Test: Verify deduplication with multiple variables works correctly
// Bug scenario: If seen_per_slot clearing is incorrect when prefix changes,
// we might incorrectly skip valid tuples or yield duplicates.
//
// Setup:
//   Pattern: F(?a, ?b) AND F2(?b, ?c)
//   E-graph with overlapping values that could produce duplicates
//
// This tests that when ?b is rebound to the same value but with different ?a,
// the seen_per_slot for ?c is correctly cleared.
TEST_F(FusedPatternNestedTest, DeduplicationWithPrefixChange) {
  // Create e-graph:
  //   a1, a2 = Const(1), Const(2)
  //   b1 = Const(10)  (shared by multiple F and F2)
  //   c1, c2 = Const(100), Const(101)
  //
  //   F(a1, b1), F(a2, b1)  - both F's share the same b1
  //   F2(b1, c1), F2(b1, c2)  - F2 has two different c values for b1
  //
  // Pattern: F(?a, ?b) AND F2(?b, ?c)
  // Expected matches:
  //   (?a=a1, ?b=b1, ?c=c1)
  //   (?a=a1, ?b=b1, ?c=c2)
  //   (?a=a2, ?b=b1, ?c=c1)
  //   (?a=a2, ?b=b1, ?c=c2)
  // Total: 4 unique matches

  auto a1 = leaf(Op::Const, 1);
  auto a2 = leaf(Op::Const, 2);
  auto b1 = leaf(Op::Const, 10);
  auto c1 = leaf(Op::Const, 100);
  auto c2 = leaf(Op::Const, 101);

  auto f1 = node(Op::F, a1, b1);   // F(a1, b1)
  auto f2 = node(Op::F, a2, b1);   // F(a2, b1)
  auto g1 = node(Op::F2, b1, c1);  // F2(b1, c1)
  auto g2 = node(Op::F2, b1, c2);  // F2(b1, c2)

  rebuild_egraph();

  // Verify e-graph structure
  ASSERT_NE(egraph.find(a1), egraph.find(a2)) << "a1 and a2 should be distinct";
  ASSERT_NE(egraph.find(c1), egraph.find(c2)) << "c1 and c2 should be distinct";
  ASSERT_NE(egraph.find(f1), egraph.find(f2)) << "f1 and f2 should be distinct";
  ASSERT_NE(egraph.find(g1), egraph.find(g2)) << "g1 and g2 should be distinct";

  // Pattern: F(?a, ?b) joined with F2(?b, ?c)
  constexpr PatternVar kVarA{0};
  constexpr PatternVar kVarB{1};
  constexpr PatternVar kVarC{2};
  constexpr PatternVar kVarF{3};
  constexpr PatternVar kVarF2{4};

  auto pattern_f = Pattern<Op>::build(Op::F, {Var{kVarA}, Var{kVarB}}, kVarF);
  auto pattern_f2 = Pattern<Op>::build(Op::F2, {Var{kVarB}, Var{kVarC}}, kVarF2);

  PatternCompiler<Op> compiler;
  std::array patterns = {pattern_f, pattern_f2};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value()) << "Compilation should succeed";

  auto bytecode = disassemble<Op>(compiled->code(), compiled->symbols());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  // Use F e-classes as candidates
  std::vector<EClassId> candidates = {egraph.find(f1), egraph.find(f2)};

  EMatchContext ctx;
  std::vector<PatternMatch> results;
  executor.execute(*compiled, candidates, ctx, results);

  // Expected: 2 F's * 2 F2's = 4 matches
  // Each (a, b, c) tuple should appear exactly once
  std::size_t expected_matches = 4;
  EXPECT_EQ(results.size(), expected_matches) << "Expected " << expected_matches << " unique matches.\n"
                                              << "If fewer, deduplication may be incorrectly filtering valid tuples.\n"
                                              << "If more, deduplication may be missing duplicates.\n"
                                              << "Bytecode:\n"
                                              << bytecode;

  // Verify all 4 combinations appear
  // From bytecode: slot[0]=?a, slot[1]=?b, slot[2]=?varF, slot[3]=?c, slot[4]=?varF2
  // The compiler assigns slots in processing order, not by PatternVar id
  constexpr std::size_t kSlotA = 0;
  constexpr std::size_t kSlotB = 1;
  constexpr std::size_t kSlotC = 3;  // NOT kVarC.id (which is 2)

  if (results.size() == expected_matches) {
    std::set<std::tuple<EClassId, EClassId, EClassId>> seen_tuples;
    for (auto const &match : results) {
      auto a_val = ctx.arena().get(match, kSlotA);
      auto b_val = ctx.arena().get(match, kSlotB);
      auto c_val = ctx.arena().get(match, kSlotC);
      seen_tuples.insert({egraph.find(a_val), egraph.find(b_val), egraph.find(c_val)});
    }
    EXPECT_EQ(seen_tuples.size(), 4) << "All 4 (a, b, c) combinations should be unique";
  }
}

// Test: Verify deduplication when same tuple can be reached via multiple iteration paths
// This tests that identical results from different code paths are deduplicated.
TEST_F(FusedPatternNestedTest, DeduplicationMultiplePaths) {
  // Create e-graph where F(x, x) with ?a=?b pattern
  // If x has multiple e-nodes in its e-class, iteration might try to yield same binding twice
  auto x = leaf(Op::Const, 1);
  auto f_xx = node(Op::F, x, x);  // F(x, x) where both children are same e-class

  // Create another Const(1) that will merge with x (structural sharing)
  [[maybe_unused]] auto x2 = leaf(Op::Const, 1);  // Same value, will merge with x
  rebuild_egraph();

  // Pattern: F(?a, ?a) - same variable for both children
  constexpr PatternVar kVarA{0};
  constexpr PatternVar kVarF{1};

  auto pattern = Pattern<Op>::build(Op::F, {Var{kVarA}, Var{kVarA}}, kVarF);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);
  std::vector<EClassId> candidates = {egraph.find(f_xx)};

  EMatchContext ctx;
  std::vector<PatternMatch> results;
  executor.execute(*compiled, candidates, ctx, results);

  // Should find exactly 1 match for F(x, x) even if x's e-class has multiple e-nodes
  EXPECT_EQ(results.size(), 1) << "Should find exactly 1 match for F(?a, ?a)";
}

// ============================================================================
// VMState Unit Tests - Direct State Manipulation
// ============================================================================

// Test: Global deduplication across candidates via bind-time dedup.
// When the same value is bound to a slot with the same prefix, it's a duplicate.
// The dedup check happens at bind time, not yield time.
TEST(VMStateTest, GlobalDeduplicationAcrossCandidates) {
  using namespace vm;

  VMState state;
  state.reset(2, 1, 1);  // 2 slots, 1 eclass reg, 1 enode reg

  // Simulate first "candidate" execution:
  // Bind slot 0 to e-class 100, slot 1 to e-class 200
  // Using try_bind_dedup for all slots (bind-time dedup)
  EXPECT_TRUE(state.try_bind_dedup(0, EClassId{100})) << "First bind of slot 0 should succeed";
  EXPECT_TRUE(state.try_bind_dedup(1, EClassId{200})) << "First bind of slot 1 should succeed";

  // Simulate moving to second "candidate"
  // seen_per_slot is NOT cleared (intentional for global dedup)
  state.pc = 0;

  // Slots retain stale values: slots[0] = 100, slots[1] = 200
  // This simulates a new candidate that happens to bind the same values

  // Bind slot 0 to the SAME value (100)
  // Since slots[0] == 100 (stale match), seen_per_slot[1] is NOT cleared
  // This is intentional: same prefix context means same dedup context
  // But seen_per_slot[0] already contains 100, so this returns false (duplicate)
  bool bind0_result = state.try_bind_dedup(0, EClassId{100});

  // Global deduplication: same value with same prefix should be filtered as duplicate
  EXPECT_FALSE(bind0_result) << "Second bind of slot 0 with same value should be deduplicated (global dedup).\n"
                             << "The stale value comparison in bind() intentionally preserves seen_per_slot\n"
                             << "when the binding value matches, enabling cross-candidate deduplication.";
}

// Test: Verify correct behavior when slot value actually changes
// When a prefix slot changes, downstream seen sets are cleared, so same values become new.
TEST(VMStateTest, BindSlotValueChange) {
  using namespace vm;

  VMState state;
  state.reset(2, 1, 1);

  // First candidate: (100, 200)
  EXPECT_TRUE(state.try_bind_dedup(0, EClassId{100})) << "First bind of slot 0 should succeed";
  EXPECT_TRUE(state.try_bind_dedup(1, EClassId{200})) << "First bind of slot 1 should succeed";

  // Second candidate with DIFFERENT slot 0 value
  state.pc = 0;

  // Different value for slot 0 - should clear seen_per_slot[1]
  EXPECT_TRUE(state.try_bind_dedup(0, EClassId{101})) << "New value for slot 0 should succeed";
  // Same value 200 for slot 1, but prefix changed, so seen_per_slot[1] was cleared
  EXPECT_TRUE(state.try_bind_dedup(1, EClassId{200})) << "Same slot 1 value with different prefix should succeed";
}

// Test: Verify deduplication still works within same candidate
// Same slot value with same prefix should be deduplicated at bind time.
TEST(VMStateTest, DeduplicationWithinCandidate) {
  using namespace vm;

  VMState state;
  state.reset(2, 1, 1);

  // Same candidate, same values twice - should be deduplicated at bind time
  EXPECT_TRUE(state.try_bind_dedup(0, EClassId{100})) << "First bind of slot 0 should succeed";
  EXPECT_TRUE(state.try_bind_dedup(1, EClassId{200})) << "First bind of slot 1 should succeed";

  // Try to bind same values again (simulating another iteration reaching same state)
  // Since slots still have same values (no prefix change), seen sets are preserved
  EXPECT_FALSE(state.try_bind_dedup(0, EClassId{100})) << "Second bind of slot 0 (same value) should be deduplicated";
}

// ============================================================================
// Binding Order Tests - Verify out-of-order binding is tracked correctly
// ============================================================================

// Test: Verify CompiledPattern tracks binding order for joined patterns with parent traversal.
// Uses the same pattern structure as DeduplicationWithPrefixChange which is known to work.
TEST_F(FusedPatternNestedTest, BindingOrderTrackedForJoinedPatterns) {
  // Create e-graph (same as DeduplicationWithPrefixChange):
  //   a1, a2 = Const(1), Const(2)
  //   b1 = Const(10)  (shared by multiple F and F2)
  //   c1, c2 = Const(100), Const(101)
  //
  //   F(a1, b1), F(a2, b1)  - both F's share the same b1
  //   F2(b1, c1), F2(b1, c2)  - F2 has two different c values for b1
  //
  // Pattern: F(?a, ?b) AS ?f AND F2(?b, ?c) AS ?f2
  // After matching F (anchor), we traverse UP from ?b to find F2 parents.
  // Binding order depends on compilation, but should be consistent.

  auto a1 = leaf(Op::Const, 1);
  auto a2 = leaf(Op::Const, 2);
  auto b1 = leaf(Op::Const, 10);
  auto c1 = leaf(Op::Const, 100);
  auto c2 = leaf(Op::Const, 101);

  auto f1 = node(Op::F, a1, b1);   // F(a1, b1)
  auto f2 = node(Op::F, a2, b1);   // F(a2, b1)
  auto g1 = node(Op::F2, b1, c1);  // F2(b1, c1)
  auto g2 = node(Op::F2, b1, c2);  // F2(b1, c2)

  rebuild_egraph();

  // Pattern: F(?a, ?b) AS ?f joined with F2(?b, ?c) AS ?f2
  constexpr PatternVar kVarA{0};
  constexpr PatternVar kVarB{1};
  constexpr PatternVar kVarF{2};
  constexpr PatternVar kVarC{3};
  constexpr PatternVar kVarF2{4};

  auto pattern_f = Pattern<Op>::build(Op::F, {Var{kVarA}, Var{kVarB}}, kVarF);
  auto pattern_f2 = Pattern<Op>::build(Op::F2, {Var{kVarB}, Var{kVarC}}, kVarF2);

  PatternCompiler<Op> compiler;
  std::array patterns = {pattern_f, pattern_f2};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value()) << "Compilation should succeed";

  // Verify binding_order is tracked
  auto binding_order = compiled->binding_order();
  ASSERT_FALSE(binding_order.empty()) << "Binding order should not be empty";

  // The binding order should contain all slots
  std::set<uint8_t> bound_slots(binding_order.begin(), binding_order.end());
  EXPECT_EQ(bound_slots.size(), compiled->num_slots()) << "All slots should appear exactly once in binding_order";

  // Verify last_bound_slot is consistent with binding_order
  ASSERT_FALSE(binding_order.empty());
  EXPECT_EQ(compiled->last_bound_slot(), binding_order.back())
      << "last_bound_slot should be the last element of binding_order";

  // Verify slots_bound_after is consistent with binding_order
  // For each position i in binding_order, slots_bound_after[binding_order[i]]
  // should contain all slots at positions > i
  for (std::size_t i = 0; i < binding_order.size(); ++i) {
    auto slot = binding_order[i];
    auto after = compiled->slots_bound_after(slot);

    // Expected: all slots at positions > i in binding_order
    std::vector<uint8_t> expected_after;
    for (std::size_t j = i + 1; j < binding_order.size(); ++j) {
      expected_after.push_back(binding_order[j]);
    }

    EXPECT_EQ(after.size(), expected_after.size())
        << "slots_bound_after[" << static_cast<int>(slot) << "] has wrong size";

    for (std::size_t k = 0; k < std::min(after.size(), expected_after.size()); ++k) {
      EXPECT_EQ(after[k], expected_after[k])
          << "slots_bound_after[" << static_cast<int>(slot) << "][" << k << "] mismatch";
    }
  }

  // Execute and verify we get the expected matches
  VMExecutorVerify<Op, NoAnalysis> executor(egraph);
  std::vector<EClassId> candidates = {egraph.find(f1), egraph.find(f2)};

  EMatchContext ctx;
  std::vector<PatternMatch> results;
  executor.execute(*compiled, candidates, ctx, results);

  // Expected: 2 F's * 2 F2's = 4 matches
  EXPECT_EQ(results.size(), 4) << "Should find 4 unique matches (2 F's * 2 F2's)";
}

// Test: Verify deduplication works correctly when binding order differs from slot index order.
// This is a regression test for the bug where clearing seen_per_slot based on slot index
// instead of binding order caused incorrect deduplication.
TEST_F(FusedPatternNestedTest, DeduplicationWithOutOfOrderBinding) {
  // Create e-graph where parent traversal causes out-of-order slot binding:
  //   val = Const(1)
  //   F1(val, Const(2))
  //   F2(val, Const(3))
  //   F2(val, Const(4))  <- two F2 parents of val
  //   Outer = Bind(_, val, _)
  //
  // Pattern: Bind(_, ?x, _) AND F(?x, ?a) AS ?f AND F2(?x, ?b) AS ?f2
  //
  // Slots: ?x=0, ?a=1, ?f=2, ?b=3, ?f2=4
  // Binding order from execution: ?x first, then ?a/?f from F parent, then ?b/?f2 from F2 parent
  // If binding order is [0, 1, 2, 3, 4] (happens to match), then we need a case where it doesn't.

  // Let's use a simpler case that guarantees out-of-order:
  // Pattern: F2(?y, ?z) AS ?f2 joined with F(?y, _)
  // If F2 is anchor, we bind ?y, ?z, ?f2 in some order
  // Then join F via parent traversal from ?y - no new bindings for F pattern itself
  // Actually this won't help because F has no new vars...

  // Better test: Single pattern with nested structure that binds out of order
  // Pattern: F(?outer_var, G(?inner_var)) AS ?root
  // With G compiled "inside", the slot order might be:
  //   ?outer_var = slot 0
  //   ?inner_var = slot 1
  //   ?root = slot 2
  // But binding order could be: root's e-class first, then children...
  // Actually this still binds in child-first order during iteration.

  // The clearest test is the multi-pattern join case:
  // Pattern 1: F(?a, ?b) AS ?f
  // Pattern 2: F2(?b, ?c) AS ?f2  <- joins via ?b
  //
  // Slot assignment: ?a=0, ?b=1, ?f=2, ?c=3, ?f2=4
  // Binding order during execution:
  //   1. Match F: bind ?f (slot 2), then ?a (slot 0), then ?b (slot 1)
  //   2. Parent traversal from ?b to F2: bind ?f2 (slot 4), then ?c (slot 3)
  // So binding order = [2, 0, 1, 4, 3] (NOT [0, 1, 2, 3, 4])

  auto a1 = leaf(Op::Const, 1);
  auto a2 = leaf(Op::Const, 2);
  auto b = leaf(Op::Const, 10);
  auto c1 = leaf(Op::Const, 100);
  auto c2 = leaf(Op::Const, 101);

  auto f1 = node(Op::F, a1, b);   // F(a1, b)
  auto f2 = node(Op::F, a2, b);   // F(a2, b)
  auto g1 = node(Op::F2, b, c1);  // F2(b, c1)
  auto g2 = node(Op::F2, b, c2);  // F2(b, c2)

  rebuild_egraph();

  // Verify setup: two different F's share the same b, which has two F2 parents
  ASSERT_EQ(egraph.find(f1), egraph.find(f1));
  ASSERT_NE(egraph.find(f1), egraph.find(f2));
  ASSERT_NE(egraph.find(g1), egraph.find(g2));

  // Pattern: F(?a, ?b) AS ?f joined with F2(?b, ?c) AS ?f2
  constexpr PatternVar kVarA{0};
  constexpr PatternVar kVarB{1};
  constexpr PatternVar kVarF{2};
  constexpr PatternVar kVarC{3};
  constexpr PatternVar kVarF2{4};

  auto pattern_f = Pattern<Op>::build(Op::F, {Var{kVarA}, Var{kVarB}}, kVarF);
  auto pattern_f2 = Pattern<Op>::build(Op::F2, {Var{kVarB}, Var{kVarC}}, kVarF2);

  PatternCompiler<Op> compiler;
  std::array patterns = {pattern_f, pattern_f2};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value());

  // Execute pattern
  VMExecutorVerify<Op, NoAnalysis> executor(egraph);
  std::vector<EClassId> candidates = {egraph.find(f1), egraph.find(f2)};

  EMatchContext ctx;
  std::vector<PatternMatch> results;
  executor.execute(*compiled, candidates, ctx, results);

  // Expected: 2 F's * 2 F2's = 4 unique matches
  // Each (a, b, c) combination should appear exactly once
  std::size_t expected_matches = 4;
  EXPECT_EQ(results.size(), expected_matches)
      << "Expected " << expected_matches << " unique matches.\n"
      << "If fewer, deduplication based on binding order may be too aggressive.\n"
      << "If more, deduplication may be missing duplicates.";

  // Verify uniqueness of results
  if (results.size() == expected_matches) {
    // Get slot indices from compiler's slot_map
    auto const &slot_map = compiler.slot_map();
    auto slot_a = static_cast<std::size_t>(slot_map.at(kVarA));
    auto slot_b = static_cast<std::size_t>(slot_map.at(kVarB));
    auto slot_c = static_cast<std::size_t>(slot_map.at(kVarC));

    std::set<std::tuple<EClassId, EClassId, EClassId>> unique_tuples;
    for (auto const &match : results) {
      auto a_val = ctx.arena().get(match, slot_a);
      auto b_val = ctx.arena().get(match, slot_b);
      auto c_val = ctx.arena().get(match, slot_c);
      unique_tuples.insert({egraph.find(a_val), egraph.find(b_val), egraph.find(c_val)});
    }

    EXPECT_EQ(unique_tuples.size(), expected_matches) << "All matches should have unique (a, b, c) tuples";
  }
}

// Test: Verify VMState binding order clears correct seen sets when slots change.
// This directly tests the bind() -> slots_bound_after clearing logic.
TEST(VMStateTest, BindingOrderClearsSlotsCorrectly) {
  using namespace vm;

  // Simulate binding order [1, 0, 2] (not sequential)
  // When slot 1 changes: clear seen for slots 0 and 2 (bound after 1)
  // When slot 0 changes: clear seen for slot 2 (bound after 0)
  // When slot 2 changes: clear nothing (last in order)

  // Build slots_bound_after based on binding_order [1, 0, 2]:
  // slot 1 (pos 0): slots after = [0, 2]
  // slot 0 (pos 1): slots after = [2]
  // slot 2 (pos 2): slots after = []
  std::vector<std::vector<uint8_t>> slots_after_storage = {
      {2},     // slot 0: clear slot 2 when changed
      {0, 2},  // slot 1: clear slots 0 and 2 when changed
      {}       // slot 2: clear nothing (last in order)
  };

  VMState state;
  state.reset(
      3,
      1,
      1,
      [&slots_after_storage](std::size_t slot) -> std::span<uint8_t const> { return slots_after_storage[slot]; },
      2  // last_bound_slot = 2
  );

  // Bind in order: slot 1, slot 0, slot 2 (using try_bind_dedup for all)
  EXPECT_TRUE(state.try_bind_dedup(1, EClassId{100})) << "First bind of slot 1 should succeed";
  EXPECT_TRUE(state.try_bind_dedup(0, EClassId{200})) << "First bind of slot 0 should succeed";
  EXPECT_TRUE(state.try_bind_dedup(2, EClassId{300})) << "First bind of slot 2 should succeed";

  // Same values should be deduplicated (slot 2 is last, triggers dedup check)
  // Try binding same value to slot 2 again - should fail (duplicate)
  EXPECT_FALSE(state.try_bind_dedup(2, EClassId{300})) << "Same slot 2 value with same prefix should be deduplicated";

  // Now change slot 0 (middle in binding order)
  // This should clear seen_per_slot[2] (slot 2 is bound after slot 0)
  EXPECT_TRUE(state.try_bind_dedup(0, EClassId{201})) << "New value for slot 0 should succeed";

  // Re-bind slot 2 with same value 300
  // This should now succeed because seen_per_slot[2] was cleared when slot 0 changed
  EXPECT_TRUE(state.try_bind_dedup(2, EClassId{300})) << "After prefix change, same slot[2] value should be new";

  // Now change slot 1 (first in binding order)
  // This should clear seen_per_slot[0] and seen_per_slot[2]
  EXPECT_TRUE(state.try_bind_dedup(1, EClassId{101})) << "New value for slot 1 should succeed";

  // Bind slot 0 back to original value (200) - should succeed because seen_per_slot[0] was cleared
  EXPECT_TRUE(state.try_bind_dedup(0, EClassId{200})) << "Original slot 0 value should be new after slot 1 changed";

  // Bind slot 2 with same value - should succeed because seen_per_slot[2] was cleared
  EXPECT_TRUE(state.try_bind_dedup(2, EClassId{300})) << "After root prefix change, tuple should be new";
}

}  // namespace memgraph::planner::core
