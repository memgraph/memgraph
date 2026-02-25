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
    if (instr.op == VMOp::IterParentsSym || instr.op == VMOp::IterParents) {
      has_iter_parents = true;
    }
    if (instr.op == VMOp::NextParent || instr.op == VMOp::NextParentFiltered) {
      has_next_parent = true;
    }
    if (instr.op == VMOp::GetENodeEClass) {
      has_get_enode_eclass = true;
    }
  }

  EXPECT_TRUE(has_iter_parents) << "Fused bytecode should have IterParents(Sym) instruction\nBytecode:\n" << bytecode;
  EXPECT_TRUE(has_next_parent) << "Fused bytecode should have NextParent or NextParentFiltered instruction\nBytecode:\n"
                               << bytecode;
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

}  // namespace memgraph::planner::core
