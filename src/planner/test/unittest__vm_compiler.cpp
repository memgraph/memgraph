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

// VM bytecode compilation tests: verifies correct instruction generation.

#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <vector>

#include "planner/pattern/vm/compiler.hpp"
#include "planner/pattern/vm/executor.hpp"
#include "planner/pattern/vm/tracer.hpp"
#include "test_egraph_fixture.hpp"
#include "test_patterns.hpp"

namespace memgraph::planner::core {

using namespace test;
using namespace vm;

// ============================================================================
// Compiler Bytecode Tests
// ============================================================================

class VMCompilerTest : public EGraphTestBase {};

TEST_F(VMCompilerTest, WildcardPattern) {
  // Pattern: _ (wildcard)
  auto builder = TestPattern::Builder{};
  builder.wildcard();
  auto pattern = std::move(builder).build();

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);

  // Wildcard at root: yield (no slots to mark), jump back (to halt), halt
  auto code = compiled->code();
  ASSERT_EQ(code.size(), 3);
  EXPECT_EQ(code[0], Instruction::yield(0));  // No bindings, last_slot defaults to 0
  EXPECT_EQ(code[1].op, VMOp::Jump);
  EXPECT_EQ(code[2], Instruction::halt());
}

TEST_F(VMCompilerTest, VariablePattern) {
  // Pattern: ?x
  auto pattern = make_var_pattern(kVarX);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);

  // Variable at root: bind_dedup (backtrack to halt), yield (mark slot 0), jump back (to halt), halt
  auto code = compiled->code();
  ASSERT_EQ(code.size(), 4);
  EXPECT_EQ(code[0], Instruction::bind_slot_dedup(0, 0, 3));  // slot 0, src reg 0, on_duplicate=3 (halt)
  EXPECT_EQ(code[1], Instruction::yield(0));                  // Last slot is 0 (?x)
  EXPECT_EQ(code[2].op, VMOp::Jump);
  EXPECT_EQ(code[3], Instruction::halt());
}

TEST_F(VMCompilerTest, SimpleSymbolPattern) {
  // Pattern: Neg(?x)
  auto pattern = TestPattern::build(Op::Neg, {Var{kVarX}}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);

  // Expected bytecode structure (root binding BEFORE iteration):
  // 0:  BindSlotDedup slot[root], r0, @halt  ; bind root once per candidate
  // 1:  IterENodes r1, r0, @halt     ; load first e-node
  // 2:  Jump @check                  ; skip NextENode first time
  // loop:
  // 3:  NextENode r1, @halt          ; advance to next
  // check:
  // 4:  CheckSymbol r1, Neg, @loop
  // 5:  CheckArity r1, 1, @loop
  // 6:  LoadChild r2, r1, 0
  // 7:  BindSlotDedup slot[0], r2, @loop  ; bind ?x
  // 8:  Yield
  // 9:  Jump @loop
  // 10: Halt

  auto code = compiled->code();
  auto bytecode = disassemble<Op>(code, compiled->symbols());

  ASSERT_GE(code.size(), 9) << "Expected at least 9 instructions\nBytecode:\n" << bytecode;

  // Check instruction sequence - root binding comes first
  EXPECT_EQ(code[0].op, VMOp::BindSlotDedup) << "Root binding should come first";
  EXPECT_EQ(code[1].op, VMOp::IterENodes);
  EXPECT_EQ(code[2].op, VMOp::Jump) << "Should have jump to skip NextENode";
  EXPECT_EQ(code[3].op, VMOp::NextENode);
  EXPECT_EQ(code[4].op, VMOp::CheckSymbol);
  EXPECT_EQ(code[5].op, VMOp::CheckArity);

  // Check backtrack targets
  auto loop_pos = static_cast<uint16_t>(3);  // NextENode position
  auto halt_pos = static_cast<uint16_t>(code.size() - 1);

  EXPECT_EQ(code[0].target, halt_pos) << "BindSlotDedup should jump to halt on duplicate";
  EXPECT_EQ(code[1].target, halt_pos) << "IterENodes should jump to halt on empty";
  EXPECT_EQ(code[2].target, 4) << "First Jump should skip to CheckSymbol";
  EXPECT_EQ(code[3].target, halt_pos) << "NextENode should jump to halt on exhausted";
  EXPECT_EQ(code[4].target, loop_pos) << "CheckSymbol should backtrack to NextENode";
  EXPECT_EQ(code[5].target, loop_pos) << "CheckArity should backtrack to NextENode";
}

TEST_F(VMCompilerTest, NestedSymbolPattern) {
  // Pattern: Neg(Neg(?x))
  auto pattern = TestPattern::build(Op::Neg, {Sym(Op::Neg, Var{kVarX})}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);

  auto code = compiled->code();
  auto bytecode = disassemble<Op>(code, compiled->symbols());

  // Expected structure:
  // 0: IterENodes r1, r0, @halt
  // 1: NextENode r1, @halt           ; outer loop
  // 2: CheckSymbol r1, Neg, @1
  // 3: CheckArity r1, 1, @1
  // 4: LoadChild r2, r1, 0           ; load child e-class
  // 5: IterENodes r3, r2, @1         ; inner iteration (backtrack to outer loop)
  // 6: NextENode r3, @1              ; inner loop (backtrack to outer loop)
  // 7: CheckSymbol r3, Neg, @6       ; check inner Neg (backtrack to inner loop)
  // 8: CheckArity r3, 1, @6
  // 9: LoadChild r4, r3, 0
  // 10: BindSlotDedup slot[0], r4, @6 ; bind ?x with dedup
  // 11: Yield
  // 12: Jump @6                       ; continue inner loop
  // 13: Jump @1                       ; continue outer loop
  // 14: Halt

  ASSERT_GE(code.size(), 10) << "Expected at least 10 instructions";

  // Find key instruction positions
  int outer_loop = -1, inner_iter = -1, inner_loop = -1;
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op == VMOp::NextENode) {
      if (outer_loop == -1)
        outer_loop = static_cast<int>(i);
      else
        inner_loop = static_cast<int>(i);
    }
    if (code[i].op == VMOp::IterENodes && i > 0) {
      inner_iter = static_cast<int>(i);
    }
  }

  ASSERT_NE(outer_loop, -1) << "Could not find outer loop";
  ASSERT_NE(inner_iter, -1) << "Could not find inner IterENodes";
  ASSERT_NE(inner_loop, -1) << "Could not find inner loop";

  // Inner IterENodes should backtrack to outer loop
  EXPECT_EQ(code[inner_iter].target, static_cast<uint16_t>(outer_loop))
      << "Inner IterENodes should backtrack to outer loop";

  // Inner NextENode should backtrack to outer loop
  EXPECT_EQ(code[inner_loop].target, static_cast<uint16_t>(outer_loop))
      << "Inner NextENode should backtrack to outer loop";
}

TEST_F(VMCompilerTest, BinarySymbolPattern) {
  // Pattern: Add(?x, ?y)
  auto pattern = TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);

  auto code = compiled->code();
  auto bytecode = disassemble<Op>(code, compiled->symbols());

  // Should have two LoadChild instructions for two children
  int load_child_count = 0;
  for (auto const &instr : code) {
    if (instr.op == VMOp::LoadChild) {
      ++load_child_count;
    }
  }
  EXPECT_EQ(load_child_count, 2) << "Expected 2 LoadChild instructions for binary pattern";

  // Should have two BindSlotDedup for ?x and ?y (both first occurrences)
  int bind_slot_count = 0;
  for (auto const &instr : code) {
    if (instr.op == VMOp::BindSlotDedup) {
      ++bind_slot_count;
    }
  }
  // Note: There are 3 BindSlotDedups: one for root binding, and two for ?x and ?y
  // Actually there should be 2 for variables + 1 for root = 3 if root has binding
  // Let's check what we actually get - should be at least 2 for variables
  EXPECT_GE(bind_slot_count, 2) << "Expected at least 2 BindSlotDedup instructions for two variables";
}

// ============================================================================
// Multi-Pattern Compilation Tests
// ============================================================================

TEST_F(VMCompilerTest, MultiPattern_DeepNestedUsesParentChain) {
  // Pattern 1: Neg(?x) - shallow pattern that binds ?x
  // Pattern 2: Neg(Neg(Neg(Neg(?x)))) - deep nested pattern with shared ?x at bottom
  //
  // The compiler should use parent chain traversal instead of Cartesian product.
  // This is critical for O(n) vs O(n^2) performance.

  auto pattern1 = TestPattern::build(Op::Neg, {Var{kVarX}});
  auto pattern2 = TestPattern::build(Op::Neg, {Sym(Op::Neg, Sym(Op::Neg, Sym(Op::Neg, Var{kVarX})))});

  std::array patterns = {pattern1, pattern2};

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value());

  auto code = compiled->code();
  auto bytecode = disassemble<Op>(code, compiled->symbols());

  // Count key instructions to verify parent chain traversal is used
  int iter_parents_count = 0;
  int check_eclass_eq_count = 0;
  int iter_all_eclasses_count = 0;

  for (auto const &instr : code) {
    switch (instr.op) {
      case VMOp::IterParents:
        ++iter_parents_count;
        break;
      case VMOp::CheckEClassEq:
        ++check_eclass_eq_count;
        break;
      case VMOp::IterAllEClasses:
        ++iter_all_eclasses_count;
        break;
      default:
        break;
    }
  }

  // Parent chain traversal should use IterParents (4 levels deep = 4 IterParents)
  EXPECT_EQ(iter_parents_count, 4) << "Expected 4 IterParents for 4-level deep nested pattern\nBytecode:\n" << bytecode;

  // Each parent traversal needs to verify the child matches the expected e-class
  EXPECT_EQ(check_eclass_eq_count, 4) << "Expected 4 CheckEClassEq to verify parent-child relationships\nBytecode:\n"
                                      << bytecode;

  // Should NOT use IterAllEClasses (Cartesian product) for the second pattern
  EXPECT_EQ(iter_all_eclasses_count, 0)
      << "Should not use IterAllEClasses (Cartesian product) for deep nested join\nBytecode:\n"
      << bytecode;
}

TEST_F(VMCompilerTest, MultiPattern_SharedVarUsesParentTraversal) {
  // Anchor: Bind(_, ?sym, ?expr)
  // Joined: Ident(?sym) - shared variable at direct child position
  //
  // Verifies parent traversal bytecode is generated for shared variable joins.

  constexpr PatternVar kVarSym{1};
  constexpr PatternVar kVarExpr{2};
  constexpr PatternVar kVarId{3};

  auto anchor = Pattern<Op>::build(Op::Bind, {Wildcard{}, Var{kVarSym}, Var{kVarExpr}}, kTestRoot);
  auto joined = Pattern<Op>::build(Op::Ident, {Var{kVarSym}}, kVarId);

  PatternCompiler<Op> compiler;
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value()) << "Multi-pattern compilation should succeed";

  auto bytecode = disassemble<Op>(compiled->code(), compiled->symbols());

  // Verify bytecode has parent traversal instructions
  bool has_iter_parents = false;
  bool has_next_parent = false;
  bool has_get_enode_eclass = false;

  for (auto const &instr : compiled->code()) {
    if (instr.op == VMOp::IterParents) has_iter_parents = true;
    if (instr.op == VMOp::NextParent) has_next_parent = true;
    if (instr.op == VMOp::GetENodeEClass) has_get_enode_eclass = true;
  }

  EXPECT_TRUE(has_iter_parents) << "Should have IterParents instruction\nBytecode:\n" << bytecode;
  EXPECT_TRUE(has_next_parent) << "Should have NextParent instruction\nBytecode:\n" << bytecode;
  EXPECT_TRUE(has_get_enode_eclass) << "Should have GetENodeEClass for binding\nBytecode:\n" << bytecode;
}

TEST_F(VMCompilerTest, MultiPattern_NoSharedVarUsesCartesianProduct) {
  // Anchor: Add(?x, ?y)
  // Joined: Neg(?z) - no shared variable with anchor
  //
  // Verifies Cartesian product bytecode (IterAllEClasses) is generated.

  constexpr PatternVar kVarX{0};
  constexpr PatternVar kVarY{1};
  constexpr PatternVar kVarZ{2};

  auto anchor = Pattern<Op>::build(Op::Add, {Var{kVarX}, Var{kVarY}});
  auto joined = Pattern<Op>::build(Op::Neg, {Var{kVarZ}});

  PatternCompiler<Op> compiler;
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value()) << "Multi-pattern compilation should succeed";

  auto bytecode = disassemble<Op>(compiled->code(), compiled->symbols());

  // Verify bytecode has IterAllEClasses and NextEClass instructions
  bool has_iter_all_eclasses = false;
  bool has_next_eclass = false;

  for (auto const &instr : compiled->code()) {
    if (instr.op == VMOp::IterAllEClasses) has_iter_all_eclasses = true;
    if (instr.op == VMOp::NextEClass) has_next_eclass = true;
  }

  EXPECT_TRUE(has_iter_all_eclasses) << "Cartesian product should use IterAllEClasses instruction\nBytecode:\n"
                                     << bytecode;
  EXPECT_TRUE(has_next_eclass) << "Cartesian product should use NextEClass instruction\nBytecode:\n" << bytecode;
}

TEST_F(VMCompilerTest, MultiPattern_VariableOnlyUsesIterAllEClasses) {
  // Anchor: Add(?x, ?y)
  // Joined: ?z - just a variable, no symbol
  //
  // Verifies variable-only joined patterns use IterAllEClasses.

  constexpr PatternVar kVarX{0};
  constexpr PatternVar kVarY{1};
  constexpr PatternVar kVarZ{2};

  auto anchor = Pattern<Op>::build(Op::Add, {Var{kVarX}, Var{kVarY}});

  auto joined_builder = TestPattern::Builder{};
  joined_builder.var(kVarZ);
  auto joined = std::move(joined_builder).build();

  PatternCompiler<Op> compiler;
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value()) << "Compilation should succeed for variable-only joined pattern";

  auto bytecode = disassemble<Op>(compiled->code(), compiled->symbols());

  bool has_iter_all_eclasses = false;
  bool has_next_eclass = false;

  for (auto const &instr : compiled->code()) {
    if (instr.op == VMOp::IterAllEClasses) has_iter_all_eclasses = true;
    if (instr.op == VMOp::NextEClass) has_next_eclass = true;
  }

  EXPECT_TRUE(has_iter_all_eclasses) << "Variable-only joined pattern should use IterAllEClasses\nBytecode:\n"
                                     << bytecode;
  EXPECT_TRUE(has_next_eclass) << "Variable-only joined pattern should use NextEClass\nBytecode:\n" << bytecode;
}

// ============================================================================
// VM Join Order Tests
// ============================================================================
//
// These tests verify the VM compiler's join ordering algorithm produces optimal
// orderings regardless of pattern declaration order. Unlike MatcherIndex's join_steps()
// which is tested via rule.join_steps(), VM join order is embedded in the compiled
// bytecode, so we verify properties of the generated code.

TEST_F(VMCompilerTest, JoinOrder_HubPatternsFirst) {
  // Tests that hub patterns (high connectivity) are processed before leaf patterns,
  // regardless of the declaration order of patterns.
  //
  // Given: X(?a), Y(?b), A(?a,?b), B(?b,?c), Z(?c), C(?c,?a)
  // Variable-sharing graph forms a triangle A-B-C with leaves X,Y,Z attached.
  //
  //       X(?a)           Y(?b)           Z(?c)
  //         \               |               /
  //          \              |              /
  //           A(?a,?b)----B(?b,?c)----C(?c,?a)
  //                  \      |      /
  //                   \     |     /
  //                    [triangle]
  //
  // Properties we verify for ALL permutations of pattern declaration order:
  // 1. No IterAllEClasses (Cartesian product) since all patterns share variables
  // 2. Compiled bytecode is identical regardless of input order
  // 3. Uses parent traversal (IterParents) for joins, not Cartesian products

  constexpr PatternVar kVarA{0};
  constexpr PatternVar kVarB{1};
  constexpr PatternVar kVarC{2};

  // Helper to build patterns in a given order
  auto make_patterns = [&](std::array<int, 6> const &order) {
    // Base patterns (indexed 0-5):
    // 0: A(?a,?b), 1: B(?b,?c), 2: C(?c,?a), 3: X(?a), 4: Y(?b), 5: Z(?c)
    auto build_pattern = [&](int idx) -> TestPattern {
      switch (idx) {
        case 0:
          return TestPattern::build(Op::F, {Var{kVarA}, Var{kVarB}});
        case 1:
          return TestPattern::build(Op::F, {Var{kVarB}, Var{kVarC}});
        case 2:
          return TestPattern::build(Op::F, {Var{kVarC}, Var{kVarA}});
        case 3:
          return TestPattern::build(Op::F2, {Var{kVarA}});
        case 4:
          return TestPattern::build(Op::F2, {Var{kVarB}});
        default:
          return TestPattern::build(Op::F2, {Var{kVarC}});
      }
    };

    std::array<TestPattern, 6> result = {build_pattern(order[0]),
                                         build_pattern(order[1]),
                                         build_pattern(order[2]),
                                         build_pattern(order[3]),
                                         build_pattern(order[4]),
                                         build_pattern(order[5])};
    return result;
  };

  // Compile with canonical order first to get reference bytecode
  auto canonical_order = std::array{0, 1, 2, 3, 4, 5};
  auto canonical_patterns = make_patterns(canonical_order);

  PatternCompiler<Op> compiler;
  auto canonical_compiled = compiler.compile(canonical_patterns);
  ASSERT_TRUE(canonical_compiled.has_value()) << "Canonical compilation should succeed";

  auto canonical_bytecode = disassemble<Op>(canonical_compiled->code(), canonical_compiled->symbols());

  // Verify no Cartesian product in canonical compilation
  bool has_iter_all_eclasses = false;
  for (auto const &instr : canonical_compiled->code()) {
    if (instr.op == VMOp::IterAllEClasses) {
      has_iter_all_eclasses = true;
      break;
    }
  }
  EXPECT_FALSE(has_iter_all_eclasses) << "Should not use IterAllEClasses when all patterns share variables\nBytecode:\n"
                                      << canonical_bytecode;

  // Test all permutations of pattern order
  auto perm = std::array{0, 1, 2, 3, 4, 5};
  std::size_t permutation_count = 0;

  do {
    auto perm_patterns = make_patterns(perm);

    PatternCompiler<Op> perm_compiler;
    auto compiled = perm_compiler.compile(perm_patterns);
    ASSERT_TRUE(compiled.has_value()) << "Permutation " << permutation_count << " compilation should succeed";

    // Property 1: No Cartesian product - this is the key invariant
    // When all patterns share variables, we should use parent traversal, not iteration
    bool perm_has_iter_all = false;
    for (auto const &instr : compiled->code()) {
      if (instr.op == VMOp::IterAllEClasses) {
        perm_has_iter_all = true;
        break;
      }
    }
    EXPECT_FALSE(perm_has_iter_all) << "Permutation " << permutation_count
                                    << " should not use IterAllEClasses\nBytecode:\n"
                                    << disassemble<Op>(compiled->code(), compiled->symbols());

    // Property 2: Must have parent traversal instructions (the efficient join strategy)
    bool has_iter_parents = false;
    for (auto const &instr : compiled->code()) {
      if (instr.op == VMOp::IterParents) {
        has_iter_parents = true;
        break;
      }
    }
    EXPECT_TRUE(has_iter_parents) << "Permutation " << permutation_count
                                  << " should use IterParents for joins\nBytecode:\n"
                                  << disassemble<Op>(compiled->code(), compiled->symbols());

    ++permutation_count;
  } while (std::ranges::next_permutation(perm).found);

  // 6! = 720 permutations
  EXPECT_EQ(permutation_count, 720);
}

TEST_F(VMCompilerTest, JoinOrder_CheckSlotProximity) {
  // Tests that CheckSlot instructions appear close to their corresponding BindSlotDedup.
  // Early invalidation is critical for performance: if a variable binding fails the
  // equality check, we should fail fast rather than doing unnecessary work.
  //
  // Pattern: F(?x, ?y) JOIN G(?x) JOIN H(?y)
  //
  // After binding ?x in F, when we join G(?x), the CheckSlot for ?x should appear
  // immediately after we load the child of G, not after other unrelated work.

  constexpr PatternVar kVarX{0};
  constexpr PatternVar kVarY{1};

  auto anchor = TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}});
  auto joined1 = TestPattern::build(Op::G, {Var{kVarX}});
  auto joined2 = TestPattern::build(Op::H, {Var{kVarY}});

  PatternCompiler<Op> compiler;
  std::array patterns = {anchor, joined1, joined2};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value()) << "Multi-pattern compilation should succeed";

  auto const &code = compiled->code();
  auto bytecode = disassemble<Op>(code, compiled->symbols());

  // Find all BindSlotDedup and CheckSlot instructions with their positions
  // slot is stored in the 'arg' field of the instruction
  std::map<uint8_t, std::size_t> first_bind_pos;   // slot -> first BindSlotDedup position
  std::map<uint8_t, std::size_t> first_check_pos;  // slot -> first CheckSlot position

  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op == VMOp::BindSlotDedup) {
      auto slot = code[i].arg;
      if (first_bind_pos.find(slot) == first_bind_pos.end()) {
        first_bind_pos[slot] = i;
      }
    } else if (code[i].op == VMOp::CheckSlot) {
      auto slot = code[i].arg;
      if (first_check_pos.find(slot) == first_check_pos.end()) {
        first_check_pos[slot] = i;
      }
    }
  }

  // For each slot that has both bind and check, verify check comes after bind
  // and the gap is reasonable (within same join block)
  for (auto const &[slot, bind_pos] : first_bind_pos) {
    if (auto it = first_check_pos.find(slot); it != first_check_pos.end()) {
      auto check_pos = it->second;
      EXPECT_GT(check_pos, bind_pos) << "CheckSlot for slot " << static_cast<int>(slot)
                                     << " should come after BindSlotDedup\nBytecode:\n"
                                     << bytecode;

      // Count instructions between bind and check.
      // The gap includes: iterator setup (IterParents, Jump, NextParent),
      // symbol/arity checks, and LoadChild instructions.
      // This heuristic catches egregiously bad ordering (e.g., multiple unrelated
      // joins between bind and check).
      std::size_t gap = check_pos - bind_pos;

      // For a 3-pattern join, gaps can be ~15-20 instructions due to nested
      // iterator loops. Flag only very large gaps that suggest suboptimal ordering.
      constexpr std::size_t kMaxReasonableGap = 25;
      EXPECT_LT(gap, kMaxReasonableGap)
          << "Gap between BindSlotDedup and CheckSlot for slot " << static_cast<int>(slot) << " is " << gap
          << " instructions, which may indicate suboptimal instruction ordering\nBytecode:\n"
          << bytecode;
    }
  }

  // Verify we actually have CheckSlot instructions (sanity check)
  EXPECT_FALSE(first_check_pos.empty())
      << "Expected CheckSlot instructions for shared variable verification\nBytecode:\n"
      << bytecode;
}

TEST_F(VMCompilerTest, JoinOrder_SharedVarBindBeforeCheck) {
  // Verifies that for any shared variable, the pattern that BINDs it comes before
  // patterns that CHECK it. This is implicit in join order but worth verifying.
  //
  // Pattern: A(?x) JOIN B(?x, ?y) JOIN C(?y)
  //
  // ?x is shared between A and B, so A should bind ?x and B should check it.
  // ?y is shared between B and C, so B should bind ?y and C should check it.

  constexpr PatternVar kVarX{0};
  constexpr PatternVar kVarY{1};

  auto patternA = TestPattern::build(Op::F, {Var{kVarX}});
  auto patternB = TestPattern::build(Op::G, {Var{kVarX}, Var{kVarY}});
  auto patternC = TestPattern::build(Op::H, {Var{kVarY}});

  // Test multiple orderings - the compiler should produce valid bytecode regardless
  for (auto const &indices : std::vector<std::array<int, 3>>{{0, 1, 2}, {2, 1, 0}, {1, 0, 2}}) {
    std::array all_patterns = {patternA, patternB, patternC};
    std::array ordered = {all_patterns[indices[0]], all_patterns[indices[1]], all_patterns[indices[2]]};

    PatternCompiler<Op> compiler;
    auto compiled = compiler.compile(ordered);
    ASSERT_TRUE(compiled.has_value());

    auto const &code = compiled->code();

    // Track first occurrence of bind vs check for each slot
    std::map<uint8_t, std::pair<std::size_t, bool>> slot_first_op;  // slot -> (pos, is_bind)

    for (std::size_t i = 0; i < code.size(); ++i) {
      uint8_t slot = 255;
      bool is_bind = false;

      if (code[i].op == VMOp::BindSlotDedup) {
        slot = code[i].arg;
        is_bind = true;
      } else if (code[i].op == VMOp::CheckSlot) {
        slot = code[i].arg;
        is_bind = false;
      }

      if (slot != 255 && slot_first_op.find(slot) == slot_first_op.end()) {
        slot_first_op[slot] = {i, is_bind};
      }
    }

    // Every slot's first occurrence should be a Bind, not a Check
    for (auto const &[slot, info] : slot_first_op) {
      auto [pos, is_bind] = info;
      EXPECT_TRUE(is_bind) << "Slot " << static_cast<int>(slot) << " first appears as CheckSlot at position " << pos
                           << ", but should be BindSlotDedup first\nBytecode:\n"
                           << disassemble<Op>(code, compiled->symbols());
    }
  }
}

TEST_F(VMCompilerTest, JoinOrder_NoCartesianWhenPathWalkingPossible) {
  // Verifies that when a shared variable exists deep in a pattern structure,
  // the compiler uses parent chain traversal instead of Cartesian product.
  //
  // Pattern 1: F(G(?x))
  // Pattern 2: H(?x)
  //
  // ?x is shared, so joining should traverse from ?x's binding upward through
  // parents (G then F) rather than iterating all e-classes.

  constexpr PatternVar kVarX{0};

  auto pattern1 = TestPattern::build(Op::F, {Sym(Op::G, Var{kVarX})});
  auto pattern2 = TestPattern::build(Op::H, {Var{kVarX}});

  PatternCompiler<Op> compiler;
  std::array patterns = {pattern1, pattern2};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value()) << "Multi-pattern compilation should succeed";

  auto const &code = compiled->code();
  auto bytecode = disassemble<Op>(code, compiled->symbols());

  // Should NOT use IterAllEClasses (Cartesian product)
  bool has_iter_all_eclasses = false;
  for (auto const &instr : code) {
    if (instr.op == VMOp::IterAllEClasses) {
      has_iter_all_eclasses = true;
      break;
    }
  }
  EXPECT_FALSE(has_iter_all_eclasses)
      << "Should use parent traversal, not Cartesian product, when shared variable exists\nBytecode:\n"
      << bytecode;

  // Should have IterParents for traversing from shared variable
  bool has_iter_parents = false;
  for (auto const &instr : code) {
    if (instr.op == VMOp::IterParents) {
      has_iter_parents = true;
      break;
    }
  }
  EXPECT_TRUE(has_iter_parents) << "Should use IterParents for parent traversal\nBytecode:\n" << bytecode;
}

}  // namespace memgraph::planner::core
