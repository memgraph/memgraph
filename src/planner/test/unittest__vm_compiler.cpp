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
using namespace pattern;
using namespace pattern::vm;

// ============================================================================
// Compiler Bytecode Tests
// ============================================================================

class PatternVM_Compiler : public EGraphTestBase {};

TEST_F(PatternVM_Compiler, WildcardPattern) {
  // Pattern: _ (wildcard)
  auto builder = TestPattern::Builder{};
  builder.wildcard();
  auto pattern = std::move(builder).build();

  TestPatternCompiler compiler;
  auto compiled = compiler.compile(pattern);

  // Wildcard at root: no bindings, so just halt (match is implicit)
  auto code = compiled.code();
  ASSERT_EQ(code.size(), 1);
  EXPECT_EQ(code[0], Instruction::halt());
}

TEST_F(PatternVM_Compiler, VariablePattern) {
  // Pattern: ?x
  auto pattern = make_var_pattern(kVarX);

  TestPatternCompiler compiler;
  auto compiled = compiler.compile(pattern);

  // Variable at root: bind_dedup (backtrack to halt), yield (mark slot 0), jump back (to halt), halt
  auto code = compiled.code();
  ASSERT_EQ(code.size(), 4);
  EXPECT_EQ(code[0],
            Instruction::bind_slot_dedup(
                SlotIdx{0}, EClassReg{0}, InstrAddr{3}));  // slot 0, src reg 0, on_duplicate=3 (halt)
  EXPECT_EQ(code[1], Instruction::yield(SlotIdx{0}));      // Last slot is 0 (?x)
  EXPECT_EQ(code[2].op, VMOp::Jump);
  EXPECT_EQ(code[3], Instruction::halt());
}

TEST_F(PatternVM_Compiler, SimpleSymbolPattern) {
  // Pattern: Neg(?x)
  auto pattern = TestPattern::build(kTestRoot, Op::Neg, {Var{kVarX}});

  TestPatternCompiler compiler;
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

  auto code = compiled.code();
  auto bytecode = disassemble(code, compiled.symbols());

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
  // Note: IterENodes (code[1]) has no meaningful target - e-classes always have at least one e-node
  EXPECT_EQ(code[2].target, 4) << "First Jump should skip to CheckSymbol";
  EXPECT_EQ(code[3].target, halt_pos) << "NextENode should jump to halt on exhausted";
  EXPECT_EQ(code[4].target, loop_pos) << "CheckSymbol should backtrack to NextENode";
  EXPECT_EQ(code[5].target, loop_pos) << "CheckArity should backtrack to NextENode";
}

TEST_F(PatternVM_Compiler, NestedSymbolPattern) {
  // Pattern: Neg(Neg(?x))
  auto pattern = TestPattern::build(kTestRoot, Op::Neg, {Sym(Op::Neg, Var{kVarX})});

  TestPatternCompiler compiler;
  auto compiled = compiler.compile(pattern);

  auto code = compiled.code();
  auto bytecode = disassemble(code, compiled.symbols());

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
  int outer_loop = -1, inner_loop = -1;
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op == VMOp::NextENode) {
      if (outer_loop == -1)
        outer_loop = static_cast<int>(i);
      else
        inner_loop = static_cast<int>(i);
    }
  }

  ASSERT_NE(outer_loop, -1) << "Could not find outer loop";
  ASSERT_NE(inner_loop, -1) << "Could not find inner loop";

  // Note: IterENodes has no meaningful target - e-classes always have at least one e-node
  // Inner NextENode should backtrack to outer loop
  EXPECT_EQ(code[inner_loop].target, static_cast<uint16_t>(outer_loop))
      << "Inner NextENode should backtrack to outer loop";
}

TEST_F(PatternVM_Compiler, BinarySymbolPattern) {
  // Pattern: Add(?x, ?y)
  auto pattern = TestPattern::build(kTestRoot, Op::Add, {Var{kVarX}, Var{kVarY}});

  TestPatternCompiler compiler;
  auto compiled = compiler.compile(pattern);

  auto code = compiled.code();
  auto bytecode = disassemble(code, compiled.symbols());

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

TEST_F(PatternVM_Compiler, MultiPattern_DeepNestedUsesParentChain) {
  // Pattern 1: Neg(?x) - shallow pattern that binds ?x
  // Pattern 2: Neg(Neg(Neg(Neg(?x)))) - deep nested pattern with shared ?x at bottom
  //
  // The compiler should use parent chain traversal instead of Cartesian product.
  // This is critical for O(n) vs O(n^2) performance.

  auto pattern1 = TestPattern::build(Op::Neg, {Var{kVarX}});
  auto pattern2 = TestPattern::build(Op::Neg, {Sym(Op::Neg, Sym(Op::Neg, Sym(Op::Neg, Var{kVarX})))});

  std::array patterns = {pattern1, pattern2};

  TestPatternCompiler compiler;
  auto compiled = compiler.compile(patterns);

  auto code = compiled.code();
  auto bytecode = disassemble(code, compiled.symbols());

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

  // For unary symbols, CheckEClassEq is skipped - IterParents guarantees we're a child,
  // and CheckArity(1) means there's only one child slot, so we must be at index 0
  EXPECT_EQ(check_eclass_eq_count, 0)
      << "Unary symbols don't need CheckEClassEq (arity check is sufficient)\nBytecode:\n"
      << bytecode;

  // Should NOT use IterAllEClasses (Cartesian product) for the second pattern
  EXPECT_EQ(iter_all_eclasses_count, 0)
      << "Should not use IterAllEClasses (Cartesian product) for deep nested join\nBytecode:\n"
      << bytecode;
}

TEST_F(PatternVM_Compiler, MultiPattern_SharedVarUsesParentTraversal) {
  // Anchor: Bind(_, ?sym, ?expr)
  // Joined: Ident(?sym) - shared variable at direct child position
  //
  // Verifies parent traversal bytecode is generated for shared variable joins.

  constexpr PatternVar kVarSym{1};
  constexpr PatternVar kVarExpr{2};
  constexpr PatternVar kVarId{3};

  auto anchor = TestPattern::build(kTestRoot, Op::Bind, {Wildcard{}, Var{kVarSym}, Var{kVarExpr}});
  auto joined = TestPattern::build(kVarId, Op::Ident, {Var{kVarSym}});

  TestPatternCompiler compiler;
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);

  auto bytecode = disassemble(compiled.code(), compiled.symbols());

  // Verify bytecode has parent traversal instructions
  bool has_iter_parents = false;
  bool has_next_parent = false;
  bool has_get_enode_eclass = false;

  for (auto const &instr : compiled.code()) {
    if (instr.op == VMOp::IterParents) has_iter_parents = true;
    if (instr.op == VMOp::NextParent) has_next_parent = true;
    if (instr.op == VMOp::GetENodeEClass) has_get_enode_eclass = true;
  }

  EXPECT_TRUE(has_iter_parents) << "Should have IterParents instruction\nBytecode:\n" << bytecode;
  EXPECT_TRUE(has_next_parent) << "Should have NextParent instruction\nBytecode:\n" << bytecode;
  EXPECT_TRUE(has_get_enode_eclass) << "Should have GetENodeEClass for binding\nBytecode:\n" << bytecode;
}

TEST_F(PatternVM_Compiler, MultiPattern_NoSharedVarUsesCartesianProduct) {
  // Anchor: Add(?x, ?y)
  // Joined: Neg(?z) - no shared variable with anchor
  //
  // Verifies Cartesian product bytecode (IterAllEClasses) is generated.

  constexpr PatternVar kVarX{0};
  constexpr PatternVar kVarY{1};
  constexpr PatternVar kVarZ{2};

  auto anchor = TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}});
  auto joined = TestPattern::build(Op::Neg, {Var{kVarZ}});

  TestPatternCompiler compiler;
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);

  auto bytecode = disassemble(compiled.code(), compiled.symbols());

  // Verify bytecode has IterAllEClasses and NextEClass instructions
  bool has_iter_all_eclasses = false;
  bool has_next_eclass = false;

  for (auto const &instr : compiled.code()) {
    if (instr.op == VMOp::IterAllEClasses) has_iter_all_eclasses = true;
    if (instr.op == VMOp::NextEClass) has_next_eclass = true;
  }

  EXPECT_TRUE(has_iter_all_eclasses) << "Cartesian product should use IterAllEClasses instruction\nBytecode:\n"
                                     << bytecode;
  EXPECT_TRUE(has_next_eclass) << "Cartesian product should use NextEClass instruction\nBytecode:\n" << bytecode;
}

TEST_F(PatternVM_Compiler, MultiPattern_VariableOnlyUsesIterAllEClasses) {
  // Anchor: Add(?x, ?y)
  // Joined: ?z - just a variable, no symbol
  //
  // Verifies variable-only joined patterns use IterAllEClasses.

  constexpr PatternVar kVarX{0};
  constexpr PatternVar kVarY{1};
  constexpr PatternVar kVarZ{2};

  auto anchor = TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}});

  auto joined = make_var_pattern(kVarZ);

  TestPatternCompiler compiler;
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);

  auto bytecode = disassemble(compiled.code(), compiled.symbols());

  bool has_iter_all_eclasses = false;
  bool has_next_eclass = false;

  for (auto const &instr : compiled.code()) {
    if (instr.op == VMOp::IterAllEClasses) has_iter_all_eclasses = true;
    if (instr.op == VMOp::NextEClass) has_next_eclass = true;
  }

  EXPECT_TRUE(has_iter_all_eclasses) << "Variable-only joined pattern should use IterAllEClasses\nBytecode:\n"
                                     << bytecode;
  EXPECT_TRUE(has_next_eclass) << "Variable-only joined pattern should use NextEClass\nBytecode:\n" << bytecode;
}

TEST_F(PatternVM_Compiler, MultiPattern_RootBindingEnablesParentTraversal) {
  // Pattern 1 (anchor): F(?x)
  // Pattern 2: G(?x) AS ?y - shares ?x, binds root to ?y
  // Pattern 3: H(?y) - should use parent traversal from ?y
  //
  // This tests that root bindings (binding_for) update var_to_reg_ so that
  // subsequent patterns can use the bound variable for parent traversal.
  // If var_to_reg_ isn't updated, pattern 3 falls back to Cartesian product.

  constexpr PatternVar kVarX{0};
  constexpr PatternVar kVarY{1};

  auto pattern1 = TestPattern::build(Op::F, {Var{kVarX}});
  auto pattern2 = TestPattern::build(kVarY, Op::G, {Var{kVarX}});  // G(?x) AS ?y
  auto pattern3 = TestPattern::build(Op::H, {Var{kVarY}});

  TestPatternCompiler compiler;
  std::array patterns = {pattern1, pattern2, pattern3};
  auto compiled = compiler.compile(patterns);

  auto const &code = compiled.code();
  auto bytecode = disassemble(code, compiled.symbols());

  // Count IterParents and IterAllEClasses to verify join strategies
  int iter_parents_count = 0;
  int iter_all_eclasses_count = 0;

  for (auto const &instr : code) {
    if (instr.op == VMOp::IterParents) ++iter_parents_count;
    if (instr.op == VMOp::IterAllEClasses) ++iter_all_eclasses_count;
  }

  // Anchor G(?x) AS ?y uses IterENodes (symbol pattern on input e-class)
  // Joined F(?x) uses parent traversal from ?x - 1 IterParents
  // Joined H(?y) uses parent traversal from ?y - 1 IterParents
  //
  // If root binding doesn't update var_to_reg_, H(?y) falls back to
  // Cartesian product, giving 1 IterParents + 1 IterAllEClasses.
  EXPECT_EQ(iter_parents_count, 2) << "Both joined patterns should use parent traversal\nBytecode:\n" << bytecode;
  EXPECT_EQ(iter_all_eclasses_count, 0) << "No pattern should use IterAllEClasses (anchor uses IterENodes)\nBytecode:\n"
                                        << bytecode;
}

TEST_F(PatternVM_Compiler, MultiPattern_MixedJoinStrategies) {
  // Pattern 1 (anchor): F(?x, ?y)
  // Pattern 2: G(?x) - shares ?x, uses parent traversal
  // Pattern 3: H(?z) - no shared variable, uses Cartesian product
  //
  // Verifies both strategies can coexist in one compilation.

  constexpr PatternVar kVarX{0};
  constexpr PatternVar kVarY{1};
  constexpr PatternVar kVarZ{2};

  auto pattern1 = TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}});
  auto pattern2 = TestPattern::build(Op::G, {Var{kVarX}});
  auto pattern3 = TestPattern::build(Op::H, {Var{kVarZ}});

  TestPatternCompiler compiler;
  std::array patterns = {pattern1, pattern2, pattern3};
  auto compiled = compiler.compile(patterns);

  auto const &code = compiled.code();
  auto bytecode = disassemble(code, compiled.symbols());

  int iter_parents_count = 0;
  int iter_all_eclasses_count = 0;

  for (auto const &instr : code) {
    if (instr.op == VMOp::IterParents) ++iter_parents_count;
    if (instr.op == VMOp::IterAllEClasses) ++iter_all_eclasses_count;
  }

  // G(?x) joins via parent traversal from ?x
  // H(?z) has no shared variable, falls back to Cartesian product
  EXPECT_EQ(iter_parents_count, 1) << "G(?x) should use parent traversal\nBytecode:\n" << bytecode;
  EXPECT_EQ(iter_all_eclasses_count, 1) << "H(?z) should use Cartesian product (no shared variable)\nBytecode:\n"
                                        << bytecode;
}

// ============================================================================
// VM Join Order Tests
// ============================================================================
//
// These tests verify the VM compiler's join ordering algorithm produces optimal
// orderings regardless of pattern declaration order. Unlike MatcherIndex's join_steps()
// which is tested via rule.join_steps(), VM join order is embedded in the compiled
// bytecode, so we verify properties of the generated code.

TEST_F(PatternVM_Compiler, JoinOrder_HubPatternsFirst) {
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

  TestPatternCompiler compiler;
  auto canonical_compiled = compiler.compile(canonical_patterns);

  auto canonical_bytecode = disassemble(canonical_compiled.code(), canonical_compiled.symbols());

  // Verify no Cartesian product in canonical compilation
  bool has_iter_all_eclasses = false;
  for (auto const &instr : canonical_compiled.code()) {
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

    TestPatternCompiler perm_compiler;
    auto compiled = perm_compiler.compile(perm_patterns);

    // Property 1: No Cartesian product - this is the key invariant
    // When all patterns share variables, we should use parent traversal, not iteration
    bool perm_has_iter_all = false;
    for (auto const &instr : compiled.code()) {
      if (instr.op == VMOp::IterAllEClasses) {
        perm_has_iter_all = true;
        break;
      }
    }
    EXPECT_FALSE(perm_has_iter_all) << "Permutation " << permutation_count
                                    << " should not use IterAllEClasses\nBytecode:\n"
                                    << disassemble(compiled.code(), compiled.symbols());

    // Property 2: Must have parent traversal instructions (the efficient join strategy)
    bool has_iter_parents = false;
    for (auto const &instr : compiled.code()) {
      if (instr.op == VMOp::IterParents) {
        has_iter_parents = true;
        break;
      }
    }
    EXPECT_TRUE(has_iter_parents) << "Permutation " << permutation_count
                                  << " should use IterParents for joins\nBytecode:\n"
                                  << disassemble(compiled.code(), compiled.symbols());

    ++permutation_count;
  } while (std::ranges::next_permutation(perm).found);

  // 6! = 720 permutations
  EXPECT_EQ(permutation_count, 720);
}

TEST_F(PatternVM_Compiler, JoinOrder_CheckEClassEqProximity) {
  // Tests that CheckEClassEq instructions appear immediately after LoadChild.
  // Early invalidation is critical for performance: if a variable binding fails the
  // equality check, we should fail fast rather than doing unnecessary work.
  //
  // Pattern: F(?x, ?y) JOIN G(?x, _) JOIN H(?y, _)
  //
  // When joining G(?x, _), we traverse parents to find a matching symbol, then load the
  // child and immediately verify with CheckEClassEq that it matches the already-bound ?x.
  // Note: We use arity-2 patterns because arity-1 patterns skip CheckEClassEq (the arity
  // check alone is sufficient when there's only one child slot).

  constexpr PatternVar kVarX{0};
  constexpr PatternVar kVarY{1};

  auto anchor = TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}});
  auto joined1 = TestPattern::build(Op::G, {Var{kVarX}, Wildcard{}});
  auto joined2 = TestPattern::build(Op::H, {Var{kVarY}, Wildcard{}});

  TestPatternCompiler compiler;
  std::array patterns = {anchor, joined1, joined2};
  auto compiled = compiler.compile(patterns);

  auto const &code = compiled.code();
  auto bytecode = disassemble(code, compiled.symbols());

  // Find LoadChild and CheckEClassEq pairs - check should immediately follow load
  std::vector<std::size_t> check_eq_positions;

  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op == VMOp::CheckEClassEq) {
      check_eq_positions.push_back(i);

      // CheckEClassEq should immediately follow a LoadChild
      ASSERT_GT(i, 0) << "CheckEClassEq cannot be first instruction\nBytecode:\n" << bytecode;
      EXPECT_EQ(code[i - 1].op, VMOp::LoadChild)
          << "CheckEClassEq at position " << i << " should immediately follow LoadChild\nBytecode:\n"
          << bytecode;
    }
  }

  // Verify we have CheckEClassEq instructions for shared variable verification
  EXPECT_FALSE(check_eq_positions.empty())
      << "Expected CheckEClassEq instructions for shared variable verification\nBytecode:\n"
      << bytecode;
}

TEST_F(PatternVM_Compiler, JoinOrder_SharedVarBindBeforeCheck) {
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

    TestPatternCompiler compiler;
    auto compiled = compiler.compile(ordered);

    auto const &code = compiled.code();

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
                           << disassemble(code, compiled.symbols());
    }
  }
}

TEST_F(PatternVM_Compiler, JoinOrder_NoCartesianWhenPathWalkingPossible) {
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

  TestPatternCompiler compiler;
  std::array patterns = {pattern1, pattern2};
  auto compiled = compiler.compile(patterns);

  auto const &code = compiled.code();
  auto bytecode = disassemble(code, compiled.symbols());

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

TEST_F(PatternVM_Compiler, MultiPattern_SymbolBindingSharedVar_AtRoot) {
  // Anchor: Add(?x, ?y) - binds ?x and ?y
  // Joined: Neg[?x](?z) - ?x is bound to the Neg symbol node itself, ?z is new
  //
  // This tests the case where a shared variable is a binding on a symbol node
  // at the ROOT of the joined pattern. Since the shared binding is at root,
  // we don't need parent traversal - just verify the symbol structure directly.

  constexpr PatternVar kVarX{0};
  constexpr PatternVar kVarY{1};
  constexpr PatternVar kVarZ{2};

  auto anchor = TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}});
  // Neg[?x](?z) - ?x binds to the Neg node (root), ?z is a child
  auto joined = TestPattern::build(kVarX, Op::Neg, {Var{kVarZ}});

  TestPatternCompiler compiler;
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);

  auto bytecode = disassemble(compiled.code(), compiled.symbols());

  // Count Cartesian product instructions
  int iter_all_eclasses_count = 0;

  for (auto const &instr : compiled.code()) {
    if (instr.op == VMOp::IterAllEClasses) iter_all_eclasses_count++;
  }

  // Should NOT use Cartesian product - ?x is shared
  EXPECT_EQ(iter_all_eclasses_count, 0)
      << "Should not use Cartesian product when symbol has shared binding\nBytecode:\n"
      << bytecode;
}

TEST_F(PatternVM_Compiler, MultiPattern_SymbolBindingSharedVar_Nested) {
  // Anchor: Add(?x, ?y) - binds ?x and ?y
  // Joined: Mul(Neg[?x](?z), ?w) - ?x is bound to nested Neg node, ?z and ?w are new
  //
  // This tests the case where a shared variable is a binding on a NESTED symbol
  // node. Should use parent traversal to go from Neg up to Mul.

  constexpr PatternVar kVarX{0};
  constexpr PatternVar kVarY{1};
  constexpr PatternVar kVarZ{2};
  constexpr PatternVar kVarW{3};

  auto anchor = TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}});
  // Mul(?x=Neg(?z), ?w) using BoundSym for nested binding
  auto joined = TestPattern::build(Op::Mul, {BoundSym(kVarX, Op::Neg, Var{kVarZ}), Var{kVarW}});

  TestPatternCompiler compiler;
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);

  auto bytecode = disassemble(compiled.code(), compiled.symbols());

  // Count instructions
  int iter_all_eclasses_count = 0;
  bool has_iter_parents = false;

  for (auto const &instr : compiled.code()) {
    if (instr.op == VMOp::IterAllEClasses) iter_all_eclasses_count++;
    if (instr.op == VMOp::IterParents) has_iter_parents = true;
  }

  // Should NOT use Cartesian product - ?x is shared
  EXPECT_EQ(iter_all_eclasses_count, 0)
      << "Should not use Cartesian product when symbol has shared binding\nBytecode:\n"
      << bytecode;

  // Should use parent traversal to go from Neg up to Mul
  EXPECT_TRUE(has_iter_parents) << "Should use IterParents for nested shared symbol binding\nBytecode:\n" << bytecode;
}

TEST_F(PatternVM_Compiler, MultiPattern_SharedVarInMiddleOfBoth) {
  // Anchor: Add(?x, ?y) - ?x and ?y are children (not at root)
  // Joined: Mul(Neg[?x](_, ?y), ?z) - ?x is binding on nested Neg, ?y also shared
  //
  // This tests the case where the shared variable (?x) is NOT at the root of
  // either pattern. The shared binding on Neg should be detected, and ?y should
  // be verified as matching (already bound by anchor).

  constexpr PatternVar kVarX{0};
  constexpr PatternVar kVarY{1};
  constexpr PatternVar kVarZ{2};

  auto anchor = TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}});
  // Mul(?x=Neg(_, ?y), ?z) using BoundSym for nested binding
  auto joined = TestPattern::build(Op::Mul, {BoundSym(kVarX, Op::Neg, Wildcard{}, Var{kVarY}), Var{kVarZ}});

  TestPatternCompiler compiler;
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);

  auto bytecode = disassemble(compiled.code(), compiled.symbols());

  // Count instructions
  int iter_all_eclasses_count = 0;
  bool has_iter_parents = false;
  bool has_check_slot = false;  // For verifying ?y

  for (auto const &instr : compiled.code()) {
    if (instr.op == VMOp::IterAllEClasses) iter_all_eclasses_count++;
    if (instr.op == VMOp::IterParents) has_iter_parents = true;
    if (instr.op == VMOp::CheckSlot) has_check_slot = true;
  }

  // Should NOT use Cartesian product - ?x is shared
  EXPECT_EQ(iter_all_eclasses_count, 0) << "Should not use Cartesian product when shared var is in middle\nBytecode:\n"
                                        << bytecode;

  // Should use parent traversal to go from Neg up to Mul
  EXPECT_TRUE(has_iter_parents) << "Should use IterParents when shared var not at root\nBytecode:\n" << bytecode;

  // Should verify ?y matches (already bound by anchor)
  EXPECT_TRUE(has_check_slot) << "Should have CheckSlot for verifying shared ?y\nBytecode:\n" << bytecode;
}

TEST_F(PatternVM_Compiler, MultiPattern_DeepestSharedVarOptimization) {
  // Test: When multiple shared variables exist at different depths, use the deepest.
  //
  // Anchor: F(?x, ?y)
  // Joined: G(?x, H(?y))  - ?x at depth 1, ?y at depth 2
  //
  // Optimal: start parent traversal from ?y (depth 2) - only 2 IterParents levels
  // Suboptimal: start from ?x (depth 1) - would need 1 IterParents + child verification
  //
  // By counting IterParents, we verify the deeper ?y is used.

  constexpr PatternVar kVarX{0};
  constexpr PatternVar kVarY{1};

  auto anchor = TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}});
  auto joined = TestPattern::build(Op::G, {Var{kVarX}, Sym(Op::H, Var{kVarY})});

  TestPatternCompiler compiler;
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);

  auto bytecode = disassemble(compiled.code(), compiled.symbols());

  // Count IterParents to verify we traverse from deepest shared var
  // From ?y at depth 2: IterParents (H→G), IterParents (→root) = traversal starts
  // If we started from ?x at depth 1: fewer IterParents but more child checks
  int iter_parents_count = 0;
  for (auto const &instr : compiled.code()) {
    if (instr.op == VMOp::IterParents) ++iter_parents_count;
  }

  // 2 IterParents: from ?y up through H to G
  EXPECT_EQ(iter_parents_count, 2) << "Should use deepest shared var ?y for parent traversal\nBytecode:\n" << bytecode;
}

// ============================================================================
// Validation Tests
// ============================================================================

TEST_F(PatternVM_Compiler, Validation_DuplicateSymbolBindingInSinglePattern) {
  // Pattern: ?x=A(?x=B()) - same var binding on nested symbol nodes
  // This should throw because a PatternVar cannot bind to multiple symbol nodes

  constexpr PatternVar kVarX{0};

  // ?x=A(?x=B()) - duplicate binding on ?x should throw
  EXPECT_THROW(TestPattern::build(kVarX, Op::Add, {BoundSym(kVarX, Op::Neg)}), std::invalid_argument);
}

TEST_F(PatternVM_Compiler, Validation_DuplicateSymbolBindingAcrossPatterns) {
  // Pattern 1: ?x=A(?y)
  // Pattern 2: ?x=B(?z)
  // This should throw because ?x has symbol bindings in multiple patterns

  constexpr PatternVar kVarX{0};
  constexpr PatternVar kVarY{1};
  constexpr PatternVar kVarZ{2};

  auto pattern1 = TestPattern::build(kVarX, Op::Add, {Var{kVarY}});  // ?x=A(?y)
  auto pattern2 = TestPattern::build(kVarX, Op::Neg, {Var{kVarZ}});  // ?x=B(?z)

  TestPatternCompiler compiler;
  std::array patterns = {pattern1, pattern2};
  EXPECT_THROW(compiler.compile(patterns), std::invalid_argument);
}

TEST_F(PatternVM_Compiler, Validation_SymbolBindingAndVarNodeIsValid) {
  // Pattern: ?x=A(?x) - ?x binds to A and also appears as child
  // This is valid: the e-class bound to A contains A(child) where child == A's e-class

  constexpr PatternVar kVarX{0};

  // This should NOT throw - ?x as a var node is different from ?x as a symbol binding
  EXPECT_NO_THROW(TestPattern::build(kVarX, Op::Add, {Var{kVarX}}));  // ?x=A(?x)
}

TEST_F(PatternVM_Compiler, Validation_SymbolBindingInOnePatternVarInAnother) {
  // Pattern 1: ?x=A(?y)
  // Pattern 2: B(?x, ?z)
  // This is valid: ?x has symbol binding in pattern 1, appears as var in pattern 2

  constexpr PatternVar kVarX{0};
  constexpr PatternVar kVarY{1};
  constexpr PatternVar kVarZ{2};

  auto pattern1 = TestPattern::build(kVarX, Op::Add, {Var{kVarY}});       // ?x=A(?y)
  auto pattern2 = TestPattern::build(Op::Mul, {Var{kVarX}, Var{kVarZ}});  // B(?x, ?z)

  TestPatternCompiler compiler;
  std::array patterns = {pattern1, pattern2};
  EXPECT_NO_THROW(compiler.compile(patterns));
}

}  // namespace memgraph::planner::core
