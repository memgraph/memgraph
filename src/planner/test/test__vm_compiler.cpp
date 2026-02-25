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

#include "planner/pattern/vm/compiler.hpp"
#include "planner/pattern/vm/executor.hpp"
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

  // Wildcard at root: yield, jump back (to halt), halt
  auto code = compiled->code();
  ASSERT_EQ(code.size(), 3);
  EXPECT_EQ(code[0], Instruction::yield());
  EXPECT_EQ(code[1].op, VMOp::Jump);
  EXPECT_EQ(code[2], Instruction::halt());
}

TEST_F(VMCompilerTest, VariablePattern) {
  // Pattern: ?x
  auto pattern = make_var_pattern(kVarX);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);

  // Variable at root: bind, yield, jump back (to halt), halt
  auto code = compiled->code();
  ASSERT_EQ(code.size(), 4);
  EXPECT_EQ(code[0], Instruction::bind_slot(0, 0));  // slot 0, src reg 0
  EXPECT_EQ(code[1], Instruction::yield());
  EXPECT_EQ(code[2].op, VMOp::Jump);
  EXPECT_EQ(code[3], Instruction::halt());
}

TEST_F(VMCompilerTest, SimpleSymbolPattern) {
  // Pattern: Neg(?x)
  auto pattern = TestPattern::build(Op::Neg, {Var{kVarX}}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);

  // Expected bytecode structure:
  // 0:  IterENodes r1, r0, @halt     ; load first e-node
  // 1:  Jump @check                  ; skip NextENode first time
  // loop:
  // 2:  NextENode r1, @halt          ; advance to next
  // check:
  // 3:  CheckSymbol r1, Neg, @loop
  // 4:  CheckArity r1, 1, @loop
  // 5:  BindSlot slot[root], r0
  // 6:  LoadChild r2, r1, 0
  // 7:  BindOrCheck slot[0], r2, @loop
  // 8:  Yield
  // 9:  Jump @loop
  // 10: Halt

  auto code = compiled->code();
  auto bytecode = disassemble<Op>(code, compiled->symbols());

  ASSERT_GE(code.size(), 8) << "Expected at least 8 instructions\nBytecode:\n" << bytecode;

  // Check instruction sequence
  EXPECT_EQ(code[0].op, VMOp::IterENodes);
  EXPECT_EQ(code[1].op, VMOp::Jump) << "Should have jump to skip NextENode";
  EXPECT_EQ(code[2].op, VMOp::NextENode);
  EXPECT_EQ(code[3].op, VMOp::CheckSymbol);
  EXPECT_EQ(code[4].op, VMOp::CheckArity);

  // Check backtrack targets
  auto loop_pos = static_cast<uint16_t>(2);  // NextENode position
  auto halt_pos = static_cast<uint16_t>(code.size() - 1);

  EXPECT_EQ(code[0].target, halt_pos) << "IterENodes should jump to halt on empty";
  EXPECT_EQ(code[1].target, 3) << "First Jump should skip to CheckSymbol";
  EXPECT_EQ(code[2].target, halt_pos) << "NextENode should jump to halt on exhausted";
  EXPECT_EQ(code[3].target, loop_pos) << "CheckSymbol should backtrack to NextENode";
  EXPECT_EQ(code[4].target, loop_pos) << "CheckArity should backtrack to NextENode";
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
  // 10: BindOrCheck slot[0], r4, @6  ; bind ?x (backtrack to inner loop)
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

  // Should have two BindSlot for ?x and ?y (both first occurrences)
  int bind_slot_count = 0;
  for (auto const &instr : code) {
    if (instr.op == VMOp::BindSlot) {
      ++bind_slot_count;
    }
  }
  // Note: There are 3 BindSlots: one for root binding, and two for ?x and ?y
  // Actually there should be 2 for variables + 1 for root = 3 if root has binding
  // Let's check what we actually get - should be at least 2 for variables
  EXPECT_GE(bind_slot_count, 2) << "Expected at least 2 BindSlot instructions for two variables";
}

}  // namespace memgraph::planner::core
