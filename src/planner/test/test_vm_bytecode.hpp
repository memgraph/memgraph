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

#pragma once

// Bytecode assertion helpers and structural invariant validators for pattern VM tests.

#include <gtest/gtest.h>

#include <ApprovalTests.hpp>
#include <algorithm>
#include <map>
#include <set>
#include <span>
#include <vector>

#include "planner/pattern/vm/instruction.hpp"
#include "planner/pattern/vm/tracer.hpp"

namespace memgraph::planner::core {

using namespace pattern::vm;

// ============================================================================
// Bytecode assertion helpers
// ============================================================================

/// Short aliases for register/address strong types, keeping expected bytecode readable.
constexpr auto R(uint8_t v) -> EClassReg { return EClassReg{v}; }

constexpr auto N(uint8_t v) -> ENodeReg { return ENodeReg{v}; }

constexpr auto At(uint16_t v) -> InstrAddr { return InstrAddr{v}; }

constexpr auto Slot(uint8_t v) -> SlotIdx { return SlotIdx{v}; }

/// Compare compiled bytecode against an expected instruction sequence.
/// On mismatch, prints a side-by-side diff of expected vs actual using the disassembler.
template <typename Symbol>
void ExpectBytecode(std::span<Instruction const> actual, std::span<Symbol const> symbols,
                    std::initializer_list<Instruction> expected_il, char const *file, int line) {
  auto expected = std::vector<Instruction>(expected_il);
  auto actual_str = disassemble(actual, symbols);

  // Build expected disassembly using the same symbol table
  auto expected_span = std::span<Instruction const>{expected};
  auto expected_str = disassemble(expected_span, symbols);

  EXPECT_EQ(actual.size(), expected.size()) << "Instruction count mismatch\n"
                                            << "Expected bytecode:\n"
                                            << expected_str << "\nActual bytecode:\n"
                                            << actual_str;

  auto n = std::min(actual.size(), expected.size());
  for (std::size_t i = 0; i < n; ++i) {
    EXPECT_EQ(actual[i], expected[i]) << "Instruction mismatch at position " << i << "\nExpected bytecode:\n"
                                      << expected_str << "\nActual bytecode:\n"
                                      << actual_str;
  }
}

#define EXPECT_BYTECODE(actual_code, symbols, ...) ExpectBytecode(actual_code, symbols, __VA_ARGS__, __FILE__, __LINE__)

/// Verify compiled bytecode matches an approved snapshot (disassembled text).
template <typename Symbol>
void VerifyBytecode(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  ApprovalTests::Approvals::verify(disassemble(code, symbols));
}

// ============================================================================
// Bytecode invariant validators
// ============================================================================

constexpr auto is_next_op(VMOp op) -> bool {
  return op == VMOp::NextENode || op == VMOp::NextEClass || op == VMOp::NextSymbolEClass || op == VMOp::NextParent;
}

/// Instructions that use target as a conditional backtrack (jump on failure).
constexpr auto has_backtrack_target(VMOp op) -> bool {
  switch (op) {
    // Iteration start: backtrack if collection is empty
    case VMOp::IterAllEClasses:
    case VMOp::IterSymbolEClasses:
    case VMOp::IterParents:
    // Iteration advance: backtrack if exhausted
    case VMOp::NextENode:
    case VMOp::NextEClass:
    case VMOp::NextSymbolEClass:
    case VMOp::NextParent:
    // Filtering: backtrack on mismatch
    case VMOp::CheckSymbol:
    case VMOp::CheckArity:
    // Binding: backtrack on duplicate or mismatch
    case VMOp::BindSlot:
    case VMOp::CheckSlot:
    case VMOp::CheckEClassEq:
      return true;
    default:
      return false;
  }
}

/// Valid backtrack destinations: NextX (loop advance), MarkSeen (slot dedup trampoline), or Halt.
constexpr auto is_valid_backtrack_target(VMOp op) -> bool {
  return is_next_op(op) || op == VMOp::MarkSeen || op == VMOp::Halt;
}

/// Verify: all backtrack targets point to a NextX, MarkSeen, or Halt.
template <typename Symbol>
void ExpectBacktracksToNextOrHalt(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (!has_backtrack_target(code[i].op)) continue;
    auto target = code[i].target;
    ASSERT_LT(target, code.size()) << "Backtrack target out of bounds at " << i << "\nBytecode:\n" << bytecode;
    auto target_op = code[target].op;
    EXPECT_TRUE(is_valid_backtrack_target(target_op))
        << "Backtrack at " << i << " (" << op_name(code[i].op) << ") targets " << target << " (" << op_name(target_op)
        << "), expected NextX, MarkSeen, or Halt\nBytecode:\n"
        << bytecode;
  }
}

/// Verify Jump targets:
/// - Backward jumps target a NextX or MarkSeen (loop back through dedup).
/// - Forward jumps skip over a NextX or MarkSeen (skip first iteration or trampoline).
template <typename Symbol>
void ExpectJumpSkipsNextOrMarkSeen(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op != VMOp::Jump) continue;
    auto target = code[i].target;
    ASSERT_LT(target, code.size()) << "Jump target out of bounds at " << i << "\nBytecode:\n" << bytecode;

    if (target <= i) {
      // Backward jump: should target a NextX or MarkSeen (loop back)
      auto target_op = code[target].op;
      EXPECT_TRUE(is_next_op(target_op) || target_op == VMOp::MarkSeen)
          << "Backward Jump at " << i << " targets " << target << " (" << op_name(target_op)
          << "), expected NextX or MarkSeen\nBytecode:\n"
          << bytecode;
    } else {
      // Forward jump: should skip over a NextX or MarkSeen
      bool skips_relevant = false;
      for (auto j = i + 1; j < target; ++j) {
        if (is_next_op(code[j].op) || code[j].op == VMOp::MarkSeen) {
          skips_relevant = true;
          break;
        }
      }
      EXPECT_TRUE(skips_relevant) << "Forward Jump at " << i << " targets " << target
                                  << " but does not skip over any NextX or MarkSeen\nBytecode:\n"
                                  << bytecode;
    }
  }
}

/// Verify every IterX has exactly one matching NextX with the same dst register.
constexpr auto iter_to_next(VMOp op) -> VMOp {
  switch (op) {
    case VMOp::IterENodes:
      return VMOp::NextENode;
    case VMOp::IterAllEClasses:
      return VMOp::NextEClass;
    case VMOp::IterSymbolEClasses:
      return VMOp::NextSymbolEClass;
    case VMOp::IterParents:
      return VMOp::NextParent;
    default:
      return op;  // not an iter op
  }
}

constexpr auto is_iter_op(VMOp op) -> bool {
  return op == VMOp::IterENodes || op == VMOp::IterAllEClasses || op == VMOp::IterSymbolEClasses ||
         op == VMOp::IterParents;
}

template <typename Symbol>
void ExpectIterationPairing(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);

  // Collect IterX -> (expected NextX op, dst register, position)
  // Then verify each has exactly one matching NextX
  struct IterInfo {
    VMOp expected_next;
    uint8_t dst;
    std::size_t pos;
  };

  std::vector<IterInfo> iters;

  for (std::size_t i = 0; i < code.size(); ++i) {
    if (is_iter_op(code[i].op)) {
      iters.push_back({iter_to_next(code[i].op), code[i].dst, i});
    }
  }

  for (auto const &iter : iters) {
    int match_count = 0;
    for (std::size_t i = 0; i < code.size(); ++i) {
      if (code[i].op == iter.expected_next && code[i].dst == iter.dst) {
        ++match_count;
      }
    }
    EXPECT_EQ(match_count, 1) << op_name(code[iter.pos].op) << " at " << iter.pos << " (dst=" << +iter.dst << ") has "
                              << match_count << " matching " << op_name(iter.expected_next)
                              << " (expected 1)\nBytecode:\n"
                              << bytecode;
  }

  // Also verify every NextX has a matching IterX
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (!is_next_op(code[i].op)) continue;
    bool has_iter = false;
    for (auto const &iter : iters) {
      if (iter.expected_next == code[i].op && iter.dst == code[i].dst) {
        has_iter = true;
        break;
      }
    }
    EXPECT_TRUE(has_iter) << op_name(code[i].op) << " at " << i << " (dst=" << +code[i].dst
                          << ") has no matching IterX\nBytecode:\n"
                          << bytecode;
  }
}

/// Verify Yield count matches bindings: exactly 1 Yield if any BindSlot exists, 0 otherwise.
template <typename Symbol>
void ExpectYieldMatchesBindings(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);
  bool has_bindings = false;
  int yield_count = 0;
  for (auto const &instr : code) {
    if (instr.op == VMOp::BindSlot) has_bindings = true;
    if (instr.op == VMOp::Yield) ++yield_count;
  }
  auto expected = has_bindings ? 1 : 0;
  EXPECT_EQ(yield_count, expected) << "Expected " << expected << " Yield (has_bindings=" << has_bindings << "), found "
                                   << yield_count << "\nBytecode:\n"
                                   << bytecode;
}

/// Verify BindSlot appears before CheckSlot for each slot index.
template <typename Symbol>
void ExpectBindBeforeCheck(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);
  std::set<uint8_t> bound_slots;
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op == VMOp::BindSlot) {
      bound_slots.insert(code[i].arg);
    } else if (code[i].op == VMOp::CheckSlot) {
      EXPECT_TRUE(bound_slots.contains(code[i].arg)) << "CheckSlot for slot[" << +code[i].arg << "] at " << i
                                                     << " appears before any BindSlot for that slot\nBytecode:\n"
                                                     << bytecode;
    }
  }
}

/// Verify IterENodes does not use its target field (e-classes always have ≥1 e-node).
template <typename Symbol>
void ExpectIterENodesTargetUnused(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op == VMOp::IterENodes) {
      EXPECT_EQ(code[i].target, 0) << "IterENodes at " << i << " has non-zero target " << code[i].target
                                   << " (e-classes always have ≥1 e-node, target should be unused)\nBytecode:\n"
                                   << bytecode;
    }
  }
}

/// Verify CheckEClassEq is always immediately preceded by LoadChild.
/// This ensures early invalidation: load the child, then immediately verify equality.
template <typename Symbol>
void ExpectCheckEClassEqFollowsLoadChild(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op != VMOp::CheckEClassEq) continue;
    ASSERT_GT(i, 0uz) << "CheckEClassEq cannot be first instruction\nBytecode:\n" << bytecode;
    EXPECT_EQ(code[i - 1].op, VMOp::LoadChild)
        << "CheckEClassEq at " << i << " should immediately follow LoadChild\nBytecode:\n"
        << bytecode;
  }
}

/// Verify each slot is bound by BindSlot exactly once.
/// First occurrence = BindSlot, subsequent = CheckSlot. Duplicate binds indicate a compiler bug.
template <typename Symbol>
void ExpectUniqueSlotBindings(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);
  std::map<uint8_t, std::size_t> bind_positions;  // slot -> first BindSlot position
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op != VMOp::BindSlot) continue;
    auto slot = code[i].arg;
    auto [it, inserted] = bind_positions.emplace(slot, i);
    EXPECT_TRUE(inserted) << "slot[" << +slot << "] bound twice: first at " << it->second << ", again at " << i
                          << "\nBytecode:\n"
                          << bytecode;
  }
}

/// Verify Yield is immediately followed by a backward Jump (continue innermost loop).
template <typename Symbol>
void ExpectYieldThenBackwardJump(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op != VMOp::Yield) continue;
    ASSERT_LT(i + 1, code.size()) << "Yield at " << i << " is the last instruction (expected Jump after)\nBytecode:\n"
                                  << bytecode;
    EXPECT_EQ(code[i + 1].op, VMOp::Jump)
        << "Yield at " << i << " should be followed by Jump, got " << op_name(code[i + 1].op) << "\nBytecode:\n"
        << bytecode;
    if (code[i + 1].op == VMOp::Jump) {
      EXPECT_LT(code[i + 1].target, i + 1)
          << "Jump after Yield at " << i << " should be backward (target=" << code[i + 1].target << ")\nBytecode:\n"
          << bytecode;
    }
  }
}

/// Verify CheckSymbol is immediately followed by CheckArity with the same src register.
template <typename Symbol>
void ExpectCheckSymbolArityPaired(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op != VMOp::CheckSymbol) continue;
    ASSERT_LT(i + 1, code.size()) << "CheckSymbol at " << i << " is the last instruction\nBytecode:\n" << bytecode;
    EXPECT_EQ(code[i + 1].op, VMOp::CheckArity) << "CheckSymbol at " << i << " should be followed by CheckArity, got "
                                                << op_name(code[i + 1].op) << "\nBytecode:\n"
                                                << bytecode;
    if (code[i + 1].op == VMOp::CheckArity) {
      EXPECT_EQ(code[i].src, code[i + 1].src)
          << "CheckSymbol at " << i << " (src=rn" << +code[i].src << ") and CheckArity at " << i + 1 << " (src=rn"
          << +code[i + 1].src << ") should use same enode register\nBytecode:\n"
          << bytecode;
    }
  }
}

/// Verify BindSlot/CheckSlot never target MarkSeen.
/// BindSlot fails when the value is already in the seen set — MarkSeen would be redundant.
/// CheckSlot fails when the value doesn't match — this is rejection, not inner exhaustion.
/// Both should advance the iterator directly (NextX), not go through a MarkSeen trampoline.
template <typename Symbol>
void ExpectBindSlotCheckSlotSkipMarkSeen(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op != VMOp::BindSlot && code[i].op != VMOp::CheckSlot) continue;
    auto target = code[i].target;
    if (target < code.size() && code[target].op == VMOp::MarkSeen) {
      FAIL() << op_name(code[i].op) << " at " << i << " targets MarkSeen at " << target
             << " — should target NextX directly (MarkSeen is for inner exhaustion, not binding failure)\nBytecode:\n"
             << bytecode;
    }
  }
}

/// Verify NextX nesting: outer loops are emitted before inner loops, so inner NextX
/// backtracks to an earlier address (enclosing NextX or MarkSeen), while the outermost
/// NextX backtracks to Halt (at the end). This encodes the loop nesting hierarchy.
template <typename Symbol>
void ExpectNextBacktracksEarlierOrHalt(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (!is_next_op(code[i].op)) continue;
    auto target = code[i].target;
    ASSERT_LT(target, code.size()) << op_name(code[i].op) << " at " << i << " has out-of-bounds target " << target
                                   << "\nBytecode:\n"
                                   << bytecode;
    auto target_op = code[target].op;
    if (target_op == VMOp::Halt) {
      // Outermost loop: backtracks to Halt (higher address)
      EXPECT_GT(target, i) << op_name(code[i].op) << " at " << i << " backtracks to Halt at " << target
                           << " which should be after the NextX\nBytecode:\n"
                           << bytecode;
    } else {
      // Inner loop: backtracks to enclosing NextX or MarkSeen (earlier address)
      EXPECT_LT(target, i) << op_name(code[i].op) << " at " << i << " backtracks to " << op_name(target_op) << " at "
                           << target << " — inner loops should backtrack to an earlier address\nBytecode:\n"
                           << bytecode;
    }
  }
}

/// Verify MarkSeen is never associated with an IterENodes/NextENode loop.
///
/// MarkSeen is only correct when an iteration level binds exactly one slot per step:
///   - IterSymbolEClasses/IterAllEClasses: one root binding per eclass
///   - IterParents: one path binding per parent (multiple parents can share an eclass)
///
/// MarkSeen must NOT appear on IterENodes loops because:
///   - Arity 1: Yield naturally accumulates seen values (no intermediate cascading clear)
///   - Arity 2+: per-slot MarkSeen causes false positives (rejects valid tuples where
///     one slot value recurs with different values for sibling slots)
///   - See TODO.md for details on the multi-child duplicate limitation
///
/// MarkSeen appears in two patterns:
///   1. emit_iter_loop: MarkSeen immediately followed by NextX (not NextENode)
///   2. Parent trampoline: MarkSeen followed by Jump targeting NextParent
template <typename Symbol>
void ExpectMarkSeenNotOnENodeLoop(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op != VMOp::MarkSeen) continue;
    ASSERT_LT(i + 1, code.size()) << "MarkSeen at " << i << " is the last instruction\nBytecode:\n" << bytecode;

    auto next_op = code[i + 1].op;
    if (next_op == VMOp::NextENode) {
      FAIL() << "MarkSeen at " << i << " is followed by NextENode at " << i + 1
             << " — MarkSeen must not be on IterENodes loops\nBytecode:\n"
             << bytecode;
    }
    if (next_op == VMOp::Jump) {
      auto jump_target = code[i + 1].target;
      if (jump_target < code.size() && code[jump_target].op == VMOp::NextENode) {
        FAIL() << "MarkSeen at " << i << " jumps to NextENode at " << jump_target
               << " — MarkSeen must not be on IterENodes loops\nBytecode:\n"
               << bytecode;
      }
    }
  }
}

/// Verify LoadChild instructions sharing the same source enode register are not
/// separated by IterENodes instructions. This ensures sibling loads from the same
/// parent enode stay at the parent's nesting level and don't end up inside a
/// nested child's IterENodes loop.
///
/// Example of what this catches (before fix):
///   LoadChild rc1, rn0, 0      ← at enode level
///   IterENodes rn1, rc1        ← starts nested loop for child 0
///   ...
///   LoadChild rc2, rn0, 1      ← BUG: inside rn1's loop, re-executes per enode
///
/// After fix, all sibling LoadChild are grouped:
///   LoadChild rc1, rn0, 0      ← at enode level
///   LoadChild rc2, rn0, 1      ← at enode level (correct)
///   IterENodes rn1, rc1        ← nested loop only after all loads
template <typename Symbol>
void ExpectSiblingLoadsNotInNestedLoop(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);

  // For each enode register used as a LoadChild source, collect the positions
  // of all LoadChild instructions and verify no IterENodes appears between them.
  std::map<uint8_t, std::vector<std::size_t>> loads_by_src;  // src enode reg -> positions
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op == VMOp::LoadChild) {
      loads_by_src[code[i].src].push_back(i);
    }
  }

  for (auto const &[src_reg, positions] : loads_by_src) {
    if (positions.size() < 2) continue;
    // Check that no IterENodes appears between the first and last LoadChild for this src
    auto first = positions.front();
    auto last = positions.back();
    for (auto j = first + 1; j < last; ++j) {
      if (code[j].op == VMOp::IterENodes) {
        FAIL() << "LoadChild from rn" << +src_reg << " at positions [";
        for (std::size_t k = 0; k < positions.size(); ++k) {
          if (k > 0) std::cerr << ", ";
          std::cerr << positions[k];
        }
        std::cerr << "] are separated by IterENodes at " << j
                  << " — sibling loads should not be inside nested loops\nBytecode:\n"
                  << bytecode << "\n";
        return;
      }
    }
  }
}

/// Verify IterParents instructions that use eclass-level registers (from
/// IterSymbolEClasses or IterAllEClasses) appear before any IterENodes that
/// uses the same register. Eclass-dependent parent traversals should be emitted
/// at eclass level, not redundantly inside the enode loop.
///
/// Example of what this catches (before fix):
///   0: IterSymbolEClasses rc0, G     ; rc0 is eclass-level
///   5: IterENodes rn0, rc0           ; enode loop for G
///   17: IterParents rn2, rc0, @7     ; BUG: inside enode loop but depends on eclass
///
/// After fix, eclass-level join is before IterENodes:
///   0: IterSymbolEClasses rc0, G
///   5: IterParents rn0, rc0, @2      ; eclass-level join (correct)
///   10: IterENodes rn1, rc0          ; enode loop after join
template <typename Symbol>
void ExpectEclassJoinsBeforeEnodeLoop(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);

  // Find registers that are destinations of eclass-level iteration.
  std::set<uint8_t> eclass_level_regs;
  for (auto const &instr : code) {
    if (instr.op == VMOp::IterSymbolEClasses || instr.op == VMOp::IterAllEClasses) {
      eclass_level_regs.insert(instr.dst);
    }
  }

  // For each eclass-level register, find the first IterENodes that uses it.
  // Any IterParents using the same register must appear before that IterENodes,
  // UNLESS it's part of the deep entry upward walk (backtracks to an address
  // at or after the enode loop, meaning it's transitively nested within it).
  for (auto reg : eclass_level_regs) {
    std::optional<std::size_t> first_iter_enodes;
    for (std::size_t i = 0; i < code.size(); ++i) {
      if (code[i].op == VMOp::IterENodes && code[i].src == reg) {
        first_iter_enodes = i;
        break;
      }
    }
    if (!first_iter_enodes) continue;

    for (std::size_t i = *first_iter_enodes + 1; i < code.size(); ++i) {
      if (code[i].op == VMOp::IterParents && code[i].src == reg) {
        // Allow if this IterParents backtracks to an address at or after the enode loop.
        // This means it's part of the deep entry upward walk or a join nested within it.
        auto bt = value_of(InstrAddr{code[i].target});
        if (bt < *first_iter_enodes) {
          FAIL() << "IterParents at " << i << " uses eclass register rc" << +reg
                 << " (set by IterSymbolEClasses/IterAllEClasses) but appears after IterENodes at "
                 << *first_iter_enodes
                 << " and backtracks before it — eclass-level joins should be emitted before the enode "
                    "loop\nBytecode:\n"
                 << bytecode;
          return;
        }
      }
    }
  }
}

/// Verify every IterX is immediately followed by a forward Jump (loop-entry skip).
/// emit_iter_loop always emits: IterX, Jump @body, [MarkSeen,] NextX.
template <typename Symbol>
void ExpectIterFollowedByJump(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (!is_iter_op(code[i].op)) continue;
    ASSERT_LT(i + 1, code.size()) << op_name(code[i].op) << " at " << i << " is the last instruction\nBytecode:\n"
                                  << bytecode;
    EXPECT_EQ(code[i + 1].op, VMOp::Jump)
        << op_name(code[i].op) << " at " << i << " should be immediately followed by Jump\nBytecode:\n"
        << bytecode;
    if (code[i + 1].op == VMOp::Jump) {
      EXPECT_GT(code[i + 1].target, i + 1)
          << "Jump after " << op_name(code[i].op) << " at " << i << " should be a forward jump\nBytecode:\n"
          << bytecode;
    }
  }
}

/// Verify CheckEClassEq never compares a register with itself (trivially true = compiler bug).
template <typename Symbol>
void ExpectCheckEClassEqDistinctRegs(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op != VMOp::CheckEClassEq) continue;
    EXPECT_NE(code[i].dst, code[i].src) << "CheckEClassEq at " << i << " compares rc" << +code[i].dst
                                        << " with itself\nBytecode:\n"
                                        << bytecode;
  }
}

/// Verify slot indices form a contiguous set [0, N) with no gaps, and all slot
/// references (CheckSlot, MarkSeen, Yield) only use indices that BindSlot has bound.
template <typename Symbol>
void ExpectSlotIndicesContiguous(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);
  std::set<uint8_t> bound_slots;
  for (auto const &instr : code) {
    if (instr.op == VMOp::BindSlot) bound_slots.insert(instr.arg);
  }
  uint8_t expected = 0;
  for (auto slot : bound_slots) {
    EXPECT_EQ(slot, expected) << "Slot indices have a gap at " << +expected << "\nBytecode:\n" << bytecode;
    ++expected;
  }
  for (std::size_t i = 0; i < code.size(); ++i) {
    auto op = code[i].op;
    if (op == VMOp::CheckSlot || op == VMOp::MarkSeen || op == VMOp::Yield) {
      EXPECT_TRUE(bound_slots.contains(code[i].arg))
          << op_name(op) << " at " << i << " references slot[" << +code[i].arg << "] never bound\nBytecode:\n"
          << bytecode;
    }
  }
}

/// Verify GetENodeEClass is immediately followed by BindSlot or IterParents.
/// These are the only two consumers of GetENodeEClass output in current codegen:
/// emit_var_binding (parent traversal binding) or next IterParents step.
template <typename Symbol>
void ExpectGetENodeEClassUsage(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op != VMOp::GetENodeEClass) continue;
    ASSERT_LT(i + 1, code.size()) << "GetENodeEClass at " << i << " is the last instruction\nBytecode:\n" << bytecode;
    auto next_op = code[i + 1].op;
    EXPECT_TRUE(next_op == VMOp::BindSlot || next_op == VMOp::IterParents)
        << "GetENodeEClass at " << i << " followed by " << op_name(next_op)
        << ", expected BindSlot or IterParents\nBytecode:\n"
        << bytecode;
  }
}

/// Classify how each instruction uses registers.
struct RegUsage {
  std::set<uint8_t> eclass_producers;  // dst writes to eclass reg
  std::set<uint8_t> enode_producers;   // dst writes to enode reg
  std::set<uint8_t> eclass_consumers;  // src reads from eclass reg
  std::set<uint8_t> enode_consumers;   // src reads from enode reg
};

inline auto collect_register_usage(std::span<Instruction const> code) -> RegUsage {
  auto usage = RegUsage{};
  for (auto const &instr : code) {
    switch (instr.op) {
      case VMOp::IterAllEClasses:
      case VMOp::NextEClass:
      case VMOp::IterSymbolEClasses:
      case VMOp::NextSymbolEClass:
      case VMOp::GetENodeEClass:
        usage.eclass_producers.insert(instr.dst);
        break;
      case VMOp::LoadChild:
        usage.eclass_producers.insert(instr.dst);
        usage.enode_consumers.insert(instr.src);
        break;
      case VMOp::CheckEClassEq:
        usage.eclass_consumers.insert(instr.dst);
        usage.eclass_consumers.insert(instr.src);
        break;
      case VMOp::IterENodes:
        usage.enode_producers.insert(instr.dst);
        usage.eclass_consumers.insert(instr.src);
        break;
      case VMOp::NextENode:
      case VMOp::NextParent:
        usage.enode_producers.insert(instr.dst);
        break;
      case VMOp::IterParents:
        usage.enode_producers.insert(instr.dst);
        usage.eclass_consumers.insert(instr.src);
        break;
      case VMOp::CheckSymbol:
      case VMOp::CheckArity:
        usage.enode_consumers.insert(instr.src);
        break;
      case VMOp::BindSlot:
      case VMOp::CheckSlot:
        usage.eclass_consumers.insert(instr.src);
        break;
      default:
        break;
    }
  }
  return usage;
}

/// Verify eclass and enode register indices are each contiguous from 0.
/// The compiler allocates registers sequentially, so gaps indicate a bug.
template <typename Symbol>
void ExpectRegisterIndicesContiguous(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);
  auto usage = collect_register_usage(code);

  auto check_contiguous = [&](std::set<uint8_t> const &regs, char const *kind) {
    uint8_t expected = 0;
    for (auto r : regs) {
      EXPECT_EQ(r, expected) << kind << " register indices have a gap at " << +expected << "\nBytecode:\n" << bytecode;
      ++expected;
    }
  };
  check_contiguous(usage.eclass_producers, "EClass");
  check_contiguous(usage.enode_producers, "ENode");
}

/// Verify every produced register is consumed and every consumed register is produced.
/// Each register should have exactly one producer (IterX/LoadChild/GetENodeEClass)
/// and at least one consumer (CheckSymbol/BindSlot/IterENodes/etc).
template <typename Symbol>
void ExpectRegisterProducerConsumer(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  auto bytecode = disassemble(code, symbols);
  auto usage = collect_register_usage(code);

  for (auto r : usage.eclass_producers) {
    EXPECT_TRUE(usage.eclass_consumers.contains(r))
        << "EClass register rc" << +r << " is produced but never consumed\nBytecode:\n"
        << bytecode;
  }
  for (auto r : usage.eclass_consumers) {
    EXPECT_TRUE(usage.eclass_producers.contains(r))
        << "EClass register rc" << +r << " is consumed but never produced\nBytecode:\n"
        << bytecode;
  }
  for (auto r : usage.enode_producers) {
    EXPECT_TRUE(usage.enode_consumers.contains(r))
        << "ENode register rn" << +r << " is produced but never consumed\nBytecode:\n"
        << bytecode;
  }
  for (auto r : usage.enode_consumers) {
    EXPECT_TRUE(usage.enode_producers.contains(r))
        << "ENode register rn" << +r << " is consumed but never produced\nBytecode:\n"
        << bytecode;
  }
}

/// Run all structural invariant checks on compiled bytecode.
template <typename Symbol>
void ExpectValidBytecode(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  ASSERT_FALSE(code.empty()) << "Bytecode must not be empty";
  EXPECT_EQ(code.back().op, VMOp::Halt) << "Last instruction must be Halt";
  ExpectBacktracksToNextOrHalt(code, symbols);
  ExpectJumpSkipsNextOrMarkSeen(code, symbols);
  ExpectIterationPairing(code, symbols);
  ExpectIterFollowedByJump(code, symbols);
  ExpectIterENodesTargetUnused(code, symbols);
  ExpectYieldMatchesBindings(code, symbols);
  ExpectYieldThenBackwardJump(code, symbols);
  ExpectBindBeforeCheck(code, symbols);
  ExpectUniqueSlotBindings(code, symbols);
  ExpectSlotIndicesContiguous(code, symbols);
  ExpectBindSlotCheckSlotSkipMarkSeen(code, symbols);
  ExpectCheckSymbolArityPaired(code, symbols);
  ExpectCheckEClassEqFollowsLoadChild(code, symbols);
  ExpectCheckEClassEqDistinctRegs(code, symbols);
  ExpectGetENodeEClassUsage(code, symbols);
  ExpectNextBacktracksEarlierOrHalt(code, symbols);
  ExpectMarkSeenNotOnENodeLoop(code, symbols);
  ExpectSiblingLoadsNotInNestedLoop(code, symbols);
  ExpectEclassJoinsBeforeEnodeLoop(code, symbols);
  ExpectRegisterIndicesContiguous(code, symbols);
  ExpectRegisterProducerConsumer(code, symbols);
}

}  // namespace memgraph::planner::core
