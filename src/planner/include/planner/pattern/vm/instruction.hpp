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

#include <cstdint>
#include <string_view>

namespace memgraph::planner::core::vm {

// =============================================================================
// VM Bytecode Contract Documentation
// =============================================================================
//
// This file defines the bytecode instruction set for the pattern matching VM.
// Understanding the contracts between components is essential for correctness.
//
// ## Component Relationships
//
//   Pattern<Symbol>  ──compile──>  CompiledPattern  ──execute──>  Matches
//        │                              │                            │
//        │                              │                            │
//   PatternCompiler              VMExecutor::execute          PatternMatch[]
//   (compiler.hpp)                (executor.hpp)              (in MatchArena)
//
// ## Producer: PatternCompiler (compiler.hpp)
//
// PatternCompiler emits Instructions with these guarantees:
//
//   1. Register Allocation:
//      - eclass_regs[0] is reserved for the input candidate e-class
//      - eclass_regs[1..N] are allocated via alloc_eclass_reg()
//      - enode_regs[0..M] are allocated via alloc_enode_reg()
//      - CompiledPattern reports num_eclass_regs and num_enode_regs
//
//   2. Jump Targets:
//      - All targets are valid instruction indices < code.size()
//      - 0xFFFF placeholders are patched to halt position before returning
//      - Backtrack targets point to NextX instructions (loop continuation)
//
//   3. Symbol Table:
//      - CheckSymbol.arg indexes into CompiledPattern::symbols()
//      - Symbols are deduplicated by get_symbol_index()
//
//   4. Slot Binding:
//      - slots[i] correspond to pattern variables
//      - binding_order tracks the order slots are bound
//      - slots_bound_after[i] lists slots to clear when slot i changes
//
//   5. Iteration Pairing:
//      - IterENodes must be followed eventually by NextENode with same dst
//      - IterParents must be followed eventually by NextParent with same dst
//      - IterAllEClasses must be followed eventually by NextEClass with same dst
//
// ## Consumer: VMExecutor (executor.hpp)
//
// VMExecutor interprets Instructions with these expectations:
//
//   1. State Initialization (before each candidate):
//      - eclass_regs[0] = candidate (must be canonical)
//      - pc = 0
//      - slots, seen_per_slot cleared appropriately
//
//   2. Register Semantics:
//      - eclass_regs[dst]: written by LoadChild, GetENodeEClass, IterAllEClasses
//      - enode_regs[dst]: written by IterENodes, IterParents
//      - Both are indexed by uint8_t (max 256 registers)
//
//   3. Canonicalization:
//      - LoadChild canonicalizes child e-class IDs via egraph.find()
//      - GetENodeEClass canonicalizes via egraph.find()
//      - Candidates must be pre-canonicalized by caller
//      - slots[] always contain canonical IDs
//
//   4. Iteration Protocol:
//      - IterX: Initialize iterator, store first element, return success/fail
//      - NextX: Advance iterator, store element, return success/fail
//      - On failure (empty/exhausted): jump to target (backtrack)
//      - Iterators are stored in VMState indexed by dst register
//
//   5. Deduplication:
//      - BindSlotDedup checks seen_per_slot[arg] before binding
//      - If value already seen, jumps to target (backtrack)
//      - MarkSeen/Yield add current slot value to seen set
//      - When slot i changes, seen_per_slot[j] cleared for j in slots_bound_after[i]
//
// ## Instruction Field Conventions
//
//   | Field  | Size | Usage                                           |
//   |--------|------|-------------------------------------------------|
//   | op     | 1B   | VMOp enum value                                 |
//   | dst    | 1B   | Destination register index                      |
//   | src    | 1B   | Source register index                           |
//   | arg    | 1B   | Symbol index, slot index, child index, or arity |
//   | target | 2B   | Jump target address (instruction index)         |
//
// ## Opcode Categories
//
//   Navigation:   LoadChild, GetENodeEClass
//   Iteration:    IterENodes/NextENode, IterAllEClasses/NextEClass, IterParents/NextParent
//   Filtering:    CheckSymbol, CheckArity
//   Binding:      BindSlotDedup, CheckSlot, CheckEClassEq, MarkSeen
//   Control:      Jump, Yield, Halt
//
// =============================================================================

/// VM opcodes for pattern matching.
///
/// @see PatternCompiler for bytecode generation
/// @see VMExecutor for bytecode interpretation
/// @see CompiledPattern for the compiled bytecode container
enum class VMOp : uint8_t {
  // ===== Navigation =====
  /// Load child e-class from current e-node: dst = enode[src].children[arg]
  LoadChild,
  /// Get e-class containing e-node in src register: dst = eclass_of(enode[src])
  GetENodeEClass,

  // ===== E-Node Iteration =====
  /// Begin iterating e-nodes in e-class[src], store first in dst
  /// Pushes iteration state. If empty, jumps to target.
  IterENodes,
  /// Advance to next e-node in current iteration, store in dst
  /// If exhausted, pops state and jumps to target.
  NextENode,

  // ===== E-Class Iteration (for Cartesian product joins) =====
  /// Begin iterating ALL canonical e-classes in the e-graph
  /// Stores first e-class ID in dst. If empty, jumps to target.
  IterAllEClasses,
  /// Advance to next e-class, store in dst
  /// If exhausted, pops state and jumps to target.
  NextEClass,

  // ===== Parent Traversal =====
  /// Begin iterating ALL parents of e-class[src] (index-based)
  /// Pushes iteration state. If empty, jumps to target.
  IterParents,
  /// Advance to next parent e-node (index-based, for IterParents), store in dst
  /// If exhausted, pops state and jumps to target.
  NextParent,

  // ===== Filtering =====
  /// If enode[src].symbol != symbols[arg], jump to target
  CheckSymbol,
  /// If enode[src].arity != arg, jump to target
  CheckArity,

  // ===== Binding =====
  /// Bind with dedup: slots[arg] = regs[src], check seen set, jump to target if duplicate
  /// Used for all bindings to enable early backtracking on duplicates.
  BindSlotDedup,
  /// Check consistency: if find(slots[arg]) != find(regs[src]), jump to target
  CheckSlot,
  /// Check two e-class registers are equal: if regs[dst] != regs[src], jump to target
  CheckEClassEq,
  /// Mark slot as seen (exhausted) for deduplication: seen[arg].insert(slots[arg])
  /// Used when an iteration exhausts to mark the controlling slot as fully explored.
  MarkSeen,

  // ===== Control =====
  /// Unconditional jump to target
  Jump,
  /// Emit current match (slots) to results
  Yield,
  /// Stop execution
  Halt,
};

/// Single VM instruction (6 bytes for cache efficiency).
///
/// ## Field Semantics by Opcode
///
/// | Opcode          | dst              | src              | arg           | target           |
/// |-----------------|------------------|------------------|---------------|------------------|
/// | LoadChild       | eclass_reg out   | enode_reg in     | child index   | -                |
/// | GetENodeEClass  | eclass_reg out   | enode_reg in     | -             | -                |
/// | IterENodes      | enode_reg out    | eclass_reg in    | -             | on_empty         |
/// | NextENode       | enode_reg out    | -                | -             | on_exhausted     |
/// | IterAllEClasses | eclass_reg out   | -                | -             | on_empty         |
/// | NextEClass      | eclass_reg out   | -                | -             | on_exhausted     |
/// | IterParents     | enode_reg out    | eclass_reg in    | -             | on_empty         |
/// | NextParent      | enode_reg out    | -                | -             | on_exhausted     |
/// | CheckSymbol     | -                | enode_reg in     | symbol_idx    | on_mismatch      |
/// | CheckArity      | -                | enode_reg in     | arity         | on_mismatch      |
/// | BindSlotDedup   | -                | eclass_reg in    | slot_idx      | on_duplicate     |
/// | CheckSlot       | -                | eclass_reg in    | slot_idx      | on_mismatch      |
/// | CheckEClassEq   | eclass_reg in    | eclass_reg in    | -             | on_mismatch      |
/// | MarkSeen        | -                | -                | slot_idx      | -                |
/// | Jump            | -                | -                | -             | destination      |
/// | Yield           | -                | -                | last_slot_idx | -                |
/// | Halt            | -                | -                | -             | -                |
///
/// @invariant sizeof(Instruction) == 6
/// @invariant All register indices < 256 (uint8_t)
/// @invariant target < code.size() after compilation (except during compilation)
///
/// @see VMOp for opcode semantics
/// @see PatternCompiler::emit_* for instruction generation
/// @see VMExecutor::exec_* for instruction execution
struct Instruction {
  VMOp op;
  uint8_t dst;      // Destination register index (eclass_reg or enode_reg depending on op)
  uint8_t src;      // Source register index (eclass_reg or enode_reg depending on op)
  uint8_t arg;      // Symbol index, slot index, child index, or arity (opcode-specific)
  uint16_t target;  // Jump target for conditional/iteration ops (instruction index)

  // Factory methods for cleaner construction
  static constexpr auto load_child(uint8_t dst, uint8_t src, uint8_t child_idx) -> Instruction {
    return {VMOp::LoadChild, dst, src, child_idx, 0};
  }

  static constexpr auto get_enode_eclass(uint8_t dst, uint8_t src) -> Instruction {
    return {VMOp::GetENodeEClass, dst, src, 0, 0};
  }

  static constexpr auto iter_enodes(uint8_t dst, uint8_t src, uint16_t on_empty) -> Instruction {
    return {VMOp::IterENodes, dst, src, 0, on_empty};
  }

  static constexpr auto next_enode(uint8_t dst, uint16_t on_exhausted) -> Instruction {
    return {VMOp::NextENode, dst, 0, 0, on_exhausted};
  }

  static constexpr auto iter_all_eclasses(uint8_t dst, uint16_t on_empty) -> Instruction {
    return {VMOp::IterAllEClasses, dst, 0, 0, on_empty};
  }

  static constexpr auto next_eclass(uint8_t dst, uint16_t on_exhausted) -> Instruction {
    return {VMOp::NextEClass, dst, 0, 0, on_exhausted};
  }

  static constexpr auto iter_parents(uint8_t dst, uint8_t src, uint16_t on_empty) -> Instruction {
    return {VMOp::IterParents, dst, src, 0, on_empty};
  }

  static constexpr auto next_parent(uint8_t dst, uint16_t on_exhausted) -> Instruction {
    return {VMOp::NextParent, dst, 0, 0, on_exhausted};
  }

  static constexpr auto check_symbol(uint8_t src, uint8_t sym_idx, uint16_t on_mismatch) -> Instruction {
    return {VMOp::CheckSymbol, 0, src, sym_idx, on_mismatch};
  }

  static constexpr auto check_arity(uint8_t src, uint8_t arity, uint16_t on_mismatch) -> Instruction {
    return {VMOp::CheckArity, 0, src, arity, on_mismatch};
  }

  static constexpr auto bind_slot_dedup(uint8_t slot_idx, uint8_t src, uint16_t on_duplicate) -> Instruction {
    return {VMOp::BindSlotDedup, 0, src, slot_idx, on_duplicate};
  }

  static constexpr auto check_slot(uint8_t slot_idx, uint8_t src, uint16_t on_mismatch) -> Instruction {
    return {VMOp::CheckSlot, 0, src, slot_idx, on_mismatch};
  }

  static constexpr auto check_eclass_eq(uint8_t dst, uint8_t src, uint16_t on_mismatch) -> Instruction {
    return {VMOp::CheckEClassEq, dst, src, 0, on_mismatch};
  }

  static constexpr auto mark_seen(uint8_t slot_idx) -> Instruction { return {VMOp::MarkSeen, 0, 0, slot_idx, 0}; }

  static constexpr auto jmp(uint16_t target) -> Instruction { return {VMOp::Jump, 0, 0, 0, target}; }

  /// Yield is a special MarkSeen: marks the slot as seen, then emits the match
  static constexpr auto yield(uint8_t last_slot) -> Instruction { return {VMOp::Yield, 0, 0, last_slot, 0}; }

  static constexpr auto halt() -> Instruction { return {VMOp::Halt, 0, 0, 0, 0}; }

  /// Equality for testing compiled bytecode
  friend constexpr auto operator==(Instruction const &a, Instruction const &b) -> bool = default;
};

static_assert(sizeof(Instruction) == 6, "Instruction should be 6 bytes");
static_assert(alignof(Instruction) == 2, "Instruction should be 2 aligned");

/// Get human-readable name for opcode
[[nodiscard]] constexpr auto op_name(VMOp op) -> std::string_view {
  switch (op) {
    case VMOp::LoadChild:
      return "LoadChild";
    case VMOp::GetENodeEClass:
      return "GetENodeEClass";
    case VMOp::IterENodes:
      return "IterENodes";
    case VMOp::NextENode:
      return "NextENode";
    case VMOp::IterAllEClasses:
      return "IterAllEClasses";
    case VMOp::NextEClass:
      return "NextEClass";
    case VMOp::IterParents:
      return "IterParents";
    case VMOp::NextParent:
      return "NextParent";
    case VMOp::CheckSymbol:
      return "CheckSymbol";
    case VMOp::CheckArity:
      return "CheckArity";
    case VMOp::BindSlotDedup:
      return "BindSlotDedup";
    case VMOp::CheckSlot:
      return "CheckSlot";
    case VMOp::CheckEClassEq:
      return "CheckEClassEq";
    case VMOp::MarkSeen:
      return "MarkSeen";
    case VMOp::Jump:
      return "Jump";
    case VMOp::Yield:
      return "Yield";
    case VMOp::Halt:
      return "Halt";
  }
  return "Unknown";
}

}  // namespace memgraph::planner::core::vm
