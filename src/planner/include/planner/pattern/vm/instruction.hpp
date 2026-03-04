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

#include "planner/pattern/vm/types.hpp"

namespace memgraph::planner::core::pattern::vm {

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
//      - slots[i] correspond to pattern variables (see CompiledPattern::var_slots())
//      - binding_order tracks the order slots are bound during execution
//      - slot_to_order[i] gives position of slot i in binding order (for clearing)
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
//      - When slot i changes, seen_per_slot cleared for slots later in binding_order
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
  uint8_t dst = 0;      // Destination register index (eclass_reg or enode_reg depending on op)
  uint8_t src = 0;      // Source register index (eclass_reg or enode_reg depending on op)
  uint8_t arg = 0;      // Symbol index, slot index, child index, or arity (opcode-specific)
  uint16_t target = 0;  // Jump target for conditional/iteration ops (instruction index)

  // Factory methods for cleaner construction with strong types
  using enum VMOp;  // C++20: allows unqualified access to VMOp enumerators

  static constexpr auto load_child(EClassReg dst, ENodeReg src, uint8_t child_idx) -> Instruction {
    return {.op = LoadChild, .dst = value_of(dst), .src = value_of(src), .arg = child_idx};
  }

  static constexpr auto get_enode_eclass(EClassReg dst, ENodeReg src) -> Instruction {
    return {.op = GetENodeEClass, .dst = value_of(dst), .src = value_of(src)};
  }

  /// Note: No on_empty target because e-classes always have at least one e-node.
  static constexpr auto iter_enodes(ENodeReg dst, EClassReg src) -> Instruction {
    return {.op = IterENodes, .dst = value_of(dst), .src = value_of(src)};
  }

  static constexpr auto next_enode(ENodeReg dst, InstrAddr on_exhausted) -> Instruction {
    return {.op = NextENode, .dst = value_of(dst), .target = value_of(on_exhausted)};
  }

  static constexpr auto iter_all_eclasses(EClassReg dst, InstrAddr on_empty) -> Instruction {
    return {.op = IterAllEClasses, .dst = value_of(dst), .target = value_of(on_empty)};
  }

  static constexpr auto next_eclass(EClassReg dst, InstrAddr on_exhausted) -> Instruction {
    return {.op = NextEClass, .dst = value_of(dst), .target = value_of(on_exhausted)};
  }

  static constexpr auto iter_parents(ENodeReg dst, EClassReg src, InstrAddr on_empty) -> Instruction {
    return {.op = IterParents, .dst = value_of(dst), .src = value_of(src), .target = value_of(on_empty)};
  }

  static constexpr auto next_parent(ENodeReg dst, InstrAddr on_exhausted) -> Instruction {
    return {.op = NextParent, .dst = value_of(dst), .target = value_of(on_exhausted)};
  }

  static constexpr auto check_symbol(ENodeReg src, uint8_t sym_idx, InstrAddr on_mismatch) -> Instruction {
    return {.op = CheckSymbol, .src = value_of(src), .arg = sym_idx, .target = value_of(on_mismatch)};
  }

  static constexpr auto check_arity(ENodeReg src, uint8_t arity, InstrAddr on_mismatch) -> Instruction {
    return {.op = CheckArity, .src = value_of(src), .arg = arity, .target = value_of(on_mismatch)};
  }

  static constexpr auto bind_slot_dedup(SlotIdx slot_idx, EClassReg src, InstrAddr on_duplicate) -> Instruction {
    return {.op = BindSlotDedup, .src = value_of(src), .arg = value_of(slot_idx), .target = value_of(on_duplicate)};
  }

  static constexpr auto check_slot(SlotIdx slot_idx, EClassReg src, InstrAddr on_mismatch) -> Instruction {
    return {.op = CheckSlot, .src = value_of(src), .arg = value_of(slot_idx), .target = value_of(on_mismatch)};
  }

  static constexpr auto check_eclass_eq(EClassReg dst, EClassReg src, InstrAddr on_mismatch) -> Instruction {
    return {.op = CheckEClassEq, .dst = value_of(dst), .src = value_of(src), .target = value_of(on_mismatch)};
  }

  static constexpr auto mark_seen(SlotIdx slot_idx) -> Instruction {
    return {.op = MarkSeen, .arg = value_of(slot_idx)};
  }

  static constexpr auto jmp(InstrAddr target) -> Instruction { return {.op = Jump, .target = value_of(target)}; }

  /// Yield is a special MarkSeen: marks the slot as seen, then emits the match
  static constexpr auto yield(SlotIdx last_slot) -> Instruction { return {.op = Yield, .arg = value_of(last_slot)}; }

  static constexpr auto halt() -> Instruction { return {.op = Halt}; }

  /// Equality for testing compiled bytecode
  friend constexpr auto operator==(Instruction const &a, Instruction const &b) -> bool = default;
};

static_assert(sizeof(Instruction) == 6, "Instruction should be 6 bytes");
static_assert(alignof(Instruction) == 2, "Instruction should be 2 aligned");

/// Get human-readable name for opcode
[[nodiscard]] constexpr auto op_name(VMOp op) -> std::string_view {
  using enum VMOp;
  switch (op) {
    case LoadChild:
      return "LoadChild";
    case GetENodeEClass:
      return "GetENodeEClass";
    case IterENodes:
      return "IterENodes";
    case NextENode:
      return "NextENode";
    case IterAllEClasses:
      return "IterAllEClasses";
    case NextEClass:
      return "NextEClass";
    case IterParents:
      return "IterParents";
    case NextParent:
      return "NextParent";
    case CheckSymbol:
      return "CheckSymbol";
    case CheckArity:
      return "CheckArity";
    case BindSlotDedup:
      return "BindSlotDedup";
    case CheckSlot:
      return "CheckSlot";
    case CheckEClassEq:
      return "CheckEClassEq";
    case MarkSeen:
      return "MarkSeen";
    case Jump:
      return "Jump";
    case Yield:
      return "Yield";
    case Halt:
      return "Halt";
  }
  return "Unknown";
}

}  // namespace memgraph::planner::core::pattern::vm
