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

/// VM opcodes for pattern matching
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

/// Single VM instruction (6 bytes, fits in cache line nicely)
struct Instruction {
  VMOp op;
  uint8_t dst;      // Destination register index
  uint8_t src;      // Source register index
  uint8_t arg;      // Symbol index, slot index, child index, or arity
  uint16_t target;  // Jump target for conditional/iteration ops

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
