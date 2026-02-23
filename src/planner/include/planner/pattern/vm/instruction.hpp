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
  /// Begin iterating ALL parents of e-class[src]
  /// Pushes iteration state. If empty, jumps to target.
  IterParents,
  /// Begin iterating parents of e-class[src] filtered by symbol[arg]
  /// Uses by-symbol index if available. Pushes state. If empty, jumps.
  IterParentsSym,
  /// Advance to next parent e-node, store in dst
  /// If exhausted, pops state and jumps to target.
  NextParent,

  // ===== Filtering =====
  /// If enode[src].symbol != symbols[arg], jump to target
  CheckSymbol,
  /// If enode[src].arity != arg, jump to target
  CheckArity,

  // ===== Binding =====
  /// Unconditional bind: slots[arg] = regs[src]
  BindSlot,
  /// Check consistency: if find(slots[arg]) != find(regs[src]), jump to target
  CheckSlot,
  /// Bind if unbound, else check: combines BindSlot + CheckSlot
  BindOrCheck,

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

  static constexpr auto iter_parents_sym(uint8_t dst, uint8_t src, uint8_t sym_idx, uint16_t on_empty) -> Instruction {
    return {VMOp::IterParentsSym, dst, src, sym_idx, on_empty};
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

  static constexpr auto bind_slot(uint8_t slot_idx, uint8_t src) -> Instruction {
    return {VMOp::BindSlot, 0, src, slot_idx, 0};
  }

  static constexpr auto check_slot(uint8_t slot_idx, uint8_t src, uint16_t on_mismatch) -> Instruction {
    return {VMOp::CheckSlot, 0, src, slot_idx, on_mismatch};
  }

  static constexpr auto bind_or_check(uint8_t slot_idx, uint8_t src, uint16_t on_conflict) -> Instruction {
    return {VMOp::BindOrCheck, 0, src, slot_idx, on_conflict};
  }

  static constexpr auto jmp(uint16_t target) -> Instruction { return {VMOp::Jump, 0, 0, 0, target}; }

  static constexpr auto yield() -> Instruction { return {VMOp::Yield, 0, 0, 0, 0}; }

  static constexpr auto halt() -> Instruction { return {VMOp::Halt, 0, 0, 0, 0}; }
};

static_assert(sizeof(Instruction) == 6, "Instruction should be 6 bytes");

/// Equality for testing compiled bytecode
constexpr auto operator==(Instruction const &a, Instruction const &b) -> bool {
  return a.op == b.op && a.dst == b.dst && a.src == b.src && a.arg == b.arg && a.target == b.target;
}

constexpr auto operator!=(Instruction const &a, Instruction const &b) -> bool { return !(a == b); }

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
    case VMOp::IterParentsSym:
      return "IterParentsSym";
    case VMOp::NextParent:
      return "NextParent";
    case VMOp::CheckSymbol:
      return "CheckSymbol";
    case VMOp::CheckArity:
      return "CheckArity";
    case VMOp::BindSlot:
      return "BindSlot";
    case VMOp::CheckSlot:
      return "CheckSlot";
    case VMOp::BindOrCheck:
      return "BindOrCheck";
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
