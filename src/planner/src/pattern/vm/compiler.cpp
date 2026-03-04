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

#include "planner/pattern/vm/compiler.hpp"

#include <algorithm>

namespace memgraph::planner::core::pattern::vm {

void PatternCompilerBase::reset() {
  code_.clear();
  seen_vars_.clear();
  var_to_reg_.clear();
  slot_map_.clear();
  binding_order_.clear();
  next_eclass_reg_ = EClassReg{1};  // eclass_regs[0] is reserved for input e-class
  next_enode_reg_ = ENodeReg{0};
}

auto PatternCompilerBase::emit_iter_loop(Instruction iter_instr, Instruction next_instr) -> InstrAddr {
  // TODO: this is confusing code structure
  code_.push_back(iter_instr);
  auto jump_pos = static_cast<uint16_t>(code_.size());
  code_.push_back(Instruction::jmp(InstrAddr{0}));  // placeholder
  auto loop_pos = InstrAddr{static_cast<uint16_t>(code_.size())};
  code_.push_back(next_instr);
  code_[jump_pos].target = static_cast<uint16_t>(code_.size());
  return loop_pos;
}

void PatternCompilerBase::emit_bind_slot(SlotIdx slot, EClassReg eclass_reg, InstrAddr backtrack) {
  // Track binding order for deduplication
  // Only add if not already in binding_order (handles repeated bindings of same variable)
  if (std::ranges::find(binding_order_, slot) == binding_order_.end()) {
    binding_order_.push_back(slot);
  }
  code_.push_back(Instruction::bind_slot_dedup(slot, eclass_reg, backtrack));
}

auto PatternCompilerBase::alloc_eclass_reg() -> EClassReg {
  // Register indices are uint8_t in instructions, so max 256 registers
  auto reg = next_eclass_reg_;
  next_eclass_reg_ = EClassReg{static_cast<uint8_t>(value_of(next_eclass_reg_) + 1)};
  return reg;
}

auto PatternCompilerBase::alloc_enode_reg() -> ENodeReg {
  // Register indices are uint8_t in instructions, so max 256 registers
  auto reg = next_enode_reg_;
  next_enode_reg_ = ENodeReg{static_cast<uint8_t>(value_of(next_enode_reg_) + 1)};
  return reg;
}

auto PatternCompilerBase::get_slot(PatternVar var) const -> SlotIdx {
  auto it = slot_map_.find(var);
  return it != slot_map_.end() ? it->second : SlotIdx{0};
}

void PatternCompilerBase::emit_var_binding(PatternVar var, EClassReg eclass_reg, InstrAddr backtrack) {
  auto slot = get_slot(var);
  if (seen_vars_.contains(var)) {
    code_.push_back(Instruction::check_slot(slot, eclass_reg, backtrack));
  } else {
    seen_vars_.insert(var);
    emit_bind_slot(slot, eclass_reg, backtrack);
  }
}

}  // namespace memgraph::planner::core::pattern::vm
