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
#include <utility>

namespace memgraph::planner::core::pattern::vm {

void PatternCompilerBase::reset() {
  code_.clear();
  seen_vars_.clear();
  var_to_reg_.clear();
  slot_map_.clear();
  binding_order_.clear();
  next_eclass_reg_ = 1;  // eclass_regs[0] is reserved for input e-class
  next_enode_reg_ = 0;
}

auto PatternCompilerBase::emit(Instruction instr) -> InstrAddr {
  auto const addr = current_addr();
  code_.push_back(instr);
  return addr;
}

auto PatternCompilerBase::current_addr() const -> InstrAddr { return InstrAddr{static_cast<uint16_t>(code_.size())}; }

void PatternCompilerBase::patch_target(InstrAddr addr, InstrAddr target) {
  code_[value_of(addr)].target = value_of(target);
}

auto PatternCompilerBase::emit_iter_loop(Instruction iter_instr, Instruction next_instr) -> InstrAddr {
  emit(iter_instr);
  auto const jump_addr = emit(Instruction::jmp(InstrAddr{0}));  // placeholder
  auto const loop_addr = emit(next_instr);
  patch_target(jump_addr, current_addr());
  return loop_addr;
}

auto PatternCompilerBase::alloc_eclass_reg() -> EClassReg {
  return EClassReg{std::exchange(next_eclass_reg_, static_cast<uint8_t>(next_eclass_reg_ + 1))};
}

auto PatternCompilerBase::alloc_enode_reg() -> ENodeReg {
  return ENodeReg{std::exchange(next_enode_reg_, static_cast<uint8_t>(next_enode_reg_ + 1))};
}

auto PatternCompilerBase::get_slot(PatternVar var) const -> SlotIdx { return slot_map_.at(var); }

void PatternCompilerBase::emit_var_binding(PatternVar var, EClassReg eclass_reg, InstrAddr backtrack) {
  auto const slot = get_slot(var);
  if (seen_vars_.contains(var)) {
    emit(Instruction::check_slot(slot, eclass_reg, backtrack));
  } else {
    seen_vars_.insert(var);
    binding_order_.push_back(slot);
    var_to_reg_[var] = eclass_reg;  // Enable parent traversal for later patterns
    emit(Instruction::bind_slot_dedup(slot, eclass_reg, backtrack));
  }
}

}  // namespace memgraph::planner::core::pattern::vm
