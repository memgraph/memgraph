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

namespace memgraph::planner::core::vm {

void PatternCompilerBase::reset() {
  code_.clear();
  seen_vars_.clear();
  var_to_reg_.clear();
  slot_map_.clear();
  binding_order_.clear();
  next_eclass_reg_ = 1;  // eclass_regs[0] is reserved for input e-class
  next_enode_reg_ = 0;
}

auto PatternCompilerBase::emit_iter_loop(Instruction iter_instr, Instruction next_instr) -> uint16_t {
  code_.push_back(iter_instr);
  auto jump_pos = static_cast<uint16_t>(code_.size());
  code_.push_back(Instruction::jmp(0));  // placeholder
  auto loop_pos = static_cast<uint16_t>(code_.size());
  code_.push_back(next_instr);
  code_[jump_pos].target = static_cast<uint16_t>(code_.size());
  return loop_pos;
}

void PatternCompilerBase::emit_bind_slot(uint8_t slot, uint8_t eclass_reg, uint16_t backtrack) {
  // Track binding order for deduplication
  // Only add if not already in binding_order (handles repeated bindings of same variable)
  if (std::find(binding_order_.begin(), binding_order_.end(), slot) == binding_order_.end()) {
    binding_order_.push_back(slot);
  }
  code_.push_back(Instruction::bind_slot_dedup(slot, eclass_reg, backtrack));
}

auto PatternCompilerBase::alloc_eclass_reg() -> uint8_t {
  // Register indices are uint8_t in instructions, so max 256 registers
  return next_eclass_reg_++;
}

auto PatternCompilerBase::alloc_enode_reg() -> uint8_t {
  // Register indices are uint8_t in instructions, so max 256 registers
  return next_enode_reg_++;
}

auto PatternCompilerBase::get_slot(PatternVar var) const -> uint8_t {
  auto it = slot_map_.find(var);
  return it != slot_map_.end() ? it->second : 0;
}

void PatternCompilerBase::emit_var_binding(PatternVar var, uint8_t eclass_reg, uint16_t backtrack) {
  auto slot = get_slot(var);
  if (seen_vars_.contains(var)) {
    code_.push_back(Instruction::check_slot(slot, eclass_reg, backtrack));
  } else {
    seen_vars_.insert(var);
    emit_bind_slot(slot, eclass_reg, backtrack);
  }
}

}  // namespace memgraph::planner::core::vm
