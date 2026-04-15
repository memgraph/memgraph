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

#include "planner/pattern/vm/compiled_matcher.hpp"

namespace memgraph::planner::core::pattern::vm {

CompiledMatcherBase::CompiledMatcherBase(std::vector<Instruction> code, std::size_t num_eclass_regs,
                                         std::size_t num_enode_regs, std::vector<SlotIdx> binding_order,
                                         VarSlotMap var_slots)
    : code_(std::move(code)),
      num_eclass_regs_(num_eclass_regs),
      num_enode_regs_(num_enode_regs),
      binding_order_(std::move(binding_order)),
      var_slots_(std::move(var_slots)) {
  // Compute inverse mapping: slot -> order position
  // For binding_order [1, 2, 0], slot_to_order_ becomes [2, 0, 1]
  // meaning slot 0 is bound at position 2, slot 1 at position 0, slot 2 at position 1
  slot_to_order_.resize(var_slots_.size());
  for (std::size_t order_idx = 0; order_idx < binding_order_.size(); ++order_idx) {
    slot_to_order_[value_of(binding_order_[order_idx])] = static_cast<uint8_t>(order_idx);
  }
}

auto CompiledMatcherBase::state_config() const -> VMStateConfig {
  return {.num_eclass_regs = num_eclass_regs_,
          .num_enode_regs = num_enode_regs_,
          .binding_order = binding_order_,
          .slot_to_order = slot_to_order_};
}

}  // namespace memgraph::planner::core::pattern::vm
