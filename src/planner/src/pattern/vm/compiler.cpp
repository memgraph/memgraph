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
#include <limits>
#include <ranges>
#include <stdexcept>
#include <utility>

namespace memgraph::planner::core::pattern::vm {

void PatternCompilerBase::reset() {
  code_.clear();
  seen_vars_.clear();
  var_to_reg_.clear();
  slot_map_.clear();
  binding_order_.clear();
  next_eclass_reg_ = 0;
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

auto PatternCompilerBase::emit_iter_loop(Instruction iter_instr, Instruction next_instr,
                                         std::optional<SlotIdx> mark_slot) -> IterLoopAddrs {
  emit(iter_instr);
  auto const jump_addr = emit(Instruction::jmp(InstrAddr{0}));                                 // placeholder
  auto const mark_addr = mark_slot ? emit(Instruction::mark_seen(*mark_slot)) : InstrAddr{0};  // filled below
  auto const loop_addr = emit(next_instr);
  patch_target(jump_addr, current_addr());  // Jump skips [MarkSeen +] NextX
  return {.exhaust = mark_slot ? mark_addr : loop_addr, .loop = loop_addr};
}

auto PatternCompilerBase::alloc_eclass_reg() -> EClassReg {
  if (next_eclass_reg_ == std::numeric_limits<strong::underlying_type_t<EClassReg>>::max()) {
    throw std::overflow_error("Pattern too complex: e-class register limit exceeded");
  }
  return EClassReg{std::exchange(next_eclass_reg_, static_cast<uint8_t>(next_eclass_reg_ + 1))};
}

auto PatternCompilerBase::alloc_enode_reg() -> ENodeReg {
  if (next_enode_reg_ == std::numeric_limits<strong::underlying_type_t<ENodeReg>>::max()) {
    throw std::overflow_error("Pattern too complex: e-node register limit exceeded");
  }
  return ENodeReg{std::exchange(next_enode_reg_, static_cast<uint8_t>(next_enode_reg_ + 1))};
}

auto PatternCompilerBase::get_slot(PatternVar var) const -> SlotIdx { return slot_map_.at(var); }

auto PatternCompilerBase::emit_var_binding(PatternVar var, EClassReg eclass_reg, InstrAddr backtrack) -> InstrAddr {
  auto const slot = get_slot(var);
  if (seen_vars_.contains(var)) {
    emit(Instruction::check_slot(slot, eclass_reg, backtrack));
  } else {
    seen_vars_.insert(var);
    binding_order_.push_back(slot);
    var_to_reg_[var] = eclass_reg;
    emit(Instruction::bind_slot(slot, eclass_reg, backtrack));
  }
  return backtrack;
}

auto PatternCompilerBase::emit_var_eclass_iter(PatternVar var, InstrAddr backtrack) -> EClassSetup {
  auto eclass_reg = alloc_eclass_reg();
  auto mark = last_unbound_slot(var);
  auto [exhaust, loop] = emit_iter_loop(
      Instruction::iter_all_eclasses(eclass_reg, backtrack), Instruction::next_eclass(eclass_reg, backtrack), mark);
  emit_var_binding(var, eclass_reg, loop);
  return {.eclass_reg = eclass_reg, .exhaust = exhaust};
}

auto PatternCompilerBase::compute_join_plan(std::span<boost::unordered_flat_set<PatternVar> const> pat_vars)
    -> JoinPlan {
  auto const n = pat_vars.size();
  if (n == 1) return {{0}};

  auto const indices = std::views::iota(0UZ, n);

  // Find pattern with most variables as anchor (max_element returns first max → lowest index on tie)
  auto anchor = *std::ranges::max_element(indices, {}, [&](auto i) { return pat_vars[i].size(); });

  // Greedy join order: pick pattern sharing most vars with already-joined set.
  // Tie-break: fewest new vars (filter early), then lowest index (determinism).
  auto remaining = indices | std::views::filter([anchor](auto i) { return i != anchor; }) |
                   std::ranges::to<boost::unordered_flat_set<std::size_t>>();

  boost::unordered_flat_set<PatternVar> bound_vars = pat_vars[anchor];
  std::vector order{anchor};
  order.reserve(n);

  auto join_priority = [&](std::size_t idx) {
    auto shared = std::ranges::count_if(pat_vars[idx], [&](auto const &var) { return bound_vars.contains(var); });
    auto new_vars = static_cast<long>(pat_vars[idx].size()) - shared;
    return std::tuple(shared, -new_vars, -static_cast<long>(idx));
  };

  while (!remaining.empty()) {
    auto best = *std::ranges::max_element(remaining, {}, join_priority);
    order.push_back(best);
    remaining.erase(best);
    bound_vars.insert(pat_vars[best].begin(), pat_vars[best].end());
  }

  return {std::move(order)};
}

void PatternCompilerBase::schedule_fragments(EmitPlan &plan, std::span<FragmentInfo const> info) {
  auto const n = plan.entries.size();
  assert(info.size() == n);

  auto eclass_available = boost::unordered_flat_set<PatternVar>{};
  auto eclass_deps_met = [&](std::size_t i) -> bool {
    if (auto const *jv = std::get_if<JoinVar>(&plan.entries[i].entry.binding)) {
      return eclass_available.contains(jv->var);
    }
    return true;
  };

  auto eclass_done = std::vector<bool>(n, false);
  auto enode_done = std::vector<bool>(n, false);

  auto emit_eclass_frags = [&](std::size_t i) {
    plan.fragments.push_back({i, Fragment::EclassIter});
    if (info[i].has_parent_walk) {
      plan.fragments.push_back({i, Fragment::ParentWalk});
    }
    eclass_done[i] = true;
    eclass_available.insert(info[i].eclass_vars.begin(), info[i].eclass_vars.end());
  };

  auto emit_enode_frag = [&](std::size_t i) {
    if (info[i].has_enode_verify) {
      plan.fragments.push_back({i, Fragment::EnodeVerify});
    }
    enode_done[i] = true;
  };

  for (std::size_t i = 0; i < n; ++i) {
    if (eclass_done[i]) continue;

    emit_eclass_frags(i);

    for (std::size_t j = i + 1; j < n; ++j) {
      if (!eclass_done[j] && eclass_deps_met(j)) {
        emit_eclass_frags(j);
      }
    }

    emit_enode_frag(i);
  }

  for (std::size_t i = 0; i < n; ++i) {
    if (!enode_done[i]) emit_enode_frag(i);
  }
}

}  // namespace memgraph::planner::core::pattern::vm
