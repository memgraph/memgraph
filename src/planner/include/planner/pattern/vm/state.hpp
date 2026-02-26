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

import memgraph.planner.core.eids;

#include <cstdint>
#include <span>
#include <vector>

#include <boost/container/small_vector.hpp>

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>
#include "utils/logging.hpp"

namespace memgraph::planner::core::vm {

/// Default register count for small patterns (avoids reallocation)
static constexpr std::size_t kDefaultRegisters = 16;

// === Specialized iteration state types ===
// Each type uses exhausted state as inactive indicator (no separate discriminator needed).
// The compiler statically knows which iteration type each register uses.

/// Span-based iterator (advances via subspan)
/// Empty span = inactive/exhausted
template <typename Id>
struct SpanIter {
  std::span<Id const> items;

  [[nodiscard]] auto exhausted() const -> bool { return items.empty(); }

  [[nodiscard]] auto remaining() const -> std::size_t { return items.size(); }

  [[nodiscard]] auto current() const -> Id { return items.front(); }

  void advance() { items = items.subspan(1); }
};

/// Iterating e-nodes in an e-class
using ENodesIter = SpanIter<ENodeId>;

/// Iterating all canonical e-classes
using AllEClassesIter = SpanIter<EClassId>;

/// Iterating parent e-nodes (index-based, set doesn't support span)
/// idx >= end = inactive/exhausted
struct ParentsIter {
  EClassId eclass;
  std::size_t idx{0};
  std::size_t end{0};

  [[nodiscard]] auto exhausted() const -> bool { return idx >= end; }

  [[nodiscard]] auto remaining() const -> std::size_t { return end - idx; }

  [[nodiscard]] auto index() const -> std::size_t { return idx; }

  void advance() { ++idx; }
};

/// Open-addressing hash set for deduplication (much better cache locality than std::unordered_set)
using FastEClassSet = boost::unordered_flat_set<EClassId>;

/// Configuration for VMState reset
struct VMStateConfig {
  std::size_t num_eclass_regs;
  std::size_t num_enode_regs;
  std::span<uint8_t const> binding_order;  // size == num_slots
  std::span<uint8_t const> slot_to_order;  // size == num_slots
};

/// VM execution state
struct VMState {
  // E-class registers (result of navigation)
  std::vector<EClassId> eclass_regs;

  // E-node registers (current e-node in iteration)
  std::vector<ENodeId> enode_regs;

  // Variable binding slots
  boost::container::small_vector<EClassId, 8> slots;

  // Per-slot seen sets for deduplication.
  // seen_per_slot[i] tracks which values we've seen at slot i for the CURRENT prefix.
  // The prefix is defined by slots bound BEFORE slot i in binding order.
  // When slot j is rebound to a different value, we clear seen_per_slot for slots
  // that are bound AFTER j (not slots with higher indices).
  std::vector<FastEClassSet> seen_per_slot;

  // Program counter
  std::size_t pc{0};

  // Iteration state: separate homogeneous vectors indexed by register
  // Each type uses exhausted state as inactive indicator (no explicit deactivation needed)
  // The compiler statically determines which type each register uses
  // When an iteration exhausts, its state is naturally inert; fresh iterations overwrite
  std::vector<ENodesIter> enodes_iters;         // E-node iterations (span-based)
  std::vector<ParentsIter> parents_iters;       // Parent iterations (index-based)
  std::vector<AllEClassesIter> eclasses_iters;  // All e-classes iterations (span-based)

  // Binding order information (set during reset, from CompiledPattern)
  // binding_order_[i] = slot at position i in binding order
  // slot_to_order_[slot] = position of slot in binding order
  std::span<uint8_t const> binding_order_;
  std::span<uint8_t const> slot_to_order_;

  /// Initialize state for execution
  void reset(VMStateConfig const &cfg) {
    DMG_ASSERT(cfg.binding_order.size() == cfg.slot_to_order.size());
    auto const num_slots = cfg.binding_order.size();
    slots.assign(num_slots, EClassId{});
    pc = 0;

    // Store binding order information as spans (no allocation)
    binding_order_ = cfg.binding_order;
    slot_to_order_ = cfg.slot_to_order;

    // Resize register arrays if needed
    if (eclass_regs.size() < cfg.num_eclass_regs) {
      eclass_regs.resize(cfg.num_eclass_regs);
      eclasses_iters.resize(cfg.num_eclass_regs);
    }
    if (enode_regs.size() < cfg.num_enode_regs) {
      enode_regs.resize(cfg.num_enode_regs);
      enodes_iters.resize(cfg.num_enode_regs);
      parents_iters.resize(cfg.num_enode_regs);
    }

    // Reset per-slot seen maps for deduplication
    seen_per_slot.resize(num_slots);
    for (auto &seen_map : seen_per_slot) {
      seen_map.clear();
    }
  }

 private:
  /// Clear seen sets for all slots bound AFTER the given slot in binding order
  void clear_dependent_seen_sets(std::size_t slot) {
    auto const order = slot_to_order_[slot];
    for (std::size_t j = order + 1; j < binding_order_.size(); ++j) {
      seen_per_slot[binding_order_[j]].clear();
    }
  }

 public:
  /// Start an e-node iteration on a register (uses span)
  void start_enode_iter(uint8_t reg, std::span<ENodeId const> nodes) { enodes_iters[reg] = ENodesIter{nodes}; }

  /// Start a parent iteration on a register (index-based)
  void start_parent_iter(uint8_t reg, EClassId eclass, std::size_t parent_count) {
    parents_iters[reg] = ParentsIter{eclass, 0, parent_count};
  }

  /// Start an all-eclasses iteration on a register (uses span of e-class IDs)
  void start_all_eclasses_iter(uint8_t reg, std::span<EClassId const> eclasses) {
    eclasses_iters[reg] = AllEClassesIter{eclasses};
  }

  /// Get e-nodes iterator for a register
  [[nodiscard]] auto get_enodes_iter(uint8_t reg) -> ENodesIter & { return enodes_iters[reg]; }

  [[nodiscard]] auto get_enodes_iter(uint8_t reg) const -> ENodesIter const & { return enodes_iters[reg]; }

  /// Get parents iterator for a register
  [[nodiscard]] auto get_parents_iter(uint8_t reg) -> ParentsIter & { return parents_iters[reg]; }

  [[nodiscard]] auto get_parents_iter(uint8_t reg) const -> ParentsIter const & { return parents_iters[reg]; }

  /// Get all e-classes iterator for a register
  [[nodiscard]] auto get_eclasses_iter(uint8_t reg) -> AllEClassesIter & { return eclasses_iters[reg]; }

  [[nodiscard]] auto get_eclasses_iter(uint8_t reg) const -> AllEClassesIter const & { return eclasses_iters[reg]; }

  /// Try to bind a slot with deduplication check.
  /// Returns false if this value has already been fully explored at this slot (backtrack).
  /// Returns true if this is a value to explore.
  ///
  /// The seen set tracks values that have been marked as exhausted (via mark_seen).
  /// A value is exhausted when we've finished exploring all paths with that binding
  /// (either yielded or exhausted all downstream iterations).
  ///
  /// When iterating e-nodes within the same e-class, the binding may be the same.
  /// We still check seen (to catch duplicate candidates), but don't clear later
  /// slots or re-bind if the value hasn't changed.
  [[nodiscard]] auto try_bind_dedup(std::size_t slot, EClassId eclass) -> bool {
    // Always check if we've already exhausted this value
    if (seen_per_slot[slot].contains(eclass)) {
      return false;  // Already exhausted - backtrack
    }

    // Only clear later slots and update if value is actually changing
    if (slots[slot] != eclass) {
      clear_dependent_seen_sets(slot);
      slots[slot] = eclass;
    }

    return true;
  }

  /// Mark a slot's current value as seen (exhausted) for deduplication.
  /// Called when an iteration exhausts (for earlier slots) or at yield time (for last slot).
  void mark_seen(std::size_t slot) { seen_per_slot[slot].insert(slots[slot]); }

  /// Get bound value
  [[nodiscard]] auto get(std::size_t slot) const -> EClassId { return slots[slot]; }
};

}  // namespace memgraph::planner::core::vm
