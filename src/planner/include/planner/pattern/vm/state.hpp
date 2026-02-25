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

#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <span>
#include <vector>

#include <boost/container/small_vector.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

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
  // slots_bound_after_[i] = list of slots bound AFTER slot i in binding order
  // Used by bind() to know which seen sets to clear when slot i changes
  std::vector<std::span<uint8_t const>> slots_bound_after_;

  /// Initialize state for execution with given number of slots and registers
  template <typename SlotsAfterAccessor>
  void reset(std::size_t num_slots, std::size_t num_eclass_regs, std::size_t num_enode_regs,
             SlotsAfterAccessor slots_bound_after_fn) {
    slots.assign(num_slots, EClassId{});
    pc = 0;

    // Store binding order information
    slots_bound_after_.resize(num_slots);
    for (std::size_t i = 0; i < num_slots; ++i) {
      slots_bound_after_[i] = slots_bound_after_fn(i);
    }

    // Resize register arrays if needed
    if (eclass_regs.size() < num_eclass_regs) {
      eclass_regs.resize(num_eclass_regs);
      eclasses_iters.resize(num_eclass_regs);
    }
    if (enode_regs.size() < num_enode_regs) {
      enode_regs.resize(num_enode_regs);
      enodes_iters.resize(num_enode_regs);
      parents_iters.resize(num_enode_regs);
    }

    // Reset per-slot seen maps for deduplication
    seen_per_slot.resize(num_slots);
    for (auto &seen_map : seen_per_slot) {
      seen_map.clear();
    }
  }

  /// Legacy reset without binding order (uses slot index order)
  void reset(std::size_t num_slots, std::size_t num_eclass_regs, std::size_t num_enode_regs) {
    // Fall back to index-based ordering (slot 0, 1, 2, ...)
    // This is used by tests that don't have CompiledPattern
    legacy_slots_bound_after_.clear();
    legacy_slots_bound_after_.resize(num_slots);
    for (std::size_t i = 0; i < num_slots; ++i) {
      for (std::size_t j = i + 1; j < num_slots; ++j) {
        legacy_slots_bound_after_[i].push_back(static_cast<uint8_t>(j));
      }
    }

    reset(num_slots, num_eclass_regs, num_enode_regs, [this](std::size_t slot) -> std::span<uint8_t const> {
      return legacy_slots_bound_after_[slot];
    });
  }

 private:
  // Storage for legacy mode (when no binding order provided)
  std::vector<std::vector<uint8_t>> legacy_slots_bound_after_;

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

  /// Bind a slot to an e-class (unconditional, for non-last slots).
  /// If the slot value changes, clears the seen sets for slots bound AFTER this one
  /// in binding order (since their prefix context has changed).
  ///
  /// This uses binding order, not slot index order. For pattern A(?x, B(?y, ?z)) with
  /// binding order [1, 2, 0], when slot 2 (?z) changes, we clear seen[0] (bound after),
  /// not seen[3..] (higher indices).
  void bind(std::size_t slot, EClassId eclass) {
    if (slots[slot] != eclass) {
      // Slot value changed - clear seen sets for slots bound AFTER this one
      for (auto after_slot : slots_bound_after_[slot]) {
        seen_per_slot[after_slot].clear();
      }
      slots[slot] = eclass;
    }
  }

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
      for (auto after_slot : slots_bound_after_[slot]) {
        seen_per_slot[after_slot].clear();
      }
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

/// Statistics collected during VM execution (for benchmarking)
struct VMStats {
  std::size_t instructions_executed{0};
  std::size_t iter_enode_calls{0};
  std::size_t iter_parent_calls{0};
  std::size_t parent_symbol_hits{0};    // Parents that matched symbol filter
  std::size_t parent_symbol_misses{0};  // Parents that failed symbol filter
  std::size_t check_slot_hits{0};       // CheckSlot that passed
  std::size_t check_slot_misses{0};     // CheckSlot that failed
  std::size_t yields{0};

  void reset() { *this = VMStats{}; }

  [[nodiscard]] auto parent_filter_rate() const -> double {
    auto total = parent_symbol_hits + parent_symbol_misses;
    return total > 0 ? static_cast<double>(parent_symbol_misses) / static_cast<double>(total) : 0.0;
  }
};

}  // namespace memgraph::planner::core::vm
