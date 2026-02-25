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
#include <bitset>
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

/// Iterating e-nodes in an e-class (span-based, advances via subspan)
/// Empty span = inactive/exhausted
// TODO: ENodesIter, AllEClassesIter are very similar, why not template + alias?
struct ENodesIter {
  std::span<ENodeId const> nodes;

  [[nodiscard]] auto exhausted() const -> bool { return nodes.empty(); }

  [[nodiscard]] auto remaining() const -> std::size_t { return nodes.size(); }

  [[nodiscard]] auto current() const -> ENodeId { return nodes.front(); }

  void advance() { nodes = nodes.subspan(1); }
};

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

/// Iterating all canonical e-classes (span-based, advances via subspan)
/// Empty span = inactive/exhausted
struct AllEClassesIter {
  std::span<EClassId const> eclasses;

  [[nodiscard]] auto exhausted() const -> bool { return eclasses.empty(); }

  [[nodiscard]] auto remaining() const -> std::size_t { return eclasses.size(); }

  [[nodiscard]] auto current() const -> EClassId { return eclasses.front(); }

  void advance() { eclasses = eclasses.subspan(1); }
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

  // Which slots are bound (for BindOrCheck)
  // Use std::bitset for O(1) operations with no dynamic allocation
  // TODO: why arbitrary 256?
  std::bitset<256> bound;

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
  // The last slot in binding order - deduplication checks this slot
  uint8_t last_bound_slot_{0};

  /// Initialize state for execution with given number of slots and registers
  template <typename SlotsAfterAccessor>
  void reset(std::size_t num_slots, std::size_t num_eclass_regs, std::size_t num_enode_regs,
             SlotsAfterAccessor slots_bound_after_fn, uint8_t last_bound_slot) {
    slots.assign(num_slots, EClassId{});
    bound.reset();
    pc = 0;

    // Store binding order information
    slots_bound_after_.resize(num_slots);
    for (std::size_t i = 0; i < num_slots; ++i) {
      slots_bound_after_[i] = slots_bound_after_fn(i);
    }
    last_bound_slot_ = last_bound_slot;

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

    reset(
        num_slots,
        num_eclass_regs,
        num_enode_regs,
        [this](std::size_t slot) -> std::span<uint8_t const> { return legacy_slots_bound_after_[slot]; },
        num_slots > 0 ? static_cast<uint8_t>(num_slots - 1) : 0);
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

  /// Bind a slot to an e-class.
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
    bound.set(slot);
  }

  /// Try to yield with deduplication.
  /// Returns false if this binding tuple has already been yielded.
  /// Returns true if this is a new unique tuple.
  /// The canonicalized_slots parameter should contain find()-canonicalized e-class IDs.
  [[nodiscard]] auto try_yield_dedup(std::span<EClassId const> canonicalized_slots) -> bool {
    if (canonicalized_slots.empty()) [[unlikely]] {
      return true;  // No variables, always yield (single match)
    }

    // Check/insert the LAST slot in binding order (not last by index).
    // The prefix for this slot is all slots bound before it.
    auto last_value = canonicalized_slots[last_bound_slot_];

    // Try to insert - returns false if already present
    return seen_per_slot[last_bound_slot_].insert(last_value).second;
  }

  /// Check if slot is bound
  [[nodiscard]] auto is_bound(std::size_t slot) const -> bool { return bound.test(slot); }

  /// Get bound value (must be bound)
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
