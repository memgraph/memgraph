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

/// Iterating filtered parents (span-based, advances via subspan)
/// Empty span = inactive/exhausted
struct ParentsFilteredIter {
  std::span<ENodeId const> nodes;

  [[nodiscard]] auto exhausted() const -> bool { return nodes.empty(); }

  [[nodiscard]] auto remaining() const -> std::size_t { return nodes.size(); }

  [[nodiscard]] auto current() const -> ENodeId { return nodes.front(); }

  void advance() { nodes = nodes.subspan(1); }
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
  std::bitset<256> bound;

  // Per-slot seen sets for deduplication.
  // seen_per_slot[i] tracks which values we've seen at slot i for the CURRENT prefix.
  // The prefix is implicitly defined by the current bindings of slots 0..i-1.
  // When slot j is rebound to a different value, we clear seen_per_slot[j+1..] since
  // those sets were for the old prefix.
  std::vector<FastEClassSet> seen_per_slot;

  // Highest slot index that has been used (for optimization in bind())
  std::size_t max_seen_slot_{0};

  // Program counter
  std::size_t pc{0};

  // Iteration state: separate homogeneous vectors indexed by register
  // Each type uses exhausted state as inactive indicator (no explicit deactivation needed)
  // The compiler statically determines which type each register uses
  // When an iteration exhausts, its state is naturally inert; fresh iterations overwrite
  std::vector<ENodesIter> enodes_iters;             // E-node iterations (span-based)
  std::vector<ParentsIter> parents_iters;           // Parent iterations (index-based)
  std::vector<ParentsFilteredIter> filtered_iters;  // Filtered parent iterations (span-based)
  std::vector<AllEClassesIter> eclasses_iters;      // All e-classes iterations (span-based)

  /// Initialize state for execution with given number of slots and registers
  void reset(std::size_t num_slots, std::size_t num_eclass_regs, std::size_t num_enode_regs) {
    slots.assign(num_slots, EClassId{});
    bound.reset();
    pc = 0;
    // Resize register arrays if needed (separate sizes for each type)
    if (eclass_regs.size() < num_eclass_regs) {
      eclass_regs.resize(num_eclass_regs);
      eclasses_iters.resize(num_eclass_regs);  // E-class iteration state (IterAllEClasses)
    }
    if (enode_regs.size() < num_enode_regs) {
      enode_regs.resize(num_enode_regs);
      enodes_iters.resize(num_enode_regs);    // E-node iteration state (IterENodes)
      parents_iters.resize(num_enode_regs);   // Parent iteration state (index-based, IterParents)
      filtered_iters.resize(num_enode_regs);  // Filtered parent iteration state (span-based, IterParentsSym)
    }
    // No need to reset iteration states - exhausted() is the inactive indicator,
    // and start_*_iter() overwrites with fresh state when re-entering loops

    // Reset per-slot seen maps for deduplication
    seen_per_slot.resize(num_slots);
    for (auto &seen_map : seen_per_slot) {
      seen_map.clear();
    }
    max_seen_slot_ = 0;
  }

  /// Start an e-node iteration on a register (uses span)
  void start_enode_iter(uint8_t reg, std::span<ENodeId const> nodes) { enodes_iters[reg] = ENodesIter{nodes}; }

  /// Start a parent iteration on a register (index-based)
  void start_parent_iter(uint8_t reg, EClassId eclass, std::size_t parent_count) {
    parents_iters[reg] = ParentsIter{eclass, 0, parent_count};
  }

  /// Start a filtered parent iteration on a register (uses span from index)
  void start_filtered_parent_iter(uint8_t reg, std::span<ENodeId const> parents) {
    filtered_iters[reg] = ParentsFilteredIter{parents};
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

  /// Get filtered parents iterator for a register
  [[nodiscard]] auto get_filtered_iter(uint8_t reg) -> ParentsFilteredIter & { return filtered_iters[reg]; }

  [[nodiscard]] auto get_filtered_iter(uint8_t reg) const -> ParentsFilteredIter const & { return filtered_iters[reg]; }

  /// Get all e-classes iterator for a register
  [[nodiscard]] auto get_eclasses_iter(uint8_t reg) -> AllEClassesIter & { return eclasses_iters[reg]; }

  [[nodiscard]] auto get_eclasses_iter(uint8_t reg) const -> AllEClassesIter const & { return eclasses_iters[reg]; }

  /// Bind a slot to an e-class.
  /// If the slot value changes, clears the seen sets for all later slots
  /// (since their prefix has changed).
  void bind(std::size_t slot, EClassId eclass) {
    if (slots[slot] != eclass) {
      // Slot value changed - clear seen sets for dependent slots
      // Only iterate up to max_seen_slot_ to avoid unnecessary work
      auto end = std::min(max_seen_slot_ + 1, seen_per_slot.size());
      for (std::size_t i = slot + 1; i < end; ++i) {
        if (!seen_per_slot[i].empty()) {
          seen_per_slot[i].clear();
        }
      }
      slots[slot] = eclass;
    }
    bound.set(slot);
  }

  /// Try to yield with deduplication.
  /// Returns false if this binding tuple has already been yielded.
  /// Returns true if this is a new unique tuple.
  /// The canonicalized_slots parameter should contain find()-canonicalized e-class IDs.
  [[nodiscard]] auto try_yield_dedup(std::vector<EClassId> const &canonicalized_slots) -> bool {
    if (canonicalized_slots.empty()) {
      return true;  // No variables, always yield (single match)
    }

    // The prefix (slots 0..n-2) is implicitly tracked by the current bindings.
    // We only need to check if the last slot's value is new for this prefix.
    auto last_slot_idx = canonicalized_slots.size() - 1;
    auto last_value = canonicalized_slots.back();

    // Track highest slot used for optimization in bind()
    if (last_slot_idx > max_seen_slot_) {
      max_seen_slot_ = last_slot_idx;
    }

    // Try to insert - returns false if already present
    return seen_per_slot[last_slot_idx].insert(last_value).second;
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
