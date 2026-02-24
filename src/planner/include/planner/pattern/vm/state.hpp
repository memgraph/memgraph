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
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/container/small_vector.hpp>

namespace memgraph::planner::core::vm {

/// Default register count for small patterns (avoids reallocation)
static constexpr std::size_t kDefaultRegisters = 16;

/// Iteration state for nested loops (e-node or parent iteration)
/// For e-nodes: uses span (e-class nodes are stored contiguously)
/// For parents: stores e-class ID for lookup (parents are in a set)
struct IterState {
  enum class Kind : uint8_t {
    Inactive,         // No active iteration for this register
    ENodes,           // Iterating e-nodes in an e-class (uses span)
    Parents,          // Iterating parent e-nodes (index-based)
    ParentsFiltered,  // Iterating filtered parents (uses span from index)
    AllEClasses       // Iterating all canonical e-classes (uses e-class span)
  };

  Kind kind{Kind::Inactive};

  // Iteration position
  std::size_t current_idx{0};
  std::size_t end_idx{0};

  // For ENodes and ParentsFiltered: span into e-class or parent index
  std::span<ENodeId const> nodes_span;

  // For AllEClasses: span of e-class IDs to iterate
  std::span<EClassId const> eclasses_span;

  // For Parents: the e-class whose parents we're iterating
  EClassId parent_eclass{};

  [[nodiscard]] auto exhausted() const -> bool { return current_idx >= end_idx; }

  [[nodiscard]] auto remaining() const -> std::size_t { return end_idx - current_idx; }

  /// Get current e-node from span (for ENodes and ParentsFiltered)
  [[nodiscard]] auto current() const -> ENodeId { return nodes_span[current_idx]; }

  void advance() {
    // TODO: If we have a span then why not subspan?
    ++current_idx;
  }

  void reset() { kind = Kind::Inactive; }

  void start_enodes(std::span<ENodeId const> node_span) {
    kind = Kind::ENodes;
    current_idx = 0;
    end_idx = node_span.size();
    nodes_span = node_span;
  }

  void start_parents(EClassId eclass, std::size_t parent_count) {
    kind = Kind::Parents;
    current_idx = 0;
    end_idx = parent_count;
    parent_eclass = eclass;
  }

  void start_parents_filtered(std::span<ENodeId const> parent_span) {
    kind = Kind::ParentsFiltered;
    current_idx = 0;
    end_idx = parent_span.size();
    nodes_span = parent_span;
  }

  void start_all_eclasses(std::span<EClassId const> eclass_span) {
    kind = Kind::AllEClasses;
    current_idx = 0;
    end_idx = eclass_span.size();
    eclasses_span = eclass_span;
  }

  /// Get current e-class from span (for AllEClasses)
  [[nodiscard]] auto current_eclass() const -> EClassId { return eclasses_span[current_idx]; }
};

/// Hash function for prefix vectors (used in deduplication)
struct PrefixHash {
  auto operator()(std::vector<EClassId> const &v) const -> std::size_t {
    std::size_t h = 0;
    for (auto id : v) {
      h ^= std::hash<EClassId>{}(id) + 0x9e3779b9 + (h << 6) + (h >> 2);
    }
    return h;
  }
};

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
  // seen_per_slot[i] is a map from prefix (slots 0..i-1) to set of seen values at slot i.
  // This allows efficient per-level deduplication without storing full tuples.
  // For slot 0, the prefix is empty, so it's just a simple set.
  std::vector<std::unordered_map<std::vector<EClassId>, std::unordered_set<EClassId>, PrefixHash>> seen_per_slot;

  // Program counter
  std::size_t pc{0};

  // Iteration state indexed by register for O(1) lookup
  // Each register can have at most one active iteration
  std::vector<IterState> iter_by_reg;

  // Stack of active register indices for cleanup ordering
  // When an iteration exhausts, we need to deactivate all iterations
  // that were started after it (nested iterations)
  boost::container::small_vector<uint8_t, 16> iter_order;

  /// Initialize state for execution with given number of slots and registers
  void reset(std::size_t num_slots, std::size_t num_registers) {
    slots.assign(num_slots, EClassId{});
    bound.reset();
    pc = 0;
    // Resize register arrays if needed
    if (eclass_regs.size() < num_registers) {
      eclass_regs.resize(num_registers);
      enode_regs.resize(num_registers);
      iter_by_reg.resize(num_registers);
    }
    // Reset all iteration states
    for (std::size_t i = 0; i < num_registers; ++i) {
      iter_by_reg[i].reset();
    }
    iter_order.clear();

    // Reset per-slot seen maps for deduplication
    seen_per_slot.resize(num_slots);
    for (auto &seen_map : seen_per_slot) {
      seen_map.clear();
    }
  }

  /// Start an e-node iteration on a register (uses span)
  void start_enode_iter(uint8_t reg, std::span<ENodeId const> nodes) {
    iter_by_reg[reg].start_enodes(nodes);
    iter_order.push_back(reg);
  }

  /// Start a parent iteration on a register (index-based)
  void start_parent_iter(uint8_t reg, EClassId eclass, std::size_t parent_count) {
    iter_by_reg[reg].start_parents(eclass, parent_count);
    iter_order.push_back(reg);
  }

  /// Start a filtered parent iteration on a register (uses span from index)
  void start_filtered_parent_iter(uint8_t reg, std::span<ENodeId const> parents) {
    iter_by_reg[reg].start_parents_filtered(parents);
    iter_order.push_back(reg);
  }

  /// Start an all-eclasses iteration on a register (uses span of e-class IDs)
  void start_all_eclasses_iter(uint8_t reg, std::span<EClassId const> eclasses) {
    iter_by_reg[reg].start_all_eclasses(eclasses);
    iter_order.push_back(reg);
  }

  /// Get iteration state for a register (O(1) lookup)
  [[nodiscard]] auto get_iter(uint8_t reg) -> IterState & { return iter_by_reg[reg]; }

  [[nodiscard]] auto get_iter(uint8_t reg) const -> IterState const & { return iter_by_reg[reg]; }

  /// Check if register has active iteration
  [[nodiscard]] auto has_active_iter(uint8_t reg) const -> bool {
    return iter_by_reg[reg].kind != IterState::Kind::Inactive;
  }

  /// Deactivate this register's iteration and all iterations started after it
  void deactivate_iter_and_nested(uint8_t reg) {
    // Find position of this register in the order stack
    auto it = std::find(iter_order.begin(), iter_order.end(), reg);
    if (it == iter_order.end()) return;

    // Deactivate all iterations from this point onward
    for (auto rit = it; rit != iter_order.end(); ++rit) {
      iter_by_reg[*rit].reset();
    }
    iter_order.erase(it, iter_order.end());
  }

  /// Bind a slot to an e-class
  void bind(std::size_t slot, EClassId eclass) {
    slots[slot] = eclass;
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

    // Build the prefix (all slots except the last)
    std::vector<EClassId> prefix(canonicalized_slots.begin(), canonicalized_slots.end() - 1);

    // Check if the last slot's value is new given this prefix
    auto last_slot_idx = canonicalized_slots.size() - 1;
    auto &seen_set = seen_per_slot[last_slot_idx][prefix];
    auto last_value = canonicalized_slots.back();

    // Try to insert - returns false if already present
    return seen_set.insert(last_value).second;
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
