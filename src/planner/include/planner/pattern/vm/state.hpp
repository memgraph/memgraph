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

#include <array>
#include <bitset>
#include <cstdint>
#include <span>

#include <boost/container/small_vector.hpp>

namespace memgraph::planner::core::vm {

/// Maximum number of registers in the VM
/// Deep patterns need ~2 registers per nesting level, so 64 supports ~30 levels
static constexpr std::size_t kMaxRegisters = 64;

/// Maximum number of variable slots
static constexpr std::size_t kMaxSlots = 32;

/// Iteration state for nested loops (e-node or parent iteration)
struct IterState {
  enum class Kind : uint8_t {
    ENodes,   // Iterating e-nodes in an e-class
    Parents,  // Iterating parent e-nodes of an e-class
  };

  Kind kind;
  uint8_t reg;  // Register being populated by this iteration

  // Iteration position - we store current index and end
  // The actual span is looked up from e-graph when needed
  std::size_t current_idx;
  std::size_t end_idx;

  // For parent iteration: the e-class whose parents we're iterating
  EClassId parent_of;

  [[nodiscard]] auto exhausted() const -> bool { return current_idx >= end_idx; }

  void advance() { ++current_idx; }
};

/// VM execution state
struct VMState {
  // E-class registers (result of navigation)
  std::array<EClassId, kMaxRegisters> eclass_regs{};

  // E-node registers (current e-node in iteration)
  std::array<ENodeId, kMaxRegisters> enode_regs{};

  // Variable binding slots
  boost::container::small_vector<EClassId, 8> slots;

  // Which slots are bound (for BindOrCheck)
  // Use std::bitset for O(1) operations with no dynamic allocation
  std::bitset<256> bound;

  // Program counter
  std::size_t pc{0};

  // Iteration state stack (for nested iterations)
  boost::container::small_vector<IterState, 8> iter_stack;

  /// Initialize state for execution with given number of slots
  void reset(std::size_t num_slots) {
    slots.assign(num_slots, EClassId{});
    bound.reset();  // std::bitset has fixed size, just reset all bits
    pc = 0;
    iter_stack.clear();
  }

  /// Bind a slot to an e-class
  void bind(std::size_t slot, EClassId eclass) {
    slots[slot] = eclass;
    bound.set(slot);
  }

  /// Check if slot is bound
  [[nodiscard]] auto is_bound(std::size_t slot) const -> bool { return bound.test(slot); }

  /// Get bound value (must be bound)
  [[nodiscard]] auto get(std::size_t slot) const -> EClassId { return slots[slot]; }

  /// Clear bindings made after a checkpoint (for backtracking within an iteration)
  /// Note: Current design doesn't need this - we rebind on each iteration
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
