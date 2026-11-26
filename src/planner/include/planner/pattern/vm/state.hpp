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

#include <algorithm>
#include <optional>
#include <span>
#include <vector>

#include <boost/container/small_vector.hpp>
#include <boost/container_hash/hash.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

import memgraph.planner.core.eids;

#include <cassert>

#include "planner/pattern/match_storage.hpp"
#include "planner/pattern/vm/types.hpp"

namespace memgraph::planner::core::pattern::vm {

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

/// Set-based iterator (for boost::unordered_flat_set iteration)
/// Default-constructed (it == end) = inactive/exhausted
template <typename Set>
struct SetIter {
  using iterator = typename Set::const_iterator;
  using value_type = typename Set::value_type;

  SetIter() = default;

  explicit SetIter(Set const &set) : it(set.begin()), end(set.end()) {}

  [[nodiscard]] auto exhausted() const -> bool { return it == end; }

  [[nodiscard]] auto remaining() const -> std::size_t { return static_cast<std::size_t>(std::distance(it, end)); }

  [[nodiscard]] auto current() const -> value_type { return *it; }

  void advance() { ++it; }

 private:
  iterator it{};
  iterator end{};
};

/// Iterating parent e-nodes
using ParentsIter = SetIter<boost::unordered_flat_set<ENodeId>>;

/// Iterating e-classes by symbol
using SymbolEClassesIter = SetIter<boost::unordered_flat_set<EClassId>>;

/// Open-addressing hash set for deduplication (much better cache locality than std::unordered_set)
using FastEClassSet = boost::unordered_flat_set<EClassId>;

/// Configuration for VMState reset
struct VMStateConfig {
  std::size_t num_eclass_regs;
  std::size_t num_enode_regs;
  std::span<SlotIdx const> binding_order;  // size == num_slots
  std::span<uint8_t const> slot_to_order;  // size == num_slots
};

/// Hashes and compares interned matches by their bindings, which live in the
/// arena: the set of yielded matches stores only offsets.
struct InternedTuple {
  MatchArena const *arena = nullptr;
  std::size_t width = 0;

  auto operator()(PatternMatch match) const -> std::size_t {
    auto const tuple = arena->bindings(match, width);
    return boost::hash_range(tuple.begin(), tuple.end());
  }

  auto operator()(PatternMatch a, PatternMatch b) const -> bool {
    return std::ranges::equal(arena->bindings(a, width), arena->bindings(b, width));
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

  /// The matches yielded so far in this execution. One tuple can be reached
  /// along several paths (different e-nodes, different wildcard or root
  /// choices), so a tuple is emitted only the first time it is reached.
  boost::unordered_flat_set<PatternMatch, InternedTuple, InternedTuple> yielded;

  // Program counter
  std::size_t pc{0};

  // Iteration state: separate homogeneous vectors indexed by register
  // Each type uses exhausted state as inactive indicator (no explicit deactivation needed)
  // The compiler statically determines which type each register uses
  // When an iteration exhausts, its state is naturally inert; fresh iterations overwrite
  std::vector<ENodesIter> enodes_iters;                   // E-node iterations (span-based)
  std::vector<ParentsIter> parents_iters;                 // Parent iterations (set-based)
  std::vector<AllEClassesIter> eclasses_iters;            // All e-classes iterations (span-based)
  std::vector<SymbolEClassesIter> symbol_eclasses_iters;  // Symbol-filtered e-class iterations (set-based)

  /// Initialize state for execution
  void reset(VMStateConfig const &cfg);

  /// Start an e-node iteration on a register (uses span)
  void start_enode_iter(uint8_t reg, std::span<ENodeId const> nodes) { enodes_iters[reg] = ENodesIter{nodes}; }

  /// Start a parent iteration on a register
  void start_parent_iter(uint8_t reg, boost::unordered_flat_set<ENodeId> const &parents) {
    parents_iters[reg] = ParentsIter{parents};
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

  /// Start a symbol-filtered e-classes iteration on a register
  /// @pre set is non-empty (caller must check before calling)
  void start_symbol_eclasses_iter(uint8_t reg, boost::unordered_flat_set<EClassId> const &set) {
    assert(!set.empty());
    symbol_eclasses_iters[reg] = SymbolEClassesIter{set};
  }

  /// Get symbol-filtered e-classes iterator for a register
  [[nodiscard]] auto get_symbol_eclasses_iter(uint8_t reg) -> SymbolEClassesIter & {
    return symbol_eclasses_iters[reg];
  }

  [[nodiscard]] auto get_symbol_eclasses_iter(uint8_t reg) const -> SymbolEClassesIter const & {
    return symbol_eclasses_iters[reg];
  }

  /// Binds a slot. Always succeeds: whether a value was explored before
  /// depends on every enclosing loop, not only on the slots bound so far, so
  /// pruning on it loses matches.
  [[nodiscard]] auto try_bind(std::size_t slot, EClassId eclass) -> bool {
    slots[slot] = eclass;
    return true;
  }

  /// Kept so that compiled MarkSeen instructions stay valid; does nothing.
  void mark_seen(std::size_t /*slot*/) {}

  /// Interns the current slots into `arena` and answers the match, unless
  /// this execution already yielded the same tuple: then nothing is kept.
  [[nodiscard]] auto yield_into(MatchArena &arena) -> std::optional<PatternMatch> {
    auto const &key = yielded.hash_function();
    if (key.arena != &arena || key.width != slots.size()) {
      auto const fresh = InternedTuple{.arena = &arena, .width = slots.size()};
      yielded = decltype(yielded)(0, fresh, fresh);
    }
    auto const match = arena.intern(slots);
    if (yielded.insert(match).second) return match;
    arena.discard_last(slots.size());
    return std::nullopt;
  }

  /// Get bound value
  [[nodiscard]] auto get(std::size_t slot) const -> EClassId { return slots[slot]; }
};

inline void VMState::reset(VMStateConfig const &cfg) {
  assert(cfg.binding_order.size() == cfg.slot_to_order.size());
  auto const num_slots = cfg.binding_order.size();
  slots.assign(num_slots, EClassId{});
  pc = 0;

  // Resize register arrays if needed
  if (eclass_regs.size() < cfg.num_eclass_regs) {
    eclass_regs.resize(cfg.num_eclass_regs);
    eclasses_iters.resize(cfg.num_eclass_regs);
    symbol_eclasses_iters.resize(cfg.num_eclass_regs);
  }
  if (enode_regs.size() < cfg.num_enode_regs) {
    enode_regs.resize(cfg.num_enode_regs);
    enodes_iters.resize(cfg.num_enode_regs);
    parents_iters.resize(cfg.num_enode_regs);
  }

  yielded.clear();
}

}  // namespace memgraph::planner::core::pattern::vm
