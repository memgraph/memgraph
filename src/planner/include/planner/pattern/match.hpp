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

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include <boost/container/small_vector.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

#include "planner/pattern/pattern.hpp"
#include "utils/logging.hpp"

import memgraph.planner.core.eids;

namespace memgraph::planner::core {

/// Partial match state during recursive pattern matching.
/// Slots are filled as variables bind; on complete match, committed to MatchArena.
/// Includes integrated undo log for O(1) checkpoint/rewind during backtracking.
class PartialMatch {
 public:
  PartialMatch() = default;

  explicit PartialMatch(std::size_t num_slots) : slots_(num_slots, EClassId{0}), bound_(num_slots) {}

  /// Bind a slot to an e-class and record in undo log
  void bind(std::size_t slot, EClassId eclass) {
    slots_[slot] = eclass;
    bound_.set(slot);
    bind_order_.push_back(slot);
  }

  [[nodiscard]] auto is_bound(std::size_t slot) const -> bool {
    DMG_ASSERT(slot < bound_.size(), "requested slot need to be in bounds");
    return bound_.test(slot);
  }

  [[nodiscard]] auto get(std::size_t slot) const -> EClassId {
    DMG_ASSERT(is_bound(slot), "requested slot need to be set already");
    return slots_[slot];
  }

  [[nodiscard]] auto size() const -> std::size_t { return slots_.size(); }

  /// Get current position in undo log (for later rewind)
  [[nodiscard]] auto checkpoint() const -> std::size_t { return bind_order_.size(); }

  /// Rewind to a previous checkpoint, unbinding all slots bound since then
  void rewind_to(std::size_t target) {
    for (auto slot : std::span{bind_order_}.subspan(target)) {
      bound_.reset(slot);
    }
    bind_order_.resize(target);
  }

  /// Reset for reuse with a new pattern (potentially different number of slots)
  void reset(std::size_t num_slots) {
    slots_.resize(num_slots);
    bound_.resize(num_slots);
    bound_.reset();
    bind_order_.clear();
  }

  /// Clear all bindings but keep slot capacity
  void clear() {
    bound_.reset();
    bind_order_.clear();
  }

 private:
  friend struct EMatchContext;
  boost::container::small_vector<EClassId, 8> slots_;
  boost::dynamic_bitset<> bound_;
  boost::container::small_vector<std::size_t, 32> bind_order_;  ///< Undo log for backtracking
};

/// A complete match of a single pattern - offset into MatchArena where bindings are stored.
class PatternMatch {
 public:
  PatternMatch() = default;

  auto operator==(PatternMatch const &other) const -> bool = default;

 private:
  friend class MatchArena;

  explicit PatternMatch(uint32_t offset) : offset_(offset) {}

  uint32_t offset_ = 0;
};

/// Location of a variable in a joined match: (pattern_index, slot_index). Precomputed for O(1) lookup.
class VarLocation {
 public:
  VarLocation() = default;

  VarLocation(uint8_t pattern_idx, uint8_t slot_idx) : pattern_index_(pattern_idx), slot_index_(slot_idx) {}

  auto operator==(VarLocation const &other) const -> bool = default;

 private:
  friend class MatchArena;

  uint8_t pattern_index_ = 0;
  uint8_t slot_index_ = 0;
};

/// Non-owning view over contiguous PatternMatches representing a joined match.
using JoinMatchView = std::span<PatternMatch const>;

/// Append-only pool for pattern match bindings. Bulk-freed via clear().
/// Uses vector instead of deque for better cache locality on append-only workloads.
/// Since we return offsets (not pointers), we don't need deque's iterator stability.
class MatchArena {
 public:
  auto intern(std::span<EClassId const> bindings) -> PatternMatch {
    auto offset = PatternMatch{static_cast<uint32_t>(pool_.size())};
    pool_.insert(pool_.end(), bindings.begin(), bindings.end());
    return offset;
  }

  [[nodiscard]] auto get(JoinMatchView view, VarLocation loc) const -> EClassId {
    return pool_[view[loc.pattern_index_].offset_ + loc.slot_index_];
  }

  [[nodiscard]] auto get(PatternMatch match, std::size_t slot_idx) const -> EClassId {
    return pool_[match.offset_ + slot_idx];
  }

  void clear() { pool_.clear(); }

  [[nodiscard]] auto size() const -> std::size_t { return pool_.size(); }

  /// Reserve capacity to avoid reallocations during matching
  void reserve(std::size_t capacity) { pool_.reserve(capacity); }

 private:
  std::vector<EClassId> pool_;
};

/// Context for e-matching: arena for storing matches, processed set for deduplication.
struct EMatchContext {
  auto arena() -> MatchArena & { return arena_; }

  void clear() {
    arena_.clear();
    processed_.clear();
  }

  /// Clear processed set but keep arena (for multi-pattern rules).
  void clear_temporaries() { processed_.clear(); }

  auto processed() -> boost::unordered_flat_set<EClassId> & { return processed_; }

  /// Prepare context for matching a pattern with given number of variable slots.
  void prepare_for_pattern(std::size_t /*num_slots*/) { processed_.clear(); }

 private:
  MatchArena arena_;
  boost::unordered_flat_set<EClassId> processed_;
};

/// A complete match from a rewrite rule - O(1) variable binding lookup via operator[].
///
/// IMPORTANT: operator[] returns e-class IDs that were canonical at match creation time.
/// If merges occurred since (e.g., from earlier matches in the same apply phase),
/// these IDs may be stale. Use ctx.find(match[var]) to get the current canonical ID.
class Match {
 public:
  Match(JoinMatchView view, boost::unordered_flat_map<PatternVar, VarLocation> const &var_locations,
        MatchArena const &arena)
      : view_(view), var_locations_(&var_locations), arena_(&arena) {}

  [[nodiscard]] auto operator[](PatternVar var) const -> EClassId {
    auto it = var_locations_->find(var);
    DMG_ASSERT(it != var_locations_->end(), "Match::operator[]: variable {} not found", var.id);
    return arena_->get(view_, it->second);
  }

 private:
  JoinMatchView view_;
  boost::unordered_flat_map<PatternVar, VarLocation> const *var_locations_;
  MatchArena const *arena_;
};

}  // namespace memgraph::planner::core
