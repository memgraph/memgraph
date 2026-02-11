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

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

#include "planner/pattern/pattern.hpp"
#include "utils/logging.hpp"

import memgraph.planner.core.eids;

namespace memgraph::planner::core {

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

/// Slot index for variable binding lookup. Precomputed for O(1) lookup.
using SlotIndex = uint8_t;

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

  /// Get all bindings for a match as a span
  [[nodiscard]] auto bindings(PatternMatch match, std::size_t num_slots) const -> std::span<EClassId const> {
    return std::span{pool_}.subspan(match.offset_, num_slots);
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

  [[nodiscard]] auto arena() const -> MatchArena const & { return arena_; }

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

/// Variable to slot index mapping for O(1) binding lookup.
using VarSlotMap = boost::unordered_flat_map<PatternVar, SlotIndex>;

/// A complete match from a rewrite rule - O(1) variable binding lookup via operator[].
///
/// IMPORTANT: operator[] returns e-class IDs that were canonical at match creation time.
/// If merges occurred since (e.g., from earlier matches in the same apply phase),
/// these IDs may be stale. Use ctx.find(match[var]) to get the current canonical ID.
class Match {
 public:
  Match(std::span<EClassId const> bindings, VarSlotMap const &var_slots)
      : bindings_(bindings), var_slots_(&var_slots) {}

  [[nodiscard]] auto operator[](PatternVar var) const -> EClassId {
    auto it = var_slots_->find(var);
    DMG_ASSERT(it != var_slots_->end(), "Match::operator[]: variable {} not found", var.id);
    return bindings_[it->second];
  }

 private:
  std::span<EClassId const> bindings_;
  VarSlotMap const *var_slots_;
};

/// Context for resolving variable bindings from pattern matches.
/// Bundles the var->slot map and arena needed to create Match objects.
class MatchBindings {
 public:
  MatchBindings(VarSlotMap const &var_slots, MatchArena const &arena)
      : var_slots_(&var_slots), arena_(&arena), num_slots_(var_slots.size()) {}

  /// Create a Match from a PatternMatch
  [[nodiscard]] auto match(PatternMatch pattern_match) const -> Match {
    return Match(arena_->bindings(pattern_match, num_slots_), *var_slots_);
  }

 private:
  VarSlotMap const *var_slots_;
  MatchArena const *arena_;
  std::size_t num_slots_;
};

/// Reusable buffers for pattern matching
struct MatcherContext {
  EMatchContext match_ctx;
  std::vector<PatternMatch> match_buffer;

  void clear() {
    match_ctx.clear();
    match_buffer.clear();
  }

  [[nodiscard]] auto arena() const -> MatchArena const & { return match_ctx.arena(); }
};

}  // namespace memgraph::planner::core
