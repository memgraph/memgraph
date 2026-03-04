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

#include <cassert>
#include <cstddef>
#include <vector>

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

#include "planner/pattern/match_storage.hpp"
#include "planner/pattern/pattern.hpp"

namespace memgraph::planner::core::pattern {

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
    auto const it = var_slots_->find(var);
    assert(it != var_slots_->end() && "Match::operator[]: variable not found");
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

}  // namespace memgraph::planner::core::pattern
