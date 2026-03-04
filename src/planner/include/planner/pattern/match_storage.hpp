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

import memgraph.planner.core.eids;

namespace memgraph::planner::core::pattern {

/// Slot index for variable binding lookup. Precomputed for O(1) lookup.
using SlotIndex = uint8_t;

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

/// Append-only pool for pattern match bindings. Bulk-freed via clear().
/// Uses vector instead of deque for better cache locality on append-only workloads.
/// Since we return offsets (not pointers), we don't need deque's iterator stability.
class MatchArena {
 public:
  auto intern(std::span<EClassId const> bindings) -> PatternMatch {
    auto const offset = PatternMatch{static_cast<uint32_t>(pool_.size())};
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

}  // namespace memgraph::planner::core::pattern
