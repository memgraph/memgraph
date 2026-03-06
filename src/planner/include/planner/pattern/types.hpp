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

#include <cstdint>
#include <functional>

#include <boost/unordered/unordered_flat_map.hpp>

#include "strong_type/strong_type.hpp"

namespace memgraph::planner::core::pattern {

/// Index into the slots array (pattern variable bindings).
/// Slots hold the e-class IDs matched to pattern variables.
using SlotIdx =
    strong::type<uint8_t, struct SlotIdx_, strong::regular, strong::ordered, strong::formattable, strong::ostreamable>;

/**
 * @brief Pattern variable for binding during e-matching
 *
 * Variables represent wildcard positions in patterns that can match any e-class.
 * Each variable has a unique ID within a pattern; multiple occurrences of the same ID
 * must bind to the same e-class during matching.
 *
 * Example: In pattern Add(?x, ?x), both ?x refer to the same variable ID and
 * will only match when both children are in the same e-class.
 */
struct PatternVar {
  uint8_t id;

  friend auto operator==(PatternVar, PatternVar) -> bool = default;
  friend auto operator<=>(PatternVar, PatternVar) = default;
};

// Boost hash support via ADL
inline std::size_t hash_value(PatternVar const &var) { return std::hash<uint8_t>{}(var.id); }

}  // namespace memgraph::planner::core::pattern

namespace std {
template <>
struct hash<memgraph::planner::core::pattern::PatternVar> {
  std::size_t operator()(memgraph::planner::core::pattern::PatternVar const &var) const noexcept {
    return std::hash<uint8_t>{}(var.id);
  }
};
}  // namespace std

namespace memgraph::planner::core::pattern {

/// Variable to slot index mapping for O(1) binding lookup.
using VarSlotMap = boost::unordered_flat_map<PatternVar, SlotIdx>;

}  // namespace memgraph::planner::core::pattern
