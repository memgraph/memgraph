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

#include <concepts>
#include <cstdint>
#include <ranges>
#include <vector>

#include <boost/unordered/unordered_flat_map.hpp>

#include "query/plan_v2/resolve/variable_set.hpp"
#include "utils/logging.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::query::plan::v2 {

/// Dense numbering of the Symbol e-classes participating in one extraction.
class VariableIndex {
 public:
  /// Assign a bit position to `eclass` if not already present.  Returns the
  /// assigned bit.  Called once per Symbol e-class during the pre-pass.
  auto assign(planner::core::EClassId eclass) -> uint16_t {
    auto [it, inserted] = by_eclass_.try_emplace(eclass, static_cast<uint16_t>(by_bit_.size()));
    if (inserted) by_bit_.push_back(eclass);
    return it->second;
  }

  /// Bit position for `eclass`.  Caller must have called `assign` previously;
  /// hitting an unknown EClassId is a planner bug (pre-pass must enumerate
  /// every Symbol e-class that can flow into a VariableSet).
  [[nodiscard]] auto bit_of(planner::core::EClassId eclass) const -> uint16_t {
    auto const it = by_eclass_.find(eclass);
    DMG_ASSERT(it != by_eclass_.end(), "VariableIndex: EClassId not registered (planner bug: pre-pass missed it)");
    return it->second;
  }

  /// EClassId for `bit`.  Inverse of `bit_of`.
  [[nodiscard]] auto eclass_of(uint16_t bit) const -> planner::core::EClassId {
    DMG_ASSERT(bit < by_bit_.size(), "VariableIndex: bit out of range");
    return by_bit_[bit];
  }

  /// The singleton set {eclass}, translated to bit-space.
  [[nodiscard]] auto to_variable_set(planner::core::EClassId eclass) const -> VariableSet {
    VariableSet out;
    out.set(bit_of(eclass));
    return out;
  }

  /// The union of the bits of every eclass in `eclasses`.
  template <std::ranges::input_range R>
    requires std::convertible_to<std::ranges::range_value_t<R>, planner::core::EClassId>
  [[nodiscard]] auto to_variable_set(R const &eclasses) const -> VariableSet {
    VariableSet out;
    for (auto e : eclasses) out.set(bit_of(e));
    return out;
  }

  [[nodiscard]] auto size() const noexcept -> std::size_t { return by_bit_.size(); }

 private:
  boost::unordered_flat_map<planner::core::EClassId, uint16_t> by_eclass_;
  std::vector<planner::core::EClassId> by_bit_;
};

}  // namespace memgraph::query::plan::v2
