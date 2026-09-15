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

#include <optional>
#include <string>
#include <string_view>

#include "storage/v2/property_constants.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/bound.hpp"

namespace memgraph::storage {

auto UpperBoundForType(PropertyValueType type) -> std::optional<utils::Bound<PropertyValue>>;

auto LowerBoundForType(PropertyValueType type) -> std::optional<utils::Bound<PropertyValue>>;

/// The stretch of the stored order a comparison against `value` can answer over.
///
/// A range reads an index by fencing it to this stretch, so it has to hold every value the
/// comparison answers for and nothing else. That is the value's own type, except for a date, a
/// local time, a local date time and a duration: those four share one stored type, ordered by
/// which of the four before anything else, and no comparison places one of them against another.
/// Each is therefore its own stretch, and a range over one stops where that one ends.
auto LowerBoundComparableWith(PropertyValue const &value) -> std::optional<utils::Bound<PropertyValue>>;

auto UpperBoundComparableWith(PropertyValue const &value) -> std::optional<utils::Bound<PropertyValue>>;

/// Whether an ordered comparison between the two answers, rather than answering Null.
///
/// The type-level question with the four temporal kinds told apart, for the same reason.
inline bool AreComparable(PropertyValue const &a, PropertyValue const &b) {
  if (!AreComparableTypes(a.type(), b.type())) return false;
  if (a.type() != PropertyValueType::TemporalData) return true;
  return a.ValueTemporalData().type == b.ValueTemporalData().type;
}

/// Compute the smallest string that is lexicographically greater than every
/// string with the given prefix.  Returns std::nullopt when no tighter bound
/// exists (empty prefix or all-0xFF bytes).
auto PrefixSuccessor(std::string_view prefix) -> std::optional<std::string>;

inline bool IsValueIncludedByLowerBound(const PropertyValue &value,
                                        std::optional<utils::Bound<PropertyValue>> const &bound) {
  if (!bound) [[unlikely]]
    return true;
  auto lb_cmp_res = value <=> bound->value();
  return is_gt(lb_cmp_res) || (bound->IsInclusive() && is_eq(lb_cmp_res));
}

inline bool IsValueIncludedByUpperBound(const PropertyValue &value,
                                        std::optional<utils::Bound<PropertyValue>> const &bound) {
  if (!bound) [[unlikely]]
    return true;
  auto ub_cmp_res = value <=> bound->value();
  return is_lt(ub_cmp_res) || (bound->IsInclusive() && is_eq(ub_cmp_res));
}

}  // namespace memgraph::storage
