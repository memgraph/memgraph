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

/// The band a stored temporal of one kind occupies.
///
/// One stored type carries all four temporal kinds and orders a value by its
/// kind before its length, so each kind's values sit together. The comparison
/// operators place no pair drawn from two kinds, so a range built from a
/// comparison against one of them reaches that kind's band and no further.
auto LowerBoundForTemporalType(TemporalType type) -> utils::Bound<PropertyValue>;

auto UpperBoundForTemporalType(TemporalType type) -> utils::Bound<PropertyValue>;

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
