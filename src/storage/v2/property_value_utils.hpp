// Copyright 2025 Memgraph Ltd.
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

#include "storage/v2/property_constants.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/bound.hpp"

namespace memgraph::storage {

auto UpperBoundForType(PropertyValueType type) -> std::optional<utils::Bound<PropertyValue>>;

auto LowerBoundForType(PropertyValueType type) -> std::optional<utils::Bound<PropertyValue>>;

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
