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

#include "storage/v2/constraints/type_constraints_validator.hpp"

namespace r = std::ranges;
namespace rv = r::views;

namespace memgraph::storage {

auto TypeConstraintsValidator::validate(PropertyStoreMemberInfo const &member_info) const
    -> std::optional<PropertyStoreConstraintViolation> {
  for (auto const &[label, constraint] : rv::zip(labels, constraints_)) {
    auto const &constraint_map = constraint.get();
    auto it = constraint_map.find(member_info.prop_id);
    if (it == constraint_map.end()) continue;
    if (member_info.temporal_type) {
      // fine grain (subtype exact check)
      if (TemporalMatch(*member_info.temporal_type, it->second)) continue;
    } else {
      // coarse grain (broad type class)
      if (TypeConstraintsKindToPropertyStoreType(it->second) == member_info.type) continue;
    }
    return PropertyStoreConstraintViolation{member_info.prop_id, label, it->second};
  }
  return std::nullopt;
}

}  // namespace memgraph::storage
