// Copyright 2024 Memgraph Ltd.
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

#include "storage/v2/id_types.hpp"

#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/property_store_types.hpp"
#include "storage/v2/temporal.hpp"

#include <cstdint>
#include <string>

#include "absl/container/flat_hash_map.h"

namespace memgraph::storage {

struct PropertyStoreConstraintViolation {
  PropertyId property_id;
  LabelId label;
  TypeConstraintKind constraint_;
};

struct PropertyStoreMemberInfo {
  PropertyId prop_id;
  PropertyStoreType type;
  std::optional<TemporalType> temporal_type;
};

struct TypeConstraintsValidator {
  void add(LabelId label, absl::flat_hash_map<PropertyId, TypeConstraintKind> const &constraints) {
    labels.emplace_back(label);
    constraints_.emplace_back(std::cref(constraints));
  }

  bool empty() const { return labels.empty(); }

  auto validate(PropertyStoreMemberInfo const &member_info) const -> std::optional<PropertyStoreConstraintViolation>;

 private:
  std::vector<LabelId> labels;
  std::vector<std::reference_wrapper<absl::flat_hash_map<PropertyId, TypeConstraintKind> const>> constraints_;
};

}  // namespace memgraph::storage
