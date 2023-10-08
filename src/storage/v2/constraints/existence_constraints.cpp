// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/constraints/existence_constraints.hpp"
#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/mvcc.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage {

bool ExistenceConstraints::ConstraintExists(LabelId label, PropertyId property) const {
  return utils::Contains(constraints_, std::make_pair(label, property));
}

void ExistenceConstraints::InsertConstraint(LabelId label, PropertyId property) {
  if (ConstraintExists(label, property)) {
    return;
  }
  constraints_.emplace_back(label, property);
}

bool ExistenceConstraints::DropConstraint(LabelId label, PropertyId property) {
  auto it = std::find(constraints_.begin(), constraints_.end(), std::make_pair(label, property));
  if (it == constraints_.end()) {
    return false;
  }
  constraints_.erase(it);
  return true;
}

std::vector<std::pair<LabelId, PropertyId>> ExistenceConstraints::ListConstraints() const { return constraints_; }

[[nodiscard]] std::optional<ConstraintViolation> ExistenceConstraints::Validate(const Vertex &vertex) {
  for (const auto &[label, property] : constraints_) {
    if (auto violation = ValidateVertexOnConstraint(vertex, label, property); violation.has_value()) {
      return violation;
    }
  }
  return std::nullopt;
}

void ExistenceConstraints::LoadExistenceConstraints(const std::vector<std::string> &keys) {
  for (const auto &key : keys) {
    const std::vector<std::string> parts = utils::Split(key, ",");
    constraints_.emplace_back(LabelId::FromString(parts[0]), PropertyId::FromString(parts[1]));
  }
}

}  // namespace memgraph::storage
