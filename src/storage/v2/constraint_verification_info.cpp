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

#include "storage/v2/constraint_verification_info.hpp"

namespace memgraph::storage {

ConstraintVerificationInfo::ConstraintVerificationInfo() = default;
ConstraintVerificationInfo::~ConstraintVerificationInfo() = default;
ConstraintVerificationInfo::ConstraintVerificationInfo(ConstraintVerificationInfo &&) noexcept = default;
ConstraintVerificationInfo &ConstraintVerificationInfo::operator=(ConstraintVerificationInfo &&) noexcept = default;

auto ConstraintVerificationInfo::GetAddedLabels(Vertex const *vertex) const -> std::vector<LabelId> {
  if (!added_labels_.contains(vertex)) {
    return {};
  }

  return added_labels_.at(vertex);
}

void ConstraintVerificationInfo::AddLabel(Vertex const *vertex, LabelId label) {
  if (!added_labels_.contains(vertex)) {
    added_labels_[vertex] = {};
  }

  added_labels_[vertex].push_back(label);
}

auto ConstraintVerificationInfo::GetAddedProperties(Vertex const *vertex) const -> std::vector<PropertyId> {
  if (!added_properties_.contains(vertex)) {
    return {};
  }

  return added_properties_.at(vertex);
}

void ConstraintVerificationInfo::AddProperty(Vertex const *vertex, PropertyId property) {
  if (!added_properties_.contains(vertex)) {
    added_properties_[vertex] = {};
  }

  added_properties_[vertex].push_back(property);
}
}  // namespace memgraph::storage
