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

#include <algorithm>

namespace memgraph::storage {

ConstraintVerificationInfo::ConstraintVerificationInfo() = default;
ConstraintVerificationInfo::~ConstraintVerificationInfo() = default;
ConstraintVerificationInfo::ConstraintVerificationInfo(ConstraintVerificationInfo &&) noexcept = default;
ConstraintVerificationInfo &ConstraintVerificationInfo::operator=(ConstraintVerificationInfo &&) noexcept = default;

void ConstraintVerificationInfo::AddedLabel(Vertex const *vertex) { added_labels_.insert(vertex); }

void ConstraintVerificationInfo::AddedProperty(Vertex const *vertex) { added_properties_.insert(vertex); }

void ConstraintVerificationInfo::RemovedProperty(Vertex const *vertex) { removed_properties_.insert(vertex); }

auto ConstraintVerificationInfo::GetVerticesForUniqueConstraintChecking() const -> std::unordered_set<Vertex const *> {
  std::unordered_set<Vertex const *> updated_vertices;

  updated_vertices.insert(added_labels_.begin(), added_labels_.end());
  updated_vertices.insert(added_properties_.begin(), added_properties_.end());

  return updated_vertices;
}

auto ConstraintVerificationInfo::GetVerticesForExistenceConstraintChecking() const
    -> std::unordered_set<Vertex const *> {
  std::unordered_set<Vertex const *> updated_vertices;

  updated_vertices.insert(added_labels_.begin(), added_labels_.end());
  updated_vertices.insert(removed_properties_.begin(), removed_properties_.end());

  return updated_vertices;
}

bool ConstraintVerificationInfo::NeedsUniqueConstraintVerification() const {
  return !added_labels_.empty() || !added_properties_.empty();
}
bool ConstraintVerificationInfo::NeedsExistenceConstraintVerification() const {
  return !added_labels_.empty() || !removed_properties_.empty();
}
}  // namespace memgraph::storage
