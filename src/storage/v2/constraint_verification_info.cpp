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

auto ConstraintVerificationInfo::GetAddedLabels(Vertex const *vertex) const -> std::unordered_set<LabelId> {
  if (!added_labels_.contains(vertex)) {
    return {};
  }

  return added_labels_.at(vertex);
}

void ConstraintVerificationInfo::AddLabel(Vertex const *vertex, LabelId label) {
  if (!added_labels_.contains(vertex)) {
    added_labels_[vertex] = {};
  }

  added_labels_[vertex].insert(label);
}

auto ConstraintVerificationInfo::GetAddedProperties(Vertex const *vertex) const -> std::unordered_set<PropertyId> {
  if (!added_properties_.contains(vertex)) {
    return {};
  }

  return added_properties_.at(vertex);
}

void ConstraintVerificationInfo::AddProperty(Vertex const *vertex, PropertyId property) {
  if (!added_properties_.contains(vertex)) {
    added_properties_[vertex] = {};
  }

  added_properties_[vertex].insert(property);
}

auto ConstraintVerificationInfo::GetRemovedProperties(Vertex const *vertex) const -> std::unordered_set<PropertyId> {
  if (!removed_properties_.contains(vertex)) {
    return {};
  }

  return removed_properties_.at(vertex);
}

void ConstraintVerificationInfo::RemoveProperty(Vertex const *vertex, PropertyId property) {
  if (!removed_properties_.contains(vertex)) {
    removed_properties_[vertex] = {};
  }

  removed_properties_[vertex].insert(property);
}

auto ConstraintVerificationInfo::GetVerticesInfoForUniqueConstraintChecking() const
    -> std::unordered_map<Vertex const *, std::pair<std::unordered_set<LabelId>, std::unordered_set<PropertyId>>> {
  using labels_info = std::unordered_set<LabelId>;
  using props_info = std::unordered_set<PropertyId>;
  using constraint_info = std::pair<labels_info, props_info>;

  std::unordered_map<Vertex const *, constraint_info> vertices_info{};

  for (const auto &[k, v] : added_labels_) {
    if (!vertices_info.contains(k)) {
      vertices_info[k] = std::make_pair(labels_info{}, props_info{});
    }
    vertices_info[k].first = v;
  }

  for (const auto &[k, v] : added_properties_) {
    if (!vertices_info.contains(k)) {
      vertices_info[k] = std::make_pair(labels_info{}, props_info{});
    }
    vertices_info[k].second = v;
  }

  return vertices_info;
}

auto ConstraintVerificationInfo::GetVerticesInfoForExistenceConstraintChecking() const
    -> std::unordered_map<Vertex const *, std::pair<std::unordered_set<LabelId>, std::unordered_set<PropertyId>>> {
  using labels_info = std::unordered_set<LabelId>;
  using props_info = std::unordered_set<PropertyId>;
  using constraint_info = std::pair<labels_info, props_info>;

  std::unordered_map<Vertex const *, constraint_info> vertices_info{};

  for (const auto &[k, v] : added_labels_) {
    if (!vertices_info.contains(k)) {
      vertices_info[k] = std::make_pair(labels_info{}, props_info{});
    }
    vertices_info[k].first = v;
  }

  for (const auto &[k, v] : removed_properties_) {
    if (!vertices_info.contains(k)) {
      vertices_info[k] = std::make_pair(labels_info{}, props_info{});
    }
    vertices_info[k].second = v;
  }

  return vertices_info;
}

bool ConstraintVerificationInfo::NeedsUniqueConstraintVerification() const {
  return !added_labels_.empty() || !added_properties_.empty();
}
bool ConstraintVerificationInfo::NeedsExistenceConstraintVerification() const {
  return !added_labels_.empty() || !removed_properties_.empty();
}
}  // namespace memgraph::storage
