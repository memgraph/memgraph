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

#include "type_constraints.hpp"
#include <optional>
#include "storage/v2/constraints/type_constraints_type.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/algorithm.hpp"

namespace memgraph::storage {

[[nodiscard]] std::optional<ConstraintViolation> TypeConstraints::ValidateVertexOnConstraint(const Vertex &vertex,
                                                                                             LabelId label,
                                                                                             PropertyId property,
                                                                                             TypeConstraintsType type) {
  if (vertex.deleted || !utils::Contains(vertex.labels, label)) {
    return std::nullopt;
  }
  auto prop_value = vertex.properties.GetProperty(property);
  if (prop_value.IsNull()) {
    return std::nullopt;
  }

  if (PropertyValueToTypeConstraintType(prop_value) != type) {
    return ConstraintViolation{ConstraintViolation::Type::TYPE, label, type, std::set<PropertyId>{property}};
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<ConstraintViolation> TypeConstraints::Validate(const Vertex &vertex) {
  for (const auto &[label_prop, type] : constraints_) {
    if (auto violation = ValidateVertexOnConstraint(vertex, label_prop.first, label_prop.second, type);
        violation.has_value()) {
      return violation;
    }
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<ConstraintViolation> TypeConstraints::Validate(const Vertex &vertex, PropertyId property) {
  for (const auto &[label_prop, type] : constraints_) {
    if (label_prop.second != property) continue;
    if (auto violation = ValidateVertexOnConstraint(vertex, label_prop.first, property, type); violation.has_value()) {
      return violation;
    }
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<ConstraintViolation> TypeConstraints::Validate(const Vertex &vertex, LabelId label) {
  for (const auto &[label_prop, type] : constraints_) {
    if (label_prop.first != label) continue;
    if (auto violation = ValidateVertexOnConstraint(vertex, label, label_prop.second, type); violation.has_value()) {
      return violation;
    }
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<ConstraintViolation> TypeConstraints::ValidateVerticesOnConstraint(
    utils::SkipList<Vertex>::Accessor vertices, LabelId label, PropertyId property, TypeConstraintsType type) {
  for (auto const &vertex : vertices) {
    if (auto violation = ValidateVertexOnConstraint(vertex, label, property, type); violation.has_value()) {
      return violation;
    }
  }
  return std::nullopt;
}

bool TypeConstraints::HasTypeConstraints() const { return !constraints_.empty(); }

bool TypeConstraints::ConstraintExists(LabelId label, PropertyId property) const {
  return constraints_.contains({label, property});
}

bool TypeConstraints::InsertConstraint(LabelId label, PropertyId property, TypeConstraintsType type) {
  if (ConstraintExists(label, property)) {
    return false;
  }
  constraints_.emplace(std::make_pair(label, property), type);
  return true;
}

bool TypeConstraints::DropConstraint(LabelId label, PropertyId property, TypeConstraintsType type) {
  auto it = constraints_.find({label, property});
  if (it == constraints_.end()) {
    return false;
  }
  if (it->second != type) {
    return false;
  }
  constraints_.erase(it);
  return true;
}

std::vector<std::tuple<LabelId, PropertyId, TypeConstraintsType>> TypeConstraints::ListConstraints() const {
  std::vector<std::tuple<LabelId, PropertyId, TypeConstraintsType>> constraints;
  constraints.reserve(constraints_.size());
  for (const auto &[label_props, type] : constraints_) {
    constraints.emplace_back(label_props.first, label_props.second, type);
  }
  return constraints;
}

void TypeConstraints::DropGraphClearConstraints() { constraints_.clear(); }

}  // namespace memgraph::storage
