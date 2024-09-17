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
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/algorithm.hpp"

namespace memgraph::storage {

[[nodiscard]] std::optional<ConstraintViolation> TypeConstraints::ValidateVertexOnConstraint(const Vertex &vertex,
                                                                                             LabelId label,
                                                                                             PropertyId property,
                                                                                             TypeConstraintsType type) {
  if (vertex.deleted || !utils::Contains(vertex.labels, label)) return std::nullopt;

  auto prop_value = vertex.properties.GetProperty(property);
  if (prop_value.IsNull()) {
    return std::nullopt;
  }

  if (PropertyValueToTypeConstraintType(prop_value) != type) {
    return ConstraintViolation{ConstraintViolation::Type::TYPE, label, type, std::set<PropertyId>{property}};
  }

  return std::nullopt;
}

[[nodiscard]] std::optional<ConstraintViolation> TypeConstraints::Validate(const Vertex &vertex) const {
  if (vertex.deleted) return std::nullopt;

  for (auto const &[property_id, property_value] : vertex.properties.Properties()) {
    for (auto label : vertex.labels) {
      auto constraint_type_it = constraints_.find({label, property_id});
      if (constraint_type_it == constraints_.end()) continue;

      auto constraint_type = constraint_type_it->second;
      if (PropertyValueToTypeConstraintType(property_value) != constraint_type) {
        return ConstraintViolation{ConstraintViolation::Type::TYPE, label, constraint_type,
                                   std::set<PropertyId>{property_id}};
      }
    }
  }

  return std::nullopt;
}

[[nodiscard]] std::optional<ConstraintViolation> TypeConstraints::Validate(const Vertex &vertex, PropertyId property_id,
                                                                           const PropertyValue &property_value) const {
  if (vertex.deleted) return std::nullopt;

  for (auto const label : vertex.labels) {
    auto constraint_type_it = constraints_.find({label, property_id});
    if (constraint_type_it == constraints_.end()) continue;

    auto constraint_type = constraint_type_it->second;
    if (PropertyValueToTypeConstraintType(property_value) != constraint_type) {
      return ConstraintViolation{ConstraintViolation::Type::TYPE, label, constraint_type,
                                 std::set<PropertyId>{property_id}};
    }
  }

  return std::nullopt;
}

[[nodiscard]] std::optional<ConstraintViolation> TypeConstraints::Validate(const Vertex &vertex,
                                                                           PropertyId property) const {
  if (vertex.deleted) return std::nullopt;

  for (auto const label : vertex.labels) {
    auto constraint_type_it = constraints_.find({label, property});
    if (constraint_type_it == constraints_.end()) continue;

    auto constraint_type = constraint_type_it->second;
    auto prop = vertex.properties.GetPropertyOfTypes(
        property, std::array{TypeConstraintsTypeToPropertyStoreType(constraint_type)});

    // Property must exist because it was just added!
    if (PropertyValueToTypeConstraintType(*prop) != constraint_type) {
      return ConstraintViolation{ConstraintViolation::Type::TYPE, label, constraint_type,
                                 std::set<PropertyId>{property}};
    }
  }

  return std::nullopt;
}

[[nodiscard]] std::optional<ConstraintViolation> TypeConstraints::Validate(const Vertex &vertex, LabelId label) const {
  if (vertex.deleted) return std::nullopt;

  for (auto const &[property, property_value] : vertex.properties.Properties()) {
    auto constraint_type_it = constraints_.find({label, property});
    if (constraint_type_it == constraints_.end()) continue;

    auto constraint_type = constraint_type_it->second;
    if (PropertyValueToTypeConstraintType(property_value) != constraint_type) {
      return ConstraintViolation{ConstraintViolation::Type::TYPE, label, constraint_type,
                                 std::set<PropertyId>{property}};
    }
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<ConstraintViolation> TypeConstraints::ValidateVertices(
    utils::SkipList<Vertex>::Accessor vertices) const {
  for (auto const &vertex : vertices) {
    if (auto violation = Validate(vertex); violation.has_value()) {
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

bool TypeConstraints::empty() const { return constraints_.empty(); }

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
