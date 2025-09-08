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

#include "storage/v2/constraints/type_constraints.hpp"

#include <storage/v2/snapshot_observer_info.hpp>
#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/algorithm.hpp"

#include <optional>
#include <set>

namespace memgraph::storage {

namespace {

TypeConstraintKind PropertyValueToTypeConstraintKind(const PropertyValue &property) {
  switch (property.type()) {
    case PropertyValueType::String:
      return TypeConstraintKind::STRING;
    case PropertyValueType::Bool:
      return TypeConstraintKind::BOOLEAN;
    case PropertyValueType::Int:
      return TypeConstraintKind::INTEGER;
    case PropertyValueType::Double:
      return TypeConstraintKind::FLOAT;
    case PropertyValueType::List:
      return TypeConstraintKind::LIST;
    case PropertyValueType::Map:
      return TypeConstraintKind::MAP;
    case PropertyValueType::TemporalData: {
      auto const temporal = property.ValueTemporalData();
      switch (temporal.type) {
        case TemporalType::Date:
          return TypeConstraintKind::DATE;
        case TemporalType::LocalTime:
          return TypeConstraintKind::LOCALTIME;
        case TemporalType::LocalDateTime:
          return TypeConstraintKind::LOCALDATETIME;
        case TemporalType::Duration:
          return TypeConstraintKind::DURATION;
      }
    }
    case PropertyValueType::ZonedTemporalData:
      return TypeConstraintKind::ZONEDDATETIME;
    case PropertyValueType::Enum:
      return TypeConstraintKind::ENUM;
    case PropertyValueType::Point2d:
    case PropertyValueType::Point3d:
      return TypeConstraintKind::POINT;
    case PropertyValueType::Null:
      MG_ASSERT(false, "Unexpected conversion from PropertyValueType::Null to TypeConstraint::Type");
  }
}

}  // namespace

[[nodiscard]] std::optional<ConstraintViolation> TypeConstraints::Validate(const Vertex &vertex) const {
  if (constraints_.empty()) return std::nullopt;

  auto validator = TypeConstraintsValidator{};
  for (auto label : vertex.labels) {
    auto it = l2p_constraints_.find(label);
    if (it == l2p_constraints_.cend()) continue;
    validator.add(label, it->second);
  }

  if (validator.empty()) return std::nullopt;

  auto violation = vertex.properties.PropertiesMatchTypes(validator);
  if (!violation) return std::nullopt;

  auto const &[prop_id, label, kind] = *violation;
  return ConstraintViolation{ConstraintViolation::Type::TYPE, label, kind, std::set{prop_id}};
}

[[nodiscard]] std::optional<ConstraintViolation> TypeConstraints::Validate(const Vertex &vertex, PropertyId property_id,
                                                                           const PropertyValue &property_value) const {
  for (auto const label : vertex.labels) {
    auto constraint_type_it = constraints_.find({label, property_id});
    if (constraint_type_it == constraints_.end()) continue;

    auto constraint_type = constraint_type_it->second;

    if (property_value.type() == PropertyValueType::TemporalData) {
      // fine grain (subtype exact check)
      if (TemporalMatch(property_value.ValueTemporalData().type, constraint_type)) continue;
    } else {
      // coarse grain (broad type class)
      if (PropertyValueToTypeConstraintKind(property_value) == constraint_type) continue;
    }

    return ConstraintViolation{
        ConstraintViolation::Type::TYPE, {label}, constraint_type, std::set<PropertyId>{property_id}};
  }

  return std::nullopt;
}

[[nodiscard]] std::optional<ConstraintViolation> TypeConstraints::Validate(const Vertex &vertex, LabelId label) const {
  auto validator = TypeConstraintsValidator{};
  auto it = l2p_constraints_.find(label);
  if (it == l2p_constraints_.end()) return std::nullopt;
  validator.add(label, it->second);

  auto violation = vertex.properties.PropertiesMatchTypes(validator);
  if (!violation) return std::nullopt;

  auto const &[prop_id, _, kind] = *violation;
  return ConstraintViolation{ConstraintViolation::Type::TYPE, label, kind, std::set{prop_id}};
}

[[nodiscard]] std::optional<ConstraintViolation> TypeConstraints::ValidateVertices(
    utils::SkipList<Vertex>::Accessor vertices, std::optional<SnapshotObserverInfo> const &snapshot_info) const {
  for (auto const &vertex : vertices) {
    if (auto violation = Validate(vertex); violation.has_value()) {
      return violation;
    }
    if (snapshot_info) {
      snapshot_info->Update(UpdateType::VERTICES);
    }
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<ConstraintViolation> TypeConstraints::ValidateVerticesOnConstraint(
    utils::SkipList<Vertex>::Accessor vertices, LabelId label, PropertyId property, TypeConstraintKind type) {
  auto validator = TypeConstraintsValidator{};
  auto constraint = absl::flat_hash_map<PropertyId, TypeConstraintKind>{{property, type}};
  validator.add(label, constraint);

  for (auto const &vertex : vertices) {
    if (vertex.deleted) continue;
    if (!utils::Contains(vertex.labels, label)) continue;

    auto violation = vertex.properties.PropertiesMatchTypes(validator);
    if (!violation) continue;

    return ConstraintViolation{ConstraintViolation::Type::TYPE, label, type, std::set{property}};
  }
  return std::nullopt;
}

bool TypeConstraints::empty() const { return constraints_.empty(); }

bool TypeConstraints::ConstraintExists(LabelId label, PropertyId property) const {
  return constraints_.contains({label, property});
}

bool TypeConstraints::InsertConstraint(LabelId label, PropertyId property, TypeConstraintKind type) {
  if (ConstraintExists(label, property)) {
    return false;
  }
  constraints_.emplace(std::make_pair(label, property), type);

  // maintain l2p_constraints_
  {
    auto it = l2p_constraints_.find(label);
    if (it != l2p_constraints_.end()) {
      it->second.emplace(property, type);
    } else {
      l2p_constraints_.emplace(label, absl::flat_hash_map<PropertyId, TypeConstraintKind>{{property, type}});
    }
  }
  return true;
}

bool TypeConstraints::DropConstraint(LabelId label, PropertyId property, TypeConstraintKind type) {
  // process constraints_
  {
    auto it = constraints_.find({label, property});
    if (it == constraints_.end()) {
      return false;
    }
    if (it->second != type) {
      return false;
    }
    constraints_.erase(it);
  }

  // maintain l2p_constraints_
  {
    auto it = l2p_constraints_.find(label);
    if (it == l2p_constraints_.end()) {
      DMG_ASSERT("logic bug, l2p_constraints_ not matching constraints_");
    }

    it->second.erase(property);
    if (it->second.empty()) {
      l2p_constraints_.erase(it);
    }
  }

  return true;
}

std::vector<std::tuple<LabelId, PropertyId, TypeConstraintKind>> TypeConstraints::ListConstraints() const {
  std::vector<std::tuple<LabelId, PropertyId, TypeConstraintKind>> constraints;
  constraints.reserve(constraints_.size());
  for (const auto &[label_props, type] : constraints_) {
    constraints.emplace_back(label_props.first, label_props.second, type);
  }
  // NOTE: sort is needed here to ensure DUMP DATABASE; and snapshot has a stable ordering
  std::ranges::sort(constraints);
  return constraints;
}

void TypeConstraints::DropGraphClearConstraints() {
  constraints_.clear();
  l2p_constraints_.clear();
}

}  // namespace memgraph::storage
