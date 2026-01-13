// Copyright 2026 Memgraph Ltd.
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

#include <optional>
#include <set>
#include <storage/v2/snapshot_observer_info.hpp>
#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"

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
    case PropertyValueType::IntList:
    case PropertyValueType::DoubleList:
    case PropertyValueType::NumericList:
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

[[nodiscard]] std::expected<void, ConstraintViolation> TypeConstraints::Validate(const Vertex &vertex,
                                                                                 ReadLockedPtr const &container) {
  if (container->constraints_.empty()) return {};

  auto validator = TypeConstraintsValidator{};
  for (auto label : vertex.labels) {
    auto it = container->l2p_constraints_.find(label);
    if (it == container->l2p_constraints_.cend()) continue;
    validator.add(label, it->second);
  }

  if (validator.empty()) return {};

  auto violation = vertex.properties.PropertiesMatchTypes(validator);
  if (!violation) return {};

  auto const &[prop_id, label, kind] = *violation;
  return std::unexpected{ConstraintViolation{ConstraintViolation::Type::TYPE, label, kind, std::set{prop_id}}};
}

[[nodiscard]] std::expected<void, ConstraintViolation> TypeConstraints::Validate(
    const Vertex &vertex, PropertyId property_id, const PropertyValue &property_value) const {
  return container_.WithReadLock([&](const auto &container) -> std::expected<void, ConstraintViolation> {
    for (auto const label : vertex.labels) {
      auto constraint_type_it = container.constraints_.find({label, property_id});
      if (constraint_type_it == container.constraints_.end()) continue;

      auto constraint_type = constraint_type_it->second.type;

      if (property_value.type() == PropertyValueType::TemporalData) {
        // fine grain (subtype exact check)
        if (TemporalMatch(property_value.ValueTemporalData().type, constraint_type)) continue;
      } else {
        // coarse grain (broad type class)
        if (PropertyValueToTypeConstraintKind(property_value) == constraint_type) continue;
      }

      return std::unexpected{ConstraintViolation{
          ConstraintViolation::Type::TYPE, {label}, constraint_type, std::set<PropertyId>{property_id}}};
    }
    return {};
  });
}

[[nodiscard]] std::expected<void, ConstraintViolation> TypeConstraints::Validate(const Vertex &vertex,
                                                                                 LabelId label) const {
  return container_.WithReadLock([&](const auto &container) -> std::expected<void, ConstraintViolation> {
    auto validator = TypeConstraintsValidator{};
    auto it = container.l2p_constraints_.find(label);
    if (it == container.l2p_constraints_.end()) return {};
    validator.add(label, it->second);

    auto violation = vertex.properties.PropertiesMatchTypes(validator);
    if (!violation) return {};
    auto const &[prop_id, _, kind] = *violation;
    return std::unexpected{ConstraintViolation{ConstraintViolation::Type::TYPE, label, kind, std::set{prop_id}}};
  });
}

[[nodiscard]] std::expected<void, ConstraintViolation> TypeConstraints::ValidateVertices(
    utils::SkipList<Vertex>::Accessor vertices, std::optional<SnapshotObserverInfo> const &snapshot_info) const {
  auto locked_container = container_.ReadLock();
  for (auto const &vertex : vertices) {
    if (auto validation_result = Validate(vertex, locked_container); !validation_result.has_value()) {
      return std::unexpected{validation_result.error()};
    }
    if (snapshot_info) {
      snapshot_info->Update(UpdateType::VERTICES);
    }
  }
  return {};
}

[[nodiscard]] std::expected<void, ConstraintViolation> TypeConstraints::ValidateVerticesOnConstraint(
    utils::SkipList<Vertex>::Accessor vertices, LabelId label, PropertyId property, TypeConstraintKind type) {
  auto validator = TypeConstraintsValidator{};
  auto constraint = absl::flat_hash_map<PropertyId, TypeConstraintKind>{{property, type}};
  validator.add(label, constraint);

  for (auto const &vertex : vertices) {
    if (vertex.deleted) continue;
    if (!std::ranges::contains(vertex.labels, label)) continue;

    auto violation = vertex.properties.PropertiesMatchTypes(validator);
    if (!violation) continue;

    return std::unexpected{ConstraintViolation{ConstraintViolation::Type::TYPE, label, type, std::set{property}}};
  }
  return {};
}

bool TypeConstraints::empty() const {
  return container_.WithReadLock([](const auto &container) { return container.constraints_.empty(); });
}

bool TypeConstraints::ConstraintExists(LabelId label, PropertyId property) const {
  return container_.WithReadLock([&](const auto &container) {
    return container.constraints_.contains({label, property});
  });
}

bool TypeConstraints::RegisterConstraint(LabelId label, PropertyId property, TypeConstraintKind type) {
  return container_.WithLock([&](auto &container) -> bool {
    auto [it, inserted] = container.constraints_.try_emplace(
        {label, property}, IndividualConstraint{.type = type, .status = ValidationStatus::VALIDATING});
    return inserted;
  });
}

void TypeConstraints::PublishConstraint(LabelId label, PropertyId property, TypeConstraintKind type) {
  container_.WithLock([&](auto &container) {
    auto it = container.constraints_.find({label, property});
    DMG_ASSERT(it != container.constraints_.end(), "Type constraint not found");
    // recovery, immediate publish, no registration
    it->second.status = ValidationStatus::READY;

    // maintain l2p_constraints_
    {
      auto it = container.l2p_constraints_.find(label);
      if (it != container.l2p_constraints_.end()) {
        it->second.emplace(property, type);
      } else {
        container.l2p_constraints_.emplace(label,
                                           absl::flat_hash_map<PropertyId, TypeConstraintKind>{{property, type}});
      }
    }
  });
}

bool TypeConstraints::DropConstraint(LabelId label, PropertyId property, TypeConstraintKind type) {
  return container_.WithLock([&](auto &container) -> bool {
    // process constraints_
    {
      auto it = container.constraints_.find({label, property});
      if (it == container.constraints_.end()) {
        return false;
      }
      // dropping pending -> violation doing creation during create constraint, l2p didn't get created
      // dropping validated -> drop constraint cypher query
      if (it->second.type != type) {
        return false;
      }
      container.constraints_.erase(it);
    }

    // maintain l2p_constraints_
    auto it = container.l2p_constraints_.find(label);
    if (it == container.l2p_constraints_.end()) {
      // possible if violation during constraint creation
      return true;
    }

    it->second.erase(property);
    if (it->second.empty()) {
      container.l2p_constraints_.erase(it);
    }

    return true;
  });
}

std::vector<std::tuple<LabelId, PropertyId, TypeConstraintKind>> TypeConstraints::ListConstraints() const {
  return container_.WithReadLock([](const auto &container) {
    std::vector<std::tuple<LabelId, PropertyId, TypeConstraintKind>> constraints;
    constraints.reserve(container.constraints_.size());
    for (const auto &[label_props, type] : container.constraints_) {
      if (type.status != ValidationStatus::READY) [[unlikely]] {
        continue;
      }
      constraints.emplace_back(label_props.first, label_props.second, type.type);
    }
    // NOTE: sort is needed here to ensure DUMP DATABASE; and snapshot has a stable ordering
    std::ranges::sort(constraints);
    return constraints;
  });
}

void TypeConstraints::DropGraphClearConstraints() {
  container_.WithLock([](auto &container) {
    container.constraints_.clear();
    container.l2p_constraints_.clear();
  });
}

absl::flat_hash_map<PropertyId, TypeConstraintKind> TypeConstraints::GetTypeConstraintsForLabel(LabelId label) const {
  return container_.WithReadLock([&](const auto &container) -> absl::flat_hash_map<PropertyId, TypeConstraintKind> {
    auto it = container.l2p_constraints_.find(label);
    if (it == container.l2p_constraints_.end()) {
      return {};
    }
    return it->second;
  });
}

}  // namespace memgraph::storage
