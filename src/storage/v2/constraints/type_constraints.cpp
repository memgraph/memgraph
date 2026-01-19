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
#include "storage/v2/storage.hpp"

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

// --- IndividualConstraint implementation ---

TypeConstraints::IndividualConstraint::~IndividualConstraint() {
  if (status.IsReady()) {
    memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveTypeConstraints);
  }
}

// --- ActiveConstraints implementation ---

bool TypeConstraints::ActiveConstraints::ConstraintRegistered(LabelId label, PropertyId property) const {
  return snapshot_->constraints_.contains({label, property});
}

std::vector<std::tuple<LabelId, PropertyId, TypeConstraintKind>> TypeConstraints::ActiveConstraints::ListConstraints(
    uint64_t start_timestamp) const {
  std::vector<std::tuple<LabelId, PropertyId, TypeConstraintKind>> constraints;
  constraints.reserve(snapshot_->constraints_.size());
  for (const auto &[label_props, constraint] : snapshot_->constraints_) {
    if (!constraint->status.IsVisible(start_timestamp)) [[unlikely]] {
      continue;
    }
    constraints.emplace_back(label_props.first, label_props.second, constraint->type);
  }
  std::ranges::sort(constraints);
  return constraints;
}

auto TypeConstraints::GetActiveConstraints() const -> std::unique_ptr<TypeActiveConstraints> {
  return std::make_unique<ActiveConstraints>(GetSnapshot());
}

// --- TypeConstraints methods ---

[[nodiscard]] std::expected<void, ConstraintViolation> TypeConstraints::Validate(const Vertex &vertex,
                                                                                 ContainerPtr const &container) {
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
  auto container = GetSnapshot();
  for (auto const label : vertex.labels) {
    auto constraint_type_it = container->constraints_.find({label, property_id});
    if (constraint_type_it == container->constraints_.end()) continue;

    auto constraint_type = constraint_type_it->second->type;

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
}

[[nodiscard]] std::expected<void, ConstraintViolation> TypeConstraints::Validate(const Vertex &vertex,
                                                                                 LabelId label) const {
  auto container = GetSnapshot();
  auto validator = TypeConstraintsValidator{};
  auto it = container->l2p_constraints_.find(label);
  if (it == container->l2p_constraints_.end()) return {};
  validator.add(label, it->second);

  auto violation = vertex.properties.PropertiesMatchTypes(validator);
  if (!violation) return {};
  auto const &[prop_id, _, kind] = *violation;
  return std::unexpected{ConstraintViolation{ConstraintViolation::Type::TYPE, label, kind, std::set{prop_id}}};
}

[[nodiscard]] std::expected<void, ConstraintViolation> TypeConstraints::ValidateVertices(
    utils::SkipList<Vertex>::Accessor vertices, std::optional<SnapshotObserverInfo> const &snapshot_info) const {
  auto container = GetSnapshot();
  for (auto const &vertex : vertices) {
    if (auto validation_result = Validate(vertex, container); !validation_result.has_value()) {
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
  auto container = GetSnapshot();
  return container->constraints_.empty();
}

bool TypeConstraints::ConstraintRegistered(LabelId label, PropertyId property) const {
  auto container = GetSnapshot();
  return container->constraints_.contains({label, property});
}

auto TypeConstraints::GetIndividualConstraint(LabelId label, PropertyId property) const -> IndividualConstraintPtr {
  return container_.WithReadLock([&](ContainerPtr const &container) -> IndividualConstraintPtr {
    auto it = container->constraints_.find({label, property});
    if (it == container->constraints_.end()) [[unlikely]] {
      return {};
    }
    return it->second;
  });
}

bool TypeConstraints::RegisterConstraint(LabelId label, PropertyId property, TypeConstraintKind type) {
  return container_.WithLock([&](ContainerPtr &container) -> bool {
    // Check if constraint already exists
    if (container->constraints_.contains({label, property})) {
      return false;
    }
    // Copy-on-write: create new container with the new constraint
    auto new_container = std::make_shared<Container>(*container);
    new_container->constraints_.try_emplace(
        {label, property},
        std::make_shared<IndividualConstraint>(type));  // Starts in populating state
    container = std::move(new_container);
    return true;
  });
}

void TypeConstraints::PublishConstraint(LabelId label, PropertyId property, TypeConstraintKind type,
                                        uint64_t commit_timestamp) {
  // Get the individual constraint for in-place status modification
  auto constraint = GetIndividualConstraint(label, property);
  if (!constraint) [[unlikely]] {
    DMG_ASSERT(false, "Type constraint not found during publish");
    return;
  }

  // Update l2p_constraints_ (still needs copy-on-write for the secondary index)
  container_.WithLock([&](ContainerPtr &container) {
    auto new_container = std::make_shared<Container>(*container);

    // maintain l2p_constraints_ (used for fast lookup during validation)
    auto l2p_it = new_container->l2p_constraints_.find(label);
    if (l2p_it != new_container->l2p_constraints_.end()) {
      l2p_it->second.emplace(property, type);
    } else {
      new_container->l2p_constraints_.emplace(label,
                                              absl::flat_hash_map<PropertyId, TypeConstraintKind>{{property, type}});
    }

    container = std::move(new_container);
  });

  // Commit status in-place (shared_ptr allows modification without copy-on-write)
  constraint->status.Commit(commit_timestamp);
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveTypeConstraints);
}

bool TypeConstraints::DropConstraint(LabelId label, PropertyId property, TypeConstraintKind type) {
  return container_.WithLock([&](ContainerPtr &container) -> bool {
    auto it = container->constraints_.find({label, property});
    if (it == container->constraints_.end()) {
      return false;
    }
    if (it->second->type != type) {
      return false;
    }

    // Copy-on-write: create new container without this constraint
    auto new_container = std::make_shared<Container>(*container);
    new_container->constraints_.erase({label, property});

    // Remove from l2p_constraints_
    auto l2p_it = new_container->l2p_constraints_.find(label);
    if (l2p_it != new_container->l2p_constraints_.end()) {
      l2p_it->second.erase(property);
      if (l2p_it->second.empty()) {
        new_container->l2p_constraints_.erase(l2p_it);
      }
    }

    container = std::move(new_container);
    return true;
  });
}

std::vector<std::tuple<LabelId, PropertyId, TypeConstraintKind>> TypeConstraints::ListConstraints(
    uint64_t start_timestamp) const {
  auto container = GetSnapshot();
  std::vector<std::tuple<LabelId, PropertyId, TypeConstraintKind>> constraints;
  constraints.reserve(container->constraints_.size());
  for (const auto &[label_props, constraint] : container->constraints_) {
    if (!constraint->status.IsVisible(start_timestamp)) [[unlikely]] {
      continue;
    }
    constraints.emplace_back(label_props.first, label_props.second, constraint->type);
  }
  // NOTE: sort is needed here to ensure DUMP DATABASE; and snapshot has a stable ordering
  std::ranges::sort(constraints);
  return constraints;
}

std::vector<std::tuple<LabelId, PropertyId, TypeConstraintKind>> TypeConstraints::ListConstraints() const {
  auto container = GetSnapshot();
  // List all ready (committed) constraints - with copy-on-write, dropped constraints are erased
  std::vector<std::tuple<LabelId, PropertyId, TypeConstraintKind>> constraints;
  constraints.reserve(container->constraints_.size());
  for (const auto &[label_props, constraint] : container->constraints_) {
    if (!constraint->status.IsReady()) [[unlikely]] {
      continue;
    }
    constraints.emplace_back(label_props.first, label_props.second, constraint->type);
  }
  // NOTE: sort is needed here to ensure DUMP DATABASE; and snapshot has a stable ordering
  std::ranges::sort(constraints);
  return constraints;
}

void TypeConstraints::DropGraphClearConstraints() {
  container_.WithLock([](ContainerPtr &container) { container = std::make_shared<Container const>(); });
}

void TypeConstraints::AbortPopulating() {
  container_.WithLock([](ContainerPtr &container) {
    // Check if any populating constraints exist
    bool has_populating = false;
    for (const auto &[_, constraint] : container->constraints_) {
      if (constraint->status.IsPopulating()) {
        has_populating = true;
        break;
      }
    }
    if (!has_populating) return;

    // Copy-on-write: create new container without populating constraints
    auto new_container = std::make_shared<Container>();
    for (const auto &[label_props, constraint] : container->constraints_) {
      if (!constraint->status.IsPopulating()) {
        new_container->constraints_.emplace(label_props, constraint);
      }
    }
    // Copy l2p_constraints_ as-is (populating constraints weren't added there)
    new_container->l2p_constraints_ = container->l2p_constraints_;
    container = std::move(new_container);
  });
}

absl::flat_hash_map<PropertyId, TypeConstraintKind> TypeConstraints::GetTypeConstraintsForLabel(LabelId label) const {
  auto container = GetSnapshot();
  auto it = container->l2p_constraints_.find(label);
  if (it == container->l2p_constraints_.end()) {
    return {};
  }
  return it->second;
}

}  // namespace memgraph::storage
