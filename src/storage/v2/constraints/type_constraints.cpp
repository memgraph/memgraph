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
#include "storage/v2/schema_info_types.hpp"
#include "storage/v2/storage.hpp"

namespace memgraph::storage {

namespace {

/// Validate all type constraints for a vertex against a container snapshot.
/// Used internally by ActiveConstraints::Validate and TypeConstraints::ValidateAllVertices.
[[nodiscard]] std::expected<void, ConstraintViolation> ValidateVertex(const Vertex &vertex,
                                                                      TypeConstraints::ContainerPtr const &container) {
  if (container->constraints_.empty()) return {};

  auto validator = TypeConstraintsValidator{};
  vertex.labels.for_each([&](uint32_t id) {
    auto label = LabelId::FromUint(id);
    auto it = container->l2p_constraints_.find(label);
    if (it == container->l2p_constraints_.cend()) return;
    validator.add(label, it->second);
  });

  if (validator.empty()) return {};

  auto violation = vertex.properties.PropertiesMatchTypes(validator);
  if (!violation) return {};

  auto const &[prop_id, label, kind] = *violation;
  return std::unexpected{ConstraintViolation{ConstraintViolation::Type::TYPE, label, kind, std::set{prop_id}}};
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
  return container_->constraints_.contains({label, property});
}

std::vector<std::tuple<LabelId, PropertyId, TypeConstraintKind>> TypeConstraints::ActiveConstraints::ListConstraints(
    uint64_t start_timestamp) const {
  std::vector<std::tuple<LabelId, PropertyId, TypeConstraintKind>> constraints;
  constraints.reserve(container_->constraints_.size());
  for (const auto &[label_props, constraint] : container_->constraints_) {
    if (!constraint->status.IsVisible(start_timestamp)) [[unlikely]] {
      continue;
    }
    constraints.emplace_back(label_props.first, label_props.second, constraint->type);
  }
  std::ranges::sort(constraints);
  return constraints;
}

bool TypeConstraints::ActiveConstraints::empty() const { return container_->constraints_.empty(); }

auto TypeConstraints::ActiveConstraints::Validate(const Vertex &vertex, LabelId label) const
    -> std::expected<void, ConstraintViolation> {
  auto validator = TypeConstraintsValidator{};
  auto it = container_->l2p_constraints_.find(label);
  if (it == container_->l2p_constraints_.end()) return {};
  validator.add(label, it->second);

  auto violation = vertex.properties.PropertiesMatchTypes(validator);
  if (!violation) return {};
  auto const &[prop_id, _, kind] = *violation;
  return std::unexpected{ConstraintViolation{ConstraintViolation::Type::TYPE, label, kind, std::set{prop_id}}};
}

auto TypeConstraints::ActiveConstraints::Validate(const Vertex &vertex, PropertyId property_id,
                                                  const PropertyValue &property_value) const
    -> std::expected<void, ConstraintViolation> {
  if (property_value.IsNull()) return {};  // Type constraints don't enforce existence

  std::optional<ConstraintViolation> violation;
  vertex.labels.for_each([&](uint32_t id) {
    if (violation) return;
    auto label = LabelId::FromUint(id);
    auto constraint_type_it = container_->constraints_.find({label, property_id});
    if (constraint_type_it == container_->constraints_.end()) return;

    auto constraint_type = constraint_type_it->second->type;
    if (PropertyValueMatchesTypeConstraint(property_value, constraint_type)) return;

    violation = ConstraintViolation{
        ConstraintViolation::Type::TYPE, {label}, constraint_type, std::set<PropertyId>{property_id}};
  });
  if (violation) return std::unexpected{*violation};
  return {};
}

auto TypeConstraints::GetActiveConstraints() const -> std::unique_ptr<ActiveConstraints> {
  return std::make_unique<ActiveConstraints>(container_.WithReadLock(std::identity{}));
}

// --- TypeConstraints methods ---

[[nodiscard]] auto TypeConstraints::ValidateAllVertices(utils::SkipList<Vertex>::Accessor vertices,
                                                        std::optional<SnapshotObserverInfo> const &snapshot_info) const
    -> std::expected<void, ConstraintViolation> {
  auto container = container_.WithReadLock(std::identity{});
  if (container->constraints_.empty()) {
    return {};
  }
  for (auto const &vertex : vertices) {
    if (auto validation_result = ValidateVertex(vertex, container); !validation_result.has_value()) {
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
    if (vertex.deleted()) continue;
    if (!ContainsLabel(vertex.labels, label)) continue;

    auto violation = vertex.properties.PropertiesMatchTypes(validator);
    if (!violation) continue;

    return std::unexpected{ConstraintViolation{ConstraintViolation::Type::TYPE, label, type, std::set{property}}};
  }
  return {};
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
    auto new_container = std::make_shared<Container>(*container);
    auto [_, inserted] = new_container->constraints_.try_emplace(
        {label, property},
        std::make_shared<IndividualConstraint>(type));  // Starts in populating state
    if (!inserted) return false;
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

void TypeConstraints::DropGraphClearConstraints() {
  container_.WithLock([](ContainerPtr &container) { container = std::make_shared<Container const>(); });
}

absl::flat_hash_map<PropertyId, TypeConstraintKind> TypeConstraints::GetTypeConstraintsForLabel(LabelId label) const {
  auto container = container_.WithReadLock(std::identity{});
  auto it = container->l2p_constraints_.find(label);
  if (it == container->l2p_constraints_.end()) {
    return {};
  }
  return it->second;
}

}  // namespace memgraph::storage
