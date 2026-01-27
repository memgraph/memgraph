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

#pragma once

#include <functional>
#include <memory>
#include <storage/v2/constraints/type_constraints_kind.hpp>
#include <utility>
#include "absl/container/flat_hash_map.h"
#include "storage/v2/constraints/constraint_violation.hpp"
#include "storage/v2/constraints/constraints_mvcc.hpp"
#include "storage/v2/durability/recovery_type.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/rw_lock.hpp"
#include "utils/skip_list.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

class TypeConstraints {
 public:
  struct MultipleThreadsConstraintValidation {
    std::optional<ConstraintViolation> operator()(const utils::SkipList<Vertex>::Accessor &vertices,
                                                  const LabelId &label, const PropertyId &property);

    const durability::ParallelizedSchemaCreationInfo &parallel_exec_info;
  };

  struct SingleThreadConstraintValidation {
    std::optional<ConstraintViolation> operator()(const utils::SkipList<Vertex>::Accessor &vertices,
                                                  const LabelId &label, const PropertyId &property);
  };

  struct IndividualConstraint {
    explicit IndividualConstraint(TypeConstraintKind t) : type(t) {}

    ~IndividualConstraint();

    TypeConstraintKind type;
    ConstraintStatus status{};
  };

  using IndividualConstraintPtr = std::shared_ptr<IndividualConstraint>;

  struct Container {
    absl::flat_hash_map<std::pair<LabelId, PropertyId>, IndividualConstraintPtr> constraints_;
    absl::flat_hash_map<LabelId, absl::flat_hash_map<PropertyId, TypeConstraintKind>> l2p_constraints_;
  };

  using ContainerPtr = std::shared_ptr<Container const>;

  struct ActiveConstraints {
    explicit ActiveConstraints(ContainerPtr container = std::make_shared<Container>())
        : container_{std::move(container)} {}

    bool ConstraintRegistered(LabelId label, PropertyId property) const;

    auto ListConstraints(uint64_t start_timestamp) const
        -> std::vector<std::tuple<LabelId, PropertyId, TypeConstraintKind>>;

    bool empty() const;

    /// Validate type constraints for a specific label being added to the vertex
    [[nodiscard]] auto Validate(const Vertex &vertex, LabelId label) const -> std::expected<void, ConstraintViolation>;

    /// Validate type constraints for a specific property being set on the vertex
    [[nodiscard]] auto Validate(const Vertex &vertex, PropertyId property_id, const PropertyValue &property_value) const
        -> std::expected<void, ConstraintViolation>;

   private:
    ContainerPtr container_;
  };

  auto GetActiveConstraints() const -> std::unique_ptr<ActiveConstraints>;

  [[nodiscard]] static auto ValidateVerticesOnConstraint(utils::SkipList<Vertex>::Accessor vertices, LabelId label,
                                                         PropertyId property, TypeConstraintKind type)
      -> std::expected<void, ConstraintViolation>;

  [[nodiscard]] auto ValidateAllVertices(utils::SkipList<Vertex>::Accessor vertices,
                                         std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt) const
      -> std::expected<void, ConstraintViolation>;

  [[nodiscard]] bool RegisterConstraint(LabelId label, PropertyId property, TypeConstraintKind type);
  void PublishConstraint(LabelId label, PropertyId property, TypeConstraintKind type, uint64_t commit_timestamp);
  /// Drops a constraint. Returns false if constraint doesn't exist or type doesn't match.
  bool DropConstraint(LabelId label, PropertyId property, TypeConstraintKind type);

  void DropGraphClearConstraints();

  /// Returns constraints for a specific label, used for registration in property store
  auto GetTypeConstraintsForLabel(LabelId label) const -> absl::flat_hash_map<PropertyId, TypeConstraintKind>;

 private:
  /// Get individual constraint for in-place status modification.
  auto GetIndividualConstraint(LabelId label, PropertyId property) const -> IndividualConstraintPtr;

  utils::Synchronized<ContainerPtr, utils::WritePrioritizedRWLock> container_{std::make_shared<Container const>()};
};

}  // namespace memgraph::storage
