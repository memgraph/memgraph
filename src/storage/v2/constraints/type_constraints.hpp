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
#include "storage/v2/constraints/active_constraints.hpp"
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

  /// Individual constraint with type and MVCC status (accessed via shared_ptr for in-place modification).
  /// This pattern matches indices and other constraints for consistency.
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

  /// ActiveConstraints implementation for type constraints.
  /// Provides snapshot-based access for a transaction's lifetime.
  class ActiveConstraints final : public TypeActiveConstraints {
   public:
    explicit ActiveConstraints(ContainerPtr snapshot) : snapshot_{std::move(snapshot)} {}

    bool ConstraintRegistered(LabelId label, PropertyId property) const override;
    std::vector<std::tuple<LabelId, PropertyId, TypeConstraintKind>> ListConstraints(
        uint64_t start_timestamp) const override;

   private:
    ContainerPtr snapshot_;
  };

  /// Creates an ActiveConstraints snapshot for transaction use.
  auto GetActiveConstraints() const -> std::unique_ptr<TypeActiveConstraints>;

  [[nodiscard]] static std::expected<void, ConstraintViolation> ValidateVerticesOnConstraint(
      utils::SkipList<Vertex>::Accessor vertices, LabelId label, PropertyId property, TypeConstraintKind type);

  [[nodiscard]] static std::expected<void, ConstraintViolation> Validate(const Vertex &vertex,
                                                                         ContainerPtr const &container);
  [[nodiscard]] std::expected<void, ConstraintViolation> Validate(const Vertex &vertex, LabelId label) const;
  [[nodiscard]] std::expected<void, ConstraintViolation> Validate(const Vertex &vertex, PropertyId property_id,
                                                                  const PropertyValue &property_value) const;
  [[nodiscard]] std::expected<void, ConstraintViolation> ValidateVertices(
      utils::SkipList<Vertex>::Accessor vertices,
      std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt) const;

  bool empty() const;

  /// Returns true if constraint is registered (even if still populating).
  bool ConstraintRegistered(LabelId label, PropertyId property) const;

  [[nodiscard]] bool RegisterConstraint(LabelId label, PropertyId property, TypeConstraintKind type);
  void PublishConstraint(LabelId label, PropertyId property, TypeConstraintKind type, uint64_t commit_timestamp);
  /// Drops a constraint. Returns false if constraint doesn't exist or type doesn't match.
  bool DropConstraint(LabelId label, PropertyId property, TypeConstraintKind type);

  ContainerPtr GetSnapshot() const { return container_.WithReadLock(std::identity{}); }

  /// Get individual constraint for in-place status modification.
  IndividualConstraintPtr GetIndividualConstraint(LabelId label, PropertyId property) const;

  /// List constraints visible at the given timestamp (for MVCC correctness).
  std::vector<std::tuple<LabelId, PropertyId, TypeConstraintKind>> ListConstraints(uint64_t start_timestamp) const;

  /// List all ready constraints (for disk storage backwards compatibility).
  std::vector<std::tuple<LabelId, PropertyId, TypeConstraintKind>> ListConstraints() const;

  void DropGraphClearConstraints();

  /// Remove all POPULATING constraints during abort.
  /// DDL operations are serialized by storage access mode (READ_ONLY/UNIQUE),
  /// so any POPULATING constraint must belong to the aborting transaction.
  void AbortPopulating();

  /// Returns constraints for a specific label, used for registration in property store
  absl::flat_hash_map<PropertyId, TypeConstraintKind> GetTypeConstraintsForLabel(LabelId label) const;

 private:
  utils::Synchronized<ContainerPtr, utils::WritePrioritizedRWLock> container_{std::make_shared<Container const>()};
};

}  // namespace memgraph::storage
