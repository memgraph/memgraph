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
#include <optional>
#include <variant>

#include "absl/container/flat_hash_map.h"
#include "storage/v2/constraint_verification_info.hpp"
#include "storage/v2/constraints/constraint_violation.hpp"
#include "storage/v2/constraints/constraints_mvcc.hpp"
#include "storage/v2/durability/recovery_type.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/rw_lock.hpp"
#include "utils/skip_list.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

class ExistenceConstraints {
 public:
  struct MultipleThreadsConstraintValidation {
    auto operator()(const utils::SkipList<Vertex>::Accessor &vertices, const LabelId &label, const PropertyId &property,
                    std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt) const
        -> std::expected<void, ConstraintViolation>;

    const durability::ParallelizedSchemaCreationInfo &parallel_exec_info;
  };

  struct SingleThreadConstraintValidation {
    auto operator()(const utils::SkipList<Vertex>::Accessor &vertices, const LabelId &label, const PropertyId &property,
                    std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt) const
        -> std::expected<void, ConstraintViolation>;
  };

  /// Key for constraint lookup in the container.
  struct ConstraintKey {
    LabelId label;
    PropertyId property;

    friend auto operator<=>(const ConstraintKey &, const ConstraintKey &) = default;

    template <typename H>
    friend H AbslHashValue(H h, const ConstraintKey &c) {
      return H::combine(std::move(h), c.label, c.property);
    }
  };

  /// Individual constraint with MVCC status (accessed via shared_ptr for in-place modification).
  /// This pattern matches indices and unique constraints for consistency.
  struct IndividualConstraint {
    ConstraintStatus status{};
    ~IndividualConstraint();
  };

  using IndividualConstraintPtr = std::shared_ptr<IndividualConstraint>;
  using Container = absl::flat_hash_map<ConstraintKey, IndividualConstraintPtr>;
  using ContainerPtr = std::shared_ptr<Container const>;

  struct ActiveConstraints {
    explicit ActiveConstraints(ContainerPtr container = std::make_shared<Container>())
        : container_{std::move(container)} {}

    auto ListConstraints(uint64_t start_timestamp) const -> std::vector<std::pair<LabelId, PropertyId>>;
    bool empty() const;

   private:
    ContainerPtr container_;
  };

  /// Creates an ActiveConstraints snapshot for transaction use.
  auto GetActiveConstraints() const -> std::unique_ptr<ActiveConstraints>;

  static auto GetCreationFunction(const std::optional<durability::ParallelizedSchemaCreationInfo> &)
      -> std::variant<MultipleThreadsConstraintValidation, SingleThreadConstraintValidation>;

  /// Returns true if constraint is registered (even if still populating). Only used by OnDisk
  bool ConstraintExists(LabelId label, PropertyId property) const;

  [[nodiscard]] bool RegisterConstraint(LabelId label, PropertyId property);

  /// Publishes a constraint after validation, making it visible at the given commit timestamp.
  /// Returns true on success, false if constraint not found.
  bool PublishConstraint(LabelId label, PropertyId property, uint64_t commit_timestamp) const;

  /// Drops a constraint. Returns false if constraint doesn't exist.
  bool DropConstraint(LabelId label, PropertyId property);

  /*
   * VALIDATION
   */

  /// Commit time validation
  auto Validate(const std::unordered_set<Vertex const *> &vertices_to_check) const
      -> std::expected<void, ConstraintViolation>;

  /// Create/Recover time validation
  [[nodiscard]] static auto ValidateVerticesOnConstraint(
      utils::SkipList<Vertex>::Accessor vertices, LabelId label, PropertyId property,
      const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info = std::nullopt,
      std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt)
      -> std::expected<void, ConstraintViolation>;

  /// [OnDisk] alternative validation performs poorly but disk will be removed soon
  auto PerVertexValidate(Vertex const &vertex) const -> std::expected<void, ConstraintViolation>;

  /// [OnDisk]
  void LoadExistenceConstraints(const std::vector<std::string> &keys);

  void DropGraphClearConstraints();

 private:
  auto GetIndividualConstraint(LabelId label, PropertyId property) const -> IndividualConstraintPtr;

  utils::Synchronized<ContainerPtr, utils::WritePrioritizedRWLock> constraints_{std::make_shared<Container const>()};
};

}  // namespace memgraph::storage
