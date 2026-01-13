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

#include <optional>
#include <variant>

#include "storage/v2/constraint_verification_info.hpp"
#include "storage/v2/constraints/constraint_violation.hpp"
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
    std::expected<void, ConstraintViolation> operator()(
        const utils::SkipList<Vertex>::Accessor &vertices, const LabelId &label, const PropertyId &property,
        std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt) const;

    const durability::ParallelizedSchemaCreationInfo &parallel_exec_info;
  };
  struct SingleThreadConstraintValidation {
    std::expected<void, ConstraintViolation> operator()(
        const utils::SkipList<Vertex>::Accessor &vertices, const LabelId &label, const PropertyId &property,
        std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt) const;
  };

  enum class ValidationStatus : bool { VALIDATING, READY };

  struct IndividualConstraint {
    LabelId label;
    PropertyId property;

    bool operator==(const IndividualConstraint &rhs) const {
      return std::tie(label, property) == std::tie(rhs.label, rhs.property);
    }

    template <typename H>
    friend H AbslHashValue(H h, const IndividualConstraint &c) {
      return H::combine(std::move(h), c.label, c.property);
    }
  };

  bool empty() const {
    return constraints_.WithReadLock([](const auto &constraints) { return constraints.empty(); });
  }

  [[nodiscard]] static std::expected<void, ConstraintViolation> ValidateVertexOnConstraint(const Vertex &vertex,
                                                                                           const LabelId &label,
                                                                                           const PropertyId &property);

  [[nodiscard]] static std::expected<void, ConstraintViolation> ValidateVerticesOnConstraint(
      utils::SkipList<Vertex>::Accessor vertices, LabelId label, PropertyId property,
      const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info = std::nullopt,
      std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  static std::variant<MultipleThreadsConstraintValidation, SingleThreadConstraintValidation> GetCreationFunction(
      const std::optional<durability::ParallelizedSchemaCreationInfo> &);

  bool ConstraintExists(LabelId label, PropertyId property) const;

  [[nodiscard]] bool RegisterConstraint(LabelId label, PropertyId property);
  void PublishConstraint(LabelId label, PropertyId property);
  bool DropConstraint(LabelId label, PropertyId property);

  std::expected<void, ConstraintViolation> Validate(std::unordered_set<Vertex const *> vertices_to_update);

  // performs poorly but disk will be removed soon
  std::expected<void, ConstraintViolation> PerVertexValidate(Vertex const &vertex);

  std::vector<std::pair<LabelId, PropertyId>> ListConstraints() const;

  void LoadExistenceConstraints(const std::vector<std::string> &keys);

  void DropGraphClearConstraints();

 private:
  utils::Synchronized<absl::flat_hash_map<IndividualConstraint, ValidationStatus>, utils::WritePrioritizedRWLock>
      constraints_;
};

}  // namespace memgraph::storage
