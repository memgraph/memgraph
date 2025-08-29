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

#pragma once

#include <storage/v2/constraints/type_constraints_kind.hpp>
#include <utility>
#include "absl/container/flat_hash_map.h"
#include "storage/v2/constraints/constraint_violation.hpp"
#include "storage/v2/durability/recovery_type.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "utils/observer.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

struct Vertex;

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

  [[nodiscard]] static std::optional<ConstraintViolation> ValidateVerticesOnConstraint(
      utils::SkipList<Vertex>::Accessor vertices, LabelId label, PropertyId property, TypeConstraintKind type);

  [[nodiscard]] std::optional<ConstraintViolation> Validate(const Vertex &vertex) const;
  [[nodiscard]] std::optional<ConstraintViolation> Validate(const Vertex &vertex, LabelId label) const;
  [[nodiscard]] std::optional<ConstraintViolation> Validate(const Vertex &vertex, PropertyId property_id,
                                                            const PropertyValue &property_value) const;
  [[nodiscard]] std::optional<ConstraintViolation> ValidateVertices(
      utils::SkipList<Vertex>::Accessor vertices,
      std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt) const;

  bool empty() const;
  bool ConstraintExists(LabelId label, PropertyId property) const;
  bool InsertConstraint(LabelId label, PropertyId property, TypeConstraintKind type);
  bool DropConstraint(LabelId label, PropertyId property, TypeConstraintKind type);

  std::vector<std::tuple<LabelId, PropertyId, TypeConstraintKind>> ListConstraints() const;
  void DropGraphClearConstraints();

 private:
  absl::flat_hash_map<std::pair<LabelId, PropertyId>, TypeConstraintKind> constraints_;
  absl::flat_hash_map<LabelId, absl::flat_hash_map<PropertyId, TypeConstraintKind>> l2p_constraints_;  // TODO: maintain
};

}  // namespace memgraph::storage
