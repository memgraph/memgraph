// Copyright 2023 Memgraph Ltd.
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

#include "storage/v2/constraints/constraint_violation.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

class ExistenceConstraints {
 public:
  [[nodiscard]] static std::optional<ConstraintViolation> ValidateVertexOnConstraint(const Vertex &vertex,
                                                                                     LabelId label,
                                                                                     PropertyId property) {
    if (!vertex.deleted && utils::Contains(vertex.labels, label) && !vertex.properties.HasProperty(property)) {
      return ConstraintViolation{ConstraintViolation::Type::EXISTENCE, label, std::set<PropertyId>{property}};
    }
    return std::nullopt;
  }

  [[nodiscard]] static std::optional<ConstraintViolation> ValidateVerticesOnConstraint(
      utils::SkipList<Vertex>::Accessor vertices, LabelId label, PropertyId property) {
    for (const auto &vertex : vertices) {
      if (auto violation = ValidateVertexOnConstraint(vertex, label, property); violation.has_value()) {
        return violation;
      }
    }
    return std::nullopt;
  }

  bool ConstraintExists(LabelId label, PropertyId property) const;

  void InsertConstraint(LabelId label, PropertyId property);

  /// Returns true if the constraint was removed, and false if it doesn't exist.
  bool DropConstraint(LabelId label, PropertyId property);

  ///  Returns `std::nullopt` if all checks pass, and `ConstraintViolation` describing the violated constraint
  ///  otherwise.
  [[nodiscard]] std::optional<ConstraintViolation> Validate(const Vertex &vertex);

  std::vector<std::pair<LabelId, PropertyId>> ListConstraints() const;

  void LoadExistenceConstraints(const std::vector<std::string> &keys);

 private:
  std::vector<std::pair<LabelId, PropertyId>> constraints_;
};

}  // namespace memgraph::storage
