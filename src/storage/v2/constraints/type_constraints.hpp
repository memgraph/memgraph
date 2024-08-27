// Copyright 2024 Memgraph Ltd.
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

#include <cstdint>
#include <set>
#include <utility>
#include "storage/v2/constraints/constraint_violation.hpp"
#include "storage/v2/durability/recovery_type.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

class TypeConstraints {
 public:
  // struct Entry {
  //   LabelId label;
  //   PropertyId property;

  //   friend auto operator<=>(Entry const &, Entry const &) = delete;
  // };

  enum class Type : uint8_t {
    STRING,
    BOOLEAN,
    INTEGER,
    FLOAT,
    LIST,
    MAP,
    DURATION,
    DATE,
    LOCALTIME,
    LOCALDATETIME,
    ZONEDDATETIME,
    ENUM,
    POINT,
  };

  struct MultipleThreadsConstraintValidation {
    std::optional<ConstraintViolation> operator()(const utils::SkipList<Vertex>::Accessor &vertices,
                                                  const LabelId &label, const PropertyId &property);

    const durability::ParallelizedSchemaCreationInfo &parallel_exec_info;
  };
  struct SingleThreadConstraintValidation {
    std::optional<ConstraintViolation> operator()(const utils::SkipList<Vertex>::Accessor &vertices,
                                                  const LabelId &label, const PropertyId &property);
  };

  [[nodiscard]] static std::optional<ConstraintViolation> ValidateVertexOnConstraint(const Vertex &vertex,
                                                                                     LabelId label, PropertyId property,
                                                                                     Type type);

  [[nodiscard]] static std::optional<ConstraintViolation> ValidateVerticesOnConstraint(
      utils::SkipList<Vertex>::Accessor vertices, LabelId label, PropertyId property, Type type);

  [[nodiscard]] std::optional<ConstraintViolation> Validate(const Vertex &vertex);

  bool ConstraintExists(LabelId label, PropertyId property) const;
  bool InsertConstraint(LabelId label, PropertyId property, Type type);
  bool DropConstraint(LabelId label, PropertyId property);

  static Type PropertyValueToType(PropertyValue const &property);

 private:
  // TODO Ivan: unordereded set doesn't work, spaceship deleted, i dont know check later
  std::map<std::pair<LabelId, PropertyId>, Type> constraints_;
};

}  // namespace memgraph::storage
