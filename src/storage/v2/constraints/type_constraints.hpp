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
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

enum class TypeConstraintsType : uint8_t {
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

inline std::string TypeConstraintsTypeToString(TypeConstraintsType type) {
  switch (type) {
    case TypeConstraintsType::STRING:
      return "STRING";
    case TypeConstraintsType::BOOLEAN:
      return "BOOL";
    case TypeConstraintsType::INTEGER:
      return "INTEGER";
    case TypeConstraintsType::FLOAT:
      return "FLOAT";
    case TypeConstraintsType::LIST:
      return "LIST";
    case TypeConstraintsType::MAP:
      return "MAP";
    case TypeConstraintsType::DURATION:
      return "DURATION";
    case TypeConstraintsType::DATE:
      return "DATE";
    case TypeConstraintsType::LOCALTIME:
      return "LOCAL TIME";
    case TypeConstraintsType::LOCALDATETIME:
      return "LOCAL DATE TIME";
    case TypeConstraintsType::ZONEDDATETIME:
      return "ZONED DATE TIME";
    case TypeConstraintsType::ENUM:
      return "ENUM";
    case TypeConstraintsType::POINT:
      return "POINT";
  }
}

inline TypeConstraintsType PropertyValueToTypeConstraintType(const PropertyValue &property) {
  switch (property.type()) {
    case PropertyValueType::String:
      return TypeConstraintsType::STRING;
    case PropertyValueType::Bool:
      return TypeConstraintsType::BOOLEAN;
    case PropertyValueType::Int:
      return TypeConstraintsType::INTEGER;
    case PropertyValueType::Double:
      return TypeConstraintsType::FLOAT;
    case PropertyValueType::List:
      return TypeConstraintsType::LIST;
    case PropertyValueType::Map:
      return TypeConstraintsType::MAP;
    case PropertyValueType::TemporalData: {
      auto const temporal = property.ValueTemporalData();
      switch (temporal.type) {
        case TemporalType::Date:
          return TypeConstraintsType::DATE;
        case TemporalType::LocalTime:
          return TypeConstraintsType::LOCALTIME;
        case TemporalType::LocalDateTime:
          return TypeConstraintsType::LOCALDATETIME;
        case TemporalType::Duration:
          return TypeConstraintsType::DURATION;
      }
    }
    case PropertyValueType::ZonedTemporalData:
      return TypeConstraintsType::ZONEDDATETIME;
    case PropertyValueType::Enum:
      return TypeConstraintsType::ENUM;
    case PropertyValueType::Point2d:
    case PropertyValueType::Point3d:
      return TypeConstraintsType::POINT;
    case PropertyValueType::Null:
      MG_ASSERT(false, "Unexpected conversion from PropertyValueType::Null to TypeConstraint::Type");
  }
}

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

  [[nodiscard]] static std::optional<ConstraintViolation> ValidateVertexOnConstraint(const Vertex &vertex,
                                                                                     LabelId label, PropertyId property,
                                                                                     TypeConstraintsType type);

  [[nodiscard]] static std::optional<ConstraintViolation> ValidateVerticesOnConstraint(
      utils::SkipList<Vertex>::Accessor vertices, LabelId label, PropertyId property, TypeConstraintsType type);

  [[nodiscard]] std::optional<ConstraintViolation> Validate(const Vertex &vertex);
  [[nodiscard]] std::optional<ConstraintViolation> Validate(const Vertex &vertex, LabelId label);
  [[nodiscard]] std::optional<ConstraintViolation> Validate(const Vertex &vertex, PropertyId property);

  bool HasTypeConstraints() const;
  bool ConstraintExists(LabelId label, PropertyId property) const;
  bool InsertConstraint(LabelId label, PropertyId property, TypeConstraintsType type);
  std::optional<TypeConstraintsType> DropConstraint(LabelId label, PropertyId property);
  void DropGraphClearConstraints();

 private:
  // TODO Ivan: unordereded set doesn't work, spaceship deleted, i dont know check later
  std::map<std::pair<LabelId, PropertyId>, TypeConstraintsType> constraints_;
};

}  // namespace memgraph::storage
