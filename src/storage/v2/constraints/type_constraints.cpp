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

#include "type_constraints.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/algorithm.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage {

[[nodiscard]] std::optional<ConstraintViolation> TypeConstraints::ValidateVertexOnConstraint(
    const Vertex &vertex, const LabelId &label, const PropertyId &property) {
  if (vertex.deleted || !utils::Contains(vertex.labels, label)) {
    return std::nullopt;
  }
  auto prop_value = vertex.properties.GetProperty(property);
  if (prop_value.IsNull()) {
    return std::nullopt;
  }

  if (!vertex.deleted && utils::Contains(vertex.labels, label) && !vertex.properties.HasProperty(property)) {
    return ConstraintViolation{ConstraintViolation::Type::EXISTENCE, label, std::set<PropertyId>{property}};
  }
  return std::nullopt;
}

// std::variant<TypeConstraints::MultipleThreadsConstraintValidation, TypeConstraints::SingleThreadConstraintValidation>
// TypeConstraints::GetCreationFunction(const std::optional<durability::ParallelizedSchemaCreationInfo> &par_exec_info)
// {
//   if (par_exec_info.has_value()) {
//     return TypeConstraints::MultipleThreadsConstraintValidation{par_exec_info.value()};
//   }
//   return TypeConstraints::SingleThreadConstraintValidation{};
// }

// [[nodiscard]] std::optional<ConstraintViolation> TypeConstraints::ValidateVerticesOnConstraint(
//     utils::SkipList<Vertex>::Accessor vertices, LabelId label, PropertyId property,
//     const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info) {
//   auto calling_existence_validation_function = GetCreationFunction(parallel_exec_info);
//   return std::visit(
//       [&vertices, &label, &property](auto &calling_object) { return calling_object(vertices, label, property); },
//       calling_existence_validation_function);
// }

bool TypeConstraints::ConstraintExists(LabelId label, PropertyId property) const {
  return constraints_.contains({label, property});
}

bool TypeConstraints::InsertConstraint(LabelId label, PropertyId property, TypeConstraints::Type type) {
  if (ConstraintExists(label, property)) {
    return false;
  }
  constraints_.emplace(std::make_pair(label, property), type);
  return true;
}

bool TypeConstraints::DropConstraint(LabelId label, PropertyId property) {
  auto it = constraints_.find({label, property});
  if (it == constraints_.end()) {
    return false;
  }
  constraints_.erase(it);
  return true;
}

TypeConstraints::Type TypeConstraints::PropertyValueToType(const PropertyValue &property) {
  switch (property.type()) {
    case PropertyValueType::String:
      return TypeConstraints::Type::STRING;
    case PropertyValueType::Bool:
      return TypeConstraints::Type::BOOLEAN;
    case PropertyValueType::Int:
      return TypeConstraints::Type::INTEGER;
    case PropertyValueType::Double:
      return TypeConstraints::Type::FLOAT;
    case PropertyValueType::List:
      return TypeConstraints::Type::LIST;
    case PropertyValueType::Map:
      return TypeConstraints::Type::MAP;
    case PropertyValueType::TemporalData: {
      auto const temporal = property.ValueTemporalData();
      switch (temporal.type) {
        case TemporalType::Date:
          return TypeConstraints::Type::DATE;
        case TemporalType::LocalTime:
          return TypeConstraints::Type::LOCALTIME;
        case TemporalType::LocalDateTime:
          return TypeConstraints::Type::LOCALDATETIME;
        case TemporalType::Duration:
          return TypeConstraints::Type::DURATION;
      }
    }
    case PropertyValueType::ZonedTemporalData:
      return TypeConstraints::Type::ZONEDDATETIME;
    case PropertyValueType::Enum:
      return TypeConstraints::Type::ENUM;
    case PropertyValueType::Point2d:
    case PropertyValueType::Point3d:
      return TypeConstraints::Type::POINT;
    case PropertyValueType::Null:
      MG_ASSERT(false, "Unexpected conversion from PropertyValueType::Null to TypeConstraint::Type");
  }
}

}  // namespace memgraph::storage
