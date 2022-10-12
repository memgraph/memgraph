// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v3/schema_validator.hpp"

#include <bits/ranges_algo.h>
#include <cstddef>
#include <ranges>

#include "storage/v3/schemas.hpp"

namespace memgraph::storage::v3 {

bool operator==(const SchemaViolation &lhs, const SchemaViolation &rhs) {
  return lhs.status == rhs.status && lhs.label == rhs.label &&
         lhs.violated_schema_property == rhs.violated_schema_property &&
         lhs.violated_property_value == rhs.violated_property_value;
}

SchemaViolation::SchemaViolation(ValidationStatus status, LabelId label) : status{status}, label{label} {}

SchemaViolation::SchemaViolation(ValidationStatus status, LabelId label, SchemaProperty violated_schema_property)
    : status{status}, label{label}, violated_schema_property{violated_schema_property} {}

SchemaViolation::SchemaViolation(ValidationStatus status, LabelId label, SchemaProperty violated_schema_property,
                                 PropertyValue violated_property_value)
    : status{status},
      label{label},
      violated_schema_property{violated_schema_property},
      violated_property_value{violated_property_value} {}

SchemaValidator::SchemaValidator(Schemas &schemas) : schemas_{schemas} {}

[[nodiscard]] std::optional<SchemaViolation> SchemaValidator::ValidateVertexCreate(
    LabelId primary_label, const std::vector<LabelId> &labels,
    const std::vector<std::pair<PropertyId, PropertyValue>> &properties) const {
  // Schema on primary label
  const auto *schema = schemas_.GetSchema(primary_label);
  if (schema == nullptr) {
    return SchemaViolation(SchemaViolation::ValidationStatus::NO_SCHEMA_DEFINED_FOR_LABEL, primary_label);
  }

  // Is there another primary label among secondary labels
  for (const auto &secondary_label : labels) {
    if (schemas_.GetSchema(secondary_label)) {
      return SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_SECONDARY_LABEL_IS_PRIMARY, secondary_label);
    }
  }

  // Check only properties defined by schema
  for (const auto &schema_type : schema->second) {
    // Check schema property existence
    auto property_pair = std::ranges::find_if(
        properties, [schema_property_id = schema_type.property_id](const auto &property_type_value) {
          return property_type_value.first == schema_property_id;
        });
    if (property_pair == properties.end()) {
      return SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_HAS_NO_PRIMARY_PROPERTY, primary_label,
                             schema_type);
    }

    // Check schema property type
    if (auto property_schema_type = PropertyTypeToSchemaType(property_pair->second);
        property_schema_type && *property_schema_type != schema_type.type) {
      return SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_PROPERTY_WRONG_TYPE, primary_label, schema_type,
                             property_pair->second);
    }
  }

  return std::nullopt;
}

[[nodiscard]] std::optional<SchemaViolation> SchemaValidator::ValidateVertexCreate(
    LabelId primary_label, const std::vector<LabelId> &labels,
    const std::vector<PropertyValue> &primary_properties) const {
  // Schema on primary label
  const auto *schema = schemas_.GetSchema(primary_label);
  if (schema == nullptr) {
    return SchemaViolation(SchemaViolation::ValidationStatus::NO_SCHEMA_DEFINED_FOR_LABEL, primary_label);
  }

  // Is there another primary label among secondary labels
  for (const auto &secondary_label : labels) {
    if (schemas_.GetSchema(secondary_label)) {
      return SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_SECONDARY_LABEL_IS_PRIMARY, secondary_label);
    }
  }

  // Check only properties defined by schema
  for (size_t i{0}; i < schema->second.size(); ++i) {
    // Check schema property type
    if (auto property_schema_type = PropertyTypeToSchemaType(primary_properties[i]);
        property_schema_type && *property_schema_type != schema->second[i].type) {
      return SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_PROPERTY_WRONG_TYPE, primary_label,
                             schema->second[i], primary_properties[i]);
    }
  }

  return std::nullopt;
}

[[nodiscard]] std::optional<SchemaViolation> SchemaValidator::ValidatePropertyUpdate(
    const LabelId primary_label, const PropertyId property_id) const {
  // Verify existence of schema on primary label
  const auto *schema = schemas_.GetSchema(primary_label);
  MG_ASSERT(schema, "Cannot validate against non existing schema!");

  // Verify that updating property is not part of schema
  if (const auto schema_property = std::ranges::find_if(
          schema->second,
          [property_id](const auto &schema_property) { return property_id == schema_property.property_id; });
      schema_property != schema->second.end()) {
    return SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_UPDATE_PRIMARY_KEY, primary_label,
                           *schema_property);
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<SchemaViolation> SchemaValidator::ValidateLabelUpdate(const LabelId label) const {
  const auto *schema = schemas_.GetSchema(label);
  if (schema) {
    return SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_UPDATE_PRIMARY_LABEL, label);
  }
  return std::nullopt;
}

const Schemas::Schema *SchemaValidator::GetSchema(LabelId label) const { return schemas_.GetSchema(label); }

VertexValidator::VertexValidator(const SchemaValidator &schema_validator, const LabelId primary_label)
    : schema_validator{&schema_validator}, primary_label_{primary_label} {}

[[nodiscard]] std::optional<SchemaViolation> VertexValidator::ValidatePropertyUpdate(PropertyId property_id) const {
  return schema_validator->ValidatePropertyUpdate(primary_label_, property_id);
};

[[nodiscard]] std::optional<SchemaViolation> VertexValidator::ValidateAddLabel(LabelId label) const {
  return schema_validator->ValidateLabelUpdate(label);
}

[[nodiscard]] std::optional<SchemaViolation> VertexValidator::ValidateRemoveLabel(LabelId label) const {
  return schema_validator->ValidateLabelUpdate(label);
}

}  // namespace memgraph::storage::v3
