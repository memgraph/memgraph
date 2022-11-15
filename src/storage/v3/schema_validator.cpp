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

#include "common/types.hpp"
#include "storage/v3/name_id_mapper.hpp"
#include "storage/v3/result.hpp"
#include "storage/v3/schemas.hpp"

namespace memgraph::storage::v3 {

SchemaValidator::SchemaValidator(Schemas &schemas, const NameIdMapper &name_id_mapper)
    : schemas_{&schemas}, name_id_mapper_{&name_id_mapper} {}

std::optional<ShardError> SchemaValidator::ValidateVertexCreate(
    LabelId primary_label, const std::vector<LabelId> &labels,
    const std::vector<PropertyValue> &primary_properties) const {
  // Schema on primary label
  const auto *schema = schemas_->GetSchema(primary_label);
  if (schema == nullptr) {
    return SHARD_ERROR(ErrorCode::SCHEMA_NO_SCHEMA_DEFINED_FOR_LABEL,
                       fmt::format("Schema not defined for label :{}", name_id_mapper_->IdToName(primary_label)));
  }

  // Is there another primary label among secondary labels
  for (const auto &secondary_label : labels) {
    if (schemas_->GetSchema(secondary_label)) {
      return SHARD_ERROR(ErrorCode::SCHEMA_VERTEX_SECONDARY_LABEL_IS_PRIMARY,
                         fmt::format("Cannot add label :{}, since it is defined as a primary label",
                                     name_id_mapper_->IdToName(secondary_label)));
    }
  }

  // Quick size check
  if (schema->second.size() != primary_properties.size()) {
    return SHARD_ERROR(ErrorCode::SCHEMA_VERTEX_PRIMARY_PROPERTIES_UNDEFINED,
                       fmt::format("Not all primary properties have been specified for :{} vertex",
                                   name_id_mapper_->IdToName(primary_label)));
  }
  // Check only properties defined by schema
  for (size_t i{0}; i < schema->second.size(); ++i) {
    // Check schema property type
    if (auto property_schema_type = PropertyTypeToSchemaType(primary_properties[i]);
        property_schema_type && *property_schema_type != schema->second[i].type) {
      return SHARD_ERROR(
          ErrorCode::SCHEMA_VERTEX_PROPERTY_WRONG_TYPE,
          fmt::format("Property {} is of wrong type, expected {}, actual {}",
                      name_id_mapper_->IdToName(schema->second[i].property_id),
                      SchemaTypeToString(schema->second[i].type), SchemaTypeToString(*property_schema_type)));
    }
  }

  return std::nullopt;
}

std::optional<ShardError> SchemaValidator::ValidatePropertyUpdate(const LabelId primary_label,
                                                                  const PropertyId property_id) const {
  // Verify existence of schema on primary label
  const auto *schema = schemas_->GetSchema(primary_label);
  MG_ASSERT(schema, "Cannot validate against non existing schema!");

  // Verify that updating property is not part of schema
  if (const auto schema_property = std::ranges::find_if(
          schema->second,
          [property_id](const auto &schema_property) { return property_id == schema_property.property_id; });
      schema_property != schema->second.end()) {
    return SHARD_ERROR(
        ErrorCode::SCHEMA_VERTEX_UPDATE_PRIMARY_KEY,
        fmt::format("Cannot update primary property {} of schema on label :{}",
                    name_id_mapper_->IdToName(schema_property->property_id), name_id_mapper_->IdToName(primary_label)));
  }
  return std::nullopt;
}

std::optional<ShardError> SchemaValidator::ValidateLabelUpdate(const LabelId label) const {
  const auto *schema = schemas_->GetSchema(label);
  if (schema) {
    return SHARD_ERROR(ErrorCode::SCHEMA_VERTEX_UPDATE_PRIMARY_LABEL,
                       fmt::format("Cannot add/remove primary label :{}", name_id_mapper_->IdToName(label)));
  }
  return std::nullopt;
}

const Schemas::Schema *SchemaValidator::GetSchema(LabelId label) const { return schemas_->GetSchema(label); }

VertexValidator::VertexValidator(const SchemaValidator &schema_validator, const LabelId primary_label)
    : schema_validator{&schema_validator}, primary_label_{primary_label} {}

std::optional<ShardError> VertexValidator::ValidatePropertyUpdate(PropertyId property_id) const {
  return schema_validator->ValidatePropertyUpdate(primary_label_, property_id);
};

std::optional<ShardError> VertexValidator::ValidateAddLabel(LabelId label) const {
  return schema_validator->ValidateLabelUpdate(label);
}

std::optional<ShardError> VertexValidator::ValidateRemoveLabel(LabelId label) const {
  return schema_validator->ValidateLabelUpdate(label);
}

}  // namespace memgraph::storage::v3
