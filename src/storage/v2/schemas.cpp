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

#include <unordered_map>
#include <utility>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/schemas.hpp"

namespace memgraph::storage {

SchemaViolation::SchemaViolation(ValidationStatus status, LabelId label) : status{status}, label{label} {}
SchemaViolation::SchemaViolation(ValidationStatus status, LabelId label, SchemaProperty violated_type)
    : status{status}, label{label}, violated_type{violated_type} {}

SchemaViolation::SchemaViolation(ValidationStatus status, LabelId label, SchemaProperty violated_type,
                                 PropertyValue violated_property_value)
    : status{status}, label{label}, violated_type{violated_type}, violated_property_value{violated_property_value} {}

Schemas::SchemasList Schemas::ListSchemas() const {
  Schemas::SchemasList ret;
  ret.reserve(schemas_.size());
  for (const auto &[label_props, schema_property] : schemas_) {
    ret.emplace_back(label_props, schema_property);
  }
  return ret;
}

Schemas::SchemasList Schemas::GetSchema(const LabelId primary_label) const {
  if (auto schema_map = schemas_.find(primary_label); schema_map != schemas_.end()) {
    return {{schema_map->first, schema_map->second}};
  }
  return {};
}

bool Schemas::CreateSchema(const LabelId primary_label, const std::vector<SchemaProperty> &schemas_types) {
  const auto res = schemas_.insert({primary_label, schemas_types}).second;
  return res;
}

bool Schemas::DeleteSchema(const LabelId primary_label) {
  const auto res = schemas_.erase(primary_label);
  return res;
}

std::optional<SchemaViolation> Schemas::ValidateVertex(const LabelId primary_label, const Vertex &vertex) {
  // TODO Check for multiple defined primary labels
  const auto schema = schemas_.find(primary_label);
  if (schema == schemas_.end()) {
    return SchemaViolation(SchemaViolation::ValidationStatus::NO_SCHEMA_DEFINED_FOR_LABEL, primary_label);
  }
  if (!utils::Contains(vertex.labels, primary_label)) {
    return SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_HAS_NO_PRIMARY_LABEL, primary_label);
  }

  for (const auto &schema_type : schema->second) {
    if (!vertex.properties.HasProperty(schema_type.property_id)) {
      return SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_HAS_NO_PROPERTY, primary_label, schema_type);
    }
    // Property type check
    //  TODO Can this be replaced with just property id check?
    if (auto vertex_property = vertex.properties.GetProperty(schema_type.property_id);
        PropertyValueTypeToSchemaProperty(vertex_property) != schema_type.type) {
      return SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_PROPERTY_WRONG_TYPE, primary_label, schema_type,
                             vertex_property);
    }
  }
  // TODO after the introduction of vertex hashing introduce check for vertex
  // primary key uniqueness

  return std::nullopt;
}

}  // namespace memgraph::storage
