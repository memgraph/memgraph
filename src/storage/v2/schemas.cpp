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

#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "storage/v2/property_value.hpp"
#include "storage/v2/schemas.hpp"

namespace memgraph::storage {

Schemas::CreationStatus Schemas::CreateSchema(
    const LabelId label, const std::vector<std::pair<PropertyId, PropertyValue::Type>> &property_types) {
  auto res = schemas_.insert({label, property_types}).second;
  return res ? Schemas::CreationStatus::SUCCESS : Schemas::CreationStatus::FAIL;
}

Schemas::DeletionStatus Schemas::DeleteSchema(const LabelId label) {
  auto res = schemas_.erase(label);
  return res != 0 ? Schemas::DeletionStatus::SUCCESS : Schemas::DeletionStatus::FAIL;
}

Schemas::ValidationStatus Schemas::ValidateVertex(const LabelId primary_label, const Vertex &vertex) {
  if (!schemas_.contains(primary_label)) {
    return Schemas::ValidationStatus::NO_SCHEMA_DEFINED_FOR_LABEL;
  }
  if (!utils::Contains(vertex.labels, primary_label)) {
    return Schemas::ValidationStatus::VERTEX_HAS_NO_PRIMARY_LABEL;
  }

  for (auto &[property_id, property_type] : schemas_[primary_label]) {
    // Property existence check
    if (!vertex.properties.HasProperty(property_id)) {
      return Schemas::ValidationStatus::VERTEX_HAS_NO_PROPERTY;
    }
    // Property type check
    if (vertex.properties.GetProperty(property_id).type() != property_type) {
      return Schemas::ValidationStatus::VERTEX_PROPERTY_WRONG_TYPE;
    }
  }

  // TODO after the introduction of vertex hashing introduce check for vertex
  // primary key uniqueness

  return Schemas::ValidationStatus::SUCCESS;
}

Schemas::SchemasList Schemas::ListSchemas() const {
  Schemas::SchemasList ret;
  ret.reserve(schemas_.size());
  for (const auto &[label_props, schema_property] : schemas_) {
    ret.emplace_back(label_props, schema_property);
  }
  return ret;
}

}  // namespace memgraph::storage
