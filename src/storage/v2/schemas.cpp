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

Schemas::CreationStatus Schemas::CreateSchema(const LabelId label,
                                              const std::pair<PropertyId, PropertyValue::Type> &property_ids) {
  auto res = schemas_.insert({label, property_ids}).second;
  return res ? Schemas::CreationStatus::SUCCESS : Schemas::CreationStatus::FAIL;
}

Schemas::DeletionStatus Schemas::DeleteSchema(const LabelId label) {
  auto res = schemas_.erase(label);
  return res != 0 ? Schemas::DeletionStatus::SUCCESS : Schemas::DeletionStatus::FAIL;
}

Schemas::ValidationStatus ValidateVertex(const LabelId primary_label, const Vertex &vertex, const Transaction &tx,
                                         const uint64_t commit_timestamp) {
  if (!schemas_.contains(primary_label)) {
    return Schemas::ValidationStatus::NO_SCHEMA_DEFINED_FOR_LABEL;
  }
  if (!utils::Contains(vertex.labels, primary_label)) {
    return Schemas::ValidationStatus::VERTEX_HAS_NO_PRIMARY_LABEL;
  }

  auto &[property_id, property_type] = schemas_[primary_label];
  // Property existence check
  if (!vertex.properties.HasProperty(property_id)) {
    return Schemas::ValidationStatus::VERTEX_HAS_NO_PROPERTY;
  }
  // Property type check
  if (vertex.properties.GetProperty(property_id).type_() != property_type) {
    return Schemas::ValidationStatus::VERTEX_PROPERTY_WRONG_TYPE;
  }

  // TODO after the introduction of vertex hashing introduce check for vertex
  // primary key uniqueness

  return Schemas::ValidationStatus::SUCCESS;
}

}  // namespace memgraph::storage
