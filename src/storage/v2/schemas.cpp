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

#include "storage/v2/schemas.hpp"

namespace memgraph::storage {

Schemas::CreationStatus Schemas::AddSchema(const LabelId label, const std::vector<PropertyId> &property_ids) {
  auto res = schemas_.insert({std::move(label), property_ids}).second;
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
  auto &[schema_label, schema_properties] = schemas_[primary_label];

  return Schemas::ValidationStatus::SUCCESS;
}

}  // namespace memgraph::storage
