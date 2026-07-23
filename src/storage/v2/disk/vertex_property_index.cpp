// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "vertex_property_index.hpp"

namespace memgraph::storage {

void DiskVertexPropertyIndex::ActiveIndices::UpdateOnSetProperty(PropertyId /*property*/, PropertyValue /*value*/,
                                                                 Vertex * /*vertex*/, uint64_t /*timestamp*/) {}

uint64_t DiskVertexPropertyIndex::ActiveIndices::ApproximateVertexCount(PropertyId /*property*/) const { return 0U; }

uint64_t DiskVertexPropertyIndex::ActiveIndices::ApproximateVertexCount(PropertyId /*property*/,
                                                                        PropertyValue const & /*value*/) const {
  return 0U;
}

uint64_t DiskVertexPropertyIndex::ActiveIndices::ApproximateVertexCount(
    PropertyId /*property*/, std::optional<utils::Bound<PropertyValue>> const & /*lower*/,
    std::optional<utils::Bound<PropertyValue>> const & /*upper*/) const {
  return 0U;
}

bool DiskVertexPropertyIndex::ActiveIndices::IndexExists(PropertyId /*property*/) const { return false; }

bool DiskVertexPropertyIndex::ActiveIndices::IndexReady(PropertyId /*property*/) const { return false; }

std::vector<PropertyId> DiskVertexPropertyIndex::ActiveIndices::ListIndices(uint64_t /*start_timestamp*/) const {
  return {};
}

VertexPropertyIndex::AbortProcessor DiskVertexPropertyIndex::ActiveIndices::GetAbortProcessor() const {
  return AbortProcessor({});
}

void DiskVertexPropertyIndex::ActiveIndices::AbortEntries(VertexPropertyIndex::AbortableInfo const & /*info*/,
                                                          uint64_t /*start_timestamp*/) {}

void DiskVertexPropertyIndex::DropGraphClearIndices() {}

std::shared_ptr<VertexPropertyIndex::ActiveIndices> DiskVertexPropertyIndex::GetActiveIndices() const {
  return std::make_shared<ActiveIndices>();
}

}  // namespace memgraph::storage
