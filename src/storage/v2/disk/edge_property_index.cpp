// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "edge_property_index.hpp"

#include "utils/exceptions.hpp"

namespace memgraph::storage {

bool DiskEdgePropertyIndex::DropIndex(PropertyId /*property*/) {
  spdlog::warn("Edge index related operations are not yet supported using on-disk storage mode.");
  return true;
}

bool DiskEdgePropertyIndex::IndexExists(PropertyId /*property*/) const {
  spdlog::warn("Edge index related operations are not yet supported using on-disk storage mode.");
  return false;
}

std::vector<PropertyId> DiskEdgePropertyIndex::ListIndices() const {
  spdlog::warn("Edge index related operations are not yet supported using on-disk storage mode.");
  return {};
}

void DiskEdgePropertyIndex::UpdateOnSetProperty(Vertex * /*from_vertex*/, Vertex * /*to_vertex*/, Edge * /*edge*/,
                                                EdgeTypeId /*edge_type*/, PropertyId /*property*/,
                                                PropertyValue /*value*/, uint64_t /*timestamp*/) {
  spdlog::warn("Edge index related operations are not yet supported using on-disk storage mode.");
}

uint64_t DiskEdgePropertyIndex::ApproximateEdgeCount(PropertyId /*property*/) const {
  spdlog::warn("Edge index related operations are not yet supported using on-disk storage mode.");
  return 0U;
}

uint64_t DiskEdgePropertyIndex::ApproximateEdgeCount(PropertyId /*property*/, const PropertyValue & /*value*/) const {
  spdlog::warn("Edge index related operations are not yet supported using on-disk storage mode.");
  return 0U;
};

uint64_t DiskEdgePropertyIndex::ApproximateEdgeCount(
    PropertyId /*property*/, const std::optional<utils::Bound<PropertyValue>> & /*lower*/,
    const std::optional<utils::Bound<PropertyValue>> & /*upper*/) const {
  spdlog::warn("Edge index related operations are not yet supported using on-disk storage mode.");
  return 0U;
};

void DiskEdgePropertyIndex::UpdateOnEdgeModification(Vertex * /*old_from*/, Vertex * /*old_to*/, Vertex * /*new_from*/,
                                                     Vertex * /*new_to*/, EdgeRef /*edge_ref*/,
                                                     EdgeTypeId /*edge_type*/, PropertyId /*property*/,
                                                     const PropertyValue & /*value*/, const Transaction & /*tx*/) {
  spdlog::warn("Edge index related operations are not yet supported using on-disk storage mode.");
}

void DiskEdgePropertyIndex::DropGraphClearIndices() {
  spdlog::warn("Edge index related operations are not yet supported using on-disk storage mode.");
}

}  // namespace memgraph::storage
