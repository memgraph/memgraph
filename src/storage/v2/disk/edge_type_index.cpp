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

#include "edge_type_index.hpp"

#include "utils/exceptions.hpp"

#include "spdlog/spdlog.h"

namespace memgraph::storage {

bool DiskEdgeTypeIndex::DropIndex(EdgeTypeId /*edge_type*/) {
  spdlog::warn("Edge-type index related operations are not yet supported using on-disk storage mode.");
  return true;
}

bool DiskEdgeTypeIndex::IndexExists(EdgeTypeId /*edge_type*/) const {
  spdlog::warn("Edge-type index related operations are not yet supported using on-disk storage mode.");
  return false;
}

std::vector<EdgeTypeId> DiskEdgeTypeIndex::ListIndices() const {
  spdlog::warn("Edge-type index related operations are not yet supported using on-disk storage mode.");
  return {};
}

uint64_t DiskEdgeTypeIndex::ApproximateEdgeCount(EdgeTypeId /*edge_type*/) const {
  spdlog::warn("Edge-type index related operations are not yet supported using on-disk storage mode.");
  return 0U;
}

void DiskEdgeTypeIndex::UpdateOnEdgeCreation(Vertex * /*from*/, Vertex * /*to*/, EdgeRef /*edge_ref*/,
                                             EdgeTypeId /*edge_type*/, const Transaction & /*tx*/) {
  spdlog::warn("Edge-type index related operations are not yet supported using on-disk storage mode.");
}

void DiskEdgeTypeIndex::UpdateOnEdgeModification(Vertex * /*old_from*/, Vertex * /*old_to*/, Vertex * /*new_from*/,
                                                 Vertex * /*new_to*/, EdgeRef /*edge_ref*/, EdgeTypeId /*edge_type*/,
                                                 const Transaction & /*tx*/) {
  spdlog::warn("Edge-type index related operations are not yet supported using on-disk storage mode.");
}

void DiskEdgeTypeIndex::DropGraphClearIndices() {
  spdlog::warn("Edge-type index related operations are not yet supported using on-disk storage mode.");
}

}  // namespace memgraph::storage
