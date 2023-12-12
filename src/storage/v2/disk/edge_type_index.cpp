// Copyright 2023 Memgraph Ltd.
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

namespace memgraph::storage {

bool DiskEdgeTypeIndex::DropIndex(EdgeTypeId /*edge_type*/) {
  throw utils::NotYetImplemented(
      "Edge-type index related operations are not yet supported using on-disk storage mode.");
}

bool DiskEdgeTypeIndex::IndexExists(EdgeTypeId /*edge_type*/) const {
  throw utils::NotYetImplemented(
      "Edge-type index related operations are not yet supported using on-disk storage mode.");
}

std::vector<EdgeTypeId> DiskEdgeTypeIndex::ListIndices() const {
  throw utils::NotYetImplemented(
      "Edge-type index related operations are not yet supported using on-disk storage mode.");
}

uint64_t DiskEdgeTypeIndex::ApproximateEdgeCount(EdgeTypeId /*edge_type*/) const {
  throw utils::NotYetImplemented(
      "Edge-type index related operations are not yet supported using on-disk storage mode.");
}

void DiskEdgeTypeIndex::UpdateOnEdgeCreation(Vertex * /*from*/, Vertex * /*to*/, EdgeTypeId /*edge_type*/,
                                             const Transaction & /*tx*/) {
  throw utils::NotYetImplemented(
      "Edge-type index related operations are not yet supported using on-disk storage mode.");
}

}  // namespace memgraph::storage
