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

#include "storage/v2/indices/edge_property_index.hpp"

#include <algorithm>
#include "storage/v2/edge.hpp"

namespace memgraph::storage {

void EdgePropertyIndexAbortProcessor::CollectOnPropertyChange(EdgeTypeId edge_type, PropertyId property,
                                                              Vertex *from_vertex, Vertex *to_vertex, Edge *edge,
                                                              PropertyValue value) {
  // A write names a property, so properties with no index on them reach here. An abort undoes
  // whatever was collected, so collecting an entry for a property no index covers would mean
  // looking later for an index that does not exist.
  if (!IsInteresting(property)) return;
  cleanup_collection_[property].emplace_back(std::move(value), from_vertex, to_vertex, edge, edge_type);
}

bool EdgePropertyIndexAbortProcessor::IsInteresting(PropertyId property) const {
  return std::ranges::binary_search(indexed_, property);
}

}  // namespace memgraph::storage
