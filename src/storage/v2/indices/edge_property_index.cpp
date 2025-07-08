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

#include "storage/v2/indices/edge_property_index.hpp"
#include "storage/v2/edge.hpp"

namespace memgraph::storage {

void EdgePropertyIndex::AbortProcessor::CollectOnPropertyChange(EdgeTypeId edge_type, PropertyId property,
                                                                Vertex *from_vertex, Vertex *to_vertex, Edge *edge,
                                                                PropertyValue value) {
  auto it = cleanup_collection_.find(property);
  if (it == cleanup_collection_.end()) {
    DMG_ASSERT(false, "Should not be possible");
    return;
  }
  it->second.emplace_back(std::move(value), from_vertex, to_vertex, edge, edge_type);
}

EdgePropertyIndex::AbortProcessor::AbortProcessor(std::span<PropertyId const> properties) {
  for (auto edge_type : properties) {
    cleanup_collection_.insert({edge_type, {}});
  }
}

bool EdgePropertyIndex::AbortProcessor::IsInteresting(PropertyId property) {
  return cleanup_collection_.contains(property);
}

}  // namespace memgraph::storage
