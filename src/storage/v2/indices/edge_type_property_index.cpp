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

#include "storage/v2/indices/edge_type_property_index.hpp"

#include "storage/v2/edge.hpp"

namespace memgraph::storage {
void EdgeTypePropertyIndex::AbortProcessor::CollectOnPropertyChange(EdgeTypeId edge_type, PropertyId property,
                                                                    Vertex *from_vertex, Vertex *to_vertex,
                                                                    Edge *edge) {
  auto it = cleanup_collection_.find({edge_type, property});
  if (it == cleanup_collection_.end()) return;
  auto value = edge->properties.GetProperty(property);
  if (value.IsNull()) return;
  it->second.emplace_back(from_vertex, to_vertex, edge, std::move(value));
}

EdgeTypePropertyIndex::AbortProcessor::AbortProcessor(std::span<std::pair<EdgeTypeId, PropertyId> const> keys) {
  for (auto const &key : keys) {
    cleanup_collection_.insert({key, {}});
    interesting_properties_.insert(key.second);
  }
}
}  // namespace memgraph::storage
