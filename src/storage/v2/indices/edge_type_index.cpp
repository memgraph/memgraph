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

#include "storage/v2/indices/edge_type_index.hpp"

namespace memgraph::storage {
void EdgeTypeIndex::AbortProcessor::CollectOnEdgeRemoval(EdgeTypeId edge_type, Vertex *from_vertex, Vertex *to_vertex,
                                                         Edge *edge) {
  auto it = cleanup_collection_.find(edge_type);
  if (it == cleanup_collection_.end()) return;
  it->second.emplace_back(from_vertex, to_vertex, edge);
}

EdgeTypeIndex::AbortProcessor::AbortProcessor(std::span<EdgeTypeId const> edge_types) {
  for (auto edge_type : edge_types) {
    cleanup_collection_.insert({edge_type, {}});
  }
}
}  // namespace memgraph::storage
