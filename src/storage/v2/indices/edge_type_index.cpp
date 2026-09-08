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

#include "storage/v2/indices/edge_type_index.hpp"

#include <algorithm>

#include <utility>

namespace memgraph::storage {
void EdgeTypeIndexAbortProcessor::CollectOnEdgeRemoval(EdgeTypeId edge_type, Vertex *from_vertex, Vertex *to_vertex,
                                                       EdgeRef edge) {
  if (!std::ranges::binary_search(indexed_, edge_type)) return;
  cleanup_collection_[edge_type].emplace_back(from_vertex, to_vertex, edge);
}
}  // namespace memgraph::storage
