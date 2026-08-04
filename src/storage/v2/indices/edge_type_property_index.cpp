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

#include "storage/v2/indices/edge_type_property_index.hpp"

#include <algorithm>

#include "storage/v2/edge.hpp"

namespace memgraph::storage {
void EdgeTypePropertyIndexAbortProcessor::CollectOnPropertyChange(EdgeTypeId edge_type, PropertyId property,
                                                                  Vertex *from_vertex, Vertex *to_vertex, Edge *edge,
                                                                  PropertyValue value) {
  DMG_ASSERT(IsInteresting(edge_type, property), "Collecting a key no index is keyed on");
  cleanup_collection_[{edge_type, property}].emplace_back(from_vertex, to_vertex, edge, std::move(value));
}

bool EdgeTypePropertyIndexAbortProcessor::IsInteresting(PropertyId id) const {
  return indexed_ != nullptr && std::ranges::binary_search(indexed_->properties, id);
}

bool EdgeTypePropertyIndexAbortProcessor::IsInteresting(EdgeTypeId edge_type, PropertyId property) const {
  return indexed_ != nullptr && std::ranges::binary_search(indexed_->keys, std::pair{edge_type, property});
}
}  // namespace memgraph::storage
