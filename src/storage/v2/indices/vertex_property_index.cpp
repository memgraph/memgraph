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

#include "storage/v2/indices/vertex_property_index.hpp"

namespace memgraph::storage {

VertexPropertyIndexAbortProcessor::VertexPropertyIndexAbortProcessor(std::span<PropertyId const> properties) {
  for (auto property : properties) {
    cleanup_collection_.insert({property, {}});
  }
}

void VertexPropertyIndexAbortProcessor::CollectOnPropertyChange(PropertyId property, Vertex *vertex,
                                                                PropertyValue value) {
  auto it = cleanup_collection_.find(property);
  if (it == cleanup_collection_.end()) {
    DMG_ASSERT(false, "Should not be possible");
    return;
  }
  it->second.emplace_back(std::move(value), vertex);
}

bool VertexPropertyIndexAbortProcessor::IsInteresting(PropertyId property) const {
  return cleanup_collection_.contains(property);
}

}  // namespace memgraph::storage
