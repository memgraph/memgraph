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

#include "storage/v2/vertex_accessor.hpp"

#include "storage/v2/disk/vertex_accessor.hpp"
#include "storage/v2/inmemory/edge_accessor.hpp"
#include "storage/v2/inmemory/vertex_accessor.hpp"

namespace memgraph::storage {

std::unique_ptr<VertexAccessor> VertexAccessor::Create(Vertex *vertex, Transaction *transaction, Indices *indices,
                                                       Constraints *constraints, Config config, View view) {
  if (config.storage_mode.type == Config::StorageMode::Type::IN_MEMORY)
    return InMemoryVertexAccessor::Create(vertex, transaction, indices, constraints, config.items, view);
  return DiskVertexAccessor::Create(vertex, transaction, indices, constraints, config.items, view);
}

Result<std::vector<std::unique_ptr<EdgeAccessor>>> VertexAccessor::InEdges(
    View view, const std::vector<EdgeTypeId> &edge_types) const {
  return InEdges(view, edge_types, nullptr);
}

Result<std::vector<std::unique_ptr<EdgeAccessor>>> VertexAccessor::InEdges(View view) const {
  return InEdges(view, {}, nullptr);
}

Result<std::vector<std::unique_ptr<EdgeAccessor>>> VertexAccessor::OutEdges(
    View view, const std::vector<EdgeTypeId> &edge_types) const {
  return OutEdges(view, edge_types, nullptr);
}

Result<std::vector<std::unique_ptr<EdgeAccessor>>> VertexAccessor::OutEdges(View view) const {
  return OutEdges(view, {}, nullptr);
}

}  // namespace memgraph::storage
