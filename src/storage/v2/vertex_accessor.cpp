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

#include "storage/v2/inmemory/edge_accessor.hpp"
#include "storage/v2/inmemory/vertex_accessor.hpp"

namespace memgraph::storage {

std::unique_ptr<VertexAccessor> VertexAccessor::Create(Vertex *vertex, Transaction *transaction, Indices *indices,
                                                       Constraints *constraints, Config::Items config, View view) {
  return InMemoryVertexAccessor::Create(vertex, transaction, indices, constraints, config, view);
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

bool operator==(const std::unique_ptr<VertexAccessor> &va1, const std::unique_ptr<VertexAccessor> &va2) noexcept {
  const auto *inMemoryVa1 = dynamic_cast<const InMemoryVertexAccessor *>(va1.get());
  const auto *inMemoryVa2 = dynamic_cast<const InMemoryVertexAccessor *>(va2.get());
  if (inMemoryVa1 && inMemoryVa2) {
    return inMemoryVa1->operator==(*inMemoryVa2);
  }
  return false;
}

}  // namespace memgraph::storage
