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

#include "storage/v2/edge_accessor.hpp"
#include <memory>

#include "storage/v2/inmemory/edge_accessor.hpp"

namespace memgraph::storage {

/// TODO(andi): Change this
std::unique_ptr<EdgeAccessor> EdgeAccessor::Create(EdgeRef edge, EdgeTypeId edge_type, Vertex *from_vertex,
                                                   Vertex *to_vertex, Transaction *transaction, Indices *indices,
                                                   Constraints *constraints, Config config, bool for_deleted) {
  return std::make_unique<InMemoryEdgeAccessor>(edge, edge_type, from_vertex, to_vertex, transaction, indices,
                                                constraints, config.items, for_deleted);
}

bool operator==(const std::unique_ptr<EdgeAccessor> &ea1, const std::unique_ptr<EdgeAccessor> &ea2) noexcept {
  const auto *inMemoryEa1 = dynamic_cast<const InMemoryEdgeAccessor *>(ea1.get());
  const auto *inMemoryEa2 = dynamic_cast<const InMemoryEdgeAccessor *>(ea2.get());
  if (inMemoryEa1 && inMemoryEa2) {
    return inMemoryEa1->operator==(*inMemoryEa2);
  }
  return false;
}

}  // namespace memgraph::storage
