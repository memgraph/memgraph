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

std::unique_ptr<EdgeAccessor> EdgeAccessor::Create(EdgeRef edge, EdgeTypeId edge_type, Vertex *from_vertex,
                                                   Vertex *to_vertex, Transaction *transaction, Indices *indices,
                                                   Constraints *constraints, Config::Items config, bool for_deleted) {
  return std::make_unique<InMemoryEdgeAccessor>(edge, edge_type, from_vertex, to_vertex, transaction, indices,
                                                constraints, config, for_deleted);
}
}  // namespace memgraph::storage
