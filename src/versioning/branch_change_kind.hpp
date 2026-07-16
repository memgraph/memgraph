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

#pragma once

#include <cstdint>

namespace memgraph::versioning {

// Graph Versioning v1 -- which KIND of vertex change a mutation records into the branch-side change
// filters (see branch_change_filter.hpp). Deliberately its own minimal, dependency-free header so
// the hot, widely-included query/vertex_accessor.hpp can name it (its inline mutators pass it to the
// out-of-line CowIfNeeded) WITHOUT pulling in <atomic>/<memory> or the full BranchContext type.
//
// Edges have no entry here: every edge mutation funnels through one choke point (CowEdge /
// DbAccessor::InsertEdge) that records `any_edge_change` on both endpoint gids directly -- there is
// no per-mutator kind to disambiguate the way vertices have (property vs. label).
enum class BranchChangeKind : uint8_t {
  kProperty,
  kLabel,
};

}  // namespace memgraph::versioning
