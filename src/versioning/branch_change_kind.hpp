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

// Which vertex change filter a mutation populates (see BranchChangeFilters). Kept dependency-free so
// query/vertex_accessor.hpp can name it without pulling in <atomic>/<memory> via branch_change_filter.hpp.
// No edge kind: adjacency changes go through RecordEdgeChange at InsertEdge, DeleteForkEdge, DeleteForkVertex.
enum class BranchChangeKind : uint8_t {
  kProperty,
  kLabel,
};

}  // namespace memgraph::versioning
