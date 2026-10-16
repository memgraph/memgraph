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

#include <vector>

#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/expanded_edges.hpp"

namespace memgraph::test {

/// The edges of an expansion, as an array.
///
/// An expansion builds each edge as it is looked at, so there is nothing to sort and no element
/// whose address can be taken. A test asserting on edges by position needs both, because adjacency
/// comes back in no order it can rely on. Nothing outside a test does.
inline auto Materialise(storage::ExpandedEdges const &edges) -> std::vector<storage::EdgeAccessor> {
  auto built = std::vector<storage::EdgeAccessor>{};
  built.reserve(edges.size());
  for (auto const &edge : edges) built.push_back(edge);
  return built;
}

}  // namespace memgraph::test
