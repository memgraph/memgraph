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

#include "query/vertex_accessor.hpp"

#include <ranges>
#include <utility>

#include "query/edge_accessor.hpp"

namespace memgraph::query {

static_assert(std::ranges::forward_range<ExpandedEdges>);
static_assert(std::ranges::sized_range<ExpandedEdges>);

namespace {

/// The same expansion as the accessors this layer deals in. The edges move across whole, so this
/// changes their type and not what was found.
auto AsQueryResult(storage::EdgesVertexAccessorResult &&result) -> ExpandedEdgesResult {
  return ExpandedEdgesResult{.edges = ExpandedEdges{std::move(result.edges)}, .expanded_count = result.expanded_count};
}

}  // namespace

storage::Result<ExpandedEdgesResult> VertexAccessor::InEdges(storage::View view) const { return InEdges(view, {}); }

storage::Result<ExpandedEdgesResult> VertexAccessor::InEdges(storage::View view,
                                                             const std::vector<storage::EdgeTypeId> &edge_types,
                                                             storage::HopsLimit *hops_limit) const {
  return impl_.InEdges(view, edge_types, nullptr, hops_limit).transform(AsQueryResult);
}

storage::Result<ExpandedEdgesResult> VertexAccessor::InEdges(storage::View view,
                                                             const std::vector<storage::EdgeTypeId> &edge_types,
                                                             const VertexAccessor &dest,
                                                             storage::HopsLimit *hops_limit) const {
  return impl_.InEdges(view, edge_types, &dest.impl_, hops_limit).transform(AsQueryResult);
}

storage::Result<ExpandedEdgesResult> VertexAccessor::OutEdges(storage::View view,
                                                              const std::vector<storage::EdgeTypeId> &edge_types,
                                                              storage::HopsLimit *hops_limit) const {
  return impl_.OutEdges(view, edge_types, nullptr, hops_limit).transform(AsQueryResult);
}

storage::Result<ExpandedEdgesResult> VertexAccessor::OutEdges(storage::View view,
                                                              const std::vector<storage::EdgeTypeId> &edge_types,
                                                              const VertexAccessor &dest,
                                                              storage::HopsLimit *hops_limit) const {
  return impl_.OutEdges(view, edge_types, &dest.impl_, hops_limit).transform(AsQueryResult);
}

storage::Result<ExpandedEdgesResult> VertexAccessor::OutEdges(storage::View view) const { return OutEdges(view, {}); }

}  // namespace memgraph::query
