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

#include <utility>

#include "query/edge_accessor.hpp"

namespace memgraph::query {

storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view,
                                                                  const std::vector<storage::EdgeTypeId> &edge_types,
                                                                  storage::HopsLimit *hops_limit) const {
  auto deferred = InEdgesDeferred(view, edge_types, hops_limit);
  if (!deferred) return std::unexpected{deferred.error()};
  return EdgeVertexAccessorResult{.edges = deferred->edges.Materialise(), .expanded_count = deferred->expanded_count};
}

storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view,
                                                                  const std::vector<storage::EdgeTypeId> &edge_types,
                                                                  const VertexAccessor &dest,
                                                                  storage::HopsLimit *hops_limit) const {
  auto deferred = InEdgesDeferred(view, edge_types, dest, hops_limit);
  if (!deferred) return std::unexpected{deferred.error()};
  return EdgeVertexAccessorResult{.edges = deferred->edges.Materialise(), .expanded_count = deferred->expanded_count};
}

storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view) const {
  return InEdges(view, {});
}

storage::Result<EdgeVertexAccessorResult> VertexAccessor::OutEdges(storage::View view,
                                                                   const std::vector<storage::EdgeTypeId> &edge_types,
                                                                   storage::HopsLimit *hops_limit) const {
  auto deferred = OutEdgesDeferred(view, edge_types, hops_limit);
  if (!deferred) return std::unexpected{deferred.error()};
  return EdgeVertexAccessorResult{.edges = deferred->edges.Materialise(), .expanded_count = deferred->expanded_count};
}

storage::Result<EdgeVertexAccessorResult> VertexAccessor::OutEdges(storage::View view,
                                                                   std::vector<storage::EdgeTypeId> const &edge_types,
                                                                   VertexAccessor const &dest,
                                                                   storage::HopsLimit *hops_limit) const {
  auto deferred = OutEdgesDeferred(view, edge_types, dest, hops_limit);
  if (!deferred) return std::unexpected{deferred.error()};
  return EdgeVertexAccessorResult{.edges = deferred->edges.Materialise(), .expanded_count = deferred->expanded_count};
}

storage::Result<ExpandedEdgesResult> VertexAccessor::InEdgesDeferred(storage::View view,
                                                                     const std::vector<storage::EdgeTypeId> &edge_types,
                                                                     storage::HopsLimit *hops_limit) const {
  auto maybe_result = impl_.InEdges(view, edge_types, nullptr, hops_limit);
  if (!maybe_result) return std::unexpected{maybe_result.error()};
  return ExpandedEdgesResult{.edges = ExpandedEdges{std::move((*maybe_result).edges)},
                             .expanded_count = (*maybe_result).expanded_count};
}

storage::Result<ExpandedEdgesResult> VertexAccessor::InEdgesDeferred(storage::View view,
                                                                     const std::vector<storage::EdgeTypeId> &edge_types,
                                                                     const VertexAccessor &dest,
                                                                     storage::HopsLimit *hops_limit) const {
  auto maybe_result = impl_.InEdges(view, edge_types, &dest.impl_, hops_limit);
  if (!maybe_result) return std::unexpected{maybe_result.error()};
  return ExpandedEdgesResult{.edges = ExpandedEdges{std::move((*maybe_result).edges)},
                             .expanded_count = (*maybe_result).expanded_count};
}

storage::Result<ExpandedEdgesResult> VertexAccessor::OutEdgesDeferred(
    storage::View view, const std::vector<storage::EdgeTypeId> &edge_types, storage::HopsLimit *hops_limit) const {
  auto maybe_result = impl_.OutEdges(view, edge_types, nullptr, hops_limit);
  if (!maybe_result) return std::unexpected{maybe_result.error()};
  return ExpandedEdgesResult{.edges = ExpandedEdges{std::move((*maybe_result).edges)},
                             .expanded_count = (*maybe_result).expanded_count};
}

storage::Result<ExpandedEdgesResult> VertexAccessor::OutEdgesDeferred(
    storage::View view, const std::vector<storage::EdgeTypeId> &edge_types, const VertexAccessor &dest,
    storage::HopsLimit *hops_limit) const {
  auto maybe_result = impl_.OutEdges(view, edge_types, &dest.impl_, hops_limit);
  if (!maybe_result) return std::unexpected{maybe_result.error()};
  return ExpandedEdgesResult{.edges = ExpandedEdges{std::move((*maybe_result).edges)},
                             .expanded_count = (*maybe_result).expanded_count};
}

storage::Result<EdgeVertexAccessorResult> VertexAccessor::OutEdges(storage::View view) const {
  return OutEdges(view, {});
}

}  // namespace memgraph::query
