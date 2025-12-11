// Copyright 2025 Memgraph Ltd.
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

#include "query/edge_accessor.hpp"

namespace memgraph::query {

storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view,
                                                                  const std::vector<storage::EdgeTypeId> &edge_types,
                                                                  query::HopsLimit *hops_limit) const {
  auto maybe_result = impl_.InEdges(view, edge_types, nullptr, hops_limit);
  if (maybe_result.HasError()) return maybe_result.GetError();

  std::vector<EdgeAccessor> edges;
  edges.reserve((*maybe_result).edges.size());
  r::transform((*maybe_result).edges, std::back_inserter(edges), [](auto const &edge) { return EdgeAccessor(edge); });

  return EdgeVertexAccessorResult{.edges = edges, .expanded_count = (*maybe_result).expanded_count};
}

storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view,
                                                                  const std::vector<storage::EdgeTypeId> &edge_types,
                                                                  const VertexAccessor &dest,
                                                                  query::HopsLimit *hops_limit) const {
  auto maybe_result = impl_.InEdges(view, edge_types, &dest.impl_, hops_limit);
  if (maybe_result.HasError()) return maybe_result.GetError();

  std::vector<EdgeAccessor> edges;
  edges.reserve((*maybe_result).edges.size());
  r::transform((*maybe_result).edges, std::back_inserter(edges), [](auto const &edge) { return EdgeAccessor(edge); });

  return EdgeVertexAccessorResult{.edges = edges, .expanded_count = (*maybe_result).expanded_count};
}

storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view) const {
  return InEdges(view, {});
}

storage::Result<EdgeVertexAccessorResult> VertexAccessor::OutEdges(storage::View view,
                                                                   const std::vector<storage::EdgeTypeId> &edge_types,
                                                                   query::HopsLimit *hops_limit) const {
  auto maybe_result = impl_.OutEdges(view, edge_types, nullptr, hops_limit);
  if (maybe_result.HasError()) return maybe_result.GetError();

  std::vector<EdgeAccessor> edges;
  edges.reserve((*maybe_result).edges.size());
  r::transform((*maybe_result).edges, std::back_inserter(edges), [](auto const &edge) { return EdgeAccessor(edge); });

  return EdgeVertexAccessorResult{.edges = edges, .expanded_count = (*maybe_result).expanded_count};
}

storage::Result<EdgeVertexAccessorResult> VertexAccessor::OutEdges(storage::View view,
                                                                   std::vector<storage::EdgeTypeId> const &edge_types,
                                                                   VertexAccessor const &dest,
                                                                   query::HopsLimit *hops_limit) const {
  auto maybe_result = impl_.OutEdges(view, edge_types, &dest.impl_, hops_limit);
  if (maybe_result.HasError()) return maybe_result.GetError();

  std::vector<EdgeAccessor> edges;
  edges.reserve((*maybe_result).edges.size());
  r::transform((*maybe_result).edges, std::back_inserter(edges), [](auto const &edge) { return EdgeAccessor(edge); });

  return EdgeVertexAccessorResult{.edges = edges, .expanded_count = (*maybe_result).expanded_count};
}

storage::Result<EdgeVertexAccessorResult> VertexAccessor::OutEdges(storage::View view) const {
  return OutEdges(view, {});
}

}  // namespace memgraph::query
