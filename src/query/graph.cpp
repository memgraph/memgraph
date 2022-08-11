// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/graph.hpp"
#include "query/path.hpp"

namespace memgraph::query {

Graph::Graph(utils::MemoryResource *memory) : vertices_(memory), edges_(memory) {}

Graph::Graph(const Graph &other, utils::MemoryResource *memory)
    : vertices_(other.vertices_, memory), edges_(other.edges_, memory) {}

Graph::Graph(Graph &&other) noexcept : Graph(std::move(other), other.GetMemoryResource()) {}

Graph::Graph(Graph &&other, utils::MemoryResource *memory)
    : vertices_(std::move(other.vertices_), memory), edges_(std::move(other.edges_), memory) {}

void Graph::Expand(const Path &path) {
  const auto path_vertices_ = path.vertices();
  const auto path_edges_ = path.edges();
  std::for_each(path_vertices_.begin(), path_vertices_.end(), [this](const VertexAccessor v) { vertices_.insert(v); });
  std::for_each(path_edges_.begin(), path_edges_.end(), [this](const EdgeAccessor e) { edges_.insert(e); });
}

std::vector<query::EdgeAccessor> Graph::OutEdges(query::VertexAccessor vertex_accessor) {
  std::vector<query::EdgeAccessor> out_edges;
  for (auto it = edges_.begin(); it != edges_.end(); ++it) {
    if (it->From() == vertex_accessor) {
      out_edges.emplace_back(*it);
    }
  }
  return out_edges;
}

/** Move assign other, utils::MemoryResource of `this` is used. */
Graph &Graph::operator=(Graph &&) = default;

/** Returns the number of expansions (edges) in this path. */
auto Graph::size() const { return edges_.size(); }

auto &Graph::vertices() { return vertices_; }
auto &Graph::edges() { return edges_; }
const auto &Graph::vertices() const { return vertices_; }
const auto &Graph::edges() const { return edges_; }

utils::MemoryResource *Graph::GetMemoryResource() const { return vertices_.get_allocator().GetMemoryResource(); }

}  // namespace memgraph::query
