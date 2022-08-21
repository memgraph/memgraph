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

Graph::Graph(const Graph &other)
    : Graph(other,
            std::allocator_traits<allocator_type>::select_on_container_copy_construction(other.GetMemoryResource())
                .GetMemoryResource()) {}

Graph::Graph(Graph &&other, utils::MemoryResource *memory)
    : vertices_(std::move(other.vertices_), memory), edges_(std::move(other.edges_), memory) {}

void Graph::Expand(const Path &path) {
  const auto &path_vertices_ = path.vertices();
  const auto &path_edges_ = path.edges();
  std::for_each(path_vertices_.begin(), path_vertices_.end(), [this](const VertexAccessor v) { vertices_.insert(v); });
  std::for_each(path_edges_.begin(), path_edges_.end(), [this](const EdgeAccessor e) { edges_.insert(e); });
}

void Graph::InsertVertex(const VertexAccessor &vertex) { vertices_.insert(vertex); }

void Graph::InsertEdge(const EdgeAccessor &edge) { edges_.insert(edge); }

bool Graph::ContainsVertex(const VertexAccessor &vertex) {
  return std::find(begin(vertices_), end(vertices_), vertex) != std::end(vertices_);
}

std::optional<VertexAccessor> Graph::RemoveVertex(const VertexAccessor &vertex) {
  if (!ContainsVertex(vertex)) {
    return std::nullopt;
  }
  auto value = vertices_.erase(vertex);
  if (value == 0) {
    return std::nullopt;
  }
  return vertex;
}

std::optional<EdgeAccessor> Graph::RemoveEdge(const EdgeAccessor &edge) {
  auto value = edges_.erase(edge);
  if (value == 0) {
    return std::nullopt;
  }
  return edge;
}

std::vector<EdgeAccessor> Graph::OutEdges(VertexAccessor vertex_accessor) {
  std::vector<EdgeAccessor> out_edges;
  for (const auto &edge : edges_) {
    if (edge.From() == vertex_accessor) {
      out_edges.emplace_back(edge);
    }
  }
  return out_edges;
}

/** Copy assign other, utils::MemoryResource of `this` is used */
Graph &Graph::operator=(const Graph &) = default;

/** Move assign other, utils::MemoryResource of `this` is used. */
Graph &Graph::operator=(Graph &&) noexcept = default;

Graph::~Graph() = default;

utils::pmr::unordered_set<VertexAccessor> &Graph::vertices() { return vertices_; }
utils::pmr::unordered_set<EdgeAccessor> &Graph::edges() { return edges_; }
const utils::pmr::unordered_set<VertexAccessor> &Graph::vertices() const { return vertices_; }
const utils::pmr::unordered_set<EdgeAccessor> &Graph::edges() const { return edges_; }

utils::MemoryResource *Graph::GetMemoryResource() const { return vertices_.get_allocator().GetMemoryResource(); }

}  // namespace memgraph::query
