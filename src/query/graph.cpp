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

#include "query/graph.hpp"
#include <ranges>
#include "query/exceptions.hpp"
#include "query/path.hpp"

namespace memgraph::query {

namespace r = std::ranges;
namespace rv = r::views;

Graph::Graph(allocator_type alloc) : vertices_(alloc), edges_(alloc) {}

Graph::Graph(const Graph &other, allocator_type alloc)
    : vertices_(other.vertices_, alloc), edges_(other.edges_, alloc) {}

Graph::Graph(Graph &&other) noexcept : Graph(std::move(other), other.get_allocator()) {}

Graph::Graph(const Graph &other)
    : Graph(other, alloc_traits::select_on_container_copy_construction(other.get_allocator())) {}

Graph::Graph(Graph &&other, allocator_type alloc)
    : vertices_(std::move(other.vertices_), alloc), edges_(std::move(other.edges_), alloc) {}

void Graph::Expand(const Path &path) {
  const auto &path_vertices_ = path.vertices();
  const auto &path_edges_ = path.edges();
  std::for_each(path_vertices_.begin(), path_vertices_.end(), [this](const VertexAccessor v) { vertices_.insert(v); });
  std::for_each(path_edges_.begin(), path_edges_.end(), [this](const EdgeAccessor e) { edges_.insert(e); });
}

void Graph::Expand(std::span<TypedValue const> const nodes, std::span<TypedValue const> const edges) {
  auto actual_nodes = nodes | rv::filter([](auto const &each) { return each.type() == TypedValue::Type::Vertex; }) |
                      rv::transform([](auto const &each) { return each.ValueVertex(); });

  auto actual_edges = edges | rv::filter([](auto const &each) { return each.type() == TypedValue::Type::Edge; }) |
                      rv::transform([](auto const &each) { return each.ValueEdge(); });

  if (r::any_of(actual_edges, [&](auto const &edge) {
        return !r::contains(actual_nodes, edge.From()) || !r::contains(actual_nodes, edge.To());
      })) {
    throw memgraph::query::QueryRuntimeException(
        "Cannot project graph with any projected relationships whose start or end nodes are not also projected.");
  }

  vertices_.insert(actual_nodes.begin(), actual_nodes.end());
  edges_.insert(actual_edges.begin(), actual_edges.end());
}

void Graph::InsertVertex(const VertexAccessor &vertex) { vertices_.insert(vertex); }

void Graph::InsertEdge(const EdgeAccessor &edge) { edges_.insert(edge); }

bool Graph::ContainsVertex(const VertexAccessor &vertex) { return vertices_.contains(vertex); }

bool Graph::ContainsEdge(const EdgeAccessor &edge) { return edges_.contains(edge); }

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

utils::pmr::unordered_set<VertexAccessor> &Graph::vertices() { return vertices_; }
utils::pmr::unordered_set<EdgeAccessor> &Graph::edges() { return edges_; }
const utils::pmr::unordered_set<VertexAccessor> &Graph::vertices() const { return vertices_; }
const utils::pmr::unordered_set<EdgeAccessor> &Graph::edges() const { return edges_; }

auto Graph::get_allocator() const -> allocator_type { return vertices_.get_allocator(); }

}  // namespace memgraph::query
