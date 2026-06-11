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

#include "query/virtual_edge.hpp"
#include "query/virtual_node.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/vector.hpp"

namespace memgraph::query {

// The virtual-graph analogue of query::Path: an ordered sequence of VirtualNodes connected by VirtualEdges (one
// more vertex than edges). Produced by named-path matching (`p = ...`) inside a `USE <graph>` subquery, where the
// matched elements are virtual rather than real. Mirrors query::Path's element-append API.
class VirtualPath final {
 public:
  using allocator_type = utils::Allocator<char>;
  using alloc_traits = std::allocator_traits<allocator_type>;

  explicit VirtualPath(allocator_type alloc) : vertices_(alloc), edges_(alloc) {}

  explicit VirtualPath(const VirtualNode &vertex, allocator_type alloc = {}) : vertices_(alloc), edges_(alloc) {
    Expand(vertex);
  }

  VirtualPath(const VirtualPath &other)
      : VirtualPath(other, alloc_traits::select_on_container_copy_construction(other.get_allocator())) {}

  VirtualPath(const VirtualPath &other, allocator_type alloc)
      : vertices_(other.vertices_, alloc), edges_(other.edges_, alloc) {}

  VirtualPath(VirtualPath &&other) noexcept : VirtualPath(std::move(other), other.get_allocator()) {}

  VirtualPath(VirtualPath &&other, allocator_type alloc)
      : vertices_(std::move(other.vertices_), alloc), edges_(std::move(other.edges_), alloc) {}

  VirtualPath &operator=(const VirtualPath &) = default;
  VirtualPath &operator=(VirtualPath &&) = default;
  ~VirtualPath() = default;

  /** Expands the path with the given vertex. */
  void Expand(const VirtualNode &vertex) { vertices_.emplace_back(vertex); }

  /** Expands the path with the given edge. */
  void Expand(const VirtualEdge &edge) { edges_.emplace_back(edge); }

  void Shrink() {
    DMG_ASSERT(!vertices_.empty(), "Vertices should not be empty in the virtual path before shrink.");
    vertices_.pop_back();
    if (!edges_.empty()) edges_.pop_back();
  }

  /** Returns the number of expansions (edges) in this path. */
  auto size() const { return edges_.size(); }

  auto &vertices() { return vertices_; }

  auto &edges() { return edges_; }

  const auto &vertices() const { return vertices_; }

  const auto &edges() const { return edges_; }

  auto get_allocator() const -> allocator_type { return vertices_.get_allocator(); }

  bool operator==(const VirtualPath &other) const { return vertices_ == other.vertices_ && edges_ == other.edges_; }

 private:
  utils::pmr::vector<VirtualNode> vertices_;
  utils::pmr::vector<VirtualEdge> edges_;
};

}  // namespace memgraph::query
