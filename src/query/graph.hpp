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

#pragma once

#include <functional>
#include <utility>

#include "query/db_accessor.hpp"
#include "query/path.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/vector.hpp"

namespace memgraph::query {

/**
 *  A data structure that holds a graph. A graph consists of at least one
 * vertex, and zero or more edges.
 */
class Graph {
 public:
  /** Allocator type so that STL containers are aware that we need one */
  using allocator_type = utils::Allocator<char>;

  /**
   * Create the graph with no elements
   * Allocations are done using the given MemoryResource.
   */
  explicit Graph(utils::MemoryResource *memory) : vertices_(memory), edges_(memory) {}

  /** Construct a copy using the given utils::MemoryResource */
  Graph(const Graph &other, utils::MemoryResource *memory)
      : vertices_(other.vertices_, memory), edges_(other.edges_, memory) {}

  /**
   * Construct with the value of other.
   * utils::MemoryResource is obtained from other. After the move, other will be
   * empty.
   */
  Graph(Graph &&other) noexcept : Graph(std::move(other), other.GetMemoryResource()) {}

  /**
   * Construct with the value of other, but use the given utils::MemoryResource.
   * After the move, other may not be empty if `*memory !=
   * *other.GetMemoryResource()`, because an element-wise move will be
   * performed.
   */
  Graph(Graph &&other, utils::MemoryResource *memory)
      : vertices_(std::move(other.vertices_), memory), edges_(std::move(other.edges_), memory) {}

  /** Expands the graph with the given path. */
  void Expand(const Path &path) {
    const auto path_vertices_ = path.vertices();
    std::for_each(path_vertices_.begin(), path_vertices_.end(),
                  [this](const VertexAccessor v) { vertices_.push_back(v); });
  }

  /** Move assign other, utils::MemoryResource of `this` is used. */
  Graph &operator=(Graph &&) = default;

  ~Graph() = default;

  /** Returns the number of expansions (edges) in this path. */
  auto size() const { return edges_.size(); }

  auto &vertices() { return vertices_; }
  auto &edges() { return edges_; }
  const auto &vertices() const { return vertices_; }
  const auto &edges() const { return edges_; }

  utils::MemoryResource *GetMemoryResource() const { return vertices_.get_allocator().GetMemoryResource(); }

 private:
  // Contains all the vertices in the Graph.
  utils::pmr::vector<VertexAccessor> vertices_;
  // Contains all the edges in the Graph
  utils::pmr::vector<EdgeAccessor> edges_;
};

}  // namespace memgraph::query
