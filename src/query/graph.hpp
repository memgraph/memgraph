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
  explicit Graph(utils::MemoryResource *memory = utils::NewDeleteResource()) : vertices_(memory), edges_(memory) {}

  /**
   * Create the graph starting with the given vertex.
   * Allocations are done using the given MemoryResource.
   */
  explicit Graph(const VertexAccessor &vertex, utils::MemoryResource *memory = utils::NewDeleteResource())
      : vertices_(memory), edges_(memory) {}

  /** Expands the graph with the given path. */
  void Expand(const Path &path) {
    const auto path_vertices_ = path.vertices();
    std::for_each(path_vertices_.begin(), path_vertices_.end(),
                  [this](const VertexAccessor v) { vertices_.push_back(v); });
  }

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
