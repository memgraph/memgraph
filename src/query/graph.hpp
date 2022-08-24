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
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/unordered_set.hpp"
#include "utils/pmr/vector.hpp"

namespace memgraph::query {

class Path;
/**
 *  A data structure that holds a graph. A graph consists of at least one
 * vertex, and zero or more edges.
 */
class Graph final {
 public:
  /** Allocator type so that STL containers are aware that we need one */
  using allocator_type = utils::Allocator<char>;

  /**
   * Create the graph with no elements
   * Allocations are done using the given MemoryResource.
   */
  explicit Graph(utils::MemoryResource *memory);

  /**
   * Construct a copy of other.
   * utils::MemoryResource is obtained by calling
   * std::allocator_traits<>::
   *     select_on_container_copy_construction(other.GetMemoryResource()).
   * Since we use utils::Allocator, which does not propagate, this means that we
   * will default to utils::NewDeleteResource().
   */
  Graph(const Graph &other);

  /** Construct a copy using the given utils::MemoryResource */
  Graph(const Graph &other, utils::MemoryResource *memory);

  /**
   * Construct with the value of other.
   * utils::MemoryResource is obtained from other. After the move, other will be
   * empty.
   */
  Graph(Graph &&other) noexcept;

  /**
   * Construct with the value of other, but use the given utils::MemoryResource.
   * After the move, other may not be empty if `*memory !=
   * *other.GetMemoryResource()`, because an element-wise move will be
   * performed.
   */
  Graph(Graph &&other, utils::MemoryResource *memory);

  /** Expands the graph with the given path. */
  void Expand(const Path &path);

  /** Inserts the vertex in the graph. */
  void InsertVertex(const VertexAccessor &vertex);

  /** Inserts the edge in the graph. */
  void InsertEdge(const EdgeAccessor &edge);

  /** Checks whether the graph contains the vertex. */
  bool ContainsVertex(const VertexAccessor &vertex);

  /** Checks whether the graph contains the edge. */
  bool ContainsEdge(const EdgeAccessor &edge);

  /** Removes the vertex from the graph if the vertex is in the graph. */
  std::optional<VertexAccessor> RemoveVertex(const VertexAccessor &vertex);

  /** Removes the vertex from the graph if the vertex is in the graph. */
  std::optional<EdgeAccessor> RemoveEdge(const EdgeAccessor &edge);

  /** Return the out edges of the given vertex. */
  std::vector<EdgeAccessor> OutEdges(VertexAccessor vertex_accessor);

  /** Copy assign other, utils::MemoryResource of `this` is used */
  Graph &operator=(const Graph &);

  /** Move assign other, utils::MemoryResource of `this` is used. */
  Graph &operator=(Graph &&) noexcept;

  ~Graph();

  utils::pmr::unordered_set<VertexAccessor> &vertices();
  utils::pmr::unordered_set<EdgeAccessor> &edges();
  const utils::pmr::unordered_set<VertexAccessor> &vertices() const;
  const utils::pmr::unordered_set<EdgeAccessor> &edges() const;

  utils::MemoryResource *GetMemoryResource() const;

 private:
  // Contains all the vertices in the Graph.
  utils::pmr::unordered_set<VertexAccessor> vertices_;
  // Contains all the edges in the Graph
  utils::pmr::unordered_set<EdgeAccessor> edges_;
};

}  // namespace memgraph::query
