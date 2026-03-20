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

#include <functional>
#include <utility>
#include "query/edge_accessor.hpp"
#include "query/typed_value.hpp"
#include "query/vertex_accessor.hpp"
#include "query/virtual_edge.hpp"
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
  using allocator_type = utils::Allocator<Graph>;
  using alloc_traits = std::allocator_traits<allocator_type>;

  /**
   * Create the graph with no elements
   * Allocations are done using the given MemoryResource.
   */
  explicit Graph(allocator_type alloc);

  /**
   * Construct a copy of other.
   * utils::MemoryResource is obtained by calling
   * std::allocator_traits<>::
   *     select_on_container_copy_construction(other.get_allocator()).
   * Since we use utils::Allocator, which does not propagate, this means that we
   * will default to utils::NewDeleteResource().
   */
  Graph(const Graph &other);

  /** Construct a copy using the given utils::MemoryResource */
  Graph(const Graph &other, allocator_type alloc);

  /**
   * Construct with the value of other.
   * utils::MemoryResource is obtained from other. After the move, other will be
   * empty.
   */
  Graph(Graph &&other) noexcept;

  /**
   * Construct with the value of other, but use the given utils::MemoryResource.
   * After the move, other may not be empty if `*memory !=
   * *other.get_allocator()`, because an element-wise move will be
   * performed.
   */
  Graph(Graph &&other, allocator_type alloc);

  /** Expands the graph with the given path. */
  void Expand(const Path &path);

  /** Expand the graph from lists of nodes and edges. Any nulls or duplicate nodes/edges in these lists are ignored.
   */
  void Expand(std::span<TypedValue const> nodes, std::span<TypedValue const> edges);

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

  void InsertVirtualEdge(const VirtualEdge &edge);

  // Insert a virtual edge only if no edge with the same (from, to, type) already exists. Returns true if inserted.
  bool InsertVirtualEdgeIfNew(const VirtualEdge &edge);

  bool ContainsVirtualEdge(const VirtualEdge &edge) const;

  utils::pmr::unordered_set<VirtualEdge> &virtual_edges();
  const utils::pmr::unordered_set<VirtualEdge> &virtual_edges() const;

  /** Copy assign other, utils::MemoryResource of `this` is used */
  Graph &operator=(const Graph &) = default;

  /** Move assign other, utils::MemoryResource of `this` is used. */
  Graph &operator=(Graph &&) noexcept = default;

  ~Graph() = default;

  utils::pmr::unordered_set<VertexAccessor> &vertices();
  utils::pmr::unordered_set<EdgeAccessor> &edges();
  const utils::pmr::unordered_set<VertexAccessor> &vertices() const;
  const utils::pmr::unordered_set<EdgeAccessor> &edges() const;

  auto get_allocator() const -> allocator_type;

 private:
  // Contains all the vertices in the Graph.
  utils::pmr::unordered_set<VertexAccessor> vertices_;
  // Contains all the edges in the Graph
  utils::pmr::unordered_set<EdgeAccessor> edges_;
  // Contains virtual (derived) edges created by project_virtual()
  utils::pmr::unordered_set<VirtualEdge> virtual_edges_;

  struct VirtualEdgeKey {
    storage::Gid from;
    storage::Gid to;
    std::string type;
    bool operator==(const VirtualEdgeKey &) const = default;
  };

  struct VirtualEdgeKeyHash {
    size_t operator()(const VirtualEdgeKey &k) const {
      auto h = std::hash<uint64_t>{}(k.from.AsUint());
      h ^= std::hash<uint64_t>{}(k.to.AsUint()) + 0x9e3779b9 + (h << 6) + (h >> 2);
      h ^= std::hash<std::string>{}(k.type) + 0x9e3779b9 + (h << 6) + (h >> 2);
      return h;
    }
  };

  std::unordered_set<VirtualEdgeKey, VirtualEdgeKeyHash> virtual_edge_dedup_;
};

}  // namespace memgraph::query
