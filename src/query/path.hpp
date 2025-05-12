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

#pragma once

#include <functional>
#include <utility>

#include "query/edge_accessor.hpp"
#include "query/vertex_accessor.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/vector.hpp"

namespace memgraph::query {

/**
 *  A data structure that holds a graph path. A path consists of at least one
 * vertex, followed by zero or more edge + vertex extensions (thus having one
 * vertex more then edges).
 */
class Path {
 public:
  /** Allocator type so that STL containers are aware that we need one */
  using allocator_type = utils::Allocator<char>;
  using alloc_traits = std::allocator_traits<allocator_type>;

  /**
   * Create the path with no elements
   * Allocations are done using the given MemoryResource.
   */
  explicit Path(allocator_type alloc) : vertices_(alloc), edges_(alloc) {}

  /**
   * Create the path starting with the given vertex.
   * Allocations are done using the given MemoryResource.
   */
  explicit Path(const VertexAccessor &vertex, allocator_type alloc = {}) : vertices_(alloc), edges_(alloc) {
    Expand(vertex);
  }

  /**
   * Create the path starting with the given vertex and containing all other
   * elements.
   * Allocations are done using the default utils::NewDeleteResource().
   */
  template <typename... TOthers>
  requires(!std::is_convertible_v<std::remove_cvref_t<TOthers>, allocator_type> &&
           ...) explicit Path(const VertexAccessor &vertex, TOthers &&...others)
      : vertices_(utils::NewDeleteResource()), edges_(utils::NewDeleteResource()) {
    Expand(vertex);
    Expand(std::forward<TOthers>(others)...);
  }

  /**
   * Create the path starting with the given vertex and containing all other
   * elements.
   * Allocations are done using the given MemoryResource.
   */
  template <typename... TOthers>
  Path(std::allocator_arg_t, allocator_type alloc, const VertexAccessor &vertex, TOthers &&...others)
      : vertices_(alloc), edges_(alloc) {
    Expand(vertex);
    Expand(std::forward<TOthers>(others)...);
  }

  /**
   * Construct a copy of other.
   * utils::MemoryResource is obtained by calling
   * std::allocator_traits<>::
   *     select_on_container_copy_construction(other.get_allocator()).
   * Since we use utils::Allocator, which does not propagate, this means that we
   * will default to utils::NewDeleteResource().
   */
  Path(const Path &other) : Path(other, alloc_traits::select_on_container_copy_construction(other.get_allocator())) {}

  /** Construct a copy using the given utils::MemoryResource */
  Path(const Path &other, allocator_type alloc) : vertices_(other.vertices_, alloc), edges_(other.edges_, alloc) {}

  /**
   * Construct with the value of other.
   * utils::MemoryResource is obtained from other. After the move, other will be
   * empty.
   */
  Path(Path &&other) noexcept : Path(std::move(other), other.get_allocator()) {}

  /**
   * Construct with the value of other, but use the given utils::MemoryResource.
   * After the move, other may not be empty if `*memory !=
   * *other.get_allocator()`, because an element-wise move will be
   * performed.
   */
  Path(Path &&other, allocator_type alloc)
      : vertices_(std::move(other.vertices_), alloc), edges_(std::move(other.edges_), alloc) {}

  /** Copy assign other, utils::MemoryResource of `this` is used */
  Path &operator=(const Path &) = default;

  /** Move assign other, utils::MemoryResource of `this` is used. */
  Path &operator=(Path &&) = default;

  ~Path() = default;

  /** Expands the path with the given vertex. */
  void Expand(VertexAccessor vertex) {
    DMG_ASSERT(vertices_.size() == edges_.size(), "Illegal path construction order");
    DMG_ASSERT(edges_.empty() || (!edges_.empty() && (edges_.back().To().Gid() == vertex.Gid() ||
                                                      edges_.back().From().Gid() == vertex.Gid())),
               "Illegal path construction order");
    vertices_.emplace_back(std::move(vertex));
  }

  /** Expands the path with the given edge. */
  void Expand(EdgeAccessor edge) {
    DMG_ASSERT(vertices_.size() - 1 == edges_.size(), "Illegal path construction order");
    DMG_ASSERT(vertices_.back().Gid() == edge.From().Gid() || vertices_.back().Gid() == edge.To().Gid(),
               "Illegal path construction order");
    edges_.emplace_back(std::move(edge));
  }

  /** Expands the path with the given elements. */
  template <typename TFirst, typename... TOthers>
  void Expand(TFirst &&first, TOthers &&...others) {
    Expand(std::forward<TFirst>(first));
    Expand(std::forward<TOthers>(others)...);
  }

  void Shrink() {
    DMG_ASSERT(!vertices_.empty(), "Vertices should not be empty in the path before shrink.");
    vertices_.pop_back();
    if (!edges_.empty()) {
      edges_.pop_back();
    }
  }

  /** Returns the number of expansions (edges) in this path. */
  auto size() const { return edges_.size(); }

  auto &vertices() { return vertices_; }
  auto &edges() { return edges_; }
  const auto &vertices() const { return vertices_; }
  const auto &edges() const { return edges_; }

  auto get_allocator() const -> allocator_type { return vertices_.get_allocator(); }

  bool operator==(const Path &other) const { return vertices_ == other.vertices_ && edges_ == other.edges_; }

 private:
  // Contains all the vertices in the path.
  utils::pmr::vector<VertexAccessor> vertices_;
  // Contains all the edges in the path (one less then there are vertices).
  utils::pmr::vector<EdgeAccessor> edges_;
};

}  // namespace memgraph::query
