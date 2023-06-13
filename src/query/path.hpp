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

  /**
   * Create the path with no elements
   * Allocations are done using the given MemoryResource.
   */
  explicit Path(utils::MemoryResource *memory) : vertices_(memory), edges_(memory) {}

  /**
   * Create the path starting with the given vertex.
   * Allocations are done using the given MemoryResource.
   */
  explicit Path(const VertexAccessor &vertex, utils::MemoryResource *memory = utils::NewDeleteResource())
      : vertices_(memory), edges_(memory) {
    Expand(vertex);
  }

  /**
   * Create the path starting with the given vertex and containing all other
   * elements.
   * Allocations are done using the default utils::NewDeleteResource().
   */
  template <typename... TOthers>
  explicit Path(const VertexAccessor &vertex, const TOthers &...others)
      : vertices_(utils::NewDeleteResource()), edges_(utils::NewDeleteResource()) {
    Expand(vertex);
    Expand(others...);
  }

  /**
   * Create the path starting with the given vertex and containing all other
   * elements.
   * Allocations are done using the given MemoryResource.
   */
  template <typename... TOthers>
  Path(std::allocator_arg_t, utils::MemoryResource *memory, const VertexAccessor &vertex, const TOthers &...others)
      : vertices_(memory), edges_(memory) {
    Expand(vertex);
    Expand(others...);
  }

  /**
   * Construct a copy of other.
   * utils::MemoryResource is obtained by calling
   * std::allocator_traits<>::
   *     select_on_container_copy_construction(other.GetMemoryResource()).
   * Since we use utils::Allocator, which does not propagate, this means that we
   * will default to utils::NewDeleteResource().
   */
  Path(const Path &other)
      : Path(other,
             std::allocator_traits<allocator_type>::select_on_container_copy_construction(other.GetMemoryResource())
                 .GetMemoryResource()) {}

  /** Construct a copy using the given utils::MemoryResource */
  Path(const Path &other, utils::MemoryResource *memory)
      : vertices_(other.vertices_, memory), edges_(other.edges_, memory) {}

  /**
   * Construct with the value of other.
   * utils::MemoryResource is obtained from other. After the move, other will be
   * empty.
   */
  Path(Path &&other) noexcept : Path(std::move(other), other.GetMemoryResource()) {}

  /**
   * Construct with the value of other, but use the given utils::MemoryResource.
   * After the move, other may not be empty if `*memory !=
   * *other.GetMemoryResource()`, because an element-wise move will be
   * performed.
   */
  Path(Path &&other, utils::MemoryResource *memory)
      : vertices_(std::move(other.vertices_), memory), edges_(std::move(other.edges_), memory) {}

  /** Copy assign other, utils::MemoryResource of `this` is used */
  Path &operator=(const Path &) = default;

  /** Move assign other, utils::MemoryResource of `this` is used. */
  Path &operator=(Path &&) = default;

  ~Path() = default;

  /** Expands the path with the given vertex. */
  void Expand(const VertexAccessor &vertex) {
    DMG_ASSERT(vertices_.size() == edges_.size(), "Illegal path construction order");
    vertices_.emplace_back(vertex);
  }

  /** Expands the path with the given edge. */
  void Expand(const EdgeAccessor &edge) {
    DMG_ASSERT(vertices_.size() - 1 == edges_.size(), "Illegal path construction order");
    edges_.emplace_back(edge);
  }

  /** Expands the path with the given elements. */
  template <typename TFirst, typename... TOthers>
  void Expand(const TFirst &first, const TOthers &...others) {
    Expand(first);
    Expand(others...);
  }

  /** Returns the number of expansions (edges) in this path. */
  auto size() const { return edges_.size(); }

  auto &vertices() { return vertices_; }
  auto &edges() { return edges_; }
  const auto &vertices() const { return vertices_; }
  const auto &edges() const { return edges_; }

  utils::MemoryResource *GetMemoryResource() const { return vertices_.get_allocator().GetMemoryResource(); }

  bool operator==(const Path &other) const { return vertices_ == other.vertices_ && edges_ == other.edges_; }

 private:
  // Contains all the vertices in the path.
  utils::pmr::vector<VertexAccessor> vertices_;
  // Contains all the edges in the path (one less than there are vertices).
  utils::pmr::vector<EdgeAccessor> edges_;
};

}  // namespace memgraph::query
