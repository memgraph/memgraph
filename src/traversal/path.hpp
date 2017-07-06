//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 23.02.17.
//

#pragma once

#include <algorithm>
#include <functional>
#include <list>

#include "enums.hpp"
#include "utils/assert.hpp"

/**
 * For a documentation of this namespace and it's
 * intended usage (namespace specialization) take
 * a look at the docs in "templates.hpp".
 */
namespace traversal_template {

/**
 * A path containing zero or more vertices. Between
 * each two vertices is an edge.
 *
 * @tparam TVertex
 * @tparam TEdge
 */
template <typename TVertex, typename TEdge>
class Path {
 public:
  Path() {}

  size_t Size() const { return vertices_.size(); }

  friend std::ostream &operator<<(std::ostream &stream, const Path &path) {
    auto vertices_it = path.vertices_.begin();
    auto vertices_end = path.vertices_.end();
    auto edges_it = path.edges_.begin();

    if (vertices_it != vertices_end) stream << *vertices_it++;

    while (vertices_it != vertices_end)

      // current vertex has incoming if it is
      if (edges_it->to() == *vertices_it)
        stream << "-" << *edges_it++ << "->" << *vertices_it++;
      else
        stream << "<-" << *edges_it++ << "-" << *vertices_it++;

    return stream;
  }

  bool operator==(const Path &other) const {
    return vertices_ == other.vertices_ && edges_ == other.edges_;
  }

  bool operator!=(const Path &other) const { return !(*this == other); }

  /**
   * Starts the path. At this moment the Path must be empty.
   *
   * @return A reference to this same path.
   */
  Path &Start(const TVertex &v) {
    debug_assert(vertices_.size() == 0,
                 "Can only start iteration on empty path");
    vertices_.push_back(v);
    return *this;
  }

  /**
   * Checks if the given vertex is contained in this path.
   */
  bool Contains(const TVertex &vertex) const {
    for (const auto &v : vertices_)
      if (v == vertex) return true;

    return false;
  }

  /**
   * Checks if the given edge is contained in this path.
   */
  bool Contains(const TEdge &edge) const {
    for (const auto &e : edges_)
      if (e == edge) return true;

    return false;
  }

  /**
   * Gets the last Vertex of this path. Fails if the path contains no elements.
   */
  const TVertex &Back() const {
    debug_assert(vertices_.size() > 0,
                 "Can only get a Vertex on non-empty path");
    return vertices_.back();
  }

  /**
   * Appends a traversal to the end of this path.
   *
   * @param edge  The edge along with the traversal happens.
   * @param vertex  The new vertex to append to the path.
   * @param direction  The direction along which the traversal happens
   *    (relative to the currently last vertex in path).
   * @return A reference to this same path.
   */
  Path &Append(const TEdge &edge, const TVertex &vertex) {
    edges_.emplace_back(edge);
    vertices_.emplace_back(vertex);
    return *this;
  }

  /**
   * Removes the last element from the path. Fails if the path contains no
   * elements.
   */
  void PopBack() {
    debug_assert(vertices_.size() > 0,
                 "Can only remove a vertex from a non-empty path");
    vertices_.pop_back();

    if (vertices_.size() > 0) edges_.pop_back();
  }

  /**
   * Gets the first Vertex of this path. Fails if the path contains no elements.
   */
  const TVertex &Front() const {
    debug_assert(vertices_.size() > 0,
                 "Can only get a vertex from a non-empty path");
    return vertices_.front();
  }

  /**
   * Prepends a traversal to the beginning of this path.
   *
   * @param edge  The edge along with the traversal happens.
   * @param vertex  The new vertex to prepend to the path.
   * @return A reference to this same path.
   */
  Path &Prepend(const TEdge &edge, const TVertex &vertex) {
    edges_.emplace_front(edge);
    vertices_.emplace_front(vertex);
    return *this;
  }

  /**
   * Removes the first element from the path. Fails if the path contains no
   * elements.
   */
  void PopFront() {
    debug_assert(vertices_.size() > 0,
                 "Can only remove a vertex from a non-empty path");
    vertices_.pop_front();

    if (vertices_.size() > 0) edges_.pop_front();
  }

  /**
   * Removes all the elements from the path.
   */
  void Clear() {
    edges_.clear();
    vertices_.clear();
  }

  /**
   * Returns all the vertices in this path.
   */
  const auto &Vertices() const { return vertices_; }

  /**
   * Returns all the edges in this path.
   */
  const auto &Edges() const { return edges_; }

 private:
  std::list<TVertex> vertices_;
  std::list<TEdge> edges_;
};

/**
 * An ordered sequence of Path reference wrappers. Used when
 * visiting cartesian products of traversals.
 */
// TODO review: do we like std::list inheritance (and why)?
// alternatively we could do:
//    A. using Paths = std::list<std::reference_wrapper<Path>>;
//    B. encapsulation
template <typename TVertex, typename TEdge>
class Paths : public std::list<std::reference_wrapper<Path<TVertex, TEdge>>> {
  using Path = Path<TVertex, TEdge>;

 public:
  bool operator==(const Paths<TVertex, TEdge> &other) const {
    return std::equal(this->begin(), this->end(), other.begin(),
                      [](const std::reference_wrapper<Path> &p1,
                         const std::reference_wrapper<Path> &p2) {
                        return p1.get() == p2.get();
                      });
  }

  bool operator!=(const Paths &other) const { return !(*this == other); }

  friend std::ostream &operator<<(std::ostream &stream, const Paths &paths) {
    stream << "[";
    auto it = paths.begin();
    auto end = paths.end();

    if (it != end) stream << *it++;
    while (it != end) stream << ", " << *it++;
    return stream << "]";
  }
};
}
