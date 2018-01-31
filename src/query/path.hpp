#pragma once

#include <functional>
#include <utility>

#include "glog/logging.h"

#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"

namespace query {

/**
 *  A data structure that holds a graph path. A path consists of at least one
 * vertex, followed by zero or more edge + vertex extensions (thus having one
 * vertex more then edges).
 */
class Path {
 public:
  /** Creates the path starting with the given vertex. */
  explicit Path(const VertexAccessor &vertex) { Expand(vertex); }

  /** Creates the path starting with the given vertex and containing all other
   * elements. */
  template <typename... TOthers>
  Path(const VertexAccessor &vertex, const TOthers &... others) {
    Expand(vertex);
    Expand(others...);
  }

  /** Expands the path with the given vertex. */
  void Expand(const VertexAccessor &vertex) {
    DCHECK(vertices_.size() == edges_.size())
        << "Illegal path construction order";
    vertices_.emplace_back(vertex);
  }

  /** Expands the path with the given edge. */
  void Expand(const EdgeAccessor &edge) {
    DCHECK(vertices_.size() - 1 == edges_.size())
        << "Illegal path construction order";
    edges_.emplace_back(edge);
  }

  /** Expands the path with the given elements. */
  template <typename TFirst, typename... TOthers>
  void Expand(const TFirst &first, const TOthers &... others) {
    Expand(first);
    Expand(others...);
  }

  /** Returns the number of expansions (edges) in this path. */
  auto size() const { return edges_.size(); }

  auto &vertices() { return vertices_; }
  auto &edges() { return edges_; }
  const auto &vertices() const { return vertices_; }
  const auto &edges() const { return edges_; }

  bool operator==(const Path &other) const {
    return vertices_ == other.vertices_ && edges_ == other.edges_;
  }

  friend std::ostream &operator<<(std::ostream &os, const Path &path) {
    DCHECK(path.vertices_.size() > 0U)
        << "Attempting to stream out an invalid path";
    os << path.vertices_[0];
    for (int i = 0; i < static_cast<int>(path.edges_.size()); i++) {
      bool arrow_to_left = path.vertices_[i] == path.edges_[i].to();
      if (arrow_to_left) os << "<";
      os << "-" << path.edges_[i] << "-";
      if (!arrow_to_left) os << ">";
      os << path.vertices_[i + 1];
    }

    return os;
  }

  /// Calls SwitchNew on all the elements of the path.
  void SwitchNew() {
    for (auto &v : vertices_) v.SwitchNew();
    for (auto &e : edges_) e.SwitchNew();
  }

  /// Calls SwitchNew on all the elements of the path.
  void SwitchOld() {
    for (auto &v : vertices_) v.SwitchOld();
    for (auto &e : edges_) e.SwitchOld();
  }

 private:
  // Contains all the vertices in the path.
  std::vector<VertexAccessor> vertices_;
  // Contains all the edges in the path (one less then there are vertices).
  std::vector<EdgeAccessor> edges_;
};
}  // namespace query
