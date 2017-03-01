//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 20.02.17.
//

#pragma once

#include <iostream>
#include <list>
#include <memory>

#include "enums.hpp"
#include "path.hpp"
#include "templates.hpp"

/**
 * A specialization of the "traversal" namespace that uses
 * test Vertex and Edge classes (as opposed to the real database
 * ones).
 */
namespace traversal_test {

// add the standard enums to this namespace
using Direction = traversal_template::Direction;
using Expansion = traversal_template::Expansion;
using Uniqueness = traversal_template::Uniqueness;

// forward declaring Edge because Vertex holds references to it
class Edge;

/**
 * A Vertex class for traversal testing. Has only the
 * traversal capabilities and an exposed id_ member.
 */
class Vertex {
  friend class Edge;

public:
  // unique identifier (uniqueness guaranteed by this class)
  const int id_;

  // vertex set by the user, for testing purposes
  const int label_;

  Vertex(int label=0) : id_(counter_++), label_(label) {}

  const auto &in() const { return *in_; }
  const auto &out() const { return *out_; }

  friend std::ostream &operator<<(std::ostream &stream, const Vertex &v) {
    return stream << "(" << v.id_ << ")";
  }

  bool operator==(const Vertex &v) const { return this->id_ == v.id_; }
  bool operator!=(const Vertex &v) const { return !(*this == v); }

private:
  // shared pointers to outgoing and incoming edges are used so that when
  // a Vertex is copied it still points to the same connectivity storage
  std::shared_ptr<std::vector<Edge>> in_ = std::make_shared<std::vector<Edge>>();
  std::shared_ptr<std::vector<Edge>> out_ = std::make_shared<std::vector<Edge>>();

  // Vertex counter, used for generating IDs
  static int counter_;

};

int Vertex::counter_ = 0;

/**
 * An Edge class for traversal testing. Has only traversal capabilities
 * and an exposed id_ member.
 */
class Edge {
public:
  // unique identifier (uniqueness guaranteed by this class)
  const int id_;

  // vertex set by the user, for testing purposes
  const int type_;

  Edge(const Vertex &from, const Vertex &to, int type=0) :
      id_(counter_++), from_(from), to_(to), type_(type) {
    from.out_->emplace_back(*this);
    to.in_->emplace_back(*this);
  }

  const Vertex &from() const { return from_; }
  const Vertex &to() const { return to_; }

  bool operator==(const Edge &e) const { return id_ == e.id_; }
  bool operator!=(const Edge &e) const { return !(*this == e); }

  friend std::ostream &operator<<(std::ostream &stream, const Edge &e) {
    return stream << "[" << e.id_ << "]";
  }

private:
  const Vertex &from_;
  const Vertex &to_;

  // Edge counter, used for generating IDs
  static int counter_;
};

int Edge::counter_ = 0;

// expose Path and Paths as class template instantiations
using Path = traversal_template::Path<Vertex, Edge>;
using Paths = traversal_template::Paths<Vertex, Edge>;

/**
 * Specialization of the traversal_template::Begin function.
 */
template<typename TCollection>
auto Begin(const TCollection &vertices, std::function<bool(const Vertex &)> vertex_filter = {}) {
  return traversal_template::Begin<TCollection, Vertex, Edge>(vertices, vertex_filter);
}

/**
 * Specialization of the traversal_template::Cartesian function that accepts
 * a single argument.
 */
template<typename TVisitable>
auto Cartesian(TVisitable &&visitable) {
  return traversal_template::Cartesian<TVisitable, Vertex, Edge>(
      std::forward<TVisitable>(visitable));
}


/**
 * Specialization of the traversal_template::Cartesian function that accepts
 * multiple arguments.
 */
 // TODO (code-review) can this be specialized more elegantly?
template<typename TVisitableFirst, typename... TVisitableOthers>
auto Cartesian(TVisitableFirst &&first, TVisitableOthers &&... others) {
  return traversal_template::CartesianBinaryType<TVisitableFirst, decltype(Cartesian(
      std::forward<TVisitableOthers>(others)...)),
      Vertex, Edge>(
      std::forward<TVisitableFirst>(first),
      Cartesian(std::forward<TVisitableOthers>(others)...)
  );
}
}

/**
 * Hash calculations for text vertex, edge, Path and Paths.
 * Only for testing purposes.
 */
namespace std{

template <>
class hash<traversal_test::Vertex> {
public:
  size_t operator()(const traversal_test::Vertex &vertex) const {
    return (size_t) vertex.id_;
  }
};

template <>
class hash<traversal_test::Edge> {
public:
  size_t operator()(const traversal_test::Edge &edge) const {
    return (size_t) edge.id_;
  }
};

template <>
class hash<traversal_test::Path> {
public:
  std::size_t operator()(const traversal_test::Path &path) const {
    std::size_t r_val = 0;

    for (const auto &vertex : path.Vertices())
      r_val ^= std::hash<traversal_test::Vertex>{}(vertex);

    return r_val;
  }
};

template <>
class hash<traversal_test::Paths> {
public:
  std::size_t operator()(const traversal_test::Paths &paths) const {
    std::size_t r_val = 0;

    for (const auto &path : paths)
      r_val ^= std::hash<traversal_test::Path>{}(path);

    return r_val;
  }
};
}

