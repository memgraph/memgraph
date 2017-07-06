//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 20.02.17.
//

#pragma once

#include <functional>
#include <list>

#include "enums.hpp"
#include "path.hpp"
#include "utils/assert.hpp"

/**
 * This namespace contains traversal class templates that must
 * be parameterized with Vertex and Edge classes. This abstraction
 * is made so that the traversal API can easily be used with real
 * MemgraphDB Vertex and Edge classes, as well as mock classes used
 * for testing independently of the DB engine.
 *
 * For an overview of the traversal API-s architecture (and a how-to-use)
 * consult the Memgraph Wiki.
 *
 * The classes have the following templates:
 *      TVertex - vertex type. Provides functions for getting
 *          incoming and outgoing relationship TEdge objects.
 *      TEdge - edge type. Provides functions for getting
 *          origin and destination TVertex objects.
 *      TIterable - a class given to Begin. Can be iterated over
 *          (iteration provides Vertex references).
 *      TVisitable - a class that provides the Visit function
 *          that accepts a single std::function<void(Path &p)>
 *          argument. The path traversal classes here all conform
 *          to this definition, and can therefore be chained.
 */
namespace traversal_template {

/**
 * An object that enables Vertex and Edge uniqueness checks
 * across multiple paths (multiple interdependent UniquenessGroups).
 *
 * It is intended that only the BeginType keeps a UniquenessGroup
 * object and all of it's expansions only provide access to it.
 *
 * @tparam TVertex
 * @tparam TEdge
 */
template <typename TVertex, typename TEdge>
class UniquenessGroup {
 public:
  UniquenessGroup(const Path<TVertex, TEdge> &path) : current_path_(path) {}

  /**
   * Checks if this group or any of it's
   * subgroups contains the given vertex.
   *
   * @param vertex
   * @return
   */
  bool Contains(const TVertex &vertex) const {
    if (current_path_.Contains(vertex)) return true;

    for (const auto &group : subgroups_)
      if (group.get().Contains(vertex)) return true;

    return false;
  }

  /**
   * Checks if this group or any of it's
   * subgroups contains the given edge.
   *
   * @param vertex
   * @return
   */
  bool Contains(const TEdge &edge) const {
    if (current_path_.Contains(edge)) return true;

    for (const auto &group : subgroups_)
      if (group.get().Contains(edge)) return true;

    return false;
  }

  /**
   * Adds the given UniquenessGroup to this groups collection
   * of subgroups.
   *
   * @param subgroup
   */
  void Add(const UniquenessGroup<TVertex, TEdge> &subgroup) {
    subgroups_.emplace_back(subgroup);
  }

 private:
  // the currently traversed path of this uniqueness group
  // set by the BeginType
  const Path<TVertex, TEdge> &current_path_;

  std::vector<std::reference_wrapper<const UniquenessGroup<TVertex, TEdge>>>
      subgroups_;
};

/**
 * Base class for path expansion. Provides functionalities common
 * to both one-relationship expansions and variable-number expansions.
 *
 * Template parameters are documented in the namespace documentation,
 * as they are the same for many classes.
 *
 * @tparam TVisitable
 * @tparam TVertex
 * @tparam TEdge
 */
template <typename TVisitable, typename TVertex, typename TEdge>
class ExpandBaseType {
  using TPath = Path<TVertex, TEdge>;
  using VertexFilter = std::function<bool(const TVertex &)>;
  using EdgeFilter = std::function<bool(const TEdge &)>;

 public:
  /**
   * @return This expander's visitable's uniqueness group.
   */
  UniquenessGroup<TVertex, TEdge> &UniquenessGroup() {
    return visitable_.UniquenessGroup();
  }

 protected:
  // tracking last appended path elements during traversal
  TVertex const *current_vertex_ = nullptr;
  TEdge const *current_edge_ = nullptr;

  // for docs on all member variables consult the only constructor
  TVisitable &visitable_;
  const Expansion expansion_;
  const Direction direction_;
  const VertexFilter vertex_filter_;
  const EdgeFilter edge_filter_;
  const Uniqueness uniqueness_;

  /**
   * @param visitable The visitable that provides paths to expand from.
   * @param expansion Indicates how the path should be expanded. Consult
   *    the enum documentation for an explanation.
   * @param direction Indicates which relationships are used for expansion
   *    from a vertex.
   * @param vertex_filter A function that accepts or rejects a Vertex that
   *    would get added to the path in this expansion. A path is only
   *    generated and visited if this function returns true (or is not
   *    provided).
   * @param edge_filter A function that accepts or rejects an Edge that
   *    would get added to the path in this expansion. A path is only
   *    generated and visited if this function returns true (or is not
   *    provided).
   * @param uniqueness Which kind of uniqueness should be applied.
   */
  ExpandBaseType(TVisitable &&visitable, Expansion expansion,
                 Direction direction, VertexFilter vertex_filter,
                 EdgeFilter edge_filter, Uniqueness uniqueness)
      : visitable_(std::forward<TVisitable>(visitable)),
        expansion_(expansion),
        direction_(direction),
        vertex_filter_(vertex_filter),
        edge_filter_(edge_filter),
        uniqueness_(uniqueness) {}

  /**
   * Visits the given visitor with every expansion of the given path
   * that is in-line with this object's expansion-control members
   * (expansion type and direction).
   *
   * @param visitor
   * @param p
   */
  void VisitExpansions(std::function<void(TPath &)> visitor, TPath &p) {
    // the start or end point of the vertex
    const auto &origin_vertex =
        expansion_ == Expansion::Back ? p.Back() : p.Front();

    if (direction_ == Direction::In || direction_ == Direction::Both)
      VisitExpansions(origin_vertex.in(), p, visitor, Direction::In);
    if (direction_ == Direction::Out || direction_ == Direction::Both)
      VisitExpansions(origin_vertex.out(), p, visitor, Direction::Out);
  }

 private:
  /**
   * Helper method that handles path expansion and visiting w.r.t.
   * expansion params.
   *
   * NOTE: This function accepts the Direction as a param (it does
   * NOT use the direction_ member) so that
   * Direction::Both can be handled with two calls to this function
   * (for ::In and ::Out).
   *
   * @tparam TVertices
   * @param vertices
   * @param p
   * @param visitor
   * @param direction
   */
  template <typename Edges>
  void VisitExpansions(Edges &edges, TPath &p,
                       std::function<void(TPath &)> visitor,
                       Direction direction) {
    for (const TEdge &e : edges) {
      // edge filtering and uniqueness
      if (edge_filter_ && !edge_filter_(e)) continue;
      if (uniqueness_ == Uniqueness::Edge && UniquenessGroup().Contains(e))
        continue;

      // vertex filtering and uniqueness
      const TVertex &v = (direction == Direction::In) ? e.from() : e.to();
      if (vertex_filter_ && !vertex_filter_(v)) continue;
      if (uniqueness_ == Uniqueness::Vertex && UniquenessGroup().Contains(v))
        continue;

      current_edge_ = &e;
      current_vertex_ = &v;

      switch (expansion_) {
        case Expansion::Front:
          p.Prepend(e, v);
          visitor(p);
          p.PopFront();
          break;
        case Expansion::Back:
          p.Append(e, v);
          visitor(p);
          p.PopBack();
          break;
      }

      current_edge_ = nullptr;
      current_vertex_ = nullptr;
    }
  }
};

/**
 * Expansion along a variable number of traversals.
 *
 * Template parameters are documented in the namespace documentation,
 * as they are the same for many classes.
 *
 * @tparam TVisitable
 * @tparam TVertex
 * @tparam TEdge
 */
template <typename TVisitable, typename TVertex, typename TEdge>
class ExpandVariableType : public ExpandBaseType<TVisitable, TVertex, TEdge> {
  using TPath = Path<TVertex, TEdge>;
  using VertexFilter = std::function<bool(const TVertex &)>;
  using EdgeFilter = std::function<bool(const TEdge &)>;

 public:
  /**
   * For most params see the ExpandBaseType::ExpandBaseType documentation.
   *
   * @param min_length Minimum number of vertices in a path for it to be
   *    visited. Inclusive.
   * @param max_length Maximum number of vertices in a path for it to be
   *    visited. Exclusive.
   */
  ExpandVariableType(TVisitable &&visitable, Expansion expansion,
                     Direction direction, VertexFilter vertex_filter,
                     EdgeFilter edge_filter, int min_length, int max_length,
                     Uniqueness uniqueness)
      : ExpandBaseType<TVisitable, TVertex, TEdge>(
            std::forward<TVisitable>(visitable), expansion, direction, {},
            edge_filter, uniqueness),
        min_length_(min_length),
        max_length_(max_length),
        current_vertex_filter_(vertex_filter) {}

  /**
   * Calls the given visitor function once for every path this traversal
   * generates.
   *
   * @param visitor
   */
  void Visit(std::function<void(TPath &)> visitor) {
    this->visitable_.Visit(
        [this, &visitor](TPath &p) { VisitRecursive(visitor, p, p.Size()); });
  }

  /**
   * Expands from this expansion along one traversal.
   * Arguments are simply passed to the expansion constructor.
   *
   * @return An expansion that generates paths one traversal longer.
   */
  auto Expand(Expansion expansion, Direction direction,
              VertexFilter vertex_filter = {}, EdgeFilter edge_filter = {});

  /**
   * Expands from this expansion along a variable number traversal.
   * Arguments are simply passed to the variable expansion constructor.
   *
   * @return An expansion that generates paths variable length longer.
   */
  auto ExpandVariable(Expansion expansion, Direction direction,
                      VertexFilter vertex_filter = {},
                      EdgeFilter edge_filter = {}, int min_length = 0,
                      int max_length = 1000);

  /**
   * Returns a reference to the vertex currently being traversed
   * by this part of the traversal chain. ONLY VALID DURING
   * TRAVERSAL!!!
   */
  const TVertex &CurrentVertex() const {
    debug_assert(this->current_vertex_ != nullptr,
                 "Current vertex not set, function most likely called outside "
                 "of traversal");
    return *this->current_vertex_;
  }

  /**
   * Returns a reference to the edge currently being traversed
   * by this part of the traversal chain. ONLY VALID DURING
   * TRAVERSAL!!!
   */
  const std::list<TEdge> &CurrentEdges() const {
    debug_assert(this->current_edge_ != nullptr,
                 "Current edge not set, function most likely called outside of "
                 "traversal");
    return current_edges_;
  }

 private:
  // see constructor documentation for member var explanation
  const int min_length_;
  const int max_length_;

  // the expand variable has another vertex filter used only on the last path
  // element
  // because traversal is done recursively using superclass functionality,
  // so we give an empty filter to superclass so it does not end traversal, and
  // use
  // this filter to see if we actually need to visit the path or not
  const VertexFilter current_vertex_filter_;

  // variable length traversal can return all the traversed edges as a list
  // store them here during traversal
  std::list<TEdge> current_edges_;

  /**
   * Helper function for recursive path visiting.
   * TODO: Try to make a stack implementation. It would be safer.
   *
   * @param visitor
   * @param p
   * @param p_start_size The size of the path before variable-length expansion.
   *    It's necessary to keep track of it because min and max length are
   *    evaluated against how much this traversal generated, not against the
   *    actual path length (there could have been plan expansions before this
   * variable length).
   */
  void VisitRecursive(std::function<void(TPath &)> visitor, TPath &p,
                      const size_t p_start_size) {
    debug_assert(p.Size() >= p_start_size,
                 "Current path must be greater then start size");

    size_t recursion_size = p.Size() - p_start_size;

    // only append to current_edges once the first traversal happened
    if (recursion_size > 0) current_edges_.emplace_back(*this->current_edge_);

    if (recursion_size >= min_length_ &&
        (!current_vertex_filter_ ||
         current_vertex_filter_(*this->current_vertex_)))
      visitor(p);

    if (recursion_size >= max_length_ - 1) return;

    // a lambda we'll inject to ExpandVisit, that calls this function
    // with the expanded path
    auto recursive_visitor = [this, &visitor, p_start_size](TPath &path) {
      VisitRecursive(visitor, path, p_start_size);
    };

    this->VisitExpansions(recursive_visitor, p);

    if (recursion_size > 0) current_edges_.pop_back();
  }
};

/**
 * Expansion along a single traversal.
 *
 * Template parameters are documented in the namespace documentation,
 * as they are the same for many classes.
 *
 * @tparam TVisitable
 * @tparam TVertex
 * @tparam TEdge
 */
template <typename TVisitable, typename TVertex, typename TEdge>
class ExpandType : public ExpandBaseType<TVisitable, TVertex, TEdge> {
  using TPath = Path<TVertex, TEdge>;
  using VertexFilter = std::function<bool(const TVertex &)>;
  using EdgeFilter = std::function<bool(const TEdge &)>;

 public:
  /**
   * For all params see the ExpandBaseType::ExpandBaseType documentation.
   */
  ExpandType(TVisitable &&visitable, Expansion expansion, Direction direction,
             VertexFilter vertex_filter, EdgeFilter edge_filter,
             Uniqueness uniqueness)
      : ExpandBaseType<TVisitable, TVertex, TEdge>(
            std::forward<TVisitable>(visitable), expansion, direction,
            vertex_filter, edge_filter, uniqueness) {}

  /**
   * Calls the given visitor function once for every path this traversal
   * generates.
   *
   * @param visitor
   */
  void Visit(std::function<void(TPath &)> visitor) {
    this->visitable_.Visit(
        [this, &visitor](TPath &p) { this->VisitExpansions(visitor, p); });
  }

  /**
   * Returns a reference to the vertex currently being traversed
   * by this part of the traversal chain. ONLY VALID DURING
   * TRAVERSAL!!!
   */
  const TVertex &CurrentVertex() const {
    debug_assert(this->current_vertex_ != nullptr,
                 "Current vertex not set, function most likely called outside "
                 "of traversal");
    return *this->current_vertex_;
  }

  /**
   * Returns a reference to the edge currently being traversed
   * by this part of the traversal chain. ONLY VALID DURING
   * TRAVERSAL!!!
   */
  const TEdge &CurrentEdge() const {
    debug_assert(this->current_edge_ != nullptr,
                 "Current edge not set, function most likely called outside of "
                 "traversal");
    return *this->current_edge_;
  }

  /**
   * Expands from this expansion along one traversal.
   * Arguments are simply passed to the expansion constructor.
   *
   * @return An expansion that generates paths one traversal longer.
   */
  auto Expand(Expansion expansion, Direction direction,
              VertexFilter vertex_filter = {}, EdgeFilter edge_filter = {}) {
    return ExpandType<ExpandType<TVisitable, TVertex, TEdge> &, TVertex, TEdge>(
        *this, expansion, direction, vertex_filter, edge_filter,
        this->uniqueness_);
  }

  /**
   * Expands from this expansion along a variable number traversal.
   * Arguments are simply passed to the variable expansion constructor.
   *
   * @return An expansion that generates paths variable length longer.
   */
  auto ExpandVariable(Expansion expansion, Direction direction,
                      VertexFilter vertex_filter = {},
                      EdgeFilter edge_filter = {}, int min_length = 0,
                      int max_length = 1000) {
    return ExpandVariableType<const ExpandType<TVisitable, TVertex, TEdge> &,
                              TVertex, TEdge>(
        *this, expansion, direction, vertex_filter, edge_filter, min_length,
        max_length, this->uniqueness_);
  }
};

/**
 * Start traversals from an iterable over Vertices.
 *
 * @tparam TIterable
 * @tparam TVertex
 * @tparam TEdge
 */
template <typename TIterable, typename TVertex, typename TEdge>
class BeginType {
  using TPath = Path<TVertex, TEdge>;
  using VertexFilter = std::function<bool(const TVertex &)>;
  using EdgeFilter = std::function<bool(const TEdge &)>;

 public:
  BeginType(const TIterable &vertices, VertexFilter vertex_filter)
      : vertices_(vertices),
        vertex_filter_(vertex_filter),
        uniqueness_group_(path) {}

  /**
   * Calls the visitor with a path containing a single vertex
   * yielded by the given collection of vertices and accepted
   * by the filter.
   *
   * @param visitor
   */
  void Visit(std::function<void(TPath &)> visitor) {
    for (const TVertex &v : vertices_) {
      if (vertex_filter_ && !vertex_filter_(v)) continue;

      path.Start(v);
      visitor(path);
      path.PopFront();
    }
  }

  /**
   * The UniquenessGroup of this BeginType (the only one that
   * exists for all the expansions from this Begin).
   */
  UniquenessGroup<TVertex, TEdge> &UniquenessGroup() {
    return uniqueness_group_;
  }

  /**
   * Returns a reference to the vertex currently being traversed
   * by this part of the traversal chain. ONLY VALID DURING
   * TRAVERSAL!!!
   */
  const TVertex &CurrentVertex() const {
    debug_assert(path.Size() > 0,
                 "Current path is empty, function most likely called outside "
                 "of traversal");
    return path.Front();
  }

  /**
   * Expands from this expansion along one traversal.
   * Arguments are simply passed to the expansion constructor.
   *
   * @return An expansion that generates paths one traversal longer.
   */
  auto Expand(Expansion expansion, Direction direction,
              VertexFilter vertex_filter = {}, EdgeFilter edge_filter = {},
              Uniqueness uniqueness = Uniqueness::Edge) {
    return ExpandType<BeginType<TIterable, TVertex, TEdge> &, TVertex, TEdge>(
        *this, expansion, direction, vertex_filter, edge_filter, uniqueness);
  }

  /**
   * Expands from this expansion along a variable number traversal.
   * Arguments are simply passed to the variable expansion constructor.
   *
   * @return An expansion that generates paths variable length longer.
   */
  auto ExpandVariable(Expansion expansion, Direction direction,
                      VertexFilter vertex_filter = {},
                      EdgeFilter edge_filter = {}, int min_length = 1,
                      int max_length = 1000,
                      Uniqueness uniqueness = Uniqueness::Edge) {
    return ExpandVariableType<BeginType<TIterable, TVertex, TEdge> &, TVertex,
                              TEdge>(*this, expansion, direction, vertex_filter,
                                     edge_filter, min_length, max_length,
                                     uniqueness);
  }

 private:
  const TIterable &vertices_;
  // the BeingType has only one path that gets appended to and emitted to
  // visitors
  TPath path;
  const VertexFilter vertex_filter_;
  // TODO review: why do I have to have namespace:: here???
  traversal_template::UniquenessGroup<TVertex, TEdge> uniqueness_group_;
};

/**
 * Creates a BeginType.
 *
 * Template arguments are explained in namespace documentation.
 *
 * Function arguments are simply forwarded to the BeginType constructor.
 *
 * @return A BeginType.
 */
template <typename TIterable, typename TVertex, typename TEdge>
auto Begin(const TIterable &vertices,
           std::function<bool(const TVertex &)> vertex_filter = {}) {
  return BeginType<TIterable, TVertex, TEdge>(vertices, vertex_filter);
}

/**
 * Creates a start point for a recursion of Cartesian wrappers.
 */
template <typename TVisitable, typename TVertex, typename TEdge>
class CartesianUnaryType {
  using TPath = Path<TVertex, TEdge>;
  using TPaths = Paths<TVertex, TEdge>;

 public:
  CartesianUnaryType(TVisitable &&visitable)
      : visitable_(std::forward<TVisitable>(visitable)) {}

  void Visit(std::function<void(TPaths &)> visitor) const {
    TPaths paths;
    visitable_.Visit([&visitor, &paths](TPath &p) {
      paths.emplace_back(p);
      visitor(paths);
      paths.pop_back();
    });
  }

 private:
  const TVisitable visitable_;
};

/**
 * Provides means to visit a cartesian product of traversals.
 *
 * @tparam TVisitableFirst A visitable whose visitor accepts a list of path
 * reference (recursion down).
 * @tparam TVisitableOthers Visitables whose visitor accepts a single path
 * reference.
 */
template <typename TVisitableFirst, typename TVisitableOthers, typename TVertex,
          typename TEdge>
class CartesianBinaryType {
  using TPath = Path<TVertex, TEdge>;
  using TPaths = Paths<TVertex, TEdge>;

 public:
  /**
   * @tparam visitable_first A visitable whose visitor accepts a list of path
   * reference (recursion down).
   * @tparam visitable_others Visitable whose visitor accepts a single path
   * reference.
   */
  CartesianBinaryType(TVisitableFirst &&visitable_first,
                      TVisitableOthers &&visitable_others)
      : visitable_first_(std::forward<TVisitableFirst>(visitable_first)),
        visitable_others_(std::forward<TVisitableOthers>(visitable_others)) {}

  /**
   * Calls the given visitor with a list of reference wrappers to Paths
   * for every combination in this cartesian.
   *
   * @param visitor
   */
  void Visit(std::function<void(TPaths &)> visitor) const {
    // TODO currently cartesian product does NOT check for uniqueness
    // for example between edges in the emitted path combinations

    visitable_first_.Visit([this, &visitor](TPath &path) {
      visitable_others_.Visit([&path, &visitor](TPaths &paths) {
        paths.emplace_front(path);
        visitor(paths);
        paths.pop_front();
      });
    });
  }

 private:
  const TVisitableFirst visitable_first_;
  const TVisitableOthers visitable_others_;
};

/**
 * Creates an object that can be visited with a function that accepts a list
 * of path reference wrappers. That function will be called one for every
 * element
 * of a cartesian product of the given visitables (that emit paths).
 *
 * @tparam TVisitableFirst  A visitable that emits paths.
 * @tparam TVisitableOthers  An arbitrary number of visitables that emit paths.
 * @param first  A visitable that emits paths.
 * @param others  An arbitrary number of visitables that emit paths.
 * @return  See above.
 */
template <typename TVisitable, typename TVertex, typename TEdge>
auto Cartesian(TVisitable &&visitable) {
  return CartesianUnaryType<TVisitable, TVertex, TEdge>(
      std::forward<TVisitable>(visitable));
}

/**
 * Creates an object that can be visited with a function that accepts a list
 * of path reference wrappers. That function will be called one for every
 * element
 * of a cartesian product of the given visitables (that emit paths).
 *
 * @tparam TVisitableFirst  A visitable that emits paths.
 * @tparam TVisitableOthers  An arbitrary number of visitables that emit paths.
 * @param first  A visitable that emits paths.
 * @param others  An arbitrary number of visitables that emit paths.
 * @return  See above.
 */
template <typename TVisitableFirst, typename Vertex, typename Edge,
          typename... TVisitableOthers>
auto Cartesian(TVisitableFirst &&first, TVisitableOthers &&... others) {
  return CartesianBinaryType<
      TVisitableFirst,
      decltype(Cartesian(std::forward<TVisitableOthers>(others)...)), Vertex,
      Edge>(std::forward<TVisitableFirst>(first),
            Cartesian(std::forward<TVisitableOthers>(others)...));
}

template <typename TVisitable, typename TVertex, typename TEdge>
auto ExpandVariableType<TVisitable, TVertex, TEdge>::Expand(
    Expansion expansion, Direction direction, VertexFilter vertex_filter,
    EdgeFilter edge_filter) {
  return ExpandType<ExpandVariableType<TVisitable, TVertex, TEdge> &, TVertex,
                    TEdge>(*this, expansion, direction, vertex_filter,
                           edge_filter, this->uniqueness_);
}

template <typename TVisitable, typename TVertex, typename TEdge>
auto ExpandVariableType<TVisitable, TVertex, TEdge>::ExpandVariable(
    Expansion expansion, Direction direction, VertexFilter vertex_filter,
    EdgeFilter edge_filter, int min_length, int max_length) {
  return ExpandVariableType<ExpandVariableType<TVisitable, TVertex, TEdge> &,
                            TVertex, TEdge>(
      *this, expansion, direction, vertex_filter, edge_filter, min_length,
      max_length, this->uniqueness_);
}
}
