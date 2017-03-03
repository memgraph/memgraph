//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 02.03.17.
//

#pragma once

#include "enums.hpp"
#include "path.hpp"
#include "templates.hpp"

#include "storage/vertex_accessor.hpp"
#include "storage/edge_accessor.hpp"

/**
 * A specialization of the "traversal" namespace that uses
 * real DB VertexAccessor and EdgeAccessor classes.
 */
namespace traversal {

// expose Path and Paths as class template instantiations
using Path = traversal_template::Path<VertexAccessor, EdgeAccessor>;
using Paths = traversal_template::Paths<VertexAccessor, EdgeAccessor>;

/**
 * Specialization of the traversal_template::Begin function.
 */
template<typename TCollection>
auto Begin(const TCollection &vertices, std::function<bool(const VertexAccessor &)> vertex_filter = {}) {
return traversal_template::Begin<TCollection, VertexAccessor, EdgeAccessor>(vertices, vertex_filter);
}

/**
 * Specialization of the traversal_template::Cartesian function that accepts
 * a single argument.
 */
template<typename TVisitable>
auto Cartesian(TVisitable &&visitable) {
  return traversal_template::Cartesian<TVisitable, VertexAccessor, EdgeAccessor>(
      std::forward<TVisitable>(visitable));
}


/**
 * Specialization of the traversal_template::Cartesian function that accepts
 * multiple arguments.
 */
template<typename TVisitableFirst, typename... TVisitableOthers>
auto Cartesian(TVisitableFirst &&first, TVisitableOthers &&... others) {
  return traversal_template::CartesianBinaryType<TVisitableFirst, decltype(Cartesian(
      std::forward<TVisitableOthers>(others)...)),
      VertexAccessor, EdgeAccessor>(
      std::forward<TVisitableFirst>(first),
      Cartesian(std::forward<TVisitableOthers>(others)...)
  );
}
}

