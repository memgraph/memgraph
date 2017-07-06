//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 23.03.17.
//

#pragma once

#include <algorithm>
#include <cstdlib>
#include <functional>
#include <vector>

#include "database/graph_db_accessor.hpp"
#include "database/graph_db_datatypes.hpp"
#include "storage/property_value.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/assert.hpp"

namespace utils {

/**
 * Returns a lambda that generates random ints
 * in the [from, to) range.
 */
auto RandomIntGenerator(int from, int to) {
  permanent_assert(from < to, "Must have from < to");
  int range = to - from;
  return [from, range]() -> int { return rand() % range + from; };
}

/**
 * Random graph generator. Define a random graph
 * over a sequence of steps, and then commit.
 * You can only commit once and can't change
 * graph afterwards.
 */
class RandomGraphGenerator {
 public:
  RandomGraphGenerator(GraphDbAccessor &dba) : dba_(dba) {}

  /**
   * Adds the given number of vertices, with
   * the given labels.
   */
  void AddVertices(uint count, std::vector<std::string> label_names) {
    permanent_assert(!did_commit_, "Already committed");

    std::vector<GraphDbTypes::Label> labels;
    for (const auto &label_name : label_names)
      labels.push_back(dba_.label(label_name));

    for (uint i = 0; i < count; ++i) {
      auto vertex = dba_.insert_vertex();
      for (auto label : labels) vertex.add_label(label);
      vertices_.push_back(vertex);
    }
  }

  /**
   * Adds the given number of edges to the graph.
   *
   * @param count The number of edges to add.
   * @param edge_type_name Name of the edge type.
   * @param from_filter Filter of from vertices for new edges.
   *    By default all vertices are accepted.
   * @param to_filter Filter of to vertices for new edges.
   *    By default all vertices are accepted.
   */
  void AddEdges(uint count, const std::string &edge_type_name,
                std::function<bool(VertexAccessor &va)> from_filter = {},
                std::function<bool(VertexAccessor &va)> = {}) {
    permanent_assert(!did_commit_, "Already committed");

    // create two temporary sets of vertices we will poll from
    auto vertices_from = Filter(vertices_, from_filter);
    auto vertices_to = Filter(vertices_, from_filter);

    auto edge_type = dba_.edge_type(edge_type_name);
    for (int i = 0; i < static_cast<int>(count); ++i)
      edges_.push_back(dba_.insert_edge(
          vertices_from[rand() % vertices_from.size()].get(),
          vertices_to[rand() % vertices_to.size()].get(), edge_type));
  }

  /**
   * Sets a generated property on a random vertex.
   *
   * @tparam TValue Type of value to set.
   * @param count The number of vertices to set the property on.
   * @param prop_name Name of the property.
   * @param predicate Filter that accepts or rejects a Vertex.
   * @param value_generator Function that accepts nothing and
   *    returns a property.
   */
  template <typename TValue>
  void SetVertexProperty(
      uint, const std::string &prop_name,
      std::function<TValue()> value_generator,
      std::function<bool(VertexAccessor &va)> predicate = {}) {
    permanent_assert(!did_commit_, "Already committed");

    auto property = dba_.property(prop_name);
    for (auto va : Filter(vertices_, predicate))
      va.get().PropsSet(property, value_generator());
  }

  /**
   * Commits the random graph to storage.
   * Can only be called once.
   */
  void Commit() {
    debug_assert(!did_commit_, "Already committed random graph");
    dba_.commit();
  }

 private:
  GraphDbAccessor &dba_;

  // storage for data we operate on
  std::vector<VertexAccessor> vertices_;
  std::vector<EdgeAccessor> edges_;

  // can't perform ops after committing
  bool did_commit_{false};

  /**
   * Helper function for filtering.
   * Accepts a vector of TItems, a predicate
   * that accepts it or not, and returns a
   * vector of reference wrappers to accepted
   * items.
    *
    * @tparam TItem Type of item.
    * @param collection The collection to filter on.
    * @param predicate A predicate. By default always true.
    * @return A collection of reference_wrappers to TItems.
    */
  template <typename TItem>
  std::vector<std::reference_wrapper<TItem>> Filter(
      std::vector<TItem> &collection,
      std::function<bool(TItem &item)> predicate = {}) {
    if (!predicate) predicate = [](TItem &) { return true; };
    std::vector<std::reference_wrapper<TItem>> r_val;
    for (TItem &item : collection)
      if (predicate(item)) r_val.emplace_back(std::ref(item));

    return r_val;
  }
};
}
