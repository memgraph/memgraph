#pragma once

#include <utility>
#include <vector>

#include "database/graph_db_datatypes.hpp"
#include "mvcc/version_list.hpp"

// forward declare Vertex and Edge because they need this data structure
class Edge;
class Vertex;

/**
 * A data stucture that holds a number of edges. This implementation assumes
 * that separate Edges instances are used for incoming and outgoing edges in a
 * vertex (and consequently that edge pointers are unique in it).
 */
class Edges {
  using vertex_ptr_t = mvcc::VersionList<Vertex> *;
  using edge_ptr_t = mvcc::VersionList<Edge> *;

  struct Element {
    vertex_ptr_t vertex;
    edge_ptr_t edge;
    GraphDbTypes::EdgeType edge_type;
  };

  /** Custom iterator that takes care of skipping edges when the destination
   * vertex or edge type are known. */
  class Iterator {
   public:
    /** Ctor that just sets the position. Used for normal iteration (that does
     * not skip any edges), and for end-iterator creation in both normal and
     * skipping iteration.
     *
     * @param iterator - Iterator in the underlying storage.
     */
    explicit Iterator(std::vector<Element>::const_iterator iterator)
        : position_(iterator) {}

    /** Ctor used for creating the beginning iterator with known destionation
     * vertex.
     *
     * @param iterator - Iterator in the underlying storage.
     * @param end - End iterator in the underlying storage.
     * @param vertex - The destination vertex vlist pointer.
     */
    Iterator(std::vector<Element>::const_iterator position,
             std::vector<Element>::const_iterator end, vertex_ptr_t vertex)
        : position_(position), end_(end), vertex_(vertex) {
      update_position();
    }

    /** Ctor used for creating the beginning iterator with known edge type.
     *
     * @param iterator - Iterator in the underlying storage.
     * @param end - End iterator in the underlying storage.
     * @param edge_type - The edge type that must be matched.
     */
    Iterator(std::vector<Element>::const_iterator position,
             std::vector<Element>::const_iterator end,
             GraphDbTypes::EdgeType edge_type)
        : position_(position), end_(end), edge_type_(edge_type) {
      update_position();
    }

    Iterator &operator++() {
      ++position_;
      update_position();
      return *this;
    }

    const Element &operator*() const { return *position_; }

    bool operator==(const Iterator &other) const {
      return position_ == other.position_;
    }

    bool operator!=(const Iterator &other) const { return !(*this == other); }

   private:
    std::vector<Element>::const_iterator position_;
    // end_ is used only in update_position() to limit find.
    std::vector<Element>::const_iterator end_;

    // Optional predicates. If set they define which edges are skipped by the
    // iterator. Only one can be not-null in the current implementation.
    vertex_ptr_t vertex_{nullptr};
    GraphDbTypes::EdgeType edge_type_{nullptr};

    /** Helper function that skips edges that don't satisfy the predicate
     * present in this iterator. */
    void update_position() {
      if (vertex_)
        position_ = std::find_if(
            position_, end_,
            [v = this->vertex_](const Element &e) { return e.vertex == v; });
      else if (edge_type_)
        position_ = std::find_if(position_, end_,
                                 [et = this->edge_type_](const Element &e) {
                                   return e.edge_type == et;
                                 });
    }
  };

 public:
  /**
   * Adds an edge to this structure.
   *
   * @param vertex - The destination vertex of the edge. That's the one opposite
   * from the vertex that contains this `Edges` instance.
   * @param edge - The edge.
   * @param edge_type - Type of the edge.
   */
  void emplace(vertex_ptr_t vertex, edge_ptr_t edge,
               GraphDbTypes::EdgeType edge_type) {
    storage_.emplace_back(Element{vertex, edge, edge_type});
  }

  /**
   * Removes an edge from this structure.
   */
  void RemoveEdge(edge_ptr_t edge) {
    auto found = std::find_if(
        storage_.begin(), storage_.end(),
        [edge](const Element &element) { return edge == element.edge; });
    debug_assert(found != storage_.end(),
                 "Removing an edge that is not present");
    *found = std::move(storage_.back());
    storage_.pop_back();
  }

  auto size() const { return storage_.size(); }
  auto begin() const { return Iterator(storage_.begin()); }
  auto end() const { return Iterator(storage_.end()); }

  /**
   * Creates a beginning iterator that will skip edges whose destination vertex
   * is not equal to the given vertex.
   */
  auto begin(vertex_ptr_t vertex) const {
    return Iterator(storage_.begin(), storage_.end(), vertex);
  }

  /*
   * Creates a beginning iterator that will skip edges whose edge type is not
   * equal to the given. Relies on the fact that edge types are immutable during
   * the whole edge lifetime.
   */
  auto begin(GraphDbTypes::EdgeType edge_type) const {
    return Iterator(storage_.begin(), storage_.end(), edge_type);
  }

 private:
  std::vector<Element> storage_;
};
