#pragma once

#include <experimental/optional>
#include <utility>
#include <vector>

#include "glog/logging.h"

#include "mvcc/distributed/version_list.hpp"
#include "storage/common/types.hpp"
#include "storage/distributed/address.hpp"
#include "storage/distributed/address_types.hpp"
#include "utils/algorithm.hpp"

/**
 * A data stucture that holds a number of edges. This implementation assumes
 * that separate Edges instances are used for incoming and outgoing edges in a
 * vertex (and consequently that edge Addresses are unique in it).
 */
class Edges {
 private:
  struct Element {
    storage::VertexAddress vertex;
    storage::EdgeAddress edge;
    storage::EdgeType edge_type;
  };

  /** Custom iterator that takes care of skipping edges when the destination
   * vertex or edge types are known. */
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

    /** Ctor used for creating the beginning iterator with known destination
     * vertex.
     *
     * @param iterator - Iterator in the underlying storage.
     * @param end - End iterator in the underlying storage.
     * @param vertex - The destination vertex address. If empty the
     * edges are not filtered on destination.
     * @param edge_types - The edge types at least one of which must be matched.
     * If nullptr edges are not filtered on type.
     */
    Iterator(std::vector<Element>::const_iterator position,
             std::vector<Element>::const_iterator end,
             std::experimental::optional<storage::VertexAddress> vertex,
             const std::vector<storage::EdgeType> *edge_types)
        : position_(position),
          end_(end),
          vertex_(vertex),
          edge_types_(edge_types) {
      update_position();
    }

    Iterator &operator++() {
      ++position_;
      update_position();
      return *this;
    }

    const Element &operator*() const { return *position_; }
    const Element *operator->() const { return &(*position_); }

    bool operator==(const Iterator &other) const {
      return position_ == other.position_;
    }

    bool operator!=(const Iterator &other) const { return !(*this == other); }

   private:
    std::vector<Element>::const_iterator position_;
    // end_ is used only in update_position() to limit find.
    std::vector<Element>::const_iterator end_;

    // Optional predicates. If set they define which edges are skipped by the
    // iterator.
    std::experimental::optional<storage::VertexAddress> vertex_;
    // For edge types we use a vector pointer because it's optional.
    const std::vector<storage::EdgeType> *edge_types_ = nullptr;

    /** Helper function that skips edges that don't satisfy the predicate
     * present in this iterator. */
    void update_position() {
      if (vertex_) {
        position_ = std::find_if(position_, end_,
                                 [v = this->vertex_.value()](const Element &e) {
                                   return e.vertex == v;
                                 });
      }
      if (edge_types_) {
        position_ = std::find_if(position_, end_, [this](const Element &e) {
          return utils::Contains(*edge_types_, e.edge_type);
        });
      }
    }
  };

 public:
  /**
   * Adds an edge to this structure.
   *
   * @param vertex - The destination vertex of the edge. That's the one
   * opposite from the vertex that contains this `Edges` instance.
   * @param edge - The edge.
   * @param edge_type - Type of the edge.
   */
  void emplace(storage::VertexAddress vertex, storage::EdgeAddress edge,
               storage::EdgeType edge_type) {
    storage_.emplace_back(Element{vertex, edge, edge_type});
  }

  /**
   * Removes an edge from this structure.
   */
  void RemoveEdge(storage::EdgeAddress edge) {
    auto found = std::find_if(
        storage_.begin(), storage_.end(),
        [edge](const Element &element) { return edge == element.edge; });
    // If the edge is not in the structure we don't care and can simply return
    if (found == storage_.end()) return;
    *found = std::move(storage_.back());
    storage_.pop_back();
  }

  auto size() const { return storage_.size(); }
  auto begin() const { return Iterator(storage_.begin()); }
  auto end() const { return Iterator(storage_.end()); }

  auto &storage() { return storage_; }

  /**
   * Creates a beginning iterator that will skip edges whose destination
   * vertex is not equal to the given vertex.
   *
   * @param vertex - The destination vertex Address. If empty the
   * edges are not filtered on destination.
   * @param edge_types - The edge types at least one of which must be matched.
   * If nullptr edges are not filtered on type.
   */
  auto begin(std::experimental::optional<storage::VertexAddress> vertex,
             const std::vector<storage::EdgeType> *edge_types) const {
    if (edge_types && edge_types->empty()) edge_types = nullptr;
    return Iterator(storage_.begin(), storage_.end(), vertex, edge_types);
  }

 private:
  std::vector<Element> storage_;
};
