#pragma once

#include <set>
#include <vector>

#include "cppitertools/chain.hpp"

#include "database/graph_db.hpp"
#include "storage/record_accessor.hpp"
#include "storage/vertex.hpp"
#include "storage/util.hpp"

#include "storage/edge_accessor.hpp"

/**
 * Provides ways for the client programmer (i.e. code generated
 * by the compiler) to interact with a Vertex.
 *
 * This class indirectly inherits MVCC data structures and
 * takes care of MVCC versioning.
 */
class VertexAccessor : public RecordAccessor<Vertex> {
 public:
  using RecordAccessor::RecordAccessor;

  /**
   * Returns the number of outgoing edges.
   * @return
   */
  size_t out_degree() const;

  /**
   * Returns the number of incoming edges.
   * @return
   */
  size_t in_degree() const;

  /**
   * Adds a label to the Vertex. If the Vertex already
   * has that label the call has no effect.
   * @param label A label.
   * @return If or not a new Label was set on this Vertex.
   */
  bool add_label(GraphDbTypes::Label label);

  /**
   * Removes a label from the Vertex.
   * @param label  The label to remove.
   * @return The number of removed labels (can be 0 or 1).
   */
  size_t remove_label(GraphDbTypes::Label label);

  /**
   * Indicates if the Vertex has the given label.
   * @param label A label.
   * @return
   */
  bool has_label(GraphDbTypes::Label label) const;

  /**
   * Returns all the Labels of the Vertex.
   * @return
   */
  const std::vector<GraphDbTypes::Label>& labels() const;

  /**
   * Returns EdgeAccessors for all incoming edges.
   */
  auto in() { return make_accessor_iterator<EdgeAccessor>(current().in_, db_accessor()); }

  /**
   * Returns EdgeAccessors for all outgoing edges.
   */
  auto out() { return make_accessor_iterator<EdgeAccessor>(current().out_, db_accessor()); }
};

std::ostream &operator<<(std::ostream &, const VertexAccessor &);
