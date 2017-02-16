#pragma once

#include <vector>
#include <set>

#include "storage/record_accessor.hpp"
#include "storage/vertex.hpp"
#include "utils/iterator/iterator.hpp"
#include "database/graph_db.hpp"

// forward declaring the EdgeAccessor because it's returned
// by some functions
class EdgeAccessor;

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
  bool add_label(GraphDb::Label label);

  /**
   * Removes a label from the Vertex.
   * @param label  The label to remove.
   * @return The number of removed labels (can be 0 or 1).
   */
  size_t remove_label(GraphDb::Label label);

  /**
   * Indicates if the Vertex has the given label.
   * @param label A label.
   * @return
   */
  bool has_label(GraphDb::Label label) const;

  /**
   * Returns all the Labels of the Vertex.
   * @return
   */
  const std::vector<GraphDb::Label>& labels() const;

  /**
   * Returns EdgeAccessors for all incoming edges.
   * @return
   */
  std::vector<EdgeAccessor> in();

  /**
   * Returns EdgeAccessors for all outgoing edges.
   * @return
   */
  std::vector<EdgeAccessor> out();
};
