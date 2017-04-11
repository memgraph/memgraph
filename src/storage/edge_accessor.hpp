#pragma once

#include "database/graph_db.hpp"
#include "storage/edge.hpp"
#include "storage/record_accessor.hpp"
#include "utils/assert.hpp"
#include "utils/reference_wrapper.hpp"

// forward declaring the VertexAccessor because it's returned
// by some functions
class VertexAccessor;

/**
 * Provides ways for the client programmer (i.e. code generated
 * by the compiler) to interact with an Edge.
 *
 * This class indirectly inherits MVCC data structures and
 * takes care of MVCC versioning.
 */
class EdgeAccessor : public RecordAccessor<Edge> {
 public:
  using RecordAccessor::RecordAccessor;

  /**
   * Returns the edge type.
   * @return
   */
  GraphDbTypes::EdgeType edge_type() const;

  /**
   * Returns an accessor to the originating Vertex of this edge.
   * @return
   */
  VertexAccessor from() const;

  /**
   * Returns an accessor to the destination Vertex of this edge.
   */
  VertexAccessor to() const;

  /** Returns true if this edge is a cycle (start and end node are
   * the same. */
  bool is_cycle() const;
};

std::ostream &operator<<(std::ostream &, const EdgeAccessor &);
