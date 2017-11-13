#pragma once

#include "storage/edge.hpp"
#include "storage/record_accessor.hpp"

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
  GraphDbTypes::EdgeType EdgeType() const;

  /**
   * Returns an accessor to the originating Vertex of this edge.
   * @return
   */
  VertexAccessor from() const;

  /** Checks if the given vertex is the source of this edge, without
   * creating an additional accessor to perform the check. */
  bool from_is(const VertexAccessor &v) const;

  /**
   * Returns an accessor to the destination Vertex of this edge.
   */
  VertexAccessor to() const;

  /** Checks ig the given vertex is the destination of this edge, without
   * creating an additional accessor to perform the check. */
  bool to_is(const VertexAccessor &v) const;

  /** Returns true if this edge is a cycle (start and end node are
   * the same. */
  bool is_cycle() const;
};

std::ostream &operator<<(std::ostream &, const EdgeAccessor &);

// hash function for the edge accessor
namespace std {
template <>
struct hash<EdgeAccessor> {
  size_t operator()(const EdgeAccessor &e) const { return e.id(); };
};
}  // namespace std
