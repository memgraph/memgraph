#pragma once

#include <experimental/optional>
#include <limits>
#include <set>
#include <vector>

#include "storage/distributed/cached_data_lock.hpp"
#include "storage/distributed/edge_accessor.hpp"
#include "storage/distributed/edges_iterator.hpp"
#include "storage/distributed/record_accessor.hpp"
#include "storage/distributed/vertex.hpp"

/**
 * Provides ways for the client programmer (i.e. code generated
 * by the compiler) to interact with a Vertex.
 *
 * This class indirectly inherits MVCC data structures and
 * takes care of MVCC versioning.
 */
class VertexAccessor final : public RecordAccessor<Vertex> {
  using VertexAddress = storage::Address<mvcc::VersionList<Vertex>>;

 public:
  VertexAccessor(VertexAddress address, database::GraphDbAccessor &db_accessor);

  /** Returns the number of outgoing edges. */
  size_t out_degree() const;

  /** Returns the number of incoming edges. */
  size_t in_degree() const;

  /** Adds a label to the Vertex. If the Vertex already has that label the call
   * has no effect. */
  void add_label(storage::Label label);

  /** Removes a label from the Vertex. */
  void remove_label(storage::Label label);

  /** Indicates if the Vertex has the given label. */
  bool has_label(storage::Label label) const;

  /** Returns all the Labels of the Vertex. */
  // TODO (vkasljevic) add proxy for labels
  std::vector<storage::Label> labels() const;

  /** Returns EdgeAccessors for all incoming edges. */
  EdgesIterable in() const {
    return EdgesIterable(*this, false);
  }

  /**
   * Returns EdgeAccessors for all incoming edges.
   *
   * @param dest - The destination vertex filter.
   * @param edge_types - Edge types filter. At least one be matched. If nullptr
   * or empty, the parameter is ignored.
   */
  EdgesIterable in(const VertexAccessor &dest,
          const std::vector<storage::EdgeType> *edge_types = nullptr) const {
    return EdgesIterable(*this, false, dest, edge_types);
  }

  /**
   * Returns EdgeAccessors for all incoming edges.
   *
   * @param edge_types - Edge types filter. At least one be matched. If nullptr
   * or empty, the parameter is ignored.
   */
  EdgesIterable in(const std::vector<storage::EdgeType> *edge_types) const {
    return EdgesIterable(*this, false, edge_types);
  }

  /** Returns EdgeAccessors for all outgoing edges. */
  EdgesIterable out() const {
    return EdgesIterable(*this, true);
  }

  /**
   * Returns EdgeAccessors for all outgoing edges whose destination is the given
   * vertex.
   *
   * @param dest - The destination vertex filter.
   * @param edge_types - Edge types filter. At least one be matched. If nullptr
   * or empty, the parameter is ignored.
   */
  EdgesIterable out(const VertexAccessor &dest,
           const std::vector<storage::EdgeType> *edge_types = nullptr) const {
    return EdgesIterable(*this, true, dest, edge_types);
  }

  /**
   * Returns EdgeAccessors for all outgoing edges.
   *
   * @param edge_types - Edge types filter. At least one be matched. If nullptr
   * or empty, the parameter is ignored.
   */
  EdgesIterable out(const std::vector<storage::EdgeType> *edge_types) const {
    return EdgesIterable(*this, true, edge_types);
  }

  /** Removes the given edge from the outgoing edges of this vertex. Note that
   * this operation should always be accompanied by the removal of the edge from
   * the incoming edges on the other side and edge deletion. */
  void RemoveOutEdge(storage::EdgeAddress edge);

  /** Removes the given edge from the incoming edges of this vertex. Note that
   * this operation should always be accompanied by the removal of the edge from
   * the outgoing edges on the other side and edge deletion. */
  void RemoveInEdge(storage::EdgeAddress edge);
};

std::ostream &operator<<(std::ostream &, const VertexAccessor &);

// hash function for the vertex accessor
namespace std {
template <>
struct hash<VertexAccessor> {
  size_t operator()(const VertexAccessor &v) const { return v.gid(); };
};
}  // namespace std
