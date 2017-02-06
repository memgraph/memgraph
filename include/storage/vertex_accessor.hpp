#pragma once

#include <set>

#include "storage/record_accessor.hpp"
#include "storage/vertex.hpp"
#include "utils/iterator/iterator.hpp"
#include "database/graph_db.hpp"

class EdgeAccessor;

class VertexAccessor : public RecordAccessor<Vertex, VertexAccessor> {
public:
  using RecordAccessor::RecordAccessor;

  size_t out_degree() const;

  size_t in_degree() const;

  bool add_label(GraphDb::Label label);

  size_t remove_label(GraphDb::Label label);

  bool has_label(GraphDb::Label label) const;

  const std::set<GraphDb::Label>& labels() const;

  // TODO add in/out functions that return (collection|iterator) over EdgeAccessor

  // returns if remove was possible due to connections
  bool remove();

  void detach_remove();

  /**
   * Adds the given Edge version list to this Vertex's incoming edges.
   *
   * @param edge_vlist  The Edge to add.
   * @param pass_key  Ensures only GraphDb has access to this method.
   */
  void attach_in(mvcc::VersionList<Edge>* edge_vlist, PassKey<GraphDb> pass_key);

  /**
   * Adds the given Edge version list to this Vertex's outgoing edges.
   *
   * @param edge_vlist  The Edge to add.
   * @param pass_key  Ensures only GraphDb has access to this method.
   */
  void attach_out(mvcc::VersionList<Edge>* edge_vlist, PassKey<GraphDb> pass_key);

};
