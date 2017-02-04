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
  bool remove() const;

  void detach_remove() const;
};
