#pragma once

#include "storage/edge.hpp"
#include "storage/record_accessor.hpp"
#include "utils/assert.hpp"
#include "utils/reference_wrapper.hpp"
#include "database/graph_db.hpp"

class VertexAccessor;

class EdgeAccessor : public RecordAccessor<Edge, EdgeAccessor> {
public:
  using RecordAccessor::RecordAccessor;

  void set_edge_type(GraphDb::EdgeType edge_type);

  EdgeType edge_type() const;

  VertexAccessor from() const;

  VertexAccessor to() const;

  void remove() const;
};
