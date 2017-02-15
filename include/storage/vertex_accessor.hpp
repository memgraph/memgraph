#pragma once

#include <vector>
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

  std::vector<EdgeAccessor> in();

  std::vector<EdgeAccessor> out();

  // returns if remove was possible due to connections
//  bool remove();
//
//  void detach_remove();
};
