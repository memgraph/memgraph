#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"

void EdgeAccessor::set_edge_type(GraphDb::EdgeType edge_type) {
  update().edge_type_ = edge_type;
}

GraphDb::EdgeType EdgeAccessor::edge_type() const {
  return view().edge_type_;
}

VertexAccessor EdgeAccessor::from() const {
  return VertexAccessor(view().from_, db_accessor_);
}

VertexAccessor EdgeAccessor::to() const {
return VertexAccessor(view().to_, db_accessor_);
}
