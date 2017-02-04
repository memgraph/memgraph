#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"

void EdgeAccessor::set_edge_type(GraphDb::EdgeType edge_type) {
  this->record_->edge_type_ = edge_type;
}

GraphDb::EdgeType EdgeAccessor::edge_type() const {
  return this->record_->edge_type_;
}

VertexAccessor EdgeAccessor::from() const {
  return VertexAccessor(this->record_->from_, this->db_trans_);
}

VertexAccessor EdgeAccessor::to() const {
  return VertexAccessor(this->record_->to_, this->db_trans_);
}

void EdgeAccessor::remove() const {
  // remove this edge's reference from the "from" vertex
  auto& vertex_from_out = from().update().record_->out_;
  std::remove(vertex_from_out.begin(), vertex_from_out.end(), record_->from_);

  // remove this edge's reference from the "to" vertex
  auto& vertex_to_in = to().update().record_->in_;
  std::remove(vertex_to_in.begin(), vertex_to_in.end(), record_->to_);

  // remove this record from the database via MVCC
  vlist_->remove(record_, db_trans_.trans);
}

