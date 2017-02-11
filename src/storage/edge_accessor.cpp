#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"

void EdgeAccessor::set_edge_type(GraphDb::EdgeType edge_type) {
  this->update()->edge_type_ = edge_type;
}

GraphDb::EdgeType EdgeAccessor::edge_type() const {
  return this->view()->edge_type_;
}

VertexAccessor EdgeAccessor::from() const {
  return VertexAccessor(this->view()->from_, this->db_accessor_->transaction_);
}

VertexAccessor EdgeAccessor::to() const {
  return VertexAccessor(this->view()->to_, this->db_accessor_->transaction_);
}

void EdgeAccessor::remove() {
  // remove this edge's reference from the "from" vertex
  auto vertex_from = from().update();
  std::remove(vertex_from->out_.begin(),
              vertex_from->out_.end(),
              vlist_);

  // remove this edge's reference from the "to" vertex
  auto vertex_to = to().update();
  std::remove(vertex_to->in_.begin(),
              vertex_to->in_.end(),
              vlist_);

  // remove this record from the database via MVCC
  vlist_->remove(update(), db_accessor_->transaction_);
}

