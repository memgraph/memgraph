#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"

void EdgeAccessor::set_edge_type(GraphDb::EdgeType edge_type) {
  this->record_->edge_type_ = edge_type;
}

GraphDb::EdgeType EdgeAccessor::edge_type() const {
  return this->record_->edge_type_;
}

VertexAccessor EdgeAccessor::from() const {
  return VertexAccessor(this->record_->from_, this->trans_);
}

VertexAccessor EdgeAccessor::to() const {
  return VertexAccessor(this->record_->to_, this->trans_);
}

void EdgeAccessor::remove() {
  // remove this edge's reference from the "from" vertex
  auto vertex_from = from();
  vertex_from.update();
  std::remove(vertex_from.record_->out_.begin(),
              vertex_from.record_->out_.end(),
              vlist_);

  // remove this edge's reference from the "to" vertex
  auto vertex_to = to();
  vertex_to.update();
  std::remove(vertex_to.record_->in_.begin(),
              vertex_to.record_->in_.end(),
              vlist_);

  // remove this record from the database via MVCC
  vlist_->remove(record_, trans_);
}

