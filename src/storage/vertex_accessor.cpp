#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"

size_t VertexAccessor::out_degree() const {
  return this->record_->out_.size();
}

size_t VertexAccessor::in_degree() const {
  return this->record_->in_.size();
}

bool VertexAccessor::add_label(GraphDb::Label label) {
  return this->record_->labels_.emplace(label).second;
}

size_t VertexAccessor::remove_label(GraphDb::Label label) {
  return this->record_->labels_.erase(label);
}

bool VertexAccessor::has_label(GraphDb::Label label) const {
  auto &label_set = this->record_->labels_;
  return label_set.find(label) != label_set.end();
}

const std::set<GraphDb::Label> &VertexAccessor::labels() const {
  return this->record_->labels_;
}

bool VertexAccessor::remove() const {
  // TODO consider if this works well with MVCC
  if (out_degree() > 0 || in_degree() > 0)
    return false;

  vlist_->remove(record_, db_trans_.trans);
  return true;
}

void VertexAccessor::detach_remove() const {
  // removing edges via accessors is both safe
  // and it should remove all the pointers in the relevant
  // vertices (including this one)
  for (auto edge_vlist : record_->out_)
    EdgeAccessor(edge_vlist, db_trans_).remove();

  for (auto edge_vlist : record_->in_)
    EdgeAccessor(edge_vlist, db_trans_).remove();

  vlist_->remove(record_, db_trans_.trans);
}

