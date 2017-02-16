#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "storage/util.hpp"

size_t VertexAccessor::out_degree() const {
  return view().out_.size();
}

size_t VertexAccessor::in_degree() const {
  return view().in_.size();
}

bool VertexAccessor::add_label(GraphDb::Label label) {
  return update().labels_.emplace(label).second;
}

size_t VertexAccessor::remove_label(GraphDb::Label label) {
  return update().labels_.erase(label);
}

bool VertexAccessor::has_label(GraphDb::Label label) const {
  auto &label_set = this->view().labels_;
  return label_set.find(label) != label_set.end();
}

const std::set<GraphDb::Label>& VertexAccessor::labels() const {
  return this->view().labels_;
}

std::vector<EdgeAccessor> VertexAccessor::in() {
  return make_accessors<EdgeAccessor>(view().in_, db_accessor_);
}

std::vector<EdgeAccessor> VertexAccessor::out() {
  return make_accessors<EdgeAccessor>(view().out_, db_accessor_);
}
