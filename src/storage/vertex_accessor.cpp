#include <algorithm>

#include "storage/edge_accessor.hpp"
#include "storage/util.hpp"
#include "storage/vertex_accessor.hpp"

size_t VertexAccessor::out_degree() const { return view().out_.size(); }

size_t VertexAccessor::in_degree() const { return view().in_.size(); }

bool VertexAccessor::add_label(GraphDbTypes::Label label) {
  auto &labels_view = view().labels_;
  auto found = std::find(labels_view.begin(), labels_view.end(), label);
  if (found != labels_view.end()) return false;

  // not a duplicate label, add it
  update().labels_.emplace_back(label);
  return true;
}

size_t VertexAccessor::remove_label(GraphDbTypes::Label label) {
  auto &labels = update().labels_;
  auto found = std::find(labels.begin(), labels.end(), label);
  if (found == labels.end()) return 0;

  std::swap(*found, labels.back());
  labels.pop_back();
  return 1;
}

bool VertexAccessor::has_label(GraphDbTypes::Label label) const {
  auto &labels = this->view().labels_;
  return std::find(labels.begin(), labels.end(), label) != labels.end();
}

const std::vector<GraphDbTypes::Label> &VertexAccessor::labels() const {
  return this->view().labels_;
}
