#include "storage/vertex_accessor.hpp"

#include <algorithm>

#include "database/graph_db_accessor.hpp"
#include "utils/algorithm.hpp"

size_t VertexAccessor::out_degree() const { return current().out_.size(); }

size_t VertexAccessor::in_degree() const { return current().in_.size(); }

bool VertexAccessor::add_label(GraphDbTypes::Label label) {
  auto &labels_view = current().labels_;
  auto found = std::find(labels_view.begin(), labels_view.end(), label);
  if (found != labels_view.end()) return false;

  // not a duplicate label, add it
  Vertex &vertex = update();
  vertex.labels_.emplace_back(label);
  auto &dba = db_accessor();
  dba.UpdateLabelIndices(label, *this, &vertex);
  dba.wal().Emplace(database::StateDelta::AddLabel(dba.transaction_id(), gid(),
                                                   dba.LabelName(label)));
  return true;
}

size_t VertexAccessor::remove_label(GraphDbTypes::Label label) {
  auto &labels = update().labels_;
  auto found = std::find(labels.begin(), labels.end(), label);
  if (found == labels.end()) return 0;

  std::swap(*found, labels.back());
  labels.pop_back();
  auto &dba = db_accessor();
  dba.wal().Emplace(database::StateDelta::RemoveLabel(
      dba.transaction_id(), gid(), dba.LabelName(label)));
  return 1;
}

bool VertexAccessor::has_label(GraphDbTypes::Label label) const {
  auto &labels = this->current().labels_;
  return std::find(labels.begin(), labels.end(), label) != labels.end();
}

const std::vector<GraphDbTypes::Label> &VertexAccessor::labels() const {
  return this->current().labels_;
}

std::ostream &operator<<(std::ostream &os, const VertexAccessor &va) {
  os << "V(";
  utils::PrintIterable(os, va.labels(), ":", [&](auto &stream, auto label) {
    stream << va.db_accessor().LabelName(label);
  });
  os << " {";
  utils::PrintIterable(os, va.Properties(), ", ",
                       [&](auto &stream, const auto &pair) {
                         stream << va.db_accessor().PropertyName(pair.first)
                                << ": " << pair.second;
                       });
  return os << "})";
}
