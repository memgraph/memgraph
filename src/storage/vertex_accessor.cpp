#include "storage/vertex_accessor.hpp"

#include <algorithm>

#include "database/graph_db_accessor.hpp"
#include "database/state_delta.hpp"
#include "utils/algorithm.hpp"

size_t VertexAccessor::out_degree() const { return current().out_.size(); }

size_t VertexAccessor::in_degree() const { return current().in_.size(); }

bool VertexAccessor::add_label(storage::Label label) {
  auto &updated = update();
  if (utils::Contains(updated.labels_, label)) return false;

  // not a duplicate label, add it
  auto &dba = db_accessor();
  ProcessDelta(database::StateDelta::AddLabel(dba.transaction_id(), gid(),
                                              label, dba.LabelName(label)));
  Vertex &vertex = update();

  if (is_local()) {
    dba.UpdateLabelIndices(label, *this, &vertex);
  }
  return true;
}

size_t VertexAccessor::remove_label(storage::Label label) {
  if (!utils::Contains(update().labels_, label)) return 0;

  auto &dba = db_accessor();
  ProcessDelta(database::StateDelta::RemoveLabel(dba.transaction_id(), gid(),
                                                 label, dba.LabelName(label)));
  return 1;
}

bool VertexAccessor::has_label(storage::Label label) const {
  auto &labels = this->current().labels_;
  return std::find(labels.begin(), labels.end(), label) != labels.end();
}

const std::vector<storage::Label> &VertexAccessor::labels() const {
  return this->current().labels_;
}

std::ostream &operator<<(std::ostream &os, const VertexAccessor &va) {
  os << "V(";
  utils::PrintIterable(os, va.labels(), ":", [&](auto &stream, auto label) {
    stream << va.db_accessor().LabelName(label);
  });
  os << " {";
  utils::PrintIterable(os, va.Properties(), ", ", [&](auto &stream,
                                                      const auto &pair) {
    stream << va.db_accessor().PropertyName(pair.first) << ": " << pair.second;
  });
  return os << "})";
}
