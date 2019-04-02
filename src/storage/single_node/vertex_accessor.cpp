#include "storage/single_node/vertex_accessor.hpp"

#include <algorithm>

#include "database/single_node/graph_db_accessor.hpp"
#include "durability/single_node/state_delta.hpp"
#include "utils/algorithm.hpp"

VertexAccessor::VertexAccessor(mvcc::VersionList<Vertex> *address,
                               database::GraphDbAccessor &db_accessor)
    : RecordAccessor(address, db_accessor) {
  Reconstruct();
}

size_t VertexAccessor::out_degree() const { return current().out_.size(); }

size_t VertexAccessor::in_degree() const { return current().in_.size(); }

void VertexAccessor::add_label(storage::Label label) {
  auto &dba = db_accessor();
  auto delta = database::StateDelta::AddLabel(dba.transaction_id(), gid(),
                                              label, dba.LabelName(label));
  Vertex &vertex = update();
  // not a duplicate label, add it
  if (!utils::Contains(vertex.labels_, label)) {
    vertex.labels_.emplace_back(label);
    dba.wal().Emplace(delta);
    dba.UpdateOnAddLabel(label, *this, &vertex);
  }
}

void VertexAccessor::remove_label(storage::Label label) {
  auto &dba = db_accessor();
  auto delta = database::StateDelta::RemoveLabel(dba.transaction_id(), gid(),
                                                 label, dba.LabelName(label));
  Vertex &vertex = update();
  if (utils::Contains(vertex.labels_, label)) {
    auto &labels = vertex.labels_;
    auto found = std::find(labels.begin(), labels.end(), delta.label);
    std::swap(*found, labels.back());
    labels.pop_back();
    dba.wal().Emplace(delta);
    dba.UpdateOnRemoveLabel(label, *this);
  }
}

bool VertexAccessor::has_label(storage::Label label) const {
  auto &labels = this->current().labels_;
  return std::find(labels.begin(), labels.end(), label) != labels.end();
}

const std::vector<storage::Label> &VertexAccessor::labels() const {
  return this->current().labels_;
}

void VertexAccessor::RemoveOutEdge(mvcc::VersionList<Edge> *edge) {
  auto &dba = db_accessor();

  SwitchNew();
  if (current().is_expired_by(dba.transaction())) return;

  update().out_.RemoveEdge(edge);
}

void VertexAccessor::RemoveInEdge(mvcc::VersionList<Edge> *edge) {
  auto &dba = db_accessor();

  SwitchNew();
  if (current().is_expired_by(dba.transaction())) return;

  update().in_.RemoveEdge(edge);
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
