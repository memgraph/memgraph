#include "storage/distributed/vertex_accessor.hpp"

#include <algorithm>

#include "database/graph_db_accessor.hpp"
#include "durability/distributed/state_delta.hpp"
#include "utils/algorithm.hpp"

VertexAccessor::VertexAccessor(VertexAddress address,
                               database::GraphDbAccessor &db_accessor)
    : RecordAccessor(address, db_accessor, db_accessor.GetVertexImpl()),
      impl_(db_accessor.GetVertexImpl()) {
  Reconstruct();
}

size_t VertexAccessor::out_degree() const { return current().out_.size(); }

size_t VertexAccessor::in_degree() const { return current().in_.size(); }

void VertexAccessor::add_label(storage::Label label) {
  return impl_->AddLabel(*this, label);
}

void VertexAccessor::remove_label(storage::Label label) {
  return impl_->RemoveLabel(*this, label);
}

bool VertexAccessor::has_label(storage::Label label) const {
  auto &labels = this->current().labels_;
  return std::find(labels.begin(), labels.end(), label) != labels.end();
}

const std::vector<storage::Label> &VertexAccessor::labels() const {
  return this->current().labels_;
}

void VertexAccessor::RemoveOutEdge(storage::EdgeAddress edge) {
  auto &dba = db_accessor();
  auto delta = database::StateDelta::RemoveOutEdge(
      dba.transaction_id(), gid(), dba.db().storage().GlobalizedAddress(edge));

  SwitchNew();
  if (current().is_expired_by(dba.transaction())) return;

  update().out_.RemoveEdge(dba.db().storage().LocalizedAddressIfPossible(edge));
  ProcessDelta(delta);
}

void VertexAccessor::RemoveInEdge(storage::EdgeAddress edge) {
  auto &dba = db_accessor();
  auto delta = database::StateDelta::RemoveInEdge(
      dba.transaction_id(), gid(), dba.db().storage().GlobalizedAddress(edge));

  SwitchNew();
  if (current().is_expired_by(dba.transaction())) return;

  update().in_.RemoveEdge(dba.db().storage().LocalizedAddressIfPossible(edge));
  ProcessDelta(delta);
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
