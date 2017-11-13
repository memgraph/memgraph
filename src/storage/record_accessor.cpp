#include "glog/logging.h"

#include "database/graph_db_accessor.hpp"
#include "storage/edge.hpp"
#include "storage/record_accessor.hpp"
#include "storage/vertex.hpp"

template <typename TRecord>
RecordAccessor<TRecord>::RecordAccessor(mvcc::VersionList<TRecord> &vlist,
                                        GraphDbAccessor &db_accessor)
    : vlist_(&vlist), db_accessor_(&db_accessor) {
  Reconstruct();
}

template <typename TRecord>
const PropertyValue &RecordAccessor<TRecord>::PropsAt(
    GraphDbTypes::Property key) const {
  return current().properties_.at(key);
}

template <>
void RecordAccessor<Vertex>::PropsSet(GraphDbTypes::Property key,
                                      PropertyValue value) {
  Vertex &vertex = update();
  vertex.properties_.set(key, value);
  auto &dba = db_accessor();
  dba.wal().PropsSetVertex(dba.transaction_id(), vlist_->id_,
                           dba.PropertyName(key), value);
  db_accessor().UpdatePropertyIndex(key, *this, &vertex);
}

template <>
void RecordAccessor<Edge>::PropsSet(GraphDbTypes::Property key,
                                    PropertyValue value) {
  update().properties_.set(key, value);
  auto &dba = db_accessor();
  dba.wal().PropsSetEdge(dba.transaction_id(), vlist_->id_,
                         dba.PropertyName(key), value);
}

template <>
size_t RecordAccessor<Vertex>::PropsErase(GraphDbTypes::Property key) {
  auto &dba = db_accessor();
  dba.wal().PropsSetVertex(dba.transaction_id(), vlist_->id_,
                           dba.PropertyName(key), PropertyValue::Null);
  return update().properties_.erase(key);
}

template <>
size_t RecordAccessor<Edge>::PropsErase(GraphDbTypes::Property key) {
  auto &dba = db_accessor();
  dba.wal().PropsSetEdge(dba.transaction_id(), vlist_->id_,
                         dba.PropertyName(key), PropertyValue::Null);
  return update().properties_.erase(key);
}

template <>
void RecordAccessor<Vertex>::PropsClear() {
  auto &updated = update();
  auto &dba = db_accessor();
  for (const auto &kv : updated.properties_)
    dba.wal().PropsSetVertex(dba.transaction_id(), vlist_->id_,
                             dba.PropertyName(kv.first), PropertyValue::Null);
  updated.properties_.clear();
}

template <>
void RecordAccessor<Edge>::PropsClear() {
  auto &updated = update();
  auto &dba = db_accessor();
  for (const auto &kv : updated.properties_)
    dba.wal().PropsSetEdge(dba.transaction_id(), vlist_->id_,
                           dba.PropertyName(kv.first), PropertyValue::Null);
  updated.properties_.clear();
}

template <typename TRecord>
const PropertyValueStore<GraphDbTypes::Property>
    &RecordAccessor<TRecord>::Properties() const {
  return current().properties_;
}

template <typename TRecord>
GraphDbAccessor &RecordAccessor<TRecord>::db_accessor() const {
  return *db_accessor_;
}

template <typename TRecord>
RecordAccessor<TRecord> &RecordAccessor<TRecord>::SwitchNew() {
  if (!new_) {
    // if new_ is not set yet, look for it
    // we can just Reconstruct the pointers, old_ will get initialized
    // to the same value as it has now, and the amount of work is the
    // same as just looking for a new_ record
    if (!Reconstruct())
      DLOG(FATAL)
          << "RecordAccessor::SwitchNew - accessor invalid after Reconstruct";
  }
  current_ = new_ ? new_ : old_;
  return *this;
}

template <typename TRecord>
RecordAccessor<TRecord> &RecordAccessor<TRecord>::SwitchOld() {
  current_ = old_ ? old_ : new_;
  return *this;
}

template <typename TRecord>
bool RecordAccessor<TRecord>::Reconstruct() {
  return db_accessor().Reconstruct(*this);
}

template <typename TRecord>
TRecord &RecordAccessor<TRecord>::update() {
  db_accessor().Update(*this);
  DCHECK(new_ != nullptr) << "RecordAccessor.new_ is null after update";
  return *new_;
}

template <typename TRecord>
const TRecord &RecordAccessor<TRecord>::current() const {
  DCHECK(current_ != nullptr) << "RecordAccessor.current_ pointer is nullptr";
  return *current_;
}

template class RecordAccessor<Vertex>;
template class RecordAccessor<Edge>;
