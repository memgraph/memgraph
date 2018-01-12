#include "glog/logging.h"

#include "database/graph_db_accessor.hpp"
#include "storage/edge.hpp"
#include "storage/record_accessor.hpp"
#include "storage/vertex.hpp"

using database::StateDelta;

template <typename TRecord>
RecordAccessor<TRecord>::RecordAccessor(AddressT address,
                                        database::GraphDbAccessor &db_accessor)
    : db_accessor_(&db_accessor), address_(address) {}

template <typename TRecord>
const PropertyValue &RecordAccessor<TRecord>::PropsAt(
    database::Property key) const {
  return current().properties_.at(key);
}

template <>
void RecordAccessor<Vertex>::PropsSet(database::Property key,
                                      PropertyValue value) {
  Vertex &vertex = update();
  vertex.properties_.set(key, value);
  auto &dba = db_accessor();
  // TODO use the delta for handling.
  dba.wal().Emplace(StateDelta::PropsSetVertex(dba.transaction_id(), gid(),
                                               dba.PropertyName(key), value));
  if (is_local()) {
    db_accessor().UpdatePropertyIndex(key, *this, &vertex);
  }
}

template <>
void RecordAccessor<Edge>::PropsSet(database::Property key,
                                    PropertyValue value) {
  update().properties_.set(key, value);
  auto &dba = db_accessor();
  // TODO use the delta for handling.
  dba.wal().Emplace(StateDelta::PropsSetEdge(dba.transaction_id(), gid(),
                                             dba.PropertyName(key), value));
}

template <>
size_t RecordAccessor<Vertex>::PropsErase(database::Property key) {
  auto &dba = db_accessor();
  // TODO use the delta for handling.
  dba.wal().Emplace(StateDelta::PropsSetVertex(
      dba.transaction_id(), gid(), dba.PropertyName(key), PropertyValue::Null));
  return update().properties_.erase(key);
}

template <>
size_t RecordAccessor<Edge>::PropsErase(database::Property key) {
  auto &dba = db_accessor();
  // TODO use the delta for handling.
  dba.wal().Emplace(StateDelta::PropsSetEdge(
      dba.transaction_id(), gid(), dba.PropertyName(key), PropertyValue::Null));
  return update().properties_.erase(key);
}

template <>
void RecordAccessor<Vertex>::PropsClear() {
  auto &updated = update();
  // TODO use the delta for handling.
  auto &dba = db_accessor();
  for (const auto &kv : updated.properties_)
    dba.wal().Emplace(StateDelta::PropsSetVertex(dba.transaction_id(), gid(),
                                                 dba.PropertyName(kv.first),
                                                 PropertyValue::Null));
  updated.properties_.clear();
}

template <>
void RecordAccessor<Edge>::PropsClear() {
  auto &updated = update();
  auto &dba = db_accessor();
  // TODO use the delta for handling.
  for (const auto &kv : updated.properties_)
    dba.wal().Emplace(StateDelta::PropsSetEdge(dba.transaction_id(), gid(),
                                               dba.PropertyName(kv.first),
                                               PropertyValue::Null));
  updated.properties_.clear();
}

template <typename TRecord>
const PropertyValueStore &RecordAccessor<TRecord>::Properties() const {
  return current().properties_;
}

template <typename TRecord>
bool RecordAccessor<TRecord>::operator==(const RecordAccessor &other) const {
  DCHECK(db_accessor_ == other.db_accessor_) << "Not in the same transaction.";
  return address_ == other.address_;
}

template <typename TRecord>
database::GraphDbAccessor &RecordAccessor<TRecord>::db_accessor() const {
  return *db_accessor_;
}

template <typename TRecord>
gid::Gid RecordAccessor<TRecord>::gid() const {
  return is_local() ? address_.local()->gid_ : address_.global_id();
}

template <typename TRecord>
storage::Address<mvcc::VersionList<TRecord>> RecordAccessor<TRecord>::address()
    const {
  return address_;
}

template <typename TRecord>
RecordAccessor<TRecord> &RecordAccessor<TRecord>::SwitchNew() {
  if (is_local()) {
    if (!new_) {
      // if new_ is not set yet, look for it
      // we can just Reconstruct the pointers, old_ will get initialized
      // to the same value as it has now, and the amount of work is the
      // same as just looking for a new_ record
      if (!Reconstruct())
        DLOG(FATAL)
            << "RecordAccessor::SwitchNew - accessor invalid after Reconstruct";
    }
  } else {
    // TODO If we have distributed execution, here it's necessary to load the
    // data from the it's home worker. When only storage is distributed, it's
    // enough just to switch to the new record if we have it.
    if (!new_) {
      new_ = db_accessor().template remote_elements<TRecord>().FindNew(
          address_.global_id(), false);
    }
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
bool RecordAccessor<TRecord>::Reconstruct() const {
  if (is_local()) {
    address_.local()->find_set_old_new(db_accessor_->transaction(), old_, new_);
  } else {
    db_accessor().template remote_elements<TRecord>().FindSetOldNew(
        db_accessor().transaction(), address_.global_id(), old_, new_);
  }
  current_ = old_ ? old_ : new_;
  return old_ != nullptr || new_ != nullptr;
}

template <typename TRecord>
TRecord &RecordAccessor<TRecord>::update() const {
  // Edges have lazily initialize mutable, versioned data (properties).
  if (std::is_same<TRecord, Edge>::value && current_ == nullptr) {
    bool reconstructed = Reconstruct();
    DCHECK(reconstructed) << "Unable to initialize record";
  }

  auto &t = db_accessor_->transaction();
  if (!new_) {
    DCHECK(!old_->is_expired_by(t))
        << "Can't update a record deleted in the current transaction+commad";
  } else {
    DCHECK(!new_->is_expired_by(t))
        << "Can't update a record deleted in the current transaction+command";
  }

  if (new_) return *new_;

  if (is_local()) {
    new_ = address_.local()->update(t);
    DCHECK(new_ != nullptr) << "RecordAccessor.new_ is null after update";
  } else {
    new_ = db_accessor().template remote_elements<TRecord>().FindNew(
        address_.global_id(), true);
  }
  return *new_;
}

template <typename TRecord>
const TRecord &RecordAccessor<TRecord>::current() const {
  // Edges have lazily initialize mutable, versioned data (properties).
  if (std::is_same<TRecord, Edge>::value && current_ == nullptr)
    RecordAccessor::Reconstruct();
  DCHECK(current_ != nullptr) << "RecordAccessor.current_ pointer is nullptr";
  return *current_;
}

template <typename TRecord>
void RecordAccessor<TRecord>::ProcessDelta(const GraphStateDelta &) const {
  LOG(ERROR) << "Delta processing not yet implemented";
  if (is_local()) {
    // TODO write delta to WAL
  } else {
    // TODO use the delta to perform a remote update.
    // TODO check for results (success, serialization_error, ...)
  }
}

template class RecordAccessor<Vertex>;
template class RecordAccessor<Edge>;
