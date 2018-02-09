#include "glog/logging.h"

#include "database/graph_db_accessor.hpp"
#include "database/state_delta.hpp"
#include "distributed/remote_data_manager.hpp"
#include "distributed/remote_updates_rpc_clients.hpp"
#include "storage/edge.hpp"
#include "storage/record_accessor.hpp"
#include "storage/vertex.hpp"
#include "threading/sync/lock_timeout_exception.hpp"

using database::StateDelta;

template <typename TRecord>
RecordAccessor<TRecord>::RecordAccessor(AddressT address,
                                        database::GraphDbAccessor &db_accessor)
    : db_accessor_(&db_accessor), address_(NormalizedAddress(address)) {}

template <typename TRecord>
const PropertyValue &RecordAccessor<TRecord>::PropsAt(
    storage::Property key) const {
  return current().properties_.at(key);
}

template <>
void RecordAccessor<Vertex>::PropsSet(storage::Property key,
                                      PropertyValue value) {
  auto &dba = db_accessor();
  ProcessDelta(StateDelta::PropsSetVertex(dba.transaction_id(), gid(), key,
                                          dba.PropertyName(key), value));
  if (is_local()) {
    dba.UpdatePropertyIndex(key, *this, &update());
  }
}

template <>
void RecordAccessor<Edge>::PropsSet(storage::Property key,
                                    PropertyValue value) {
  auto &dba = db_accessor();
  ProcessDelta(StateDelta::PropsSetEdge(dba.transaction_id(), gid(), key,
                                        dba.PropertyName(key), value));
}

template <>
void RecordAccessor<Vertex>::PropsErase(storage::Property key) {
  auto &dba = db_accessor();
  ProcessDelta(StateDelta::PropsSetVertex(dba.transaction_id(), gid(), key,
                                          dba.PropertyName(key),
                                          PropertyValue::Null));
}

template <>
void RecordAccessor<Edge>::PropsErase(storage::Property key) {
  auto &dba = db_accessor();
  ProcessDelta(StateDelta::PropsSetEdge(dba.transaction_id(), gid(), key,
                                        dba.PropertyName(key),
                                        PropertyValue::Null));
}

template <typename TRecord>
void RecordAccessor<TRecord>::PropsClear() {
  auto &dba = db_accessor();
  std::vector<storage::Property> to_remove;
  for (const auto &kv : update().properties_) to_remove.emplace_back(kv.first);
  for (const auto &prop : to_remove) {
    if (std::is_same<TRecord, Vertex>::value) {
      ProcessDelta(StateDelta::PropsSetVertex(dba.transaction_id(), gid(), prop,
                                              dba.PropertyName(prop),
                                              PropertyValue::Null));
    } else {
      ProcessDelta(StateDelta::PropsSetEdge(dba.transaction_id(), gid(), prop,
                                            dba.PropertyName(prop),
                                            PropertyValue::Null));
    }
  }
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
  return is_local() ? address_.local()->gid_ : address_.gid();
}

template <typename TRecord>
storage::Address<mvcc::VersionList<TRecord>> RecordAccessor<TRecord>::address()
    const {
  return address_;
}

template <typename TRecord>
storage::Address<mvcc::VersionList<TRecord>>
RecordAccessor<TRecord>::GlobalAddress() const {
  return is_local() ? storage::Address<mvcc::VersionList<TRecord>>(
                          gid(), db_accessor_->db_.WorkerId())
                    : address_;
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
    // A remote record only sees local updates, until the command is advanced.
    // So this does nothing, as the old/new switch happens below.
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
  auto &dba = db_accessor();
  if (is_local()) {
    address_.local()->find_set_old_new(dba.transaction(), old_, new_);
  } else {
    // It's not possible that we have a global address for a graph element
    // that's local, because that is resolved in the constructor.
    // TODO in write queries it's possible the command has been advanced and
    // we need to invalidate the RemoteCache and really get the latest stuff.
    // But only do that after the command has been advanced.
    auto &remote_cache =
        dba.db().remote_data_manager().template Elements<TRecord>(
            dba.transaction_id());
    remote_cache.FindSetOldNew(dba.transaction().id_, address_.worker_id(),
                               address_.gid(), old_, new_);
  }
  current_ = old_ ? old_ : new_;
  return old_ != nullptr || new_ != nullptr;
}

template <typename TRecord>
TRecord &RecordAccessor<TRecord>::update() const {
  auto &dba = db_accessor();
  // Edges have lazily initialize mutable, versioned data (properties).
  if (std::is_same<TRecord, Edge>::value && current_ == nullptr) {
    bool reconstructed = Reconstruct();
    DCHECK(reconstructed) << "Unable to initialize record";
  }

  const auto &t = dba.transaction();
  if (!new_ && old_->is_expired_by(t))
    throw RecordDeletedError();
  else if (new_ && new_->is_expired_by(t))
    throw RecordDeletedError();

  if (new_) return *new_;

  if (is_local()) {
    new_ = address_.local()->update(t);
  } else {
    auto &remote_cache =
        dba.db().remote_data_manager().template Elements<TRecord>(
            dba.transaction_id());
    new_ = remote_cache.FindNew(address_.gid());
  }
  DCHECK(new_ != nullptr) << "RecordAccessor.new_ is null after update";
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
void RecordAccessor<TRecord>::ProcessDelta(
    const database::StateDelta &delta) const {
  // Apply the delta both on local and remote data. We need to see the changes
  // we make to remote data, even if it's not applied immediately.
  auto &updated = update();
  switch (delta.type) {
    case StateDelta::Type::TRANSACTION_BEGIN:
    case StateDelta::Type::TRANSACTION_COMMIT:
    case StateDelta::Type::TRANSACTION_ABORT:
    case StateDelta::Type::CREATE_VERTEX:
    case StateDelta::Type::CREATE_EDGE:
    case StateDelta::Type::REMOVE_VERTEX:
    case StateDelta::Type::REMOVE_EDGE:
    case StateDelta::Type::BUILD_INDEX:
      LOG(FATAL)
          << "Can only apply record update deltas for remote graph element";
    case StateDelta::Type::SET_PROPERTY_VERTEX:
    case StateDelta::Type::SET_PROPERTY_EDGE:
      updated.properties_.set(delta.property, delta.value);
      break;
    case StateDelta::Type::ADD_LABEL:
      // It is only possible that ADD_LABEL gets calld on a VertexAccessor.
      reinterpret_cast<Vertex &>(updated).labels_.emplace_back(delta.label);
      break;
    case StateDelta::Type::REMOVE_LABEL: {
      // It is only possible that REMOVE_LABEL gets calld on a VertexAccessor.
      auto &labels = reinterpret_cast<Vertex &>(updated).labels_;
      auto found = std::find(labels.begin(), labels.end(), delta.label);
      std::swap(*found, labels.back());
      labels.pop_back();
    } break;
  }

  if (is_local()) {
    db_accessor().wal().Emplace(delta);
  } else {
    auto result = db_accessor().db().remote_updates_clients().RemoteUpdate(
        address().worker_id(), delta);
    switch (result) {
      case distributed::RemoteUpdateResult::DONE:
        break;
      case distributed::RemoteUpdateResult::SERIALIZATION_ERROR:
        throw mvcc::SerializationError();
      case distributed::RemoteUpdateResult::UPDATE_DELETED_ERROR:
        throw RecordDeletedError();
      case distributed::RemoteUpdateResult::LOCK_TIMEOUT_ERROR:
        throw LockTimeoutException("Lock timeout on remote worker");
    }
  }
}

template <>
RecordAccessor<Vertex>::AddressT RecordAccessor<Vertex>::NormalizedAddress(
    AddressT address) const {
  if (address.is_local()) return address;
  if (address.worker_id() == db_accessor().db_.WorkerId()) {
    return AddressT(db_accessor().LocalVertexAddress(address.gid()));
  }

  return address;
}

template <>
RecordAccessor<Edge>::AddressT RecordAccessor<Edge>::NormalizedAddress(
    AddressT address) const {
  if (address.is_local()) return address;
  if (address.worker_id() == db_accessor().db_.WorkerId()) {
    return AddressT(db_accessor().LocalEdgeAddress(address.gid()));
  }

  return address;
}

template class RecordAccessor<Vertex>;
template class RecordAccessor<Edge>;
