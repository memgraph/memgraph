#include "storage/distributed/record_accessor.hpp"

#include <glog/logging.h>

#include "database/distributed/graph_db_accessor.hpp"
#include "distributed/data_manager.hpp"
#include "distributed/updates_rpc_clients.hpp"
#include "durability/distributed/state_delta.hpp"
#include "storage/distributed/cached_data_lock.hpp"
#include "storage/distributed/edge.hpp"
#include "storage/distributed/vertex.hpp"

using database::StateDelta;

template <typename TRecord>
RecordAccessor<TRecord>::RecordAccessor(AddressT address,
                                        database::GraphDbAccessor &db_accessor)
    : db_accessor_(&db_accessor),
      address_(db_accessor.db().storage().LocalizedAddressIfPossible(address)) {
}

template <typename TRecord>
PropertyValue RecordAccessor<TRecord>::PropsAt(storage::Property key) const {
  auto guard = storage::GetDataLock(*this);
  return current().properties_.at(key);
}

template <>
void RecordAccessor<Vertex>::PropsSet(storage::Property key,
                                      PropertyValue value) {
  auto &dba = db_accessor();
  auto delta = StateDelta::PropsSetVertex(dba.transaction_id(), gid(), key,
                                          dba.PropertyName(key), value);
  auto guard = storage::GetDataLock(*this);
  update().properties_.set(key, value);
  if (is_local()) {
    dba.UpdatePropertyIndex(key, *this, &update());
  }
  ProcessDelta(delta);
}

template <>
void RecordAccessor<Edge>::PropsSet(storage::Property key,
                                    PropertyValue value) {
  auto &dba = db_accessor();
  auto delta = StateDelta::PropsSetEdge(dba.transaction_id(), gid(), key,
                                        dba.PropertyName(key), value);

  auto guard = storage::GetDataLock(*this);
  update().properties_.set(key, value);
  ProcessDelta(delta);
}

template <>
void RecordAccessor<Vertex>::PropsErase(storage::Property key) {
  auto &dba = db_accessor();
  auto delta =
      StateDelta::PropsSetVertex(dba.transaction_id(), gid(), key,
                                 dba.PropertyName(key), PropertyValue::Null);
  auto guard = storage::GetDataLock(*this);
  update().properties_.set(key, PropertyValue::Null);
  ProcessDelta(delta);
}

template <>
void RecordAccessor<Edge>::PropsErase(storage::Property key) {
  auto &dba = db_accessor();
  auto delta =
      StateDelta::PropsSetEdge(dba.transaction_id(), gid(), key,
                               dba.PropertyName(key), PropertyValue::Null);

  auto guard = storage::GetDataLock(*this);
  update().properties_.set(key, PropertyValue::Null);
  ProcessDelta(delta);
}

template <typename TRecord>
void RecordAccessor<TRecord>::PropsClear() {
  std::vector<storage::Property> to_remove;
  auto guard = storage::GetDataLock(*this);
  for (const auto &kv : update().properties_) to_remove.emplace_back(kv.first);
  for (const auto &prop : to_remove) {
    PropsErase(prop);
  }
}

template <typename TRecord>
const PropertyValueStore &RecordAccessor<TRecord>::Properties() const {
  auto guard = storage::GetDataLock(*this);
  return current().properties_;
}

template <typename TRecord>
bool RecordAccessor<TRecord>::operator==(const RecordAccessor &other) const {
  DCHECK(db_accessor_->transaction_id() == other.db_accessor_->transaction_id())
      << "Not in the same transaction.";
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
typename RecordAccessor<TRecord>::AddressT RecordAccessor<TRecord>::address()
    const {
  return address_;
}

template <typename TRecord>
typename RecordAccessor<TRecord>::AddressT
RecordAccessor<TRecord>::GlobalAddress() const {
  return is_local() ? storage::Address<mvcc::VersionList<TRecord>>(
                          gid(), db_accessor_->worker_id())
                    : address();
}

template <typename TRecord>
RecordAccessor<TRecord> &RecordAccessor<TRecord>::SwitchNew() {
  auto guard = storage::GetDataLock(*this);
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
TRecord *RecordAccessor<TRecord>::GetNew() const {
  if (!is_local()) {
    DCHECK(remote_.lock_counter > 0) << "Remote data is missing";
  }

  return new_;
}

template <typename TRecord>
RecordAccessor<TRecord> &RecordAccessor<TRecord>::SwitchOld() {
  current_ = old_ ? old_ : new_;
  return *this;
}

template <typename TRecord>
TRecord *RecordAccessor<TRecord>::GetOld() const {
  if (!is_local()) {
    DCHECK(remote_.lock_counter > 0) << "Remote data is missing";
  }

  return old_;
}

template <typename TRecord>
bool RecordAccessor<TRecord>::Reconstruct() const {
  auto &dba = db_accessor();
  auto guard = storage::GetDataLock(*this);
  if (is_local()) {
    address().local()->find_set_old_new(dba.transaction(), &old_, &new_);
  } else {
    // It's not possible that we have a global address for a graph element
    // that's local, because that is resolved in the constructor.
    // TODO in write queries it's possible the command has been advanced and
    // we need to invalidate the Cache and really get the latest stuff.
    // But only do that after the command has been advanced.
    dba.data_manager().template FindSetOldNew<TRecord>(
        dba.transaction_id(), address().worker_id(), gid(), &old_, &new_);
  }
  current_ = old_ ? old_ : new_;
  return old_ != nullptr || new_ != nullptr;
}

template <typename TRecord>
TRecord &RecordAccessor<TRecord>::update() const {
  auto &dba = db_accessor();
  auto guard = storage::GetDataLock(*this);
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

  if (address().is_local()) {
    new_ = address().local()->update(dba.transaction());
  } else {
    new_ = dba.data_manager().template FindNew<TRecord>(dba.transaction_id(),
                                                         address().gid());
  }

  DCHECK(new_ != nullptr) << "RecordAccessor.new_ is null after update";
  return *new_;
}

template <typename TRecord>
int64_t RecordAccessor<TRecord>::CypherId() const {
  auto &dba = db_accessor();
  if (is_local()) return address().local()->cypher_id();
  // Fetch data from the cache.
  //
  // NOTE: This part is executed when we need to migrate
  // a vertex and it has edges that don't belong to it. A machine that owns
  // the vertex still need to figure out what is the cypher_id for each
  // remote edge because the machine has to initiate remote edge creation
  // and for that call it has to know the remote cypher_ids.
  // TODO (buda): If we save cypher_id similar/next to edge_type we would save
  // a network call.
  return db_accessor_->data_manager()
      .template Find<TRecord>(dba.transaction().id_, address().worker_id(),
                              gid())
      .cypher_id;
}

template <typename TRecord>
void RecordAccessor<TRecord>::HoldCachedData() const {
  if (!is_local()) {
    if (remote_.lock_counter == 0) {
      // TODO (vkasljevic) uncomment once Remote has beed implemented
      // remote_.data = db_accessor_->data_manager().template Find<TRecord>(
      //     db_accessor_->transaction().id_, Address().worker_id(),
      //     Address().gid(), remote_.has_updated);
    }

    ++remote_.lock_counter;
    DCHECK(remote_.lock_counter <= 10000)
        << "Something wrong with lock counter";
  }
}

template <typename TRecord>
void RecordAccessor<TRecord>::ReleaseCachedData() const {
  if (!is_local()) {
    DCHECK(remote_.lock_counter > 0) << "Lock should exist at this point";
    --remote_.lock_counter;
    if (remote_.lock_counter == 0) {
      // TODO (vkasljevic) uncomment once Remote has beed implemented
      // remote_.data = nullptr;
    }
  }
}

template <typename TRecord>
const TRecord &RecordAccessor<TRecord>::current() const {
  if (!is_local()) {
    DCHECK(remote_.lock_counter > 0) << "Remote data is missing";
  }

  // Edges have lazily initialize mutable, versioned data (properties).
  if (std::is_same<TRecord, Edge>::value && current_ == nullptr) {
    bool reconstructed = Reconstruct();
    DCHECK(reconstructed) << "Unable to initialize record";
  }
  DCHECK(current_ != nullptr) << "RecordAccessor.current_ pointer is nullptr";
  return *current_;
}

template <typename TRecord>
void RecordAccessor<TRecord>::ProcessDelta(
    const database::StateDelta &delta) const {
  if (is_local()) {
    db_accessor().wal().Emplace(delta);
  } else {
    SendDelta(delta);
  }
}

template <typename TRecord>
void RecordAccessor<TRecord>::SendDelta(
    const database::StateDelta &delta) const {
  auto result =
      db_accessor_->updates_clients().Update(address().worker_id(), delta);
  switch (result) {
    case distributed::UpdateResult::DONE:
      break;
    default:
      // Update methods sends UpdateRpc to UpdatesRpcServer, server
      // appends delta to list and returns UpdateResult::DONE
      LOG(FATAL) << "Update should always return DONE";
  }
}

template class RecordAccessor<Vertex>;
template class RecordAccessor<Edge>;
