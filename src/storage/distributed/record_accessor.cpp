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
  if (is_local()) {
    new (&local_) Local();
  } else {
    new (&remote_) Remote();
  }
}

template <typename TRecord>
RecordAccessor<TRecord>::RecordAccessor(const RecordAccessor &other)
    : db_accessor_(other.db_accessor_), address_(other.address_) {
  is_initialized_ = other.is_initialized_;
  if (other.is_local()) {
    new (&local_) Local();
    local_.old = other.local_.old;
    local_.newr = other.local_.newr;
  } else {
    DCHECK(other.remote_.lock_counter == 0);
    new (&remote_) Remote();
    remote_.has_updated = other.remote_.has_updated;
  }

  current_ = other.current_;
}

template <typename TRecord>
RecordAccessor<TRecord> &RecordAccessor<TRecord>::operator=(
    const RecordAccessor &other) {
  if (is_local()) {
    local_.~Local();
  } else {
    DCHECK(remote_.lock_counter == 0);
    remote_.~Remote();
  }

  db_accessor_ = other.db_accessor_;
  is_initialized_ = other.is_initialized_;
  if (other.is_local()) {
    new (&local_) Local();
    local_.old = other.local_.old;
    local_.newr = other.local_.newr;
  } else {
    new (&remote_) Remote();
    remote_.has_updated = other.remote_.has_updated;
  }

  address_ = other.address_;
  current_ = other.current_;
  return *this;
}

template <typename TRecord>
RecordAccessor<TRecord>::~RecordAccessor() {
  if (is_local()) {
    local_.~Local();
  } else {
    remote_.~Remote();
  }
}

template <typename TRecord>
PropertyValue RecordAccessor<TRecord>::PropsAt(storage::Property key) const {
  auto guard = storage::GetDataLock(*this);
  return GetCurrent()->properties_.at(key);
}

template <>
void RecordAccessor<Vertex>::PropsSet(storage::Property key,
                                      PropertyValue value) {
  auto &dba = db_accessor();
  auto delta = StateDelta::PropsSetVertex(dba.transaction_id(), gid(), key,
                                          dba.PropertyName(key), value);
  auto guard = storage::GetDataLock(*this);
  update();
  GetNew()->properties_.set(key, value);
  if (is_local()) {
    dba.UpdatePropertyIndex(key, *this, GetNew());
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
  update();
  GetNew()->properties_.set(key, value);
  ProcessDelta(delta);
}

template <>
void RecordAccessor<Vertex>::PropsErase(storage::Property key) {
  auto &dba = db_accessor();
  auto delta =
      StateDelta::PropsSetVertex(dba.transaction_id(), gid(), key,
                                 dba.PropertyName(key), PropertyValue::Null);
  auto guard = storage::GetDataLock(*this);
  update();
  GetNew()->properties_.set(key, PropertyValue::Null);
  ProcessDelta(delta);
}

template <>
void RecordAccessor<Edge>::PropsErase(storage::Property key) {
  auto &dba = db_accessor();
  auto delta =
      StateDelta::PropsSetEdge(dba.transaction_id(), gid(), key,
                               dba.PropertyName(key), PropertyValue::Null);

  auto guard = storage::GetDataLock(*this);
  update();
  GetNew()->properties_.set(key, PropertyValue::Null);
  ProcessDelta(delta);
}

template <typename TRecord>
void RecordAccessor<TRecord>::PropsClear() {
  std::vector<storage::Property> to_remove;
  auto guard = storage::GetDataLock(*this);
  update();
  for (const auto &kv : GetNew()->properties_) to_remove.emplace_back(kv.first);
  for (const auto &prop : to_remove) {
    PropsErase(prop);
  }
}

template <typename TRecord>
PropertyValueStore RecordAccessor<TRecord>::Properties() const {
  auto guard = storage::GetDataLock(*this);
  return GetCurrent()->properties_;
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
  if (is_local()) {
    if (!local_.newr) {
      // if local new record is not set yet, look for it we can just Reconstruct
      // the pointers, local old record will get initialized to the same value
      // as it has now, and the amount of work is the same as just looking for a
      // new record
      if (!Reconstruct())
        DLOG(FATAL)
            << "RecordAccessor::SwitchNew - accessor invalid after Reconstruct";
    }
    current_ = local_.newr ? CurrentRecord::NEW : CurrentRecord::OLD;

  } else {
    auto guard = storage::GetDataLock(*this);
    current_ =
        remote_.data->new_record ? CurrentRecord::NEW : CurrentRecord::OLD;
  }

  return *this;
}

template <typename TRecord>
TRecord *RecordAccessor<TRecord>::GetNew() const {
  if (is_local()) {
    return local_.newr;
  } else {
    DCHECK(remote_.data) << "Remote data is missing";
    return remote_.data->new_record.get();
  }
}

template <typename TRecord>
RecordAccessor<TRecord> &RecordAccessor<TRecord>::SwitchOld() {
  if (is_local()) {
    current_ = local_.old ? CurrentRecord::OLD : CurrentRecord::NEW;
  } else {
    auto guard = storage::GetDataLock(*this);
    current_ = remote_.data->old_record.get() ? CurrentRecord::OLD
                                              : CurrentRecord::NEW;
  }

  return *this;
}

template <typename TRecord>
TRecord *RecordAccessor<TRecord>::GetOld() const {
  if (is_local()) {
    return local_.old;
  } else {
    DCHECK(remote_.data) << "Remote data is missing";
    return remote_.data->old_record.get();
  }
}

template <typename TRecord>
bool RecordAccessor<TRecord>::Reconstruct() const {
  is_initialized_ = true;
  auto &dba = db_accessor();

  if (is_local()) {
    address().local()->find_set_old_new(dba.transaction(), &local_.old,
                                        &local_.newr);
    current_ = local_.old ? CurrentRecord::OLD : CurrentRecord::NEW;
    return local_.old != nullptr || local_.newr != nullptr;

  } else {
    auto guard =  storage::GetDataLock(*this);
    TRecord *old_ = remote_.data->old_record.get();
    TRecord *new_ = remote_.data->new_record.get();
    current_ = old_ ? CurrentRecord::OLD : CurrentRecord::NEW;
    return old_ != nullptr || new_ != nullptr;
  }
}

template <typename TRecord>
void RecordAccessor<TRecord>::update() const {
  // Edges have lazily initialize mutable, versioned data (properties).
  if (std::is_same<TRecord, Edge>::value && is_initialized_ == false) {
    bool reconstructed = Reconstruct();
    DCHECK(reconstructed) << "Unable to initialize record";
  }

  auto &dba = db_accessor();
  auto guard =  storage::GetDataLock(*this);
  const auto &t = dba.transaction();
  TRecord *old = is_local() ? local_.old : remote_.data->old_record.get();
  TRecord *newr = is_local() ? local_.newr : remote_.data->new_record.get();

  if (!newr && old->is_expired_by(t))
    throw RecordDeletedError();
  else if (newr && newr->is_expired_by(t))
    throw RecordDeletedError();

  if (newr) return;

  if (is_local()) {
    local_.newr = address().local()->update(t);
    DCHECK(local_.newr != nullptr)
        << "RecordAccessor.new_ is null after update";

  } else {
    remote_.has_updated = true;

    if (remote_.lock_counter > 0) {
      remote_.data = db_accessor_->data_manager().Find<TRecord>(
          dba.transaction_id(), dba.worker_id(), address().worker_id(),
          address().gid(), remote_.has_updated);
    }

    DCHECK(remote_.data->new_record)
        << "RecordAccessor.new_ is null after update";
  }
}

template <typename TRecord>
bool RecordAccessor<TRecord>::Visible(const tx::Transaction &t,
                                      bool current_state) const {
  auto guard =  storage::GetDataLock(*this);
  TRecord *old = is_local() ? local_.old : remote_.data->old_record.get();
  TRecord *newr = is_local() ? local_.newr : remote_.data->new_record.get();
  return (old && !(current_state && old->is_expired_by(t))) ||
         (current_state && newr && !newr->is_expired_by(t));
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
      .template Find<TRecord>(dba.transaction().id_, dba.worker_id(),
                              address().worker_id(), gid())
      ->cypher_id;
}

template <typename TRecord>
void RecordAccessor<TRecord>::HoldCachedData() const {
  if (!is_local()) {
    if (remote_.lock_counter == 0) {
      remote_.data = db_accessor_->data_manager().template Find<TRecord>(
          db_accessor_->transaction().id_, db_accessor_->worker_id(),
          address().worker_id(), address().gid(), remote_.has_updated);
    }

    ++remote_.lock_counter;
    DCHECK(remote_.lock_counter <= 10000)
        << "Something wrong with RemoteDataLock";
  }
}

template <typename TRecord>
void RecordAccessor<TRecord>::ReleaseCachedData() const {
 if (!is_local()) {
    DCHECK(remote_.lock_counter > 0) << "Lock should exist at this point";
    --remote_.lock_counter;
    if (remote_.lock_counter == 0) {
      remote_.data = nullptr;
    }
  }
}

template <typename TRecord>
TRecord *RecordAccessor<TRecord>::GetCurrent() const {
  // Edges have lazily initialize mutable, versioned data (properties).
  if (std::is_same<TRecord, Edge>::value && is_initialized_ == false) {
    Reconstruct();
  }

  if (is_local()) {
    return current_ == CurrentRecord::OLD ? local_.old : local_.newr;
  } else {
    DCHECK(remote_.data) << "CachedDataRecord is missing";
    if (current_ == CurrentRecord::NEW) {
      if (!remote_.data->new_record && remote_.data->old_record) {
        current_ = CurrentRecord::OLD;
        return remote_.data->old_record.get();
      }
      return remote_.data->new_record.get();
    }
    return remote_.data->old_record.get();
  }
}

template <typename TRecord>
void RecordAccessor<TRecord>::ProcessDelta(
    const database::StateDelta &delta) const {
  if (is_local()) {
    db_accessor().wal().Emplace(delta);
  } else {
    SendDelta(delta);
    // This is needed because Update() method creates new record if it doesn't
    // exist. If that record is evicted from cache and fetched again it wont
    // have new record. Once delta has been sent record will have new so we
    // don't have to update anymore.
    remote_.has_updated = false;
  }
}

template <typename TRecord>
void RecordAccessor<TRecord>::SendDelta(
    const database::StateDelta &delta) const {
  auto result = db_accessor_->updates_clients().Update(
      db_accessor_->worker_id(), address().worker_id(), delta);
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
