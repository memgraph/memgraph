#include <functional>
#include <future>

#include "glog/logging.h"

#include "database/graph_db_accessor.hpp"
#include "database/state_delta.hpp"
#include "distributed/index_rpc_messages.hpp"
#include "distributed/remote_data_manager.hpp"
#include "distributed/remote_updates_rpc_clients.hpp"
#include "storage/address_types.hpp"
#include "storage/edge.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/atomic.hpp"
#include "utils/on_scope_exit.hpp"

namespace database {

#define LOCALIZED_ADDRESS_SPECIALIZATION(type)              \
  template <>                                               \
  storage::type##Address GraphDbAccessor::LocalizedAddress( \
      storage::type##Address address) const {               \
    if (address.is_local()) return address;                 \
    if (address.worker_id() == db().WorkerId()) {           \
      return Local##type##Address(address.gid());           \
    }                                                       \
    return address;                                         \
  }

LOCALIZED_ADDRESS_SPECIALIZATION(Vertex)
LOCALIZED_ADDRESS_SPECIALIZATION(Edge)

#undef LOCALIZED_ADDRESS_SPECIALIZATION

#define GLOBALIZED_ADDRESS_SPECIALIZATION(type)              \
  template <>                                                \
  storage::type##Address GraphDbAccessor::GlobalizedAddress( \
      storage::type##Address address) const {                \
    if (address.is_remote()) return address;                 \
    return {address.local()->gid_, db().WorkerId()};         \
  }

GLOBALIZED_ADDRESS_SPECIALIZATION(Vertex)
GLOBALIZED_ADDRESS_SPECIALIZATION(Edge)

#undef GLOBALIZED_ADDRESS_SPECIALIZATION

GraphDbAccessor::GraphDbAccessor(GraphDb &db)
    : db_(db),
      transaction_(*db.tx_engine().Begin()),
      transaction_starter_{true} {}

GraphDbAccessor::GraphDbAccessor(GraphDb &db, tx::transaction_id_t tx_id)
    : db_(db),
      transaction_(*db.tx_engine().RunningTransaction(tx_id)),
      transaction_starter_{false} {}

GraphDbAccessor::~GraphDbAccessor() {
  if (transaction_starter_ && !commited_ && !aborted_) {
    this->Abort();
  }
}

tx::transaction_id_t GraphDbAccessor::transaction_id() const {
  return transaction_.id_;
}

void GraphDbAccessor::AdvanceCommand() {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  db_.tx_engine().Advance(transaction_.id_);
}

void GraphDbAccessor::Commit() {
  DCHECK(!commited_ && !aborted_) << "Already aborted or commited transaction.";
  db_.tx_engine().Commit(transaction_);
  commited_ = true;
}

void GraphDbAccessor::Abort() {
  DCHECK(!commited_ && !aborted_) << "Already aborted or commited transaction.";
  db_.tx_engine().Abort(transaction_);
  aborted_ = true;
}

bool GraphDbAccessor::should_abort() const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return transaction_.should_abort();
}

durability::WriteAheadLog &GraphDbAccessor::wal() { return db_.wal(); }

VertexAccessor GraphDbAccessor::InsertVertex(
    std::experimental::optional<gid::Gid> requested_gid) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";

  auto gid = db_.storage().vertex_generator_.Next(requested_gid);
  auto vertex_vlist = new mvcc::VersionList<Vertex>(transaction_, gid);

  bool success =
      db_.storage().vertices_.access().insert(gid, vertex_vlist).second;
  CHECK(success) << "Attempting to insert a vertex with an existing GID: "
                 << gid;
  wal().Emplace(
      database::StateDelta::CreateVertex(transaction_.id_, vertex_vlist->gid_));
  return VertexAccessor(vertex_vlist, *this);
}

VertexAccessor GraphDbAccessor::InsertVertexIntoRemote(
    int worker_id, const std::vector<storage::Label> &labels,
    const std::unordered_map<storage::Property, query::TypedValue>
        &properties) {
  CHECK(worker_id != db().WorkerId())
      << "Not allowed to call InsertVertexIntoRemote for local worker";

  gid::Gid gid = db().remote_updates_clients().RemoteCreateVertex(
      worker_id, transaction_id(), labels, properties);

  auto vertex = std::make_unique<Vertex>();
  vertex->labels_ = labels;
  for (auto &kv : properties) vertex->properties_.set(kv.first, kv.second);

  db().remote_data_manager()
      .Vertices(transaction_id())
      .emplace(gid, nullptr, std::move(vertex));
  return VertexAccessor({gid, worker_id}, *this);
}

std::experimental::optional<VertexAccessor> GraphDbAccessor::FindVertex(
    gid::Gid gid, bool current_state) {
  VertexAccessor record_accessor(LocalVertexAddress(gid), *this);
  if (!record_accessor.Visible(transaction(), current_state))
    return std::experimental::nullopt;
  return record_accessor;
}

VertexAccessor GraphDbAccessor::FindVertexChecked(gid::Gid gid,
                                                  bool current_state) {
  auto found = FindVertex(gid, current_state);
  CHECK(found) << "Unable to find vertex for id: " << gid;
  return *found;
}

std::experimental::optional<EdgeAccessor> GraphDbAccessor::FindEdge(
    gid::Gid gid, bool current_state) {
  EdgeAccessor record_accessor(LocalEdgeAddress(gid), *this);
  if (!record_accessor.Visible(transaction(), current_state))
    return std::experimental::nullopt;
  return record_accessor;
}

EdgeAccessor GraphDbAccessor::FindEdgeChecked(gid::Gid gid,
                                              bool current_state) {
  auto found = FindEdge(gid, current_state);
  CHECK(found) << "Unable to find edge for id: " << gid;
  return *found;
}

void GraphDbAccessor::BuildIndex(storage::Label label,
                                 storage::Property property) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  DCHECK(db_.type() != GraphDb::Type::DISTRIBUTED_WORKER)
      << "BuildIndex invoked on worker";

  db_.storage().index_build_tx_in_progress_.access().insert(transaction_.id_);

  // on function exit remove the create index transaction from
  // build_tx_in_progress
  utils::OnScopeExit on_exit_1([this] {
    auto removed = db_.storage().index_build_tx_in_progress_.access().remove(
        transaction_.id_);
    DCHECK(removed) << "Index creation transaction should be inside set";
  });

  const LabelPropertyIndex::Key key(label, property);
  if (db_.storage().label_property_index_.CreateIndex(key) == false) {
    throw IndexExistsException(
        "Index is either being created by another transaction or already "
        "exists.");
  }

  // Everything that happens after the line above ended will be added to the
  // index automatically, but we still have to add to index everything that
  // happened earlier. We have to first wait for every transaction that
  // happend before, or a bit later than CreateIndex to end.
  {
    auto wait_transactions = transaction_.engine_.GlobalActiveTransactions();
    auto active_index_creation_transactions =
        db_.storage().index_build_tx_in_progress_.access();
    for (auto id : wait_transactions) {
      if (active_index_creation_transactions.contains(id)) continue;
      while (transaction_.engine_.Info(id).is_active()) {
        // Active index creation set could only now start containing that id,
        // since that thread could have not written to the set set and to avoid
        // dead-lock we need to make sure we keep track of that
        if (active_index_creation_transactions.contains(id)) continue;
        // TODO reconsider this constant, currently rule-of-thumb chosen
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    }
  }

  // This accessor's transaction surely sees everything that happened before
  // CreateIndex.
  GraphDbAccessor dba(db_);

  std::experimental::optional<std::vector<std::future<bool>>>
      index_rpc_completions;

  // Notify all workers to start building an index if we are the master since
  // they don't have to wait anymore
  if (db_.type() == GraphDb::Type::DISTRIBUTED_MASTER) {
    index_rpc_completions.emplace(
        db_.index_rpc_clients().ExecuteOnWorkers<bool>(
            this->db_.WorkerId(),
            [label, property,
             this](communication::rpc::ClientPool &client_pool) {
              return client_pool.Call<distributed::BuildIndexRpc>(
                         distributed::IndexLabelPropertyTx{
                             label, property, transaction_id()}) != nullptr;
            }));
  }

  // Add transaction to the build_tx_in_progress as this transaction doesn't
  // change data and shouldn't block other parallel index creations
  auto read_transaction_id = dba.transaction().id_;
  db_.storage().index_build_tx_in_progress_.access().insert(
      read_transaction_id);
  // on function exit remove the read transaction from build_tx_in_progress
  utils::OnScopeExit on_exit_2([read_transaction_id, this] {
    auto removed = db_.storage().index_build_tx_in_progress_.access().remove(
        read_transaction_id);
    DCHECK(removed) << "Index building (read) transaction should be inside set";
  });

  dba.PopulateIndex(key);

  // Check if all workers sucesfully built their indexes and after this we can
  // set the index as built
  if (index_rpc_completions) {
    // Wait first, check later - so that every thread finishes and none
    // terminates - this can probably be optimized in case we fail early so that
    // we notify other workers to stop building indexes
    for (auto &index_built : *index_rpc_completions) index_built.wait();
    for (auto &index_built : *index_rpc_completions) {
      if (!index_built.get()) {
        db_.storage().label_property_index_.DeleteIndex(key);
        throw IndexCreationOnWorkerException("Index exists on a worker");
      }
    }
  }

  dba.EnableIndex(key);
  dba.Commit();
}

void GraphDbAccessor::EnableIndex(const LabelPropertyIndex::Key &key) {
  // Commit transaction as we finished applying method on newest visible
  // records. Write that transaction's ID to the WAL as the index has been
  // built at this point even if this DBA's transaction aborts for some
  // reason.
  auto wal_build_index_tx_id = transaction_id();
  wal().Emplace(database::StateDelta::BuildIndex(
      wal_build_index_tx_id, key.label_, LabelName(key.label_), key.property_,
      PropertyName(key.property_)));

  // After these two operations we are certain that everything is contained in
  // the index under the assumption that the original index creation transaction
  // contained no vertex/edge insert/update before this method was invoked.
  db_.storage().label_property_index_.IndexFinishedBuilding(key);
}

void GraphDbAccessor::PopulateIndex(const LabelPropertyIndex::Key &key) {
  for (auto vertex : Vertices(key.label_, false)) {
    if (vertex.PropsAt(key.property_).type() == PropertyValue::Type::Null)
      continue;
    db_.storage().label_property_index_.UpdateOnLabelProperty(
        vertex.address().local(), vertex.current_);
  }
}

void GraphDbAccessor::UpdateLabelIndices(storage::Label label,
                                         const VertexAccessor &vertex_accessor,
                                         const Vertex *const vertex) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  DCHECK(vertex_accessor.is_local()) << "Only local vertices belong in indexes";
  auto *vlist_ptr = vertex_accessor.address().local();
  db_.storage().labels_index_.Update(label, vlist_ptr, vertex);
  db_.storage().label_property_index_.UpdateOnLabel(label, vlist_ptr, vertex);
}

void GraphDbAccessor::UpdatePropertyIndex(
    storage::Property property, const RecordAccessor<Vertex> &vertex_accessor,
    const Vertex *const vertex) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  DCHECK(vertex_accessor.is_local()) << "Only local vertices belong in indexes";
  db_.storage().label_property_index_.UpdateOnProperty(
      property, vertex_accessor.address().local(), vertex);
}

int64_t GraphDbAccessor::VerticesCount() const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_.storage().vertices_.access().size();
}

int64_t GraphDbAccessor::VerticesCount(storage::Label label) const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_.storage().labels_index_.Count(label);
}

int64_t GraphDbAccessor::VerticesCount(storage::Label label,
                                       storage::Property property) const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  const LabelPropertyIndex::Key key(label, property);
  DCHECK(db_.storage().label_property_index_.IndexExists(key))
      << "Index doesn't exist.";
  return db_.storage().label_property_index_.Count(key);
}

int64_t GraphDbAccessor::VerticesCount(storage::Label label,
                                       storage::Property property,
                                       const PropertyValue &value) const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  const LabelPropertyIndex::Key key(label, property);
  DCHECK(db_.storage().label_property_index_.IndexExists(key))
      << "Index doesn't exist.";
  return db_.storage()
      .label_property_index_.PositionAndCount(key, value)
      .second;
}

int64_t GraphDbAccessor::VerticesCount(
    storage::Label label, storage::Property property,
    const std::experimental::optional<utils::Bound<PropertyValue>> lower,
    const std::experimental::optional<utils::Bound<PropertyValue>> upper)
    const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  const LabelPropertyIndex::Key key(label, property);
  DCHECK(db_.storage().label_property_index_.IndexExists(key))
      << "Index doesn't exist.";
  CHECK(lower || upper) << "At least one bound must be provided";
  CHECK(!lower || lower.value().value().type() != PropertyValue::Type::Null)
      << "Null value is not a valid index bound";
  CHECK(!upper || upper.value().value().type() != PropertyValue::Type::Null)
      << "Null value is not a valid index bound";

  if (!upper) {
    auto lower_pac = db_.storage().label_property_index_.PositionAndCount(
        key, lower.value().value());
    int64_t size = db_.storage().label_property_index_.Count(key);
    return std::max(0l,
                    size - lower_pac.first -
                        (lower.value().IsInclusive() ? 0l : lower_pac.second));

  } else if (!lower) {
    auto upper_pac = db_.storage().label_property_index_.PositionAndCount(
        key, upper.value().value());
    return upper.value().IsInclusive() ? upper_pac.first + upper_pac.second
                                       : upper_pac.first;

  } else {
    auto lower_pac = db_.storage().label_property_index_.PositionAndCount(
        key, lower.value().value());
    auto upper_pac = db_.storage().label_property_index_.PositionAndCount(
        key, upper.value().value());
    auto result = upper_pac.first - lower_pac.first;
    if (lower.value().IsExclusive()) result -= lower_pac.second;
    if (upper.value().IsInclusive()) result += upper_pac.second;
    return std::max(0l, result);
  }
}

bool GraphDbAccessor::RemoveVertex(VertexAccessor &vertex_accessor) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";

  if (!vertex_accessor.is_local()) {
    LOG(ERROR) << "Remote vertex deletion not implemented";
    // TODO support distributed
    // call remote RemoveVertex(gid), return it's result. The result can be
    // (true, false), or an error can occur (serialization, timeout). In case
    // of error the remote worker will be asking for a transaction abort,
    // not sure what to do here.
    return false;
  }
  vertex_accessor.SwitchNew();
  // it's possible the vertex was removed already in this transaction
  // due to it getting matched multiple times by some patterns
  // we can only delete it once, so check if it's already deleted
  if (vertex_accessor.current().is_expired_by(transaction_)) return true;
  if (vertex_accessor.out_degree() > 0 || vertex_accessor.in_degree() > 0)
    return false;

  auto *vlist_ptr = vertex_accessor.address().local();
  wal().Emplace(
      database::StateDelta::RemoveVertex(transaction_.id_, vlist_ptr->gid_));
  vlist_ptr->remove(vertex_accessor.current_, transaction_);
  return true;
}

void GraphDbAccessor::DetachRemoveVertex(VertexAccessor &vertex_accessor) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  if (!vertex_accessor.is_local()) {
    LOG(ERROR) << "Remote vertex deletion not implemented";
    // TODO support distributed
    // call remote DetachRemoveVertex(gid). It can either succeed or an error
    // can occur. See discussion in the RemoveVertex method above.
  }
  vertex_accessor.SwitchNew();
  for (auto edge_accessor : vertex_accessor.in())
    RemoveEdge(edge_accessor, true, false);
  vertex_accessor.SwitchNew();
  for (auto edge_accessor : vertex_accessor.out())
    RemoveEdge(edge_accessor, false, true);

  vertex_accessor.SwitchNew();
  // it's possible the vertex was removed already in this transaction
  // due to it getting matched multiple times by some patterns
  // we can only delete it once, so check if it's already deleted
  if (!vertex_accessor.current().is_expired_by(transaction_))
    vertex_accessor.address().local()->remove(vertex_accessor.current_,
                                              transaction_);
}

EdgeAccessor GraphDbAccessor::InsertEdge(
    VertexAccessor &from, VertexAccessor &to, storage::EdgeType edge_type,
    std::experimental::optional<gid::Gid> requested_gid) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";

  // The address of an edge we'll create.
  storage::EdgeAddress edge_address;

  Vertex *from_updated;
  if (from.is_local()) {
    auto gid = db_.storage().edge_generator_.Next(requested_gid);
    edge_address = new mvcc::VersionList<Edge>(
        transaction_, gid, from.address(), to.address(), edge_type);
    // We need to insert edge_address to edges_ before calling update since
    // update can throw and edge_vlist will not be garbage collected if it is
    // not in edges_ skiplist.
    bool success =
        db_.storage().edges_.access().insert(gid, edge_address.local()).second;
    CHECK(success) << "Attempting to insert an edge with an existing GID: "
                   << gid;

    from.SwitchNew();
    from_updated = &from.update();

    // TODO when preparing WAL for distributed, most likely never use
    // `CREATE_EDGE`, but always have it split into 3 parts (edge insertion,
    // in/out modification).
    wal().Emplace(database::StateDelta::CreateEdge(
        transaction_.id_, gid, from.gid(), to.gid(), edge_type,
        EdgeTypeName(edge_type)));

  } else {
    edge_address = db().remote_updates_clients().RemoteCreateEdge(
        transaction_id(), from, to, edge_type);

    from_updated = db().remote_data_manager()
                       .Vertices(transaction_id())
                       .FindNew(from.gid());

    // Create an Edge and insert it into the RemoteCache so we see it locally.
    db().remote_data_manager()
        .Edges(transaction_id())
        .emplace(
            edge_address.gid(), nullptr,
            std::make_unique<Edge>(from.address(), to.address(), edge_type));
  }
  from_updated->out_.emplace(to.address(), edge_address, edge_type);

  Vertex *to_updated;
  if (to.is_local()) {
    // ensure that the "to" accessor has the latest version (Switch new)
    // WARNING: must do that after the above "from.update()" for cases when
    // we are creating a cycle and "from" and "to" are the same vlist
    to.SwitchNew();
    to_updated = &to.update();
  } else {
    // The RPC call for the `to` side is already handled if `from` is not local.
    if (from.is_local() ||
        from.address().worker_id() != to.address().worker_id()) {
      db().remote_updates_clients().RemoteAddInEdge(
          transaction_id(), from, GlobalizedAddress(edge_address), to,
          edge_type);
    }
    to_updated =
        db().remote_data_manager().Vertices(transaction_id()).FindNew(to.gid());
  }
  to_updated->in_.emplace(from.address(), edge_address, edge_type);

  return EdgeAccessor(edge_address, *this, from.address(), to.address(),
                      edge_type);
}

EdgeAccessor GraphDbAccessor::InsertOnlyEdge(
    storage::VertexAddress from, storage::VertexAddress to,
    storage::EdgeType edge_type,
    std::experimental::optional<gid::Gid> requested_gid) {
  auto gid = db_.storage().edge_generator_.Next(requested_gid);
  auto edge_vlist =
      new mvcc::VersionList<Edge>(transaction_, gid, from, to, edge_type);
  // We need to insert edge_vlist to edges_ before calling update since update
  // can throw and edge_vlist will not be garbage collected if it is not in
  // edges_ skiplist.
  bool success = db_.storage().edges_.access().insert(gid, edge_vlist).second;
  CHECK(success) << "Attempting to insert an edge with an existing GID: "
                 << gid;
  return EdgeAccessor(edge_vlist, *this, from, to, edge_type);
}

int64_t GraphDbAccessor::EdgesCount() const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_.storage().edges_.access().size();
}

void GraphDbAccessor::RemoveEdge(EdgeAccessor &edge_accessor,
                                 bool remove_from_from, bool remove_from_to) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  if (!edge_accessor.is_local()) {
    LOG(ERROR) << "Remote edge deletion not implemented";
    // TODO support distributed
    // call remote RemoveEdge(gid, true, true). It can either succeed or an
    // error can occur. See discussion in the RemoveVertex method above.
  }
  // it's possible the edge was removed already in this transaction
  // due to it getting matched multiple times by some patterns
  // we can only delete it once, so check if it's already deleted
  edge_accessor.SwitchNew();
  if (edge_accessor.current().is_expired_by(transaction_)) return;
  if (remove_from_from)
    edge_accessor.from().update().out_.RemoveEdge(edge_accessor.address());
  if (remove_from_to)
    edge_accessor.to().update().in_.RemoveEdge(edge_accessor.address());
  edge_accessor.address().local()->remove(edge_accessor.current_, transaction_);
  wal().Emplace(
      database::StateDelta::RemoveEdge(transaction_.id_, edge_accessor.gid()));
}

storage::Label GraphDbAccessor::Label(const std::string &label_name) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_.label_mapper().value_to_id(label_name);
}

const std::string &GraphDbAccessor::LabelName(storage::Label label) const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_.label_mapper().id_to_value(label);
}

storage::EdgeType GraphDbAccessor::EdgeType(const std::string &edge_type_name) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_.edge_type_mapper().value_to_id(edge_type_name);
}

const std::string &GraphDbAccessor::EdgeTypeName(
    storage::EdgeType edge_type) const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_.edge_type_mapper().id_to_value(edge_type);
}

storage::Property GraphDbAccessor::Property(const std::string &property_name) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_.property_mapper().value_to_id(property_name);
}

const std::string &GraphDbAccessor::PropertyName(
    storage::Property property) const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_.property_mapper().id_to_value(property);
}

int64_t GraphDbAccessor::Counter(const std::string &name) {
  return db_.counters().Get(name);
}

void GraphDbAccessor::CounterSet(const std::string &name, int64_t value) {
  db_.counters().Set(name, value);
}

std::vector<std::string> GraphDbAccessor::IndexInfo() const {
  std::vector<std::string> info;
  for (storage::Label label : db_.storage().labels_index_.Keys()) {
    info.emplace_back(":" + LabelName(label));
  }
  for (LabelPropertyIndex::Key key :
       db_.storage().label_property_index_.Keys()) {
    info.emplace_back(fmt::format(":{}({})", LabelName(key.label_),
                                  PropertyName(key.property_)));
  }
  return info;
}

mvcc::VersionList<Vertex> *GraphDbAccessor::LocalVertexAddress(
    gid::Gid gid) const {
  auto access = db_.storage().vertices_.access();
  auto found = access.find(gid);
  CHECK(found != access.end()) << "Failed to find vertex for gid: " << gid;
  return found->second;
}

mvcc::VersionList<Edge> *GraphDbAccessor::LocalEdgeAddress(gid::Gid gid) const {
  auto access = db_.storage().edges_.access();
  auto found = access.find(gid);
  CHECK(found != access.end()) << "Failed to find edge for gid: " << gid;
  return found->second;
}

}  // namespace database
