#include "database/graph_db_accessor.hpp"

#include <chrono>
#include <thread>

#include <glog/logging.h>

#include "database/state_delta.hpp"
#include "storage/address_types.hpp"
#include "storage/edge.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/cast.hpp"
#include "utils/on_scope_exit.hpp"

namespace database {

GraphDbAccessor::GraphDbAccessor(GraphDb &db)
    : db_(db),
      transaction_(*db.tx_engine().Begin()),
      transaction_starter_{true} {}

GraphDbAccessor::GraphDbAccessor(GraphDb &db, tx::TransactionId tx_id)
    : db_(db),
      transaction_(*db.tx_engine().RunningTransaction(tx_id)),
      transaction_starter_{false} {}

GraphDbAccessor::~GraphDbAccessor() {
  if (transaction_starter_ && !commited_ && !aborted_) {
    this->Abort();
  }
}

tx::TransactionId GraphDbAccessor::transaction_id() const {
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
    std::experimental::optional<gid::Gid> requested_gid,
    std::experimental::optional<int64_t> cypher_id) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";

  auto gid = db_.storage().vertex_generator_.Next(requested_gid);
  if (!cypher_id) cypher_id = utils::MemcpyCast<int64_t>(gid);
  auto vertex_vlist =
      new mvcc::VersionList<Vertex>(transaction_, gid, *cypher_id);

  bool success =
      db_.storage().vertices_.access().insert(gid, vertex_vlist).second;
  CHECK(success) << "Attempting to insert a vertex with an existing GID: "
                 << gid;
  wal().Emplace(database::StateDelta::CreateVertex(
      transaction_.id_, vertex_vlist->gid_, vertex_vlist->cypher_id()));
  auto va = VertexAccessor(storage::VertexAddress(vertex_vlist), *this);
  return va;
}

std::experimental::optional<VertexAccessor> GraphDbAccessor::FindVertexOptional(
    gid::Gid gid, bool current_state) {
  VertexAccessor record_accessor(
      storage::VertexAddress(db_.storage().LocalAddress<Vertex>(gid)), *this);
  if (!record_accessor.Visible(transaction(), current_state))
    return std::experimental::nullopt;
  return record_accessor;
}

VertexAccessor GraphDbAccessor::FindVertex(gid::Gid gid, bool current_state) {
  auto found = FindVertexOptional(gid, current_state);
  CHECK(found) << "Unable to find vertex for id: " << gid;
  return *found;
}

std::experimental::optional<EdgeAccessor> GraphDbAccessor::FindEdgeOptional(
    gid::Gid gid, bool current_state) {
  EdgeAccessor record_accessor(
      storage::EdgeAddress(db_.storage().LocalAddress<Edge>(gid)), *this);
  if (!record_accessor.Visible(transaction(), current_state))
    return std::experimental::nullopt;
  return record_accessor;
}

EdgeAccessor GraphDbAccessor::FindEdge(gid::Gid gid, bool current_state) {
  auto found = FindEdgeOptional(gid, current_state);
  CHECK(found) << "Unable to find edge for id: " << gid;
  return *found;
}

void GraphDbAccessor::BuildIndex(storage::Label label,
                                 storage::Property property) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";

  db_.storage().index_build_tx_in_progress_.access().insert(transaction_.id_);

  // on function exit remove the create index transaction from
  // build_tx_in_progress
  utils::OnScopeExit on_exit_1([this] {
    auto removed = db_.storage().index_build_tx_in_progress_.access().remove(
        transaction_.id_);
    DCHECK(removed) << "Index creation transaction should be inside set";
  });

  // Create the index
  const LabelPropertyIndex::Key key(label, property);
  if (db_.storage().label_property_index_.CreateIndex(key) == false) {
    throw IndexExistsException(
        "Index is either being created by another transaction or already "
        "exists.");
  }
  // Call the hook for inherited classes.
  PostCreateIndex(key);

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
  auto dba = db_.Access();

  // Add transaction to the build_tx_in_progress as this transaction doesn't
  // change data and shouldn't block other parallel index creations
  auto read_transaction_id = dba->transaction().id_;
  db_.storage().index_build_tx_in_progress_.access().insert(
      read_transaction_id);
  // on function exit remove the read transaction from build_tx_in_progress
  utils::OnScopeExit on_exit_2([read_transaction_id, this] {
    auto removed = db_.storage().index_build_tx_in_progress_.access().remove(
        read_transaction_id);
    DCHECK(removed) << "Index building (read) transaction should be inside set";
  });

  dba->PopulateIndexFromBuildIndex(key);

  dba->EnableIndex(key);
  dba->Commit();
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

bool GraphDbAccessor::RemoveVertex(VertexAccessor &vertex_accessor,
                                   bool check_empty) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  vertex_accessor.SwitchNew();
  // it's possible the vertex was removed already in this transaction
  // due to it getting matched multiple times by some patterns
  // we can only delete it once, so check if it's already deleted
  if (vertex_accessor.current().is_expired_by(transaction_)) return true;
  if (check_empty &&
      vertex_accessor.out_degree() + vertex_accessor.in_degree() > 0)
    return false;

  auto *vlist_ptr = vertex_accessor.address().local();
  wal().Emplace(database::StateDelta::RemoveVertex(
      transaction_.id_, vlist_ptr->gid_, check_empty));
  vlist_ptr->remove(vertex_accessor.current_, transaction_);
  return true;
}

void GraphDbAccessor::DetachRemoveVertex(VertexAccessor &vertex_accessor) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";

  vertex_accessor.SwitchNew();

  // Note that when we call RemoveEdge we must take care not to delete from the
  // collection we are iterating over. This invalidates the iterator in a subtle
  // way that does not fail in tests, but is NOT correct.
  for (auto edge_accessor : vertex_accessor.in())
    RemoveEdge(edge_accessor, true, false);
  vertex_accessor.SwitchNew();
  for (auto edge_accessor : vertex_accessor.out())
    RemoveEdge(edge_accessor, false, true);

  RemoveVertex(vertex_accessor, false);
}

EdgeAccessor GraphDbAccessor::InsertEdge(
    VertexAccessor &from, VertexAccessor &to, storage::EdgeType edge_type,
    std::experimental::optional<gid::Gid> requested_gid,
    std::experimental::optional<int64_t> cypher_id) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";

  auto edge_address =
      InsertEdgeOnFrom(&from, &to, edge_type, requested_gid, cypher_id);

  InsertEdgeOnTo(&from, &to, edge_type, edge_address);
  return EdgeAccessor(edge_address, *this, from.address(), to.address(),
                      edge_type);
}

storage::EdgeAddress GraphDbAccessor::InsertEdgeOnFrom(
    VertexAccessor *from, VertexAccessor *to,
    const storage::EdgeType &edge_type,
    const std::experimental::optional<gid::Gid> &requested_gid,
    const std::experimental::optional<int64_t> &cypher_id) {
  auto edge_accessor = InsertOnlyEdge(from->address(), to->address(), edge_type,
                                      requested_gid, cypher_id);
  auto edge_address = edge_accessor.address();

  from->SwitchNew();
  auto from_updated = &from->update();

  // TODO when preparing WAL for distributed, most likely never use
  // `CREATE_EDGE`, but always have it split into 3 parts (edge insertion,
  // in/out modification).
  wal().Emplace(database::StateDelta::CreateEdge(
      transaction_.id_, edge_accessor.gid(), edge_accessor.cypher_id(),
      from->gid(), to->gid(), edge_type, EdgeTypeName(edge_type)));

  from_updated->out_.emplace(
      db_.storage().LocalizedAddressIfPossible(to->address()), edge_address,
      edge_type);
  return edge_address;
}

void GraphDbAccessor::InsertEdgeOnTo(VertexAccessor *from, VertexAccessor *to,
                                     const storage::EdgeType &edge_type,
                                     const storage::EdgeAddress &edge_address) {
  // ensure that the "to" accessor has the latest version (Switch new)
  // WARNING: must do that after the above "from->update()" for cases when
  // we are creating a cycle and "from" and "to" are the same vlist
  to->SwitchNew();
  auto *to_updated = &to->update();
  to_updated->in_.emplace(
      db_.storage().LocalizedAddressIfPossible(from->address()), edge_address,
      edge_type);
}

EdgeAccessor GraphDbAccessor::InsertOnlyEdge(
    storage::VertexAddress from, storage::VertexAddress to,
    storage::EdgeType edge_type,
    std::experimental::optional<gid::Gid> requested_gid,
    std::experimental::optional<int64_t> cypher_id) {
  CHECK(from.is_local())
      << "`from` address should be local when calling InsertOnlyEdge";
  auto gid = db_.storage().edge_generator_.Next(requested_gid);
  if (!cypher_id) cypher_id = utils::MemcpyCast<int64_t>(gid);
  auto edge_vlist = new mvcc::VersionList<Edge>(transaction_, gid, *cypher_id,
                                                from, to, edge_type);
  // We need to insert edge_vlist to edges_ before calling update since update
  // can throw and edge_vlist will not be garbage collected if it is not in
  // edges_ skiplist.
  bool success = db_.storage().edges_.access().insert(gid, edge_vlist).second;
  CHECK(success) << "Attempting to insert an edge with an existing GID: "
                 << gid;
  auto ea = EdgeAccessor(storage::EdgeAddress(edge_vlist), *this, from, to,
                         edge_type);
  return ea;
}

int64_t GraphDbAccessor::EdgesCount() const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_.storage().edges_.access().size();
}

void GraphDbAccessor::RemoveEdge(EdgeAccessor &edge, bool remove_out_edge,
                                 bool remove_in_edge) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  // it's possible the edge was removed already in this transaction
  // due to it getting matched multiple times by some patterns
  // we can only delete it once, so check if it's already deleted
  edge.SwitchNew();
  if (edge.current().is_expired_by(transaction_)) return;
  if (remove_out_edge) edge.from().RemoveOutEdge(edge.address());
  if (remove_in_edge) edge.to().RemoveInEdge(edge.address());

  edge.address().local()->remove(edge.current_, transaction_);
  wal().Emplace(database::StateDelta::RemoveEdge(transaction_.id_, edge.gid()));
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
}  // namespace database
